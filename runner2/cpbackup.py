"""Back-up en herstel van de control-plane-state; lokaal op GERARD en (optioneel) off-site naar een PRIVÉ Cloudflare R2-bucket.

Geen enkele stap hangt van de Mac af. `python3 -m runner2.cpbackup <opdracht>`:
    run                  snapshot maken, verifiëren, oude opruimen, en off-site uploaden als backup-r2.env is ingesteld (de timer)
    snapshot             alleen een lokale snapshot
    verify PAD           controleert manifest en sha256 van een snapshot (lokaal pad)
    list                 lokale snapshots en (indien ingesteld) de objecten in R2
    export-units PAD DOEL   schrijf units naar een nieuwe inspectiemap, zonder activatie
    restore PAD [--apply] [--force]   herstel (standaard dry-run); PAD mag ook r2:<sleutel> zijn
    offsite-test         schrijft, leest en verwijdert een klein testobject in R2 (bewijst bereik en rechten)

Wat erin zit: supervision/ (dossiers, events, audit), chains/, supervisors/, jobs/ (de centrale opdrachtwachtrij), config/ (machine.toml, nodes.toml, control.toml,
control-plane.json), control/notifier/ (outbox, ledger), tasks/*/state.json (taakgeschiedenis, zonder transcripten), de
runner2-eenheden en een git-bundle van ~/runner2-src. Wat er BEWUST niet in zit: control.env, cloudflared.env, backup-r2.env,
ssh-sleutels en genoemde geheimenbestanden; systemd-unitinhoud wordt integraal opgenomen en kan zelf geheimen bevatten.
De snapshot is niet versleuteld; lokale toegangsrechten en de bestaande privé-R2-bestemming zijn daarom belangrijk.
"""
import argparse
import contextlib
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import stat
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

from . import config, store
from .log import log

LOGNAME = "backup.log"
KEEP_LOCAL = 40
KEEP_REMOTE = 60
HOME = Path.home()
SRC_REPO = HOME / "runner2-src"
CONFIG_FILES = ("machine.toml", "nodes.toml", "control.toml", "control-plane.json")
DIRS = ("supervision", "chains", "supervisors", "jobs", "discussions")   # jobs = de centrale opdrachtwachtrij (queue.json, vorige generatie, audit)
NEVER = ("control.env", "cloudflared.env", "backup-r2.env", "cloudflare.env", "r2.env", "id_", ".pem", "token", "secret")


def backups_dir() -> Path:
    return config.r2_home() / "control" / "backups"


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _excluded(rel: str) -> bool:
    name = rel.rsplit("/", 1)[-1].lower()
    return any(n in name for n in NEVER) or name.endswith((".tmp", ".lock")) or ".tmp" in name


def _collect(root: Path) -> list:
    """(archiefpad, bronpad) van alles wat mee moet; geheimen en tijdelijke bestanden vallen af."""
    items = []
    for d in DIRS:
        for p in sorted((root / d).rglob("*")) if (root / d).exists() else []:
            rel = p.relative_to(root).as_posix()
            if p.is_file() and not p.is_symlink() and not _excluded(rel):
                items.append((rel, p))
    for n in CONFIG_FILES:
        if (root / "config" / n).is_file():
            items.append((f"config/{n}", root / "config" / n))
    for p in sorted((root / "control" / "notifier").glob("*")) if (root / "control" / "notifier").exists() else []:
        if p.is_file() and p.name != "heartbeat.json":
            items.append((f"control/notifier/{p.name}", p))
    for p in sorted((root / "tasks").glob("R2-*/state.json")) if (root / "tasks").exists() else []:
        items.append((f"tasks/{p.parent.name}/state.json", p))
    return items


def _stage(root: Path, stage: Path) -> list:
    entries = []
    for rel, src in _collect(root):
        dst = stage / "files" / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(src, dst)
        except FileNotFoundError:   # verdwenen tijdens het kopiëren (atomair vervangen bestand): volgende snapshot pakt het op
            continue
        entries.append({"path": rel, "sha256": _sha(dst), "size": dst.stat().st_size})
    return entries


def _check_dossiers(stage: Path) -> list:
    bad = []
    for p in (stage / "files" / "supervision").glob("*/dossier.json") if (stage / "files" / "supervision").exists() else []:
        try:
            json.loads(p.read_text())
        except ValueError:
            bad.append(p.parent.name)
    return bad


def _stage_units(stage: Path) -> tuple[list, list]:
    """Bewaar unitbestanden en drop-ins integraal, zonder koppelingen.

    Herstel van units blijft handmatig: een restore mag geen services activeren.
    """
    unitroot = HOME / ".config" / "systemd" / "user"
    deel = HOME
    for component in (".config", "systemd", "user"):
        deel = deel / component
        try:
            info = deel.lstat()
        except FileNotFoundError:
            return [], []
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("onveilige systemd-map")
    entries, directories = [], []
    target = stage / "files" / "units"
    def copy_one(src, rel):
        fd = os.open(src, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > 1024 * 1024:
                raise ValueError("onveilig unitbestand")
            with os.fdopen(fd, "rb") as stream:
                fd = -1
                data = stream.read(1024 * 1024 + 1)
            if len(data) > 1024 * 1024:
                raise ValueError("unitbestand te groot")
            dst = target / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(data)
            entries.append({"path": "units/" + rel, "sha256": _sha(dst), "size": len(data)})
        finally:
            if fd >= 0:
                os.close(fd)
    for unit in sorted(unitroot.glob("runner2-*")):
        if unit.is_symlink():
            raise ValueError("systemd-koppeling niet toegestaan")
        if unit.is_file():
            copy_one(unit, unit.name)
        elif unit.is_dir() and unit.name.endswith(".d"):
            directories.append("units/" + unit.name)
            (target / unit.name).mkdir(parents=True, exist_ok=True)
            for item in sorted(unit.iterdir()):
                if item.is_symlink() or not item.is_file() or not item.name.endswith(".conf"):
                    raise ValueError("onveilige systemd drop-in")
                copy_one(item, unit.name + "/" + item.name)
        else:
            raise ValueError("onveilige systemd-unit")
    return entries, directories


def snapshot() -> Path:
    root = config.r2_home()
    out = backups_dir()
    out.mkdir(parents=True, exist_ok=True)
    name = f"r2control-{_stamp()}"
    with tempfile.TemporaryDirectory(prefix="r2snap-", dir=out) as tmp:
        stage = Path(tmp)
        for attempt in range(3):   # dossiers worden atomair geschreven; een kapot gelezen dossier betekent: nog eens
            shutil.rmtree(stage / "files", ignore_errors=True)
            entries = _stage(root, stage)
            if not _check_dossiers(stage):
                break
            time.sleep(1)
        else:
            raise RuntimeError("dossier tijdens snapshot onleesbaar; geen snapshot gemaakt")
        extra, unit_dirs = _stage_units(stage)
        if (SRC_REPO / ".git").exists():
            b = stage / "files" / "runner2-src.bundle"
            r = subprocess.run(["git", "-C", str(SRC_REPO), "bundle", "create", str(b), "--all"], capture_output=True, text=True)
            if r.returncode == 0 and b.exists():
                extra.append({"path": "runner2-src.bundle", "sha256": _sha(b), "size": b.stat().st_size})
        manifest = {"format": 1, "created": store.now(), "host": config.local_node(), "r2_home": str(root),
                    "revision": (HOME / "runner2-code" / "REVISION").read_text().strip() if (HOME / "runner2-code" / "REVISION").exists() else None,
                    "projects": sorted(p.name for p in (root / "supervision").iterdir() if p.is_dir()) if (root / "supervision").exists() else [],
                    "files": entries + extra, "unit_dirs": unit_dirs}
        (stage / "MANIFEST.json").write_text(json.dumps(manifest, indent=1))
        final = out / f"{name}.tar.gz"
        part = out / f".{name}.tar.gz.part"
        partfd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(partfd, "wb") as bestand:
                partfd = -1
                with tarfile.open(fileobj=bestand, mode="w:gz") as tf:
                    tf.add(stage / "MANIFEST.json", arcname="MANIFEST.json")
                    tf.add(stage / "files", arcname="files")
                bestand.flush()
                os.fsync(bestand.fileno())
            os.replace(part, final)
        finally:
            if partfd >= 0: os.close(partfd)
    (out / f"{name}.tar.gz.sha256").write_text(f"{_sha(final)}  {final.name}\n")
    return final


def verify(path: Path) -> dict:
    """Controleert dat het archief heel is en elk bestand klopt met het manifest. Gooit ValueError bij een afwijking."""
    path = Path(path)
    side = path.with_name(path.name + ".sha256")
    if side.exists():
        want = side.read_text().split()[0]
        if _sha(path) != want:
            raise ValueError(f"sha256 van het archief klopt niet met {side.name}")
    with tarfile.open(path, "r:gz") as tf:
        names = tf.getnames()
        if len(names) != len(set(names)):
            raise ValueError("dubbele archiefpaden")
        for n in names:
            if n.startswith("/") or ".." in Path(n).parts:
                raise ValueError(f"onveilig pad in archief: {n}")
        manifest = json.load(tf.extractfile("MANIFEST.json"))
        unit_dirs = manifest.get("unit_dirs", [])
        if not isinstance(unit_dirs, list):
            raise ValueError("ongeldige unitmappen")
        for directory in unit_dirs:
            if not isinstance(directory, str) or not re.fullmatch(r"units/runner2-[A-Za-z0-9_.@:\\-]+\.d", directory):
                raise ValueError("onveilig unitmappad")
            try:
                member = tf.getmember("files/" + directory)
            except KeyError as error:
                raise ValueError("unitmap ontbreekt") from error
            if not member.isdir():
                raise ValueError("onveilige unitmap")
        bestandspaden = [e["path"] for e in manifest["files"]]
        if len(bestandspaden) != len(set(bestandspaden)):
            raise ValueError("dubbele manifestpaden")
        for e in manifest["files"]:
            pad = e["path"]
            if not isinstance(pad, str) or pad.startswith("/") or ".." in Path(pad).parts:
                raise ValueError("onveilig manifestpad")
            try:
                member = tf.getmember(f"files/{pad}")
            except KeyError as error:
                raise ValueError("bestand ontbreekt in archief") from error
            if not member.isfile():
                raise ValueError("archiefbestand is geen gewoon bestand")
            f = tf.extractfile(member)
            if f is None:
                raise ValueError(f"ontbreekt in archief: {e['path']}")
            data = f.read()
            if hashlib.sha256(data).hexdigest() != e["sha256"] or len(data) != e["size"]:
                raise ValueError(f"checksum klopt niet: {e['path']}")
    return manifest


def prune_local(keep: int = KEEP_LOCAL) -> list:
    snaps = sorted(backups_dir().glob("r2control-*.tar.gz"))
    gone = []
    for p in snaps[:-keep]:
        p.unlink(missing_ok=True)
        p.with_name(p.name + ".sha256").unlink(missing_ok=True)
        gone.append(p.name)
    return gone


@contextlib.contextmanager
def _bevroren_archief(path: Path):
    """Lees één gewone bron naar een private tijdelijke kopie voor controle én gebruik."""
    path = Path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        voor = os.fstat(fd)
        if not stat.S_ISREG(voor.st_mode):
            raise ValueError("archief is geen gewoon bestand")
        with tempfile.TemporaryDirectory(prefix="r2-verified-") as tijdelijke_map:
            frozen = Path(tijdelijke_map) / "snapshot.tar.gz"
            with os.fdopen(fd, "rb", closefd=False) as src, open(frozen, "xb") as dst:
                shutil.copyfileobj(src, dst)
            na = os.fstat(fd)
            if (voor.st_dev, voor.st_ino, voor.st_size, voor.st_mtime_ns, voor.st_ctime_ns) != (
                    na.st_dev, na.st_ino, na.st_size, na.st_mtime_ns, na.st_ctime_ns):
                raise ValueError("archief wijzigde tijdens lezen")
            side = path.with_name(path.name + ".sha256")
            if os.path.lexists(side):
                sfd = os.open(side, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                try:
                    info = os.fstat(sfd)
                    if not stat.S_ISREG(info.st_mode) or info.st_size > 4096:
                        raise ValueError("onveilige archiefchecksum")
                    with os.fdopen(sfd, "rb") as f:
                        sfd = -1
                        raw = f.read(4097)
                    frozen.with_name(frozen.name + ".sha256").write_bytes(raw)
                finally:
                    if sfd >= 0: os.close(sfd)
            yield frozen
    finally:
        os.close(fd)


# --- herstel ---

def restore(path: Path, apply: bool = False, force: bool = False) -> dict:
    with _bevroren_archief(path) as frozen:
        return _restore_bevroren(frozen, apply, force, naam=Path(path).name)


def _restore_bevroren(path: Path, apply: bool = False, force: bool = False, *, naam: str) -> dict:
    manifest = verify(path)
    root = config.r2_home()
    plan = {"snapshot": naam, "created": manifest["created"], "from_host": manifest["host"], "projects": manifest["projects"],
            "replace": [], "add_only": [], "units_handmatig": [], "applied": False}
    plan["units_handmatig"].extend(manifest.get("unit_dirs", []))
    for e in manifest["files"]:
        p = e["path"]
        if p.startswith("units/"):
            plan["units_handmatig"].append(p)
        if p.startswith(tuple(f"{d}/" for d in DIRS) + ("config/", "control/notifier/")):
            plan["replace"].append(p)
        elif p.startswith("tasks/") and not (root / p).exists():
            plan["add_only"].append(p)
    if not apply:
        return plan
    holder = store.MachineLock("-")._holder()
    if holder and not force:
        raise RuntimeError("er draait een taak (machine.lock vast); herstel met --force of wacht")
    safe = root / "control" / f"pre-restore-{_stamp()}"
    for d in DIRS:
        if (root / d).exists():
            safe.mkdir(parents=True, exist_ok=True)
            shutil.copytree(root / d, safe / d)
    if (root / "config").exists():
        safe.mkdir(parents=True, exist_ok=True)
        shutil.copytree(root / "config", safe / "config", ignore=shutil.ignore_patterns("*.env"))
    plan["safe_copy"] = str(safe)
    with tarfile.open(path, "r:gz") as tf:
        for d in DIRS:   # volledige vervanging van de gedeelde state (geen vermenging van oud en nieuw)
            if any(e["path"].startswith(d + "/") for e in manifest["files"]):
                shutil.rmtree(root / d, ignore_errors=True)
        for e in manifest["files"]:
            p = e["path"]
            if p.startswith(("units/",)) or p == "runner2-src.bundle":
                continue
            dst = root / p
            if p.startswith("tasks/") and dst.exists():
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            data = tf.extractfile(f"files/{p}").read()
            store.atomic_write(dst, data.decode("utf-8")) if p.endswith((".json", ".toml")) else dst.write_bytes(data)
    plan["applied"] = True
    log(f"herstel toegepast uit {Path(path).name} (voorgaande staat: {safe})", LOGNAME)
    return plan


def export_units(path: Path, destination: Path) -> dict:
    """Exporteer één gecontroleerde archiefkopie zonder activatie."""
    with _bevroren_archief(path) as frozen:
        return _export_units_bevroren(frozen, destination)


def _export_units_bevroren(path: Path, destination: Path) -> dict:
    manifest = verify(path)
    dirs = manifest.get("unit_dirs", [])
    files = [e for e in manifest["files"] if e["path"].startswith("units/")]
    for entry in files:
        rel = entry["path"][6:]
        if not re.fullmatch(r"runner2-[A-Za-z0-9_.@:\\-]+(?:\.d/[A-Za-z0-9_.@:\\-]+\.conf)?", rel):
            raise ValueError("onveilig unitbestand in manifest")
    destination = Path(os.path.abspath(destination))
    backup_root = Path(os.path.abspath(backups_dir()))
    systemd_root = Path(os.path.abspath(HOME / ".config" / "systemd"))
    if destination.parent != backup_root / "unit-inspectie" or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", destination.name):
        raise ValueError("doel moet een nieuwe map onder back-ups/unit-inspectie zijn")
    if os.path.commonpath([str(backup_root), str(systemd_root)]) in (str(backup_root), str(systemd_root)):
        raise ValueError("inspectie en systemd-mappen mogen niet overlappen")
    # Elke oudercomponent wordt zonder symlinks geopend; alle verdere writes
    # verlopen relatief aan de vastgehouden inspectiemap.
    basefd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    inspectfd = targetfd = None
    try:
        for deel in backup_root.parts[1:]:
            nieuw = os.open(deel, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=basefd)
            os.close(basefd)
            basefd = nieuw
        try:
            os.mkdir("unit-inspectie", 0o700, dir_fd=basefd)
        except FileExistsError:
            pass
        inspectfd = os.open("unit-inspectie", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=basefd)
        os.mkdir(destination.name, 0o700, dir_fd=inspectfd)
        targetfd = os.open(destination.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=inspectfd)
        with tarfile.open(path, "r:gz") as tf:
            for name in dirs:
                os.mkdir(name[6:], 0o700, dir_fd=targetfd)
            for entry in files:
                rel = entry["path"][6:]
                delen = rel.split("/")
                mapfd = targetfd
                if len(delen) == 2:
                    mapfd = os.open(delen[0], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=targetfd)
                try:
                    data = tf.extractfile("files/" + entry["path"]).read()
                    fd = os.open(delen[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=mapfd)
                    with os.fdopen(fd, "wb") as out:
                        out.write(data)
                finally:
                    if mapfd != targetfd:
                        os.close(mapfd)
    finally:
        if targetfd is not None: os.close(targetfd)
        if inspectfd is not None: os.close(inspectfd)
        os.close(basefd)
    return {"doel": str(destination), "bestanden": len(files), "mappen": len(dirs), "geactiveerd": False}


# --- R2 / S3 (SigV4, alleen standaardbibliotheek) ---

def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def sign(method: str, host: str, path: str, query: dict, headers: dict, payload_hash: str, creds: dict, now: dt.datetime,
         region: str = "auto", service: str = "s3") -> dict:
    """AWS Signature V4; geeft de headers terug inclusief Authorization. Getest tegen het officiële AWS-voorbeeld."""
    amz = now.strftime("%Y%m%dT%H%M%SZ")
    day = amz[:8]
    hdrs = {k.lower(): str(v).strip() for k, v in headers.items()}
    hdrs.update({"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz})
    signed = ";".join(sorted(hdrs))
    canon_q = "&".join(f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(str(v), safe='-_.~')}" for k, v in sorted(query.items()))
    canon_h = "".join(f"{k}:{hdrs[k]}\n" for k in sorted(hdrs))
    canon = "\n".join([method, urllib.parse.quote(path, safe="/-_.~"), canon_q, canon_h, signed, payload_hash])
    scope = f"{day}/{region}/{service}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canon.encode()).hexdigest()])
    k = _hmac(_hmac(_hmac(_hmac(("AWS4" + creds["secret"]).encode(), day), region), service), "aws4_request")
    sig = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    hdrs["authorization"] = f"AWS4-HMAC-SHA256 Credential={creds['key']}/{scope}, SignedHeaders={signed}, Signature={sig}"
    return hdrs


def r2_settings() -> dict | None:
    """Uit backup-r2.env (of de omgeving). None als off-site niet is ingesteld."""
    env = dict(os.environ)
    p = HOME / ".config" / "runner2" / "backup-r2.env"
    if p.exists():
        for line in p.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, _, v = line.partition("=")
                env.setdefault(k.strip(), v.strip())
    need = ("R2CP_ENDPOINT", "R2CP_BUCKET", "R2CP_ACCESS_KEY_ID", "R2CP_SECRET_ACCESS_KEY")
    if not all(env.get(k) for k in need):
        return None
    media = HOME / ".config" / "gerard" / "r2.env"
    if media.exists() and f"GERARD_R2_BUCKET={env['R2CP_BUCKET']}" in media.read_text():
        raise RuntimeError("R2CP_BUCKET is de mediabucket; back-ups horen in een aparte privé-bucket")
    return {"endpoint": env["R2CP_ENDPOINT"].rstrip("/"), "bucket": env["R2CP_BUCKET"], "prefix": env.get("R2CP_PREFIX", "runner2-control/").lstrip("/"),
            "creds": {"key": env["R2CP_ACCESS_KEY_ID"], "secret": env["R2CP_SECRET_ACCESS_KEY"]}}


def _s3(cfg: dict, method: str, key: str = "", query: dict | None = None, body: bytes = b"", timeout: float = 120):
    u = urllib.parse.urlparse(cfg["endpoint"])
    path = f"/{cfg['bucket']}/{key}" if key else f"/{cfg['bucket']}"
    ph = hashlib.sha256(body).hexdigest()
    hdrs = sign(method, u.netloc, path, query or {}, {"content-type": "application/octet-stream"} if body else {}, ph, cfg["creds"],
                dt.datetime.now(dt.timezone.utc))
    url = f"{u.scheme}://{u.netloc}{urllib.parse.quote(path, safe='/-_.~')}" + (("?" + urllib.parse.urlencode(sorted((query or {}).items()), quote_via=urllib.parse.quote)) if query else "")
    req = urllib.request.Request(url, data=body if method == "PUT" else None, method=method, headers={k: v for k, v in hdrs.items() if k != "host"})
    with urllib.request.urlopen(req, timeout=timeout) as r:   # noqa: S310 - endpoint komt uit eigen 0600-config
        return r.status, r.read()


def r2_put(cfg: dict, key: str, data: bytes) -> None:
    status, _ = _s3(cfg, "PUT", cfg["prefix"] + key, body=data)
    if status not in (200, 201):
        raise RuntimeError(f"R2 PUT gaf {status}")


def r2_get(cfg: dict, key: str) -> bytes:
    return _s3(cfg, "GET", cfg["prefix"] + key)[1]


def r2_list(cfg: dict) -> list:
    _, body = _s3(cfg, "GET", "", {"list-type": "2", "prefix": cfg["prefix"]})
    ns = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(body)
    keys = [e.text for e in root.findall(".//s:Contents/s:Key", ns)] or [e.text for e in root.findall(".//Contents/Key")]
    return sorted(k[len(cfg["prefix"]):] for k in keys)


def r2_delete(cfg: dict, key: str) -> None:
    _s3(cfg, "DELETE", cfg["prefix"] + key)


def offsite(snap: Path) -> dict:
    cfg = r2_settings()
    if not cfg:
        return {"ok": False, "configured": False}
    data = snap.read_bytes()
    r2_put(cfg, snap.name, data)
    r2_put(cfg, snap.name + ".sha256", (snap.with_name(snap.name + ".sha256")).read_bytes())
    if hashlib.sha256(r2_get(cfg, snap.name)).hexdigest() != hashlib.sha256(data).hexdigest():   # terugleescontrole
        raise RuntimeError("R2-terugleescontrole mislukt")
    olds = [k for k in r2_list(cfg) if k.endswith(".tar.gz")]
    for k in olds[:-KEEP_REMOTE]:
        r2_delete(cfg, k)
        r2_delete(cfg, k + ".sha256")
    return {"ok": True, "configured": True, "object": cfg["prefix"] + snap.name, "bytes": len(data), "epoch": time.time(), "at": store.now(),
            "pruned": max(0, len(olds) - KEEP_REMOTE)}


def run() -> dict:
    snap = snapshot()
    manifest = verify(snap)
    gone = prune_local()
    res = {"snapshot": snap.name, "files": len(manifest["files"]), "pruned_local": len(gone)}
    try:
        off = offsite(snap)
    except Exception as e:   # noqa: BLE001 - off-site faalt zichtbaar, lokale back-up blijft geldig
        off = {"ok": False, "configured": True, "error": f"{type(e).__name__}: {str(e)[:200]}", "at": store.now()}
        log(f"off-site mislukt: {off['error']}", LOGNAME)
    if off.get("configured") or not (backups_dir() / "offsite.json").exists():
        store.atomic_write(backups_dir() / "offsite.json", json.dumps(off))
    res["offsite"] = off
    log(f"back-up {snap.name} ({res['files']} bestanden), off-site {off}", LOGNAME)
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="runner2.cpbackup")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run")
    sub.add_parser("snapshot")
    sub.add_parser("list")
    sub.add_parser("offsite-test")
    u = sub.add_parser("export-units")
    u.add_argument("path")
    u.add_argument("destination")
    v = sub.add_parser("verify")
    v.add_argument("path")
    r = sub.add_parser("restore")
    r.add_argument("path")
    r.add_argument("--apply", action="store_true")
    r.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "run":
            res = run()
            print(json.dumps(res, indent=1))
            return 0
        if a.cmd == "snapshot":
            print(snapshot())
        elif a.cmd == "export-units":
            print(json.dumps(export_units(Path(a.path), Path(a.destination)), indent=1))
        elif a.cmd == "verify":
            m = verify(Path(a.path))
            print(f"OK: {len(m['files'])} bestanden, gemaakt {m['created']} op {m['host']}")
        elif a.cmd == "list":
            for p in sorted(backups_dir().glob("r2control-*.tar.gz")):
                print(p.name, p.stat().st_size)
            cfg = r2_settings()
            print("off-site:", "niet ingesteld" if not cfg else r2_list(cfg)[-5:])
        elif a.cmd == "offsite-test":
            cfg = r2_settings()
            if not cfg:
                print("off-site niet ingesteld (~/.config/runner2/backup-r2.env ontbreekt of is onvolledig)")
                return 2
            key = f"_probe-{_stamp()}"
            r2_put(cfg, key, b"runner2 probe")
            ok = r2_get(cfg, key) == b"runner2 probe"
            r2_delete(cfg, key)
            print("R2 schrijven/lezen/verwijderen:", "OK" if ok else "MISLUKT")
            return 0 if ok else 1
        elif a.cmd == "restore":
            path = a.path
            if path.startswith("r2:"):
                cfg = r2_settings()
                if not cfg:
                    print("off-site niet ingesteld")
                    return 2
                # Remote keys are opaque: never use one as a local pathname.
                # Units may contain credentials, so downloads live only inside a
                # private temporary directory and are created private from byte one.
                with tempfile.TemporaryDirectory(prefix="r2-download-") as directory:
                    downloaded = Path(directory) / "snapshot.tar.gz"
                    for destination, remote_key in ((downloaded, path[3:]),
                                                    (downloaded.with_name(downloaded.name + ".sha256"), path[3:] + ".sha256")):
                        data = r2_get(cfg, remote_key)
                        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                        with os.fdopen(fd, "wb") as output:
                            output.write(data)
                    print(json.dumps(restore(downloaded, a.apply, a.force), indent=1))
            else:
                print(json.dumps(restore(Path(path), a.apply, a.force), indent=1))
    except (ValueError, RuntimeError, OSError, urllib.error.URLError, tarfile.TarError) as e:
        print(f"FOUT: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
