"""Overdracht van de supervisie-state van de oude control plane (Mac) naar GERARD, met terugdraaien.

Dit bestand gebruikt ALLEEN de standaardbibliotheek en importeert niets uit runner2, zodat het ook op de Mac draait met
de oude Runner-2-versie (`python3 handover.py export ...`). Op GERARD: `python3 -m runner2.handover import ...`.

Op de OUDE control plane (Mac):
    export  [--home DIR] [--out DIR] [--push] [--no-fence]
        1. hek: supervision/ wordt hernoemd naar supervision.handed-over-<tijd>/ (de oude watch ziet dan geen projecten meer en
           kan dus niets meer beslissen of starten); er komt een standby-markering in config/control-plane.json;
        2. na een korte rustperiode wordt uit de weggezette map een bundel gemaakt (dossiers, events, audit, back-ups) met
           manifest en sha256 per bestand, plus naslagmateriaal (machine.toml zonder geheimen, nodes.toml, takenindex);
        3. met --push gaat de bundel via de bestaande ssh-route naar GERARD (~/.runner2/inbox/) en wordt de sha256 daar gecontroleerd;
        4. mislukt er iets na het hek, dan wordt het hek automatisch teruggedraaid (supervision/ staat dan weer terug).
    unfence [--home DIR]       zet het hek terug (rollback) - ALLEEN nadat GERARD is gedemoot (`cplane demote`)
    verify BUNDEL              controleert een bundel

De opdrachtwachtrij (jobs/) gaat mee: het hek schrijft jobs/HANDED_OVER.json (de oude dispatcher en elke bediening
weigeren dan), de bundel bevat queue.json, queue.prev.json en audit.jsonl, en de import zet ze op de nieuwe control plane.
Een lopende opdracht loopt gewoon door op zijn Runner; de nieuwe dispatcher volgt hem via het taak-id (idempotente start).
Met --to NODE kan de overdracht ook de andere kant op (bijv. van GERARD terug naar de Mac); standaard GERARD.

Formaat 2 (masterverhuizing) draagt daarnaast mee: chains/ en supervisors/ (samenvoegen op id, nooit overschrijven), de notifier-ledger
en -outbox (samenvoegen), actionreview/, jobs/tekst-backups/, en als naslag de state.json van alle taken met een index van de open
acties (naar archive/, nooit naar tasks/: een kopie van taken in de live map zou de watch ze laten herstellen). Export weigert zolang een
keten of supervisor nog actief is (--allow-active om dat bewust te negeren).
    parity SRC_HOME DST_HOME   vergelijkt bron en doel na een overdracht (alleen lezen); exit 1 bij een verschil

Op GERARD (en bij een terugdraai-import op de Mac):
    import BUNDEL [--apply] [--promote] [--replace-existing]
        standaard dry-run; alles-of-niets; botsende projecten worden nooit overschreven, tenzij --replace-existing (de bestaande
        versie wordt dan weggezet in supervision/.replaced-<tijd>/, niet verwijderd)
        Standby eerst: heeft de ontvangende machine geen rolbestand (= "legacy primary"), dan zet de import eerst een expliciete standby-
        markering VOOR er ook maar één wachtrij-, supervisie- of ketenbestand staat. Pas --promote maakt de machine primary (met een
        epoch hoger dan die van de bron). Zonder --promote blijft de ontvanger standby.
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SECRET_WORDS = ("token", "secret", "password", "key")
SETTLE_S = 4
FORMAT = 2
KNOWN_FORMATS = (1, 2)
ROLE_FILE = ("config", "control-plane.json")
NOTIFIER_FILES = ("ledger.json", "outbox.jsonl")
RUN_KINDS = (("chains", "chain.json", "chain_id"), ("supervisors", "supervisor.json", "supervisor_id"))


def home_dir(arg: str | None) -> Path:
    return Path(arg or os.environ.get("R2_HOME") or Path.home() / ".runner2").expanduser()


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def _atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}")
    tmp.write_text(text)
    os.replace(tmp, path)


def _redact_toml(text: str) -> str:
    out = []
    for line in text.splitlines():
        k = line.split("=", 1)[0].lower()
        out.append(f"{line.split('=', 1)[0]}= \"<verborgen>\"" if "=" in line and any(w in k for w in SECRET_WORDS) else line)
    return "\n".join(out) + "\n"


def _is_empty_supervision(d: Path) -> bool:
    return d.exists() and not any(p.is_dir() for p in d.iterdir())


# --- export (oude control plane) ---

JOBS_FENCE = "HANDED_OVER.json"
JOBS_FILES = ("queue.json", "queue.prev.json", "audit.jsonl")


def local_node(home: Path) -> str:
    try:
        node = tomllib.loads((home / "config" / "machine.toml").read_text()).get("node")
    except (OSError, ValueError):
        node = None
    return str(node or "MAC").upper()


def _has_jobs(home: Path) -> bool:
    return (home / "jobs" / "queue.json").is_file()


def _jobs_lock(home: Path):
    """Dezelfde bestandslock als runner2.jobs (jobs/.lock): het hek valt nooit midden in een wachtrijtransactie."""
    import fcntl
    (home / "jobs").mkdir(parents=True, exist_ok=True)
    f = open(home / "jobs" / ".lock", "a+")
    fcntl.flock(f, fcntl.LOCK_EX)
    return f


def fence_jobs(home: Path, to: str) -> bool:
    """Wachtrij afsluiten: na dit hek start de oude dispatcher niets meer en weigert elke bediening. True als er een wachtrij was."""
    if not _has_jobs(home):
        return False
    f = _jobs_lock(home)
    try:
        _atomic(home / "jobs" / JOBS_FENCE, json.dumps({"to": to, "from": local_node(home), "at": _stamp()}, indent=1))
    finally:
        f.close()
    return True


def unfence_jobs(home: Path) -> bool:
    p = home / "jobs" / JOBS_FENCE
    if not p.exists():
        return False
    f = _jobs_lock(home)
    try:
        p.unlink()
    finally:
        f.close()
    return True


def _read_role(home: Path) -> dict | None:
    try:
        d = json.loads(home.joinpath(*ROLE_FILE).read_text())
        return d if isinstance(d, dict) else None
    except (OSError, ValueError):
        return None


def role_epoch(home: Path) -> int:
    try:
        return int((_read_role(home) or {}).get("epoch", 0))
    except (TypeError, ValueError):
        return 0


def _mark_standby(home: Path, to: str) -> None:
    """Expliciete standby-markering; onthoudt de vorige rol (voor unfence) en verhoogt de epoch altijd (nooit terug naar 1)."""
    cur = _read_role(home)
    previous = (cur or {}).get("previous") if (cur or {}).get("role") == "standby" and (cur or {}).get("previous") else cur
    doc = {"role": "standby", "host": local_node(home), "primary_host": to, "at": _stamp(), "epoch": role_epoch(home) + 1,
           "note": f"supervisie overgedragen aan {to}; terugdraaien: handover unfence"}
    if previous:
        doc["previous"] = previous
    _atomic(home.joinpath(*ROLE_FILE), json.dumps(doc, indent=1))


def ensure_standby(home: Path, source: str) -> bool:
    """Zet deze machine expliciet op standby als er nog geen rolbestand is (een machine zonder rolbestand geldt als primary).
    Geeft True als er iets is geschreven. Een bestaande rol (primary of standby) blijft ongemoeid."""
    if home.joinpath(*ROLE_FILE).exists():
        return False
    _atomic(home.joinpath(*ROLE_FILE), json.dumps(
        {"role": "standby", "host": local_node(home), "primary_host": source, "at": _stamp(), "epoch": 1,
         "note": f"ontvangende machine voor de overdracht van {source}; promote pas na controle"}, indent=1))
    return True


def fence(home: Path, to: str = "GERARD") -> Path | None:
    """supervision/ wegzetten en de wachtrij afsluiten; geeft het nieuwe supervisiepad (None als er geen projecten waren)."""
    sup = home / "supervision"
    jobs_fenced = fence_jobs(home, to)
    if not sup.exists() or not any(p.is_dir() for p in sup.iterdir()):
        if jobs_fenced:
            _mark_standby(home, to)
        return None
    dst = home / f"supervision.handed-over-{_stamp()}"
    os.rename(sup, dst)
    _mark_standby(home, to)
    return dst


def unfence(home: Path) -> str:
    handed = sorted(home.glob("supervision.handed-over-*"))
    jobs_back = unfence_jobs(home)
    if not handed:
        if jobs_back:
            _drop_standby(home)
            return "opdrachtwachtrij weer vrijgegeven (geen supervisieprojecten weggezet)"
        return "niets terug te zetten (geen supervision.handed-over-* gevonden)"
    src, dst = handed[-1], home / "supervision"
    if dst.exists():
        if not _is_empty_supervision(dst):
            raise SystemExit(f"{dst} bevat al projecten; handmatig samenvoegen nodig, niets gewijzigd")
        shutil.rmtree(dst)   # alleen de lege map die de oude watch ondertussen aanmaakte (attention.json)
    os.rename(src, dst)
    _drop_standby(home)
    return f"supervisie teruggezet uit {src.name}" + ("; opdrachtwachtrij weer vrijgegeven" if jobs_back else "")


def _drop_standby(home: Path) -> None:
    """Hek weg: de vorige rol komt terug (met een hogere epoch); had de machine geen rolbestand, dan verdwijnt de markering."""
    marker = home.joinpath(*ROLE_FILE)
    cur = _read_role(home)
    if not cur or cur.get("role") != "standby":
        return
    prev = cur.get("previous")
    if isinstance(prev, dict) and prev.get("role"):
        prev = dict(prev, epoch=int(cur.get("epoch", 0)) + 1, restored_at=_stamp(), note="terugdraaien overdracht (unfence)")
        _atomic(marker, json.dumps(prev, indent=1))
    else:
        try:
            marker.unlink()
        except OSError:
            pass


def _index_tasks(home: Path) -> list:
    out = []
    for p in sorted((home / "tasks").glob("R2-*/state.json")) if (home / "tasks").exists() else []:
        try:
            s = json.loads(p.read_text())
        except (OSError, ValueError):
            continue
        out.append({k: s.get(k) for k in ("task_id", "status", "title", "repo", "created", "updated", "chain_id", "supervisor_id", "branch")})
    return out


def active_runs(home: Path) -> list:
    """Actieve ketens en supervisors (id's). Die worden op de ontvanger door de watch doorgeschakeld; daarom nooit zomaar meenemen."""
    out = []
    for kind, fname, key in RUN_KINDS:
        for p in sorted((home / kind).glob(f"*/{fname}")) if (home / kind).exists() else []:
            try:
                d = json.loads(p.read_text())
            except (OSError, ValueError):
                out.append(f"{p.parent.name} (onleesbaar)")
                continue
            if d.get("state") == "active":
                out.append(d.get(key) or p.parent.name)
    return out


def open_actions_index(home: Path) -> list:
    """Open acties per taak (alleen lezen uit tasks/*/state.json), voor de naslag op de ontvanger."""
    out = []
    for p in sorted((home / "tasks").glob("R2-*/state.json")) if (home / "tasks").exists() else []:
        try:
            s = json.loads(p.read_text())
        except (OSError, ValueError):
            continue
        for it in s.get("human_actions") or []:
            if isinstance(it, dict) and it.get("status") == "open":
                out.append({"task_id": s.get("task_id") or p.parent.name, "task_status": s.get("status"), "title": s.get("title"),
                            "action_id": it.get("id"), "text": it.get("text"), "reason": it.get("reason"),
                            "created_at": it.get("created_at"), "topic": it.get("topic")})
    return out


def _copy_tree_files(src: Path, dst: Path) -> None:
    for p in sorted(src.rglob("*")) if src.exists() else []:
        if p.is_file() and not p.is_symlink() and ".tmp" not in p.name and p.name != ".lock":
            t = dst / p.relative_to(src)
            t.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, t)


def stage_state_sets(home: Path, stage: Path) -> None:
    """Voeg chains/, supervisors/, notifier, actionreview, tekst-backups en (als naslag) taakstaat + open acties aan de bundel toe."""
    for kind, fname, _key in RUN_KINDS:
        for p in sorted((home / kind).glob(f"*/{fname}")) if (home / kind).exists() else []:
            json.loads(p.read_text())   # nooit een onleesbaar bestand overdragen
            t = stage / kind / p.parent.name / fname
            t.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, t)
    nd = home / "control" / "notifier"
    for n in NOTIFIER_FILES:
        if (nd / n).is_file():
            (stage / "notifier").mkdir(parents=True, exist_ok=True)
            shutil.copy2(nd / n, stage / "notifier" / n)
    _copy_tree_files(home / "actionreview", stage / "actionreview")
    _copy_tree_files(home / "jobs" / "tekst-backups", stage / "jobs-tekst-backups")
    ref = stage / "reference"
    for p in sorted((home / "tasks").glob("R2-*/state.json")) if (home / "tasks").exists() else []:
        try:
            json.loads(p.read_text())
        except (OSError, ValueError):
            continue
        t = ref / "tasks-state" / p.parent.name / "state.json"
        t.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, t)
    (ref / "open-actions.json").write_text(json.dumps(open_actions_index(home), indent=1))


def _queue_meta(path: Path) -> dict | None:
    """Kerngetallen van een wachtrij voor het manifest (versheid en controle van een replica); None als er geen leesbare wachtrij is."""
    try:
        q = json.loads(path.read_text())
        jobs = q["jobs"].values()
    except (OSError, ValueError, KeyError, AttributeError):
        return None
    paused = q.get("paused") or {}
    return {"version": q.get("version"), "sha256": _sha(path), "jobs": len(q["jobs"]), "held": sum(1 for j in jobs if j.get("held") and j.get("status") == "queued"),
            "active": sum(1 for j in jobs if j.get("status") in ("starting", "running")), "paused_all": bool(paused.get("all")),
            "paused_nodes": sorted((paused.get("nodes") or {}))}


def make_bundle(home: Path, sup: Path | None, out_dir: Path, with_state: bool = True, kind: str = "handover",
                extra_files: dict | None = None) -> Path:
    """kind="replica": passieve momentopname zonder hek (noodovername); import weigert zo'n bundel tenzij bewust (allow_replica).
    extra_files: {relatief pad: bronbestand} (bijv. Monitor-2-toestand); komt als naslag in reference/extra/ en is gechecksumd."""
    out_dir.mkdir(parents=True, exist_ok=True)
    name = f"handover-{os.uname().nodename.split('.')[0]}-{_stamp()}"
    files = []
    with tempfile.TemporaryDirectory(prefix="handover-") as tmp:
        stage = Path(tmp) / "files"
        stage.mkdir(parents=True)
        for p in sorted(sup.rglob("*")) if sup and sup.exists() else []:
            if p.is_file() and not p.is_symlink() and ".tmp" not in p.name and p.name != ".lock":
                dst = stage / "supervision" / p.relative_to(sup)
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dst)
        if _has_jobs(home):
            (stage / "jobs").mkdir(parents=True, exist_ok=True)
            f = _jobs_lock(home)   # consistente momentopname (het hek staat er al; dit is extra zekerheid)
            try:
                for n in JOBS_FILES:
                    if (home / "jobs" / n).is_file():
                        shutil.copy2(home / "jobs" / n, stage / "jobs" / n)
            finally:
                f.close()
            json.loads((stage / "jobs" / "queue.json").read_text())   # nooit een onleesbare wachtrij overdragen
        ref = stage / "reference"
        ref.mkdir(parents=True, exist_ok=True)
        if (home / "config" / "machine.toml").exists():
            (ref / "machine.toml").write_text(_redact_toml((home / "config" / "machine.toml").read_text()))
        if (home / "config" / "nodes.toml").exists():
            shutil.copy2(home / "config" / "nodes.toml", ref / "nodes.toml")
        (ref / "tasks-index.json").write_text(json.dumps(_index_tasks(home), indent=1))
        for d in ("chains", "supervisors"):
            if (home / d).exists():
                shutil.copytree(home / d, ref / d, ignore=shutil.ignore_patterns("*.tmp*", ".lock"))
        if with_state:
            stage_state_sets(home, stage)
        for rel, src in sorted((extra_files or {}).items()):
            if Path(src).is_file() and not Path(src).is_symlink():
                (ref / "extra" / rel).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, ref / "extra" / rel)
        for p in sorted(stage.rglob("*")):
            if p.is_file():
                files.append({"path": p.relative_to(stage).as_posix(), "sha256": _sha(p), "size": p.stat().st_size})
        projects = sorted(p.name for p in (stage / "supervision").iterdir() if p.is_dir()) if (stage / "supervision").exists() else []
        sets = {k: sum(1 for f in files if f["path"].startswith(k + "/")) for k in ("chains", "supervisors", "notifier", "actionreview", "jobs-tekst-backups")}
        sets["tasks_state"] = sum(1 for f in files if f["path"].startswith("reference/tasks-state/"))
        sets["open_actions"] = len(open_actions_index(home))
        manifest = {"format": FORMAT if with_state else 1, "created": _stamp(), "source_host": os.uname().nodename, "source_node": local_node(home),
                    "source_epoch": role_epoch(home), "projects": projects, "jobs": (stage / "jobs" / "queue.json").exists(),
                    "kind": kind, "queue": _queue_meta(stage / "jobs" / "queue.json") if (stage / "jobs" / "queue.json").exists() else None,
                    "sets": sets, "active_runs": active_runs(home), "files": files}
        (Path(tmp) / "MANIFEST.json").write_text(json.dumps(manifest, indent=1))
        final = out_dir / f"{name}.tar.gz"
        with tarfile.open(final, "w:gz") as tf:
            tf.add(Path(tmp) / "MANIFEST.json", arcname="MANIFEST.json")
            tf.add(stage, arcname="files")
    os.chmod(final, 0o600)
    final.with_name(final.name + ".sha256").write_text(f"{_sha(final)}  {final.name}\n")
    return final


def verify_bundle(path: Path) -> dict:
    path = Path(path)
    side = path.with_name(path.name + ".sha256")
    if side.exists() and _sha(path) != side.read_text().split()[0]:
        raise ValueError("sha256 van de bundel klopt niet")
    with tarfile.open(path, "r:gz") as tf:
        for n in tf.getnames():
            if n.startswith("/") or ".." in Path(n).parts:
                raise ValueError(f"onveilig pad in bundel: {n}")
        m = json.load(tf.extractfile("MANIFEST.json"))
        if m.get("format") not in KNOWN_FORMATS:
            raise ValueError("onbekend bundelformaat")
        for e in m["files"]:
            f = tf.extractfile(f"files/{e['path']}")
            data = f.read() if f else None
            if data is None or hashlib.sha256(data).hexdigest() != e["sha256"]:
                raise ValueError(f"checksum klopt niet: {e['path']}")
    return m


def _gerard_spec(home: Path, to: str = "GERARD") -> dict:
    nodes = tomllib.loads((home / "config" / "nodes.toml").read_text())
    spec = dict(next(v for k, v in nodes.items() if k.upper() == to.upper()))
    if spec.get("nodes_json"):
        nj = Path(os.environ.get("OBF_NODES_JSON") or "~/obf-runner-bridge/studio-obf-nodes/nodes.json").expanduser()
        base = json.loads(nj.read_text())["nodes"][spec["nodes_json"]]
        spec = {"host": base["host"], "user": base["user"], "identity_file": base.get("identity_file"), "port": base.get("port", 22), **spec}
    return spec


def push(bundle: Path, home: Path, remote_dir: str = ".runner2/inbox", to: str = "GERARD") -> str:
    spec = _gerard_spec(home, to)
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-p", str(spec.get("port", 22))]
    if spec.get("identity_file"):
        ssh += ["-i", str(Path(spec["identity_file"]).expanduser()), "-o", "IdentitiesOnly=yes"]
    if spec.get("known_hosts_file"):
        ssh += ["-o", f"UserKnownHostsFile={Path(spec['known_hosts_file']).expanduser()}", "-o", "StrictHostKeyChecking=yes"]
    target = f"{spec['user']}@{spec['host']}"
    name = bundle.name
    for src, dst in ((bundle, name), (bundle.with_name(name + ".sha256"), name + ".sha256")):
        with open(src, "rb") as f:
            r = subprocess.run(ssh + [target, f"mkdir -p {remote_dir} && cat > {remote_dir}/.{dst}.part && mv {remote_dir}/.{dst}.part {remote_dir}/{dst}"],
                               stdin=f, capture_output=True, text=True, timeout=600)
        if r.returncode:
            raise RuntimeError(f"push mislukt: {r.stderr.strip()[-200:]}")
    r = subprocess.run(ssh + [target, f"cd {remote_dir} && sha256sum -c {name}.sha256"], capture_output=True, text=True, timeout=60)
    if r.returncode:
        raise RuntimeError(f"sha256-controle op {to} mislukt: {r.stdout.strip()} {r.stderr.strip()}")
    return f"{target}:{remote_dir}/{name}"


def export(home: Path, out_dir: Path | None, do_push: bool, do_fence: bool, to: str = "GERARD", allow_active: bool = False) -> dict:
    if to.upper() == local_node(home):
        raise ValueError(f"doel {to} is deze machine zelf; een overdracht naar jezelf is zinloos en zou alleen een hek zetten")
    running = active_runs(home)
    if running and not allow_active:   # VOOR het hek: er is dan niets te herstellen
        raise ValueError(f"er zijn nog actieve ketens/supervisors ({', '.join(running)}); laat ze eerst eindigen of gebruik --allow-active")
    sup = home / "supervision"
    out_dir = out_dir or home / "handover"
    fenced = None
    jobs_fenced = False
    try:
        if do_fence:
            jobs_fenced = _has_jobs(home)
            fenced = fence(home, to)
            if fenced or jobs_fenced:
                time.sleep(SETTLE_S)   # lopende schrijfacties van de oude watch laten afronden
        src = fenced or sup
        has_sup = src.exists() and any(p.is_dir() for p in src.iterdir())
        if not has_sup and not _has_jobs(home):
            return {"ok": True, "projects": [], "jobs": False, "note": "geen supervisieprojecten en geen opdrachtwachtrij op deze machine; niets over te dragen"}
        bundle = make_bundle(home, src if has_sup else None, out_dir)
        manifest = verify_bundle(bundle)
        res = {"ok": True, "bundle": str(bundle), "projects": manifest["projects"], "jobs": manifest.get("jobs", False),
               "files": len(manifest["files"]), "fenced": fenced.name if fenced else None, "jobs_fenced": jobs_fenced, "to": to,
               "sets": manifest.get("sets"), "source_epoch": manifest.get("source_epoch")}
        if do_push:
            res["pushed_to"] = push(bundle, home, to=to)
        return res
    except BaseException:
        if fenced or jobs_fenced:   # na het hek mislukt: terug naar de uitgangssituatie
            print("export mislukt; hek wordt teruggedraaid:", unfence(home), file=sys.stderr)
        raise


# --- import (GERARD) ---

def import_bundle(path: Path, home: Path, apply: bool = False, promote: bool = False, replace_existing: bool = False,
                  allow_active: bool = False, allow_replica: bool = False, quarantine_active: bool = False, keep_local_runs: bool = False,
                  pause_reason: str | None = None, promote_info: dict | None = None) -> dict:
    """allow_replica: een passieve momentopname (kind=replica) mag alleen bewust, bij een noodovername; de gewone import weigert hem.
    quarantine_active: actieve ketens/supervisors uit de bundel komen NIET in de live map maar in failover/quarantine-<tijd>/ (ze horen bij de
    node van de oude master; een kopie zou hier taken kunnen starten). keep_local_runs: bij een verschil blijft de lokale keten/supervisor staan,
    ook met replace_existing (node-lokale toestand wordt nooit door een oudere kopie vervangen). pause_reason: pauzeer de hele wachtrij NA de
    installatie en VÓÓR promote, zodat de nieuwe master niets start voordat een mens hem vrijgeeft."""
    m = verify_bundle(path)
    sup = home / "supervision"
    quarantined = {k: [] for k, _f, _key in RUN_KINDS}
    if m.get("kind") == "replica" and not allow_replica:
        return {"bundle": Path(path).name, "applied": False, "error": "dit is een passieve replica-momentopname, geen overdracht; importeren kan alleen bij een "
                "bewuste noodovername (noodovername promote); niets geïmporteerd"}
    plan = {"bundle": Path(path).name, "projects": m["projects"], "new": [], "identical": [], "conflicts": [], "applied": False,
            "jobs": _jobs_plan(m, home, replace_existing), "state": _state_plan(m, home, set(m.get("active_runs") or []) if quarantine_active else set())}
    if quarantine_active:
        plan["quarantined"] = sorted(m.get("active_runs") or [])
    if m.get("active_runs") and not allow_active and not quarantine_active:
        plan["error"] = f"de bundel bevat actieve ketens/supervisors ({', '.join(m['active_runs'])}); niets geïmporteerd (gebruik --allow-active)"
        return plan
    with tarfile.open(path, "r:gz") as tf:
        by_proj = {}
        for e in m["files"]:
            if e["path"].startswith("supervision/") and e["path"].count("/") >= 2:   # supervision/<project>/<bestand>; bestanden op rootniveau (attention.json) zijn geen project
                by_proj.setdefault(e["path"].split("/")[1], []).append(e)
        for pid, ents in by_proj.items():
            if not (sup / pid).exists():
                plan["new"].append(pid)
                continue
            same = all((sup / e["path"].split("/", 1)[1]).is_file() and _sha(sup / e["path"].split("/", 1)[1]) == e["sha256"] for e in ents
                       if not e["path"].endswith((".bak.json", "audit.jsonl")))
            (plan["identical"] if same else plan["conflicts"]).append(pid)
        if plan["conflicts"] and not replace_existing:
            plan["error"] = "projecten bestaan al met andere inhoud; niets geïmporteerd (alles-of-niets); gebruik --replace-existing voor een terugdraai-import"
            return plan
        if plan["jobs"].get("error"):
            plan["error"] = plan["jobs"]["error"]
            return plan
        clash = [f"{k}:{i}" for k in ("chains", "supervisors") for i in plan["state"][k]["conflicts"]]
        if clash and not replace_existing and not keep_local_runs:
            plan["error"] = f"ketens/supervisors bestaan al met andere inhoud ({', '.join(clash[:5])}); niets geïmporteerd (alles-of-niets); gebruik --replace-existing"
            return plan
        plan["replace"] = list(plan["conflicts"]) if replace_existing else []
        if not apply:
            return plan
        arch = home / "archive" / f"{m['source_node']}-{m['created']}"
        home.mkdir(parents=True, exist_ok=True)
        # STANDBY EERST: zonder rolbestand geldt deze machine als primary en zou de dispatcher de net geïnstalleerde wachtrij meteen oppakken
        plan["standby_set"] = ensure_standby(home, m.get("source_node") or "MAC")
        stage = Path(tempfile.mkdtemp(prefix="import-", dir=home))
        try:
            tf.extractall(stage, filter="data")
            for pid in plan["new"] + plan["replace"]:   # eerst controleren dat het dossier heel is, dan pas op zijn plek zetten
                d = stage / "files" / "supervision" / pid / "dossier.json"
                json.loads(d.read_text())
            if plan["jobs"]["action"] in ("install", "replace"):
                _check_queue(stage / "files" / "jobs" / "queue.json")
                _install_jobs(stage / "files" / "jobs", home, plan["jobs"])
            plan["state_applied"] = _apply_state(stage / "files", home, plan["state"], replace_existing and not keep_local_runs)
            if keep_local_runs:
                plan["kept_local_runs"] = [f"{k}:{i}" for k in ("chains", "supervisors") for i in plan["state"][k]["conflicts"]]
            if quarantine_active and plan["quarantined"]:
                plan["quarantine_dir"] = _quarantine_runs(stage / "files", home, plan["state"])
            sup.mkdir(parents=True, exist_ok=True)
            aside = sup / f".replaced-{_stamp()}"
            for pid in plan["replace"]:   # bestaande versie blijft bewaard, alleen weggezet (rollback van een overdracht)
                aside.mkdir(exist_ok=True)
                os.rename(sup / pid, aside / pid)
            for pid in plan["new"] + plan["replace"]:
                os.rename(stage / "files" / "supervision" / pid, sup / pid)
            arch.parent.mkdir(parents=True, exist_ok=True)
            if (stage / "files" / "reference").exists():
                shutil.copytree(stage / "files" / "reference", arch, dirs_exist_ok=True)
            for f in (stage / "files" / "supervision").glob("*") if (stage / "files" / "supervision").exists() else []:
                if f.is_file() and f.name != "attention.json":   # bijv. projects-index op rootniveau
                    shutil.copy2(f, sup / f.name) if not (sup / f.name).exists() else None
        finally:
            shutil.rmtree(stage, ignore_errors=True)
        log = home / "control" / "handover-log.jsonl"
        log.parent.mkdir(parents=True, exist_ok=True)
        with open(log, "a") as f:
            f.write(json.dumps({"at": _stamp(), "bundle": Path(path).name, "imported": plan["new"], "identical": plan["identical"],
                                "jobs": plan["jobs"].get("action")}) + "\n")
        plan["applied"] = True
        plan["archive"] = str(arch)
        bad = []   # integriteit via de echte dossierlaag (ontbreekt bij een losse run op de Mac; dan alleen de JSON-controle hierboven)
        with _env_home(home):
            try:
                from . import dossier
                for pid in plan["new"] + plan["replace"]:
                    d, problems = dossier.read(pid)
                    if problems or not d:
                        bad.append(pid)
            except ImportError:
                plan["integrity_checked"] = False
            plan["integrity_problems"] = bad
            if pause_reason and not bad:
                from . import jobs
                if (home / "jobs" / "queue.json").exists():
                    jobs.act("pause_queue", {"node": "ALL", "reason": pause_reason}, "noodovername")
                    plan["paused_all"] = True
            if promote:
                if bad:
                    plan["promote"] = "niet gepromoveerd: integriteitsproblemen"
                else:
                    from . import cplane
                    doc = cplane.set_role("primary", primary_host=cplane.config.local_node(), promoted_from=m.get("source_node") or "MAC",
                                          bundle=Path(path).name, epoch_floor=int(m.get("source_epoch") or 0) + 1, **(promote_info or {}))
                    plan["promote"], plan["epoch"] = doc["role"], doc["epoch"]
    return plan


def _state_plan(m: dict, home: Path, skip: set | None = None) -> dict:
    """Wat de import met ketens, supervisors, notifier, actionreview en tekst-backups zou doen (alleen lezen). `skip`: id's die niet de live map in gaan (quarantaine)."""
    files = {e["path"]: e for e in m["files"]}
    out = {}
    for kind, fname, _key in RUN_KINDS:
        ids = sorted({p.split("/")[1] for p in files if p.startswith(kind + "/") and p.endswith("/" + fname)})
        new, same, clash = [], [], []
        held = [i for i in ids if i in (skip or ())]
        ids = [i for i in ids if i not in (skip or ())]
        for i in ids:
            loc = home / kind / i / fname
            (new if not loc.exists() else same if _sha(loc) == files[f"{kind}/{i}/{fname}"]["sha256"] else clash).append(i)
        out[kind] = {"new": new, "identical": same, "conflicts": clash, "quarantine": held}
    for key, prefix, rel in (("actionreview", "actionreview/", Path("actionreview")), ("tekst_backups", "jobs-tekst-backups/", Path("jobs") / "tekst-backups")):
        names = [p[len(prefix):] for p in files if p.startswith(prefix)]
        out[key] = {"new": sorted(n for n in names if not (home / rel / n).exists()), "kept": sum(1 for n in names if (home / rel / n).exists())}
    out["notifier"] = {n: ("none" if f"notifier/{n}" not in files else "install" if not (home / "control" / "notifier" / n).exists() else "merge")
                       for n in NOTIFIER_FILES}
    return out


def _quarantine_runs(files: Path, home: Path, st: dict) -> str:
    """Actieve ketens/supervisors uit een replica: apart bewaren (niet in chains/ of supervisors/), zodat niets ze doorschakelt."""
    qd = home / "failover" / f"quarantine-{_stamp()}"
    for kind, fname, _key in RUN_KINDS:
        for i in st[kind].get("quarantine", []):
            _put(files / kind / i / fname, qd / kind / i / fname)
    return str(qd)


def _put(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(f".{dst.name}.import")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def _merge_notifier(stage: Path, home: Path, plan: dict) -> dict:
    """Ledger: unie van openstaande condities (lokale blijven); outbox: alleen regels die er nog niet zijn. Zo meldt de nieuwe notifier niets opnieuw."""
    nd = home / "control" / "notifier"
    nd.mkdir(parents=True, exist_ok=True)
    res = {}
    if plan.get("ledger.json") != "none":
        src = json.loads((stage / "notifier" / "ledger.json").read_text())
        try:
            cur = json.loads((nd / "ledger.json").read_text())
        except (OSError, ValueError):
            cur = {"open": {}}
        cur.setdefault("open", {})
        added = [k for k in (src.get("open") or {}) if k not in cur["open"]]
        for k in added:
            cur["open"][k] = src["open"][k]
        _atomic(nd / "ledger.json", json.dumps(cur, indent=1))
        res["ledger_added"] = len(added)
    if plan.get("outbox.jsonl") != "none":
        have = set((nd / "outbox.jsonl").read_text(encoding="utf-8").splitlines()) if (nd / "outbox.jsonl").exists() else set()
        add = [l for l in (stage / "notifier" / "outbox.jsonl").read_text(encoding="utf-8").splitlines() if l and l not in have]
        if add:
            with open(nd / "outbox.jsonl", "a", encoding="utf-8") as f:
                f.write("\n".join(add) + "\n")
        res["outbox_added"] = len(add)
    return res


def _apply_state(files: Path, home: Path, st: dict, replace_existing: bool) -> dict:
    done = {}
    ts = _stamp()
    for kind, fname, _key in RUN_KINDS:
        todo = st[kind]["new"] + (st[kind]["conflicts"] if replace_existing else [])
        for i in st[kind]["conflicts"] if replace_existing else []:   # bestaande versie blijft bewaard, alleen weggezet
            aside = home / kind / f".replaced-{ts}"
            aside.mkdir(parents=True, exist_ok=True)
            os.rename(home / kind / i, aside / i)
        for i in todo:
            _put(files / kind / i / fname, home / kind / i / fname)
        done[kind] = len(todo)
    for key, sub, rel in (("actionreview", "actionreview", Path("actionreview")), ("tekst_backups", "jobs-tekst-backups", Path("jobs") / "tekst-backups")):
        for n in st[key]["new"]:   # nooit een bestaand bestand overschrijven
            _put(files / sub / n, home / rel / n)
        done[key] = len(st[key]["new"])
    if (files / "notifier").exists():
        done["notifier"] = _merge_notifier(files, home, st["notifier"])
    return done


@contextmanager
def _env_home(home: Path):
    """dossier.read en cplane.set_role werken op R2_HOME; zorg dat dat tijdens de import ook echt de ontvanger is (niet de aanroeper)."""
    old = os.environ.get("R2_HOME")
    os.environ["R2_HOME"] = str(home)
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("R2_HOME", None)
        else:
            os.environ["R2_HOME"] = old


def _local_queue_state(home: Path) -> str:
    """'none' (geen of lege wachtrij), 'fenced' (deze machine heeft hem zelf overgedragen: verouderd) of 'active'."""
    q = home / "jobs" / "queue.json"
    if not q.exists():
        return "none"
    if (home / "jobs" / JOBS_FENCE).exists():
        return "fenced"
    try:
        return "active" if json.loads(q.read_text()).get("jobs") else "none"
    except (OSError, ValueError):
        return "active"   # onleesbaar: nooit stil overschrijven


def _jobs_plan(m: dict, home: Path, replace_existing: bool) -> dict:
    if not m.get("jobs"):
        return {"action": "none"}
    local = _local_queue_state(home)
    bundled = next((e["sha256"] for e in m["files"] if e["path"] == "jobs/queue.json"), None)
    q = home / "jobs" / "queue.json"
    if q.exists() and bundled and _sha(q) == bundled and local != "fenced":
        return {"action": "identical"}
    if local == "none":
        return {"action": "install"}
    if local == "fenced" or replace_existing:
        return {"action": "replace", "local": local}
    return {"action": "conflict", "error": "op deze machine staat al een actieve opdrachtwachtrij; niets geïmporteerd (alles-of-niets). "
                                           "Laat die eerst leeglopen of gebruik --replace-existing (de huidige gaat dan opzij)."}


def _check_queue(path: Path) -> None:
    d = json.loads(path.read_text())
    if not isinstance(d, dict) or not isinstance(d.get("jobs"), dict) or not isinstance(d.get("order"), dict):
        raise ValueError("de opdrachtwachtrij in de bundel is onvolledig")


def _install_jobs(src: Path, home: Path, plan: dict) -> None:
    """Zet de overgedragen wachtrij neer onder de jobs-lock; een bestaande (verouderde) gaat opzij, nooit weg."""
    jd = home / "jobs"
    f = _jobs_lock(home)
    try:
        if plan["action"] == "replace":
            aside = home / f"jobs.replaced-{_stamp()}"
            aside.mkdir(parents=True)
            for p in jd.iterdir():
                if p.name == ".lock":
                    continue
                if p.is_dir():   # bijv. tekst-backups/
                    shutil.copytree(p, aside / p.name)
                else:
                    shutil.copy2(p, aside / p.name)
            plan["aside"] = str(aside)
        for n in JOBS_FILES:
            if (src / n).is_file():
                tmp = jd / f".{n}.import"
                shutil.copy2(src / n, tmp)
                os.replace(tmp, jd / n)
            elif (jd / n).exists() and n != "queue.json":
                (jd / n).unlink()
        (jd / JOBS_FENCE).unlink(missing_ok=True)   # pas als de wachtrij er staat, mag de dispatcher hier weer werken
    finally:
        f.close()


# --- pariteit (alleen lezen) ---

def _sup_dir(home: Path) -> Path:
    sup = home / "supervision"
    if sup.exists() and any(p.is_dir() for p in sup.iterdir()):
        return sup
    handed = sorted(home.glob("supervision.handed-over-*"))
    return handed[-1] if handed else sup


def _file_map(root: Path, pattern: str = "*") -> dict:
    return {p.relative_to(root).as_posix(): _sha(p) for p in sorted(root.rglob(pattern))
            if p.is_file() and not p.is_symlink() and ".tmp" not in p.name and p.name != ".lock"} if root.exists() else {}


def parity(src: Path, dst: Path) -> dict:
    """Vergelijkt de bron (na het hek) met de ontvanger (na de import). Schrijft niets. ok=False bij elk verschil dat gegevens zou kosten."""
    checks = []

    def add(name: str, ok: bool, detail=None):
        checks.append({"name": name, "ok": bool(ok), **({"detail": detail} if detail is not None else {})})

    def jload(p: Path):
        try:
            return json.loads(p.read_text())
        except (OSError, ValueError):
            return None

    sq, dq = jload(src / "jobs" / "queue.json"), jload(dst / "jobs" / "queue.json")
    if sq is None and dq is None:
        add("wachtrij", True, "geen wachtrij aan beide kanten")
    else:
        same = (src / "jobs" / "queue.json").exists() and (dst / "jobs" / "queue.json").exists() and \
            _sha(src / "jobs" / "queue.json") == _sha(dst / "jobs" / "queue.json")
        held = lambda q: sum(1 for j in (q or {}).get("jobs", {}).values() if j.get("held") and j.get("status") == "queued")
        add("wachtrij byte-identiek (sha256 queue.json)", same, {"opdrachten": [len((sq or {}).get("jobs", {})), len((dq or {}).get("jobs", {}))],
                                                                  "held": [held(sq), held(dq)], "version": [(sq or {}).get("version"), (dq or {}).get("version")]})
        add("volgorde per node (order) gelijk", (sq or {}).get("order") == (dq or {}).get("order"))
        add("idempotentietabel (requests) gelijk", (sq or {}).get("requests") == (dq or {}).get("requests"), [len((sq or {}).get("requests", {})), len((dq or {}).get("requests", {}))])
        add("pauzes (paused) gelijk", (sq or {}).get("paused") == (dq or {}).get("paused"))
        for n in ("queue.prev.json", "audit.jsonl"):
            if (src / "jobs" / n).exists():
                add(f"jobs/{n} gelijk", (dst / "jobs" / n).exists() and _sha(src / "jobs" / n) == _sha(dst / "jobs" / n))
    ss, ds = _file_map(_sup_dir(src)), _file_map(dst / "supervision")
    ss = {k: v for k, v in ss.items() if k.count("/") >= 1}   # alleen bestanden binnen een project
    add("supervisie: projecten en bestanden gelijk", all(ds.get(k) == v for k, v in ss.items()),
        {"projecten": sorted({k.split("/")[0] for k in ss}), "ontbreekt": sorted(k for k, v in ss.items() if ds.get(k) != v)[:5]})
    for kind, fname, _key in RUN_KINDS:
        sm = {k: v for k, v in _file_map(src / kind, fname).items()}
        dm = _file_map(dst / kind, fname)
        add(f"{kind}: alle bronbestanden aanwezig en gelijk", all(dm.get(k) == v for k, v in sm.items()),
            {"bron": len(sm), "doel": len(dm), "verschil": sorted(k for k, v in sm.items() if dm.get(k) != v)[:5]})
    sl = (jload(src / "control" / "notifier" / "ledger.json") or {}).get("open", {})
    dl = (jload(dst / "control" / "notifier" / "ledger.json") or {}).get("open", {})
    add("notifier-ledger: alle bronvermeldingen aanwezig", set(sl) <= set(dl), {"bron": len(sl), "doel": len(dl)})
    so = set((src / "control" / "notifier" / "outbox.jsonl").read_text(encoding="utf-8").splitlines()) if (src / "control" / "notifier" / "outbox.jsonl").exists() else set()
    do = set((dst / "control" / "notifier" / "outbox.jsonl").read_text(encoding="utf-8").splitlines()) if (dst / "control" / "notifier" / "outbox.jsonl").exists() else set()
    add("notifier-outbox: alle bronregels aanwezig", so <= do, {"bron": len(so), "doel": len(do)})
    for name, a, b in (("actionreview", src / "actionreview", dst / "actionreview"), ("jobs/tekst-backups", src / "jobs" / "tekst-backups", dst / "jobs" / "tekst-backups")):
        sm, dm = _file_map(a), _file_map(b)
        add(f"{name}: alle bronbestanden aanwezig", all(k in dm for k in sm), {"bron": len(sm), "doel": len(dm)})
    want = {i["action_id"] for i in open_actions_index(src)}
    have = {i["action_id"] for f in sorted((dst / "archive").glob("*/open-actions.json")) for i in (jload(f) or [])} if (dst / "archive").exists() else set()
    add("open acties van de bron als naslag aanwezig", want <= have, {"bron": len(want), "doel_naslag": len(have)})
    st_src = {p.parent.name for p in (src / "tasks").glob("R2-*/state.json")} if (src / "tasks").exists() else set()
    st_dst = {p.parent.name for f in (dst / "archive").glob("*/tasks-state") for p in f.glob("R2-*/state.json")} if (dst / "archive").exists() else set()
    add("taakstaat (state.json) als naslag aanwezig", st_src <= st_dst, {"bron": len(st_src), "doel_naslag": len(st_dst)})
    sr, dr = _read_role(src) or {}, _read_role(dst) or {}
    add("rollen (bron/doel)", True, {"bron": {k: sr.get(k) for k in ("role", "host", "primary_host", "epoch")},
                                      "doel": {k: dr.get(k) for k in ("role", "host", "primary_host", "epoch")}})
    add("rollen: ten hoogste één primary", [r.get("role") for r in (sr, dr)].count("primary") <= 1 and bool(dr),
        "ontvanger heeft een expliciet rolbestand" if dr else "ontvanger heeft GEEN rolbestand (= legacy primary)")
    if dr.get("role") == "primary":
        add("rollen: epoch ontvanger > epoch bron", int(dr.get("epoch", 0)) > int(sr.get("epoch", 0)), [sr.get("epoch"), dr.get("epoch")])
    return {"ok": all(c["ok"] for c in checks), "checks": checks}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="handover")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("--home")
    e.add_argument("--out")
    e.add_argument("--push", action="store_true")
    e.add_argument("--no-fence", action="store_true")
    e.add_argument("--to", default="GERARD", help="de nieuwe control plane (node uit nodes.toml), standaard GERARD")
    e.add_argument("--allow-active", action="store_true", help="ook met actieve ketens/supervisors (normaal geweigerd)")
    pa = sub.add_parser("parity")
    pa.add_argument("src")
    pa.add_argument("dst")
    u = sub.add_parser("unfence")
    u.add_argument("--home")
    v = sub.add_parser("verify")
    v.add_argument("bundle")
    i = sub.add_parser("import")
    i.add_argument("bundle")
    i.add_argument("--home")
    i.add_argument("--apply", action="store_true")
    i.add_argument("--promote", action="store_true")
    i.add_argument("--replace-existing", action="store_true")
    i.add_argument("--allow-active", action="store_true")
    i.add_argument("--quarantine-active", action="store_true", help="actieve ketens/supervisors van de bron NIET in de live map zetten maar in failover/quarantine-<tijd>/ (ze horen bij de node van de bron)")
    i.add_argument("--keep-local-runs", action="store_true", help="lokale ketens/supervisors blijven staan bij een verschil, ook met --replace-existing (terugschakelen na een noodovername)")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "export":
            print(json.dumps(export(home_dir(a.home), Path(a.out) if a.out else None, a.push, not a.no_fence, a.to.upper(), a.allow_active), indent=1))
        elif a.cmd == "unfence":
            print(unfence(home_dir(a.home)))
        elif a.cmd == "parity":
            res = parity(Path(a.src).expanduser(), Path(a.dst).expanduser())
            print(json.dumps(res, indent=1, ensure_ascii=False))
            return 0 if res["ok"] else 1
        elif a.cmd == "verify":
            m = verify_bundle(Path(a.bundle))
            print(f"OK: projecten {m['projects']}, {len(m['files'])} bestanden")
        elif a.cmd == "import":
            plan = import_bundle(Path(a.bundle), home_dir(a.home), a.apply, a.promote, a.replace_existing, a.allow_active, keep_local_runs=a.keep_local_runs, quarantine_active=a.quarantine_active)
            print(json.dumps(plan, indent=1))
            return 1 if plan.get("error") else 0
    except (ValueError, RuntimeError, OSError, tarfile.TarError, subprocess.SubprocessError) as ex:
        print(f"FOUT: {type(ex).__name__}: {ex}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
