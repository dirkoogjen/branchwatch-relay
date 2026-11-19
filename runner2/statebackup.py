"""Controleerbare, alleen-lezen back-up van ALLE control-plane-toestand, voor de masterverhuizing (herstelpunt vóór elke stap).

Dit bestand gebruikt ALLEEN de standaardbibliotheek en importeert niets uit runner2. Het schrijft nooit in de bron: de bron wordt gelezen
(bestandslocks worden niet genomen; alle state wordt atomair geschreven, de wachtrij wordt tot twee gelijke leesbare lezingen herhaald),
de uitvoer komt in een nieuwe, privé (0700) map BUITEN ~/.runner2, zodat geen enkele watch hem ooit scant.

    python3 -m runner2.statebackup create [--home DIR] [--out DIR] [--tasks state|full] [--repo NAAM=PAD ...] [--no-repos]
    python3 -m runner2.statebackup verify DIR
    python3 -m runner2.statebackup restore DIR --target LEGE_MAP     (alleen naar een nieuwe, lege map; nooit naar een live ~/.runner2)

Inhoud: jobs/ (wachtrij, vorige generatie, audit, tekst-backups), supervision/, chains/, supervisors/, control/notifier + monitor.json,
actionreview/, config/{machine,nodes,control}.toml + control-plane.json, tasks/*/state.json (open acties zitten daarin; `--tasks full` neemt
ook transcripten en logs mee), en per opgegeven git-repository een `git bundle --all` met ref-lijst. Wat er BEWUST niet in zit: control.env,
cloudflared.env, backup-r2.env, ssh-sleutels en elk ander geheim (alleen een inventaris met naam, rechten en grootte: secrets-inventaris.json).
SHA256SUMS is `sha256sum -c`-compatibel.
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import tempfile
import tomllib
from datetime import datetime, timezone
from pathlib import Path

NEVER = ("control.env", "cloudflared.env", "backup-r2.env", "cloudflare.env", "r2.env", "id_", ".pem", "token", "secret", "credentials")
DIRS = ("jobs", "supervision", "chains", "supervisors", "actionreview")
CONFIG_FILES = ("machine.toml", "nodes.toml", "control.toml", "control-plane.json")
DEFAULT_REPOS = (("runner2-src", "~/runner2-src"), ("monitor2-app", "~/monitor2-app"))
SECRET_FILES = ("~/.config/runner2/control.env", "~/.config/runner2/cloudflared.env", "~/.config/runner2/backup-r2.env",
                "~/.config/runner2/connector-url.txt", "~/.ssh/r2_control_ed25519")


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


def _excluded(rel: str) -> bool:
    name = rel.rsplit("/", 1)[-1].lower()
    return any(n in name for n in NEVER) or name.endswith((".tmp", ".lock", ".import")) or ".tmp" in name or name.startswith(".")


def _copy(src: Path, dst: Path) -> bool:
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(src, dst)
    except FileNotFoundError:   # atomair vervangen tijdens het kopiëren: de volgende lezing pakt de nieuwe
        return False
    os.chmod(dst, 0o600)
    return True


def _stable_copy(src: Path, dst: Path, must_parse: bool = False) -> None:
    """Kopieer tot twee opeenvolgende lezingen dezelfde sha256 geven (en, voor JSON, leesbaar zijn)."""
    for _ in range(8):
        a = _sha(src)
        if not _copy(src, dst):
            continue
        if _sha(dst) == a and _sha(src) == a:
            if must_parse:
                json.loads(dst.read_text())
            return
        time.sleep(0.05)
    raise RuntimeError(f"{src.name} bleef veranderen tijdens het kopiëren; probeer opnieuw")


def _collect(home: Path, tasks: str) -> list:
    items = []
    for d in (*DIRS, *(x.name for x in sorted(home.glob("supervision.handed-over-*")))):   # incl. een weggezette (gehekte) supervisie
        for p in sorted((home / d).rglob("*")) if (home / d).exists() else []:
            rel = p.relative_to(home).as_posix()
            if p.is_file() and not p.is_symlink() and not any(_excluded(part) for part in rel.split("/")[1:]):
                items.append(rel)
    for n in CONFIG_FILES:
        if (home / "config" / n).is_file():
            items.append(f"config/{n}")
    for n in ("ledger.json", "outbox.jsonl", "heartbeat.json"):
        if (home / "control" / "notifier" / n).is_file():
            items.append(f"control/notifier/{n}")
    if (home / "control" / "monitor.json").is_file():
        items.append("control/monitor.json")
    for p in sorted((home / "tasks").glob("R2-*")) if (home / "tasks").exists() else []:
        if tasks == "full":
            for q in sorted(p.rglob("*")):
                if q.is_file() and not q.is_symlink() and ".tmp" not in q.name and not _excluded(q.name):
                    items.append(q.relative_to(home).as_posix())
        elif (p / "state.json").is_file():
            items.append(f"tasks/{p.name}/state.json")
    return items


def _git(repo: Path, *a) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(repo), *a], capture_output=True, text=True, timeout=600)


def _bundle_repo(name: str, repo: Path, out: Path) -> dict:
    repo = repo.expanduser()
    if not (repo / ".git").exists() and not (repo / "HEAD").exists():
        return {"name": name, "ok": False, "error": "geen git-repository"}
    (out / "repos").mkdir(parents=True, exist_ok=True)
    b = out / "repos" / f"{name}.bundle"
    r = _git(repo, "bundle", "create", str(b), "--all")
    if r.returncode:
        return {"name": name, "ok": False, "error": r.stderr.strip()[-200:]}
    v = _git(repo, "bundle", "verify", str(b))
    refs = _git(repo, "for-each-ref", "--format=%(refname) %(objectname)").stdout
    (out / "repos" / f"{name}.refs").write_text(refs)
    os.chmod(b, 0o600)
    os.chmod(out / "repos" / f"{name}.refs", 0o600)
    return {"name": name, "ok": v.returncode == 0, "refs": len(refs.splitlines()), "refs_sha256": hashlib.sha256(refs.encode()).hexdigest(),
            "head": _git(repo, "rev-parse", "HEAD").stdout.strip()}


def _secret_inventory() -> list:
    out = []
    for s in SECRET_FILES:
        p = Path(s).expanduser()
        try:
            st = p.stat()
            out.append({"path": s, "exists": True, "mode": oct(st.st_mode & 0o777), "size": st.st_size})
        except OSError:
            out.append({"path": s, "exists": False})
    return out


def create(home: Path, out: Path | None = None, tasks: str = "state", repos=DEFAULT_REPOS, with_repos: bool = True) -> dict:
    home = home.resolve()
    out = (out or Path.home() / "obf-backups" / f"masterverhuizing-{_stamp()}").expanduser().resolve()
    if home in out.parents or out == home:
        raise ValueError("de back-up mag niet binnen de bron-home komen (een watch zou hem kunnen scannen)")
    if out.exists() and any(out.iterdir()):
        raise ValueError(f"{out} bestaat al en is niet leeg; een back-up overschrijft nooit")
    out.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(out, 0o700)
    files = out / "files"
    entries, skipped = [], []
    for rel in _collect(home, tasks):
        dst = files / rel
        try:
            _stable_copy(home / rel, dst, must_parse=rel.endswith(("queue.json", "queue.prev.json", "dossier.json", "chain.json", "supervisor.json", "ledger.json")))
        except (RuntimeError, ValueError, OSError) as e:
            skipped.append({"path": rel, "error": str(e)[:120]})
            continue
        entries.append({"path": f"files/{rel}", "sha256": _sha(dst), "size": dst.stat().st_size})
    repo_res = [_bundle_repo(n, Path(p), out) for n, p in repos] if with_repos else []
    for r in repo_res:
        for ext in (".bundle", ".refs"):
            f = out / "repos" / f"{r['name']}{ext}"
            if f.exists():
                entries.append({"path": f"repos/{f.name}", "sha256": _sha(f), "size": f.stat().st_size})
    q = _read_json(files / "jobs" / "queue.json") or {}
    node = None
    try:
        node = tomllib.loads((home / "config" / "machine.toml").read_text()).get("node")
    except (OSError, ValueError):
        pass
    meta = {"format": 1, "created": _stamp(), "host": os.uname().nodename, "node": node, "source_home": str(home), "tasks": tasks,
            "files": len(entries), "skipped": skipped, "repos": repo_res,
            "queue": {"sha256": _sha(files / "jobs" / "queue.json") if (files / "jobs" / "queue.json").exists() else None,
                      "version": q.get("version"), "jobs": len(q.get("jobs") or {}),
                      "held": sum(1 for j in (q.get("jobs") or {}).values() if j.get("held") and j.get("status") == "queued"),
                      "paused": q.get("paused"), "order": {k: len(v) for k, v in (q.get("order") or {}).items()}},
            "counts": {k: len(list((files / k).glob("*/"))) if (files / k).exists() else 0 for k in ("supervision", "chains", "supervisors")}
            | {"tasks": len(list((files / "tasks").glob("R2-*"))) if (files / "tasks").exists() else 0},
            "role": _read_json(files / "config" / "control-plane.json")}
    (out / "secrets-inventaris.json").write_text(json.dumps(_secret_inventory(), indent=1))
    (out / "MANIFEST.json").write_text(json.dumps(meta, indent=1))
    for n in ("secrets-inventaris.json", "MANIFEST.json"):
        os.chmod(out / n, 0o600)
        entries.append({"path": n, "sha256": _sha(out / n), "size": (out / n).stat().st_size})
    sums = "".join(f"{e['sha256']}  {e['path']}\n" for e in sorted(entries, key=lambda e: e["path"]))
    (out / "SHA256SUMS").write_text(sums)
    os.chmod(out / "SHA256SUMS", 0o600)
    res = verify(out)
    res["out"] = str(out)
    res["manifest"] = {k: meta[k] for k in ("created", "node", "files", "queue", "counts", "repos", "skipped")}
    return res


def _read_json(p: Path):
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


def verify(out: Path) -> dict:
    """Controleer sha256 van alle bestanden, de leesbaarheid van de wachtrij, git-bundels en (optioneel) tegen de bron."""
    out = Path(out)
    problems, n = [], 0
    sums = out / "SHA256SUMS"
    if not sums.exists():
        return {"ok": False, "problems": ["SHA256SUMS ontbreekt"], "files": 0}
    listed = set()
    for line in sums.read_text().splitlines():
        digest, _, rel = line.partition("  ")
        listed.add(rel)
        f = out / rel
        if ".." in Path(rel).parts or rel.startswith("/"):
            problems.append(f"onveilig pad: {rel}")
        elif not f.is_file():
            problems.append(f"ontbreekt: {rel}")
        elif _sha(f) != digest:
            problems.append(f"sha256 klopt niet: {rel}")
        n += 1
    for f in out.rglob("*"):
        if f.is_file() and f.relative_to(out).as_posix() not in listed and f.name != "SHA256SUMS":
            problems.append(f"niet in SHA256SUMS: {f.relative_to(out).as_posix()}")
    q = out / "files" / "jobs" / "queue.json"
    if q.exists() and _read_json(q) is None:
        problems.append("queue.json is niet leesbaar")
    bundles = sorted((out / "repos").glob("*.bundle")) if (out / "repos").exists() else []
    if bundles:   # `git bundle verify` heeft een repository nodig: een wegwerp-lege repo is genoeg voor --all-bundels (geen vereisten)
        with tempfile.TemporaryDirectory(prefix="sb-verify-") as tmp:
            subprocess.run(["git", "init", "-q", tmp], capture_output=True, check=False)
            for b in bundles:
                r = subprocess.run(["git", "-C", tmp, "bundle", "verify", str(b.resolve())], capture_output=True, text=True, timeout=600)
                if r.returncode:
                    problems.append(f"git-bundel ongeldig: {b.name}")
    mode = oct(out.stat().st_mode & 0o777)
    if mode != "0o700":
        problems.append(f"map heeft rechten {mode}, verwacht 0o700")
    return {"ok": not problems, "files": n, "problems": problems}


def restore(out: Path, target: Path) -> dict:
    """Pak een back-up uit in een NIEUWE lege map (voor inspectie, replica of herstelproef). Nooit naar een live ~/.runner2."""
    target = Path(target).expanduser().resolve()
    live = Path.home() / ".runner2"
    if target == live or live in target.parents or (target / "config").exists() or (target.exists() and any(target.iterdir())):
        raise ValueError("doelmap moet nieuw/leeg zijn en mag geen bestaande Runner-2-home zijn")
    v = verify(out)
    if not v["ok"]:
        raise ValueError(f"back-up klopt niet: {v['problems'][:3]}")
    shutil.copytree(Path(out) / "files", target, dirs_exist_ok=True)
    for p in target.rglob("*"):
        if p.is_dir():
            os.chmod(p, 0o700)
    os.chmod(target, 0o700)
    return {"ok": True, "target": str(target), "files": v["files"]}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="statebackup")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("create")
    c.add_argument("--home")
    c.add_argument("--out")
    c.add_argument("--tasks", choices=("state", "full"), default="state")
    c.add_argument("--repo", action="append", default=[], metavar="NAAM=PAD")
    c.add_argument("--no-repos", action="store_true")
    v = sub.add_parser("verify")
    v.add_argument("dir")
    r = sub.add_parser("restore")
    r.add_argument("dir")
    r.add_argument("--target", required=True)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "create":
            repos = tuple(tuple(x.split("=", 1)) for x in a.repo) or DEFAULT_REPOS
            res = create(home_dir(a.home), Path(a.out) if a.out else None, a.tasks, repos, not a.no_repos)
        elif a.cmd == "verify":
            res = verify(Path(a.dir))
        else:
            res = restore(Path(a.dir), Path(a.target))
        print(json.dumps(res, indent=1, ensure_ascii=False))
        return 0 if res.get("ok") else 1
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as e:
        print(f"FOUT: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
