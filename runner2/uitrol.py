"""Uitrol van Runner 2 vanaf de Mac naar MAC, LINUX, WINDOWS en GERARD: controle vooraf, back-up, herstart, nacontrole
en terugrol. Draai dit op de Mac in de Runner-2-worktree (tak runner2/dev), met de Python van de Mac:

    python3 -m runner2.uitrol controle                         alleen kijken; wijzigt niets
    python3 -m runner2.uitrol uitrol [--node N ...] [--ja]     uitrollen (vraagt eerst bevestiging, tenzij --ja)
    python3 -m runner2.uitrol terugrol --stempel S [--node N]  een eerdere uitrol terugdraaien
    python3 -m runner2.uitrol lijst                            eerdere uitrollen (stempels)

Veiligheid (fail closed):
- De bron is HEAD van deze git-checkout en moet schoon zijn; wat uitgerold wordt is precies `git archive HEAD runner2`.
- Per Runner moet de huidige code exact overeenkomen met een versie uit de geschiedenis van deze tak. Is de code daar
  anders (bijvoorbeeld nieuwer), dan stopt de uitrol voor alle Runners voordat er iets verandert.
- Er mag op geen enkele Runner een taak, keten of supervisorrun lopen.
- Op GERARD moet ~/runner2-src een schone git-checkout zijn met dezelfde code; daar komt de nieuwe versie als tak
  runner2/integratie (eigen commit bovenop de geschiedenis daar) en wordt uitgerold zoals `runner2.cplane deploy` dat doet.
- Vóór elke wijziging een gedateerde back-up (<code>.backup-<stempel>); die blijft staan voor de terugrol.
- Volgorde LINUX, WINDOWS, MAC, GERARD; gaat de nacontrole van een Runner mis, dan wordt die Runner meteen
  teruggerold en stopt de uitrol.
- Lopende taken worden nooit gestopt: de watch-herstart laat taakprocessen ongemoeid (eigen sessie/eenheid).
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ORDER = ("LINUX", "WINDOWS", "MAC", "GERARD")
BOOT = "import sys;exec(sys.stdin.buffer.read(int(sys.stdin.buffer.readline())))"
SRC = Path(__file__).resolve().parents[1]
MAC_WATCH_LABEL = "com.studioobf.runner2.watch"
MONITOR_LABEL = "com.studioobf.obf-monitor2"
MONITOR_URL = "http://127.0.0.1:8772"
GERARD_UNITS = ("runner2-watch.service", "runner2-mcp.service", "runner2-notifier.service")
CALL_TIMEOUT_S = 180

# Het script dat op elke Runner draait (alleen standaardbibliotheek; Python 3.11+). CFG komt via %r erin.
REMOTE = r'''
import glob, hashlib, io, json, os, shutil, subprocess, sys, tarfile, time
CFG = json.loads(%r)
data = sys.stdin.buffer.read()
home = os.path.expanduser("~")
code = os.path.expanduser(CFG["code"])
r2home = os.environ.get("R2_HOME") or os.path.join(home, ".runner2")

def manifest(root):
    out = {}
    base = os.path.join(root, "runner2")
    for dp, dn, fn in os.walk(base):
        dn[:] = sorted(d for d in dn if d != "__pycache__")
        for f in fn:
            if f.endswith((".pyc", ".pyo")):
                continue
            p = os.path.join(dp, f)
            with open(p, "rb") as fh:
                b = fh.read()
            out[os.path.relpath(p, root).replace(os.sep, "/")] = hashlib.sha1(b"blob %%d\0" %% len(b) + b).hexdigest()
    return out

def revision(root):
    try:
        return open(os.path.join(root, "REVISION")).read().strip()
    except OSError:
        return None

def git(*a, cwd=None):
    r = subprocess.run(["git", *a], cwd=cwd, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError("git " + " ".join(a[:2]) + ": " + (r.stderr or r.stdout).strip()[-300:])
    return r.stdout.strip()

def heartbeat():
    try:
        return json.load(open(os.path.join(r2home, "watch.json")))
    except (OSError, ValueError):
        return {}

def extract(dst):
    tf = tarfile.open(fileobj=io.BytesIO(data))
    try:
        tf.extractall(dst, filter="data")
    except TypeError:
        for m in tf.getmembers():
            if m.name.startswith("/") or ".." in m.name.split("/"):
                raise RuntimeError("onveilig pad in de uitrol: " + m.name)
        tf.extractall(dst)

def backup(stamp, extra):
    bak = code + ".backup-" + stamp
    if os.path.exists(bak):
        raise RuntimeError("back-up bestaat al: " + bak)
    shutil.copytree(code, bak, ignore=shutil.ignore_patterns("__pycache__"))
    meta = {"stamp": stamp, "from_revision": revision(code), **extra}
    with open(os.path.join(bak, "BACKUP.json"), "w") as fh:
        json.dump(meta, fh)
    return bak

def stop_windows_watch(hb):
    """Stopt alleen de watch zelf (zonder /T), en alleen als het proces met de pid uit watch.json nog echt de watch is:
    is de watch al weg, dan kan die pid inmiddels van een ander proces zijn (Windows hergebruikt pid's)."""
    pid = hb.get("pid")
    if not pid:
        return "geen watch-pid in watch.json"
    sys.path.insert(0, code)
    try:
        from runner2 import procs, winproc
        row = next((r for r in procs.snapshot() if r["pid"] == int(pid)), None)
        if not row:
            return "watch draaide niet"
        if hb.get("start") and row["start"] != hb["start"]:
            return "pid " + str(pid) + " is hergebruikt door een ander proces; niets gestopt"
        if "runner2.watch" not in row["cmd"]:
            return "pid " + str(pid) + " is geen watch (" + row["cmd"][:60] + "); niets gestopt"
        if getattr(winproc, "kill_exact", None) and procs.WINDOWS:
            return "gestopt" if winproc.kill_exact(row["pid"], row["start"]) else "stoppen mislukt"
        try:
            procs.terminate(row["pid"], row["start"])
        except TypeError:   # oudere code (terugrol): terminate zonder identiteit, vlak na de controle hierboven
            procs.terminate(row["pid"])
        return "gestopt"
    except Exception as e:   # noqa: BLE001 -- liever niets stoppen dan een verkeerd proces
        return "niet gecontroleerd (" + type(e).__name__ + "); niets gestopt"

def swap_in(new):
    prev = code + ".prev"
    shutil.rmtree(prev, ignore_errors=True)
    if os.path.exists(code):
        os.rename(code, prev)
    os.rename(new, code)

op = CFG["op"]
res = {"ok": True, "op": op}
try:
    if op == "probe":
        res["code_exists"] = os.path.isdir(os.path.join(code, "runner2"))
        res["revision"] = revision(code)
        res["manifest"] = manifest(code) if res["code_exists"] else {}
        res["backups"] = sorted(os.path.basename(p) for p in glob.glob(code + ".backup-*"))
        res["heartbeat"] = heartbeat()
        res["python"] = sys.version.split()[0]
        src = CFG.get("src")
        if src:
            src = os.path.expanduser(src)
            res["src"] = {"exists": os.path.isdir(src), "git": os.path.isdir(os.path.join(src, ".git"))}
            if res["src"]["git"]:
                res["src"]["head"] = git("rev-parse", "HEAD", cwd=src)
                res["src"]["branch"] = subprocess.run(["git", "symbolic-ref", "-q", "--short", "HEAD"], cwd=src,
                                                      capture_output=True, text=True).stdout.strip()
                res["src"]["dirty"] = git("status", "--porcelain", "--untracked-files=no", cwd=src)
                res["src"]["manifest"] = manifest(src)
                # Staat de uitgerolde code als (oudere) commit in de geschiedenis van runner2-src? Dan is hij bekend.
                want = res.get("manifest") or {}
                for c in git("rev-list", "--max-count=60", "HEAD", "--", "runner2", cwd=src).split():
                    tree = {}
                    for line in git("ls-tree", "-r", c, "--", "runner2", cwd=src).splitlines():
                        meta, path = line.split("\t", 1)
                        if "/__pycache__/" not in path and not path.endswith((".pyc", ".pyo")):
                            tree[path] = meta.split()[2]
                    if tree == want:
                        res["src"]["code_commit"] = c
                        break
    elif op == "deploy":
        new = code + ".new"
        shutil.rmtree(new, ignore_errors=True)
        os.makedirs(new)
        extract(new)
        with open(os.path.join(new, "REVISION"), "w") as fh:
            fh.write(CFG["rev"] + "\n")
        if manifest(new) != CFG["expect"]:
            shutil.rmtree(new, ignore_errors=True)
            raise RuntimeError("uitgepakte code wijkt af van de bron; niets gewijzigd")
        res["backup"] = backup(CFG["stamp"], {})
        swap_in(new)
        res["revision"] = revision(code)
    elif op == "deploy_src":   # GERARD: via ~/runner2-src (git) en de gewone cplane-uitrol
        src = os.path.expanduser(CFG["src"])
        prev_ref = subprocess.run(["git", "symbolic-ref", "-q", "--short", "HEAD"], cwd=src, capture_output=True,
                                  text=True).stdout.strip() or git("rev-parse", "HEAD", cwd=src)
        res["backup"] = backup(CFG["stamp"], {"src": src, "src_prev_ref": prev_ref})
        # Eigen commit bovenop de geschiedenis van runner2-src (die geschiedenis hoeft niet gelijk te zijn aan de onze):
        # tak runner2/integratie, alleen runner2/ vervangen door precies de bron.
        git("checkout", "-q", "-B", CFG["branch"], cwd=src)
        git("rm", "-r", "-q", "--ignore-unmatch", "runner2", cwd=src)
        shutil.rmtree(os.path.join(src, "runner2"), ignore_errors=True)
        extract(src)
        git("add", "-A", "runner2", cwd=src)
        git("-c", "user.name=Studio OBF", "-c", "user.email=release@example.invalid", "commit", "-q", "--allow-empty", "-m",
            "runner2: uitrol " + CFG["rev"] + " (bron " + CFG["commit"] + ", stempel " + CFG["stamp"] + ")", cwd=src)
        if manifest(src) != CFG["expect"]:
            raise RuntimeError("runner2-src wijkt na de commit af van de bron")
        res["src_commit"] = git("rev-parse", "HEAD", cwd=src)
        # zelfde stap als `runner2.cplane deploy ~/runner2-src`: src/runner2 -> <code>.new, REVISION, atomair wisselen (.prev)
        new = code + ".new"
        shutil.rmtree(new, ignore_errors=True)
        shutil.copytree(os.path.join(src, "runner2"), os.path.join(new, "runner2"),
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        with open(os.path.join(new, "REVISION"), "w") as fh:
            fh.write(git("rev-parse", "--short", "HEAD", cwd=src) + "\n")
        swap_in(new)
        if manifest(code) != CFG["expect"]:
            raise RuntimeError("runner2-code wijkt na de uitrol af van de bron")
        res["revision"] = revision(code)
    elif op == "rollback":
        bak = code + ".backup-" + CFG["stamp"]
        if not os.path.isdir(bak):
            raise RuntimeError("geen back-up " + bak)
        meta = json.load(open(os.path.join(bak, "BACKUP.json")))
        if meta.get("src"):
            git("checkout", "-q", meta["src_prev_ref"], cwd=meta["src"])
        failed = code + ".failed-" + time.strftime("%%Y%%m%%dT%%H%%M%%S")
        if os.path.exists(code):
            os.rename(code, failed)
            res["failed_copy"] = failed
        shutil.copytree(bak, code, ignore=shutil.ignore_patterns("BACKUP.json"))
        res["revision"] = revision(code)
        res["src_ref"] = meta.get("src_prev_ref")
    elif op == "restart":
        before = heartbeat().get("pid")
        kind = CFG["kind"]
        if kind == "launchd":
            for label in CFG["labels"]:
                r = subprocess.run(["launchctl", "kickstart", "-k", "gui/%%d/%%s" %% (os.getuid(), label)], capture_output=True, text=True)
                if r.returncode:
                    raise RuntimeError("launchctl kickstart " + label + ": " + r.stderr.strip()[-200:])
        elif kind == "systemd":
            env = dict(os.environ)
            env.setdefault("XDG_RUNTIME_DIR", "/run/user/%%d" %% os.getuid())
            env.setdefault("DBUS_SESSION_BUS_ADDRESS", "unix:path=" + env["XDG_RUNTIME_DIR"] + "/bus")
            done = []
            for unit in CFG["units"]:
                if subprocess.run(["systemctl", "--user", "is-enabled", "--quiet", unit], env=env).returncode:
                    continue
                r = subprocess.run(["systemctl", "--user", "restart", unit], env=env, capture_output=True, text=True)
                if r.returncode:
                    raise RuntimeError("systemctl restart " + unit + ": " + r.stderr.strip()[-200:])
                done.append(unit)
            res["restarted"] = done
        elif kind == "schtasks":
            task = CFG["task"]
            subprocess.run(["schtasks", "/End", "/TN", task], capture_output=True)
            res["watch_stop"] = stop_windows_watch(heartbeat())
            if res["watch_stop"] == "stoppen mislukt":
                raise RuntimeError("de oude watch kon niet worden gestopt")
            if res["watch_stop"] == "gestopt":
                time.sleep(2)   # watch.lock vrij laten komen voordat de nieuwe watch start
            r = subprocess.run(["schtasks", "/Run", "/TN", task], capture_output=True, text=True)
            if r.returncode:
                raise RuntimeError("schtasks /Run: " + (r.stderr or r.stdout).strip()[-200:])
        elif kind == "signal":   # watch zonder dienstbeheer (proefopstelling): stoppen; de bewaker start hem opnieuw
            import signal
            if before:
                try:
                    os.kill(int(before), signal.SIGTERM)
                except ProcessLookupError:
                    pass
        elif kind != "none":
            raise RuntimeError("onbekende herstart " + kind)
        start = time.time()
        while time.time() - start < CFG.get("wait_s", 45):
            hb = heartbeat()
            if hb.get("pid") and hb.get("pid") != before and hb.get("epoch", 0) > start - 1:
                break
            time.sleep(1)
        hb = heartbeat()
        res["heartbeat"] = hb
        res["watch_restarted"] = bool(hb.get("pid") and hb.get("pid") != before and hb.get("epoch", 0) > start - 1)
    else:
        raise RuntimeError("onbekende handeling " + op)
except Exception as e:
    res = {"ok": False, "op": op, "error": type(e).__name__ + ": " + str(e)}
sys.stdout.write("\n" + json.dumps(res) + "\n")
'''


class UitrolError(Exception):
    pass


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _git(*a) -> str:
    r = subprocess.run(["git", "-C", str(SRC), *a], capture_output=True)
    if r.returncode:
        raise UitrolError(f"git {' '.join(a[:2])}: {r.stderr.decode(errors='replace').strip()}")
    return r.stdout.decode(errors="replace").strip()


def source() -> dict:
    """De bron: HEAD van deze checkout, schoon, met het manifest van runner2/."""
    if _git("status", "--porcelain", "--", "runner2"):
        raise UitrolError(f"{SRC}/runner2 heeft niet-vastgelegde wijzigingen; leg eerst vast of zet ze opzij")
    head = _git("rev-parse", "HEAD")
    return {"commit": head, "rev": head[:8], "manifest": tree_manifest(head)}


_trees: dict = {}


def tree_manifest(commit: str) -> dict:
    if commit not in _trees:
        out = {}
        for line in _git("ls-tree", "-r", commit, "--", "runner2").splitlines():
            meta, path = line.split("\t", 1)
            if not path.endswith((".pyc", ".pyo")) and "/__pycache__/" not in path:
                out[path] = meta.split()[2]
        _trees[commit] = out
    return _trees[commit]


def known_version(manifest: dict, limit: int = 400) -> str | None:
    """Welke commit uit de geschiedenis van HEAD heeft precies deze runner2-code? None = onbekend (bijv. nieuwer)."""
    if not manifest:
        return None
    # --full-history: ook een zijtak die met `merge -s ours` is opgenomen (de stand van één Runner) blijft zichtbaar
    for c in _git("rev-list", "--full-history", f"--max-count={limit}", "HEAD", "--", "runner2").splitlines():
        if tree_manifest(c) == manifest:
            return c
    return None


# --- transport ---

def specs() -> dict:
    """MAC (lokaal) plus de Runners uit ~/.runner2/config/nodes.toml van de Mac.

    Per node kan nodes.toml de standaard aanpassen met uitrol_code, uitrol_src en uitrol_restart (inline-tabel), en een
    [MAC]-sectie met local = true; dat gebruikt de proefopstelling, de echte opstelling heeft het niet nodig."""
    from . import remote
    out = {"MAC": {"local": True, "code": "~/runner2-code", "python": sys.executable, "restart": {"kind": "launchd", "labels": [MAC_WATCH_LABEL]}}}
    for name, spec in remote.nodes().items():
        spec = dict(spec)
        if spec.get("local"):
            out[name] = {**out.get(name, {}), **spec, "python": sys.executable}
            continue
        if name == "GERARD":
            spec.update(src="~/runner2-src", restart={"kind": "systemd", "units": list(GERARD_UNITS)})
        elif remote.is_windows(spec):
            spec["restart"] = {"kind": "schtasks", "task": "StudioOBF-Runner2-Watch"}
        else:
            spec["restart"] = {"kind": "systemd", "units": ["runner2-watch.service"]}
        out[name] = spec
    for name, spec in out.items():
        spec["code"] = spec.pop("uitrol_code", None) or spec["code"]
        if spec.get("uitrol_src"):
            spec["src"] = spec.pop("uitrol_src")
        if spec.get("uitrol_restart"):
            spec["restart"] = dict(spec.pop("uitrol_restart"))
    return out


def ssh_base(spec: dict) -> list:
    """Zelfde ssh-opties en sleutel als het node-transport, maar zonder gedeelde master-verbinding (ControlMaster):
    de uitrol hangt dan niet af van de multiplex-socket van de draaiende diensten. Op 2026-10-18 bleef een probe via
    die gedeelde verbinding naar WINDOWS af en toe 60 s hangen, waarna de uitrol onnodig terugrolde."""
    from . import remote
    base, out, i = remote._ssh_cmd(spec)[:-2], [], 0
    while i < len(base):
        if base[i] == "-o" and i + 1 < len(base) and base[i + 1].startswith("Control"):
            i += 2
            continue
        out.append(base[i])
        i += 1
    return out + ["-o", "ControlMaster=no", "-o", "ControlPath=none"]


def run_remote(spec: dict, cfg: dict, payload: bytes = b"", timeout: float = CALL_TIMEOUT_S) -> dict:
    script = (REMOTE % json.dumps(cfg)).encode()
    stdin = f"{len(script)}\n".encode() + script + payload
    if spec.get("local"):
        cmd = [spec["python"], "-c", BOOT]
    else:
        from . import remote
        py = str(spec.get("python") or ("python" if remote.is_windows(spec) else "python3"))
        cmd = ssh_base(spec) + [f"{spec['user']}@{spec['host']}", f'"{py}" -c "{BOOT}"']
    # Alleen de probe (alleen lezen) mag na een time-out nog één keer; uitrol, herstart en terugrol nooit blind opnieuw.
    for attempt in range(2 if cfg.get("op") == "probe" else 1):
        try:
            r = subprocess.run(cmd, input=stdin, capture_output=True, timeout=timeout)
            break
        except subprocess.TimeoutExpired:
            if attempt or cfg.get("op") != "probe":
                raise UitrolError(f"geen antwoord binnen {timeout:.0f} s ({cfg.get('op')})") from None
    lines = [ln for ln in r.stdout.decode(errors="replace").splitlines() if ln.strip()]
    try:
        out = json.loads(lines[-1])
    except (ValueError, IndexError):
        raise UitrolError(f"onbegrijpelijk antwoord (exit {r.returncode}): {r.stderr.decode(errors='replace').strip()[-300:]}") from None
    if not out.get("ok"):
        raise UitrolError(out.get("error") or "onbekende fout")
    return out


def _status(name: str) -> dict:
    from . import controller, remote
    if specs()[name].get("local") and name == (os.environ.get("R2_UITROL_LOCAL_NODE") or "MAC"):
        return controller.status()
    return remote.call(name, "runner2_status", {})


def node_activity(name: str) -> str | None:
    """Iets actiefs op deze Runner (taak, keten, supervisorrun)? Geeft een omschrijving, None = vrij."""
    try:
        s = _status(name)
    except Exception as e:   # noqa: BLE001
        return f"status onbekend: {getattr(e, 'message', e)}"
    if s.get("active_task"):
        return f"taak {s['active_task']} loopt"
    for kind in ("chains", "supervisors"):
        for x in s.get(kind) or []:
            if (x.get("state") or "active") == "active":
                return f"{kind[:-1]} {x.get('id')} actief"
    return None


# --- de stappen ---

def controle(nodes: list, src: dict) -> dict:
    sp = specs()
    report, problems = {}, []
    for n in nodes:
        if n not in sp:
            problems.append(f"{n}: geen route (niet in nodes.toml van de Mac)")
            continue
        spec = sp[n]
        row = report.setdefault(n, {})
        try:
            p = run_remote(spec, {"op": "probe", "code": spec["code"], "src": spec.get("src")}, timeout=60)
        except UitrolError as e:
            problems.append(f"{n}: niet bereikbaar of probe mislukt: {e}")
            continue
        row.update(revision=p.get("revision"), python=p.get("python"), backups=len(p.get("backups") or []))
        same = p["manifest"] == src["manifest"]
        ver = None if same else known_version(p["manifest"])
        s = p.get("src") or {}
        # Control plane: code is een oudere commit uit de eigen geschiedenis van runner2-src, en die runner2-src zelf is
        # een bekende versie -> de nieuwe code bevat alles; niets gaat verloren.
        older = bool(not same and not ver and s.get("code_commit") and known_version(s.get("manifest") or {}))
        row["code"] = ("al gelijk aan de bron" if same else f"bekende versie {ver[:8]}" if ver else
                       f"oudere versie {s['code_commit'][:8]} uit runner2-src (runner2-src zelf is bekend)" if older else "ONBEKEND")
        if not p.get("code_exists"):
            problems.append(f"{n}: geen runner2-code gevonden in {spec['code']}")
        elif not same and not ver and not older:
            problems.append(f"{n}: de huidige code (REVISION {p.get('revision')}) komt niet voor in de geschiedenis van deze tak; "
                            "mogelijk nieuwer. Uitrol gestopt om niets te overschrijven.")
        if spec.get("src"):
            row["src"] = {k: s.get(k) for k in ("exists", "git", "branch", "head")}
            if not s.get("exists") or not s.get("git"):
                problems.append(f"{n}: ~/runner2-src ontbreekt of is geen git-checkout; uitrol op de control plane gaat alleen via die weg")
            elif s.get("dirty"):
                problems.append(f"{n}: ~/runner2-src heeft niet-vastgelegde wijzigingen")
            elif s.get("manifest") != p["manifest"] and not older:
                problems.append(f"{n}: ~/runner2-src en ~/runner2-code verschillen; eerst uitzoeken welke de echte stand is")
        hb = p.get("heartbeat") or {}
        row["watch"] = f"hartslag {time.time() - hb['epoch']:.0f}s geleden" if hb.get("epoch") else "geen hartslag"
        act = node_activity(n)
        row["actief"] = act or "vrij"
        if act:
            problems.append(f"{n}: {act}; wacht tot de Runner vrij is")
    return {"report": report, "problems": problems, "source": src["rev"]}


def deploy_node(n: str, spec: dict, src: dict, stamp: str, payload: bytes) -> dict:
    if spec.get("src"):
        out = run_remote(spec, {"op": "deploy_src", "code": spec["code"], "src": spec["src"], "stamp": stamp,
                                "branch": "runner2/integratie", "commit": src["commit"], "rev": src["rev"],
                                "expect": src["manifest"]}, payload)
    else:
        out = run_remote(spec, {"op": "deploy", "code": spec["code"], "stamp": stamp, "rev": src["rev"],
                                "expect": src["manifest"]}, payload)
    rs = run_remote(spec, {"op": "restart", "code": spec["code"], **spec["restart"]}, timeout=120)
    if not rs.get("watch_restarted"):
        raise UitrolError(f"{n}: de watch draait na de herstart niet (geen nieuwe hartslag)")
    check = nacontrole(n, spec, src)
    return {"deploy": {k: out.get(k) for k in ("backup", "revision")}, "restart": rs.get("restarted"), "nacontrole": check}


def nacontrole(n: str, spec: dict, src: dict) -> dict:
    p = run_remote(spec, {"op": "probe", "code": spec["code"], "src": spec.get("src")}, timeout=60)
    if p["manifest"] != src["manifest"]:
        raise UitrolError(f"{n}: code na uitrol wijkt af")
    from . import remote
    from .controller import ControllerError
    remote._down.pop(n, None)
    try:
        s = _status(n)
    except ControllerError as e:
        raise UitrolError(f"{n}: runner2_status na uitrol mislukt: {e.message}") from None
    out = {"revision": p.get("revision"), "watch_healthy": (s.get("watch") or {}).get("healthy")}
    if not out["watch_healthy"]:
        raise UitrolError(f"{n}: watch niet gezond na uitrol: {s.get('watch')}")
    if n == "GERARD":
        try:
            j = remote.call("GERARD", "runner2_jobs", {"action": "list", "by": "uitrol"})
        except ControllerError as e:
            raise UitrolError(f"GERARD: opdrachtwachtrij niet bereikbaar na uitrol: {e.message}") from None
        out["jobs"] = {"schema": j.get("schema"), "versie": j.get("version")}
    return out


def monitor_restart() -> dict:
    r = subprocess.run(["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{MONITOR_LABEL}"], capture_output=True, text=True)
    if r.returncode:
        return {"ok": False, "error": f"launchctl: {r.stderr.strip()[-200:]}"}
    end = time.time() + 40
    last = None
    while time.time() < end:
        try:
            with urllib.request.urlopen(MONITOR_URL + "/healthz", timeout=3) as h:
                health = json.loads(h.read())
            with urllib.request.urlopen(MONITOR_URL + "/api/jobs", timeout=15) as h:
                jobs = json.loads(h.read())
            return {"ok": True, "healthz": {k: v for k, v in health.items() if k != "nodes"}, "jobs_ok": "nodes" in jobs or "error" not in jobs}
        except Exception as e:   # noqa: BLE001
            last = str(e)
            time.sleep(2)
    return {"ok": False, "error": f"Monitor 2 antwoordt niet na de herstart: {last}"}


def logdir() -> Path:
    from . import config
    d = config.r2_home() / "uitrol"
    d.mkdir(parents=True, exist_ok=True)
    return d


def uitrol(nodes: list, yes: bool, monitor: bool = True) -> int:
    src = source()
    c = controle(nodes, src)
    print(render(c))
    if c["problems"]:
        print("\nUITROL GESTOPT: los eerst de punten hierboven op. Er is niets gewijzigd.")
        return 1
    if not yes and input(f"\nRunner 2 {src['rev']} uitrollen naar {', '.join(nodes)}? Typ ja: ").strip().lower() != "ja":
        print("Niets gedaan.")
        return 1
    stamp = _stamp()
    payload = subprocess.run(["git", "-C", str(SRC), "archive", "--format=tar", src["commit"], "runner2"], capture_output=True, check=True).stdout
    sp, log = specs(), {"stamp": stamp, "source": src["commit"], "nodes": {}, "order": []}
    path = logdir() / f"{stamp}.json"
    status = 0
    for n in [x for x in ORDER if x in nodes]:
        print(f"\n== {n} ==", flush=True)
        log["order"].append(n)
        try:
            log["nodes"][n] = {"result": deploy_node(n, sp[n], src, stamp, payload), "ok": True}
            print(f"{n}: uitgerold en gecontroleerd ({log['nodes'][n]['result']['nacontrole']})")
        except UitrolError as e:
            print(f"{n}: FOUT: {e}\n{n}: wordt teruggerold naar de back-up {stamp} ...", flush=True)
            log["nodes"][n] = {"ok": False, "error": str(e)}
            try:
                rb = run_remote(sp[n], {"op": "rollback", "code": sp[n]["code"], "stamp": stamp})
                log["nodes"][n]["rollback"] = rb
                print(f"{n}: code teruggezet naar {rb.get('revision') or 'de vorige versie'}")
                rs = run_remote(sp[n], {"op": "restart", "code": sp[n]["code"], **sp[n]["restart"]}, timeout=120)
                print(f"{n}: watch {'herstart' if rs.get('watch_restarted') else 'NIET herstart: controleer de dienst van de watch'}")
            except UitrolError as e2:
                log["nodes"][n]["rollback_error"] = str(e2)
                print(f"{n}: TERUGROL MISLUKT: {e2}. Handmatig: zie {path}")
            done = [x for x in log["order"] if log["nodes"].get(x, {}).get("ok")]
            if done:
                print(f"Al uitgerold en in orde: {', '.join(done)} (blijven op de nieuwe versie; terugrol met de stempel hieronder).")
            status = 1
            break
        finally:
            path.write_text(json.dumps(log, indent=1))
    if status == 0 and "MAC" in nodes and monitor:
        log["monitor2"] = monitor_restart()
        path.write_text(json.dumps(log, indent=1))
        print(f"\nMonitor 2: {'herstart en bereikbaar' if log['monitor2']['ok'] else 'PROBLEEM: ' + log['monitor2']['error']}")
        status = 0 if log["monitor2"]["ok"] else 1
    print(f"\nLogboek: {path}\nTerugrollen: python3 -m runner2.uitrol terugrol --stempel {stamp}")
    return status


def terugrol(stamp: str, nodes: list, monitor: bool = True) -> int:
    path = logdir() / f"{stamp}.json"
    log = json.loads(path.read_text()) if path.exists() else {"order": nodes}
    sp, status = specs(), 0
    for n in reversed([x for x in log.get("order", nodes) if x in nodes]):
        try:
            rb = run_remote(sp[n], {"op": "rollback", "code": sp[n]["code"], "stamp": stamp})
            rs = run_remote(sp[n], {"op": "restart", "code": sp[n]["code"], **sp[n]["restart"]}, timeout=120)
            print(f"{n}: teruggezet naar {rb.get('revision')}" + (f", runner2-src op {rb['src_ref']}" if rb.get("src_ref") else "")
                  + ("" if rs.get("watch_restarted") else " (LET OP: watch niet herstart)"))
        except UitrolError as e:
            print(f"{n}: terugrol mislukt: {e}")
            status = 1
    if "MAC" in nodes and monitor:
        m = monitor_restart()
        print("Monitor 2: " + ("herstart" if m["ok"] else m["error"]))
    return status


def render(c: dict) -> str:
    lines = [f"Bron: {c['source']}"]
    for n, row in c["report"].items():
        lines.append(f"  {n:8} code: {row.get('code')}, REVISION {row.get('revision')}, {row.get('watch')}, {row.get('actief')}"
                     + (f", runner2-src: {row['src']}" if row.get("src") else ""))
    lines += [f"  PROBLEEM: {p}" for p in c["problems"]] or ["  geen problemen"]
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="runner2.uitrol", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("controle", "uitrol", "terugrol"):
        p = sub.add_parser(name)
        p.add_argument("--node", action="append", choices=ORDER, help="alleen deze Runner(s); standaard alle vier")
        if name == "uitrol":
            p.add_argument("--ja", action="store_true", help="niet om bevestiging vragen")
        if name in ("uitrol", "terugrol"):
            p.add_argument("--zonder-monitor", action="store_true", help="Monitor 2 niet herstarten")
        if name == "terugrol":
            p.add_argument("--stempel", required=True)
    sub.add_parser("lijst")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "lijst":
            for p in sorted(logdir().glob("*.json")):
                d = json.loads(p.read_text())
                print(p.stem, d.get("source", "")[:8], {n: v.get("ok") for n, v in d.get("nodes", {}).items()})
            return 0
        nodes = a.node or list(ORDER)
        if a.cmd == "controle":
            c = controle(nodes, source())
            print(render(c))
            return 1 if c["problems"] else 0
        if a.cmd == "uitrol":
            return uitrol(nodes, a.ja, not a.zonder_monitor)
        return terugrol(a.stempel, nodes, not a.zonder_monitor)
    except UitrolError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
