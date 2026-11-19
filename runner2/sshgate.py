"""Least-privilege ssh-poort voor de NUC als Runner-2-master: een sleutel die ALLEEN de drie dingen mag die een master van een node nodig heeft.

Dit bestand gebruikt ALLEEN de standaardbibliotheek en importeert niets uit runner2; het wordt los op de node gezet (bijv.
~/.config/runner2/ssh-gate.py) en staat in `authorized_keys` als gedwongen opdracht (command=), zodat de sleutel nooit een shell geeft:

    restrict,from="192.0.2.22",command="/usr/bin/python3 /home/operator/.config/runner2/ssh-gate.py --code /home/operator/runner2-code" ssh-ed25519 AAAA… runner2-control-nuc

`restrict` zet pty, agent-/poort-/X11-doorsturing en ~/.ssh/rc uit; `from=` beperkt tot het tailnetadres van de NUC. Toegestaan is
uitsluitend wat de control plane via het node-transport doet, en alles wordt exact (niet met een shell) gematcht:

    rpc            env PYTHONPATH=<code> <python> -m runner2.rpc          (Runner-2-tools op deze node; zelfde als remote.py)
    inbox-put      mkdir -p .runner2/inbox && cat > …/.NAAM.part && mv …  (alleen handover-<host>-<tijd>.tar.gz[.sha256])
    inbox-check    cd .runner2/inbox && sha256sum -c NAAM.sha256          (alleen dat bestand)
    mac-status     cat ~/.config/monitor2/mac-integrator-status.json      (alleen-lezen relay voor Monitor 2; uit te zetten met --no-mac-status)
    role-read      cat ~/.runner2/config/control-plane.json               (alleen-lezen rol van deze machine; nodig voor de stale-master-bewaking van de peer)

Al het andere (andere commando's, shells, scp/sftp, poortdoorsturing) wordt geweigerd en met de eerste 120 tekens gelogd in
~/.runner2/logs/ssh-gate.log (geen geheimen: de invoer van stdin wordt nooit gelogd). Code-uitrol naar deze node via de gate is BEWUST niet toegestaan.

    python3 ssh-gate.py render   --pub PUBFILE --from IP[,IP…] --gate PAD --code DIR [--python PY]   toont de authorized_keys-regel
    python3 ssh-gate.py install  --pub PUBFILE --from IP[,IP…] --gate PAD --code DIR [--authorized FILE] [--dry-run]
                                 voegt de regel toe (back-up, atomair, idempotent, rechten 0600); verwijdert of wijzigt niets anders
"""
import argparse
import hashlib
import os
import re
import shlex
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

NAME_RE = re.compile(r"handover-[A-Za-z0-9_.-]{1,64}-\d{8}T\d{6}Z\.tar\.gz(\.sha256)?")
REMOTE_DIR = ".runner2/inbox"
MAC_STATUS = ".config/monitor2/mac-integrator-status.json"
ROLE_FILE = ".runner2/config/control-plane.json"
ALLOWED_FROM = re.compile(r"[0-9a-fA-F:.]{3,45}")
KEY_RE = re.compile(r"^(ssh-ed25519|ecdsa-sha2-nistp\d+|ssh-rsa) [A-Za-z0-9+/=]{20,} ?[\w@.:-]*$")


def classify(cmd: str, code: str, python: str = "/usr/bin/python3", allow_mac_status: bool = True) -> tuple | None:
    """(actie, argument) voor een toegestaan commando, anders None. Geen shell, geen interpretatie: exacte vergelijking en regex."""
    cmd = (cmd or "").strip()
    if cmd == f"env PYTHONPATH={code} {python} -m runner2.rpc" or cmd == f"env PYTHONPATH={code} python3 -m runner2.rpc":
        return ("rpc", None)
    m = re.fullmatch(rf"mkdir -p {re.escape(REMOTE_DIR)} && cat > {re.escape(REMOTE_DIR)}/\.(?P<n>[^/ ;&|$`<>()\"']+)\.part && "
                     rf"mv {re.escape(REMOTE_DIR)}/\.(?P=n)\.part {re.escape(REMOTE_DIR)}/(?P=n)", cmd)
    if m and NAME_RE.fullmatch(m["n"]):
        return ("inbox-put", m["n"])
    m = re.fullmatch(rf"cd {re.escape(REMOTE_DIR)} && sha256sum -c (?P<n>[^/ ;&|$`<>()\"']+\.sha256)", cmd)
    if m and NAME_RE.fullmatch(m["n"]):
        return ("inbox-check", m["n"])
    if allow_mac_status and cmd in (f"cat ~/{MAC_STATUS}", f"cat $HOME/{MAC_STATUS}"):
        return ("mac-status", None)
    if cmd in (f"cat ~/{ROLE_FILE}", f"cat $HOME/{ROLE_FILE}"):
        return ("role-read", None)
    return None


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def inbox_put(name: str, home: Path, stdin, limit: int = 2 << 30) -> int:
    """Schrijf stdin naar inbox/.NAAM.part en hernoem atomair; nooit een bestaand bestand stil overschrijven met een kleiner/leeg bestand."""
    d = home / REMOTE_DIR
    d.mkdir(parents=True, exist_ok=True, mode=0o700)
    part = d / f".{name}.part"
    n = 0
    with open(part, "wb") as f:
        os.chmod(part, 0o600)
        for chunk in iter(lambda: stdin.read(1 << 20), b""):
            n += len(chunk)
            if n > limit:
                part.unlink(missing_ok=True)
                return 1
            f.write(chunk)
    if n == 0:
        part.unlink(missing_ok=True)
        return 1
    os.replace(part, d / name)
    return 0


def inbox_check(name: str, home: Path, out) -> int:
    d = home / REMOTE_DIR
    try:
        line = (d / name).read_text().split()
        target = d / name[: -len(".sha256")]
        if len(line) < 2 or Path(line[1]).name != target.name:
            out.write(f"{name}: ONGELDIG\n")
            return 1
        ok = _sha(target) == line[0]
    except OSError as e:
        out.write(f"{name}: {type(e).__name__}\n")
        return 1
    out.write(f"{target.name}: {'OK' if ok else 'FAILED'}\n")
    return 0 if ok else 1


def log_denied(home: Path, cmd: str) -> None:
    try:
        p = home / "logs" / "ssh-gate.log"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a") as f:
            f.write(f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} GEWEIGERD: {cmd[:120]!r}\n")
    except OSError:
        pass


def run(argv=None, environ=None, stdin=None, stdout=None, exec_fn=os.execve) -> int:
    ap = argparse.ArgumentParser(prog="ssh-gate")
    ap.add_argument("--code", required=True, help="pad van runner2-code op deze node")
    ap.add_argument("--python", default="/usr/bin/python3")
    ap.add_argument("--home", default=None, help="Runner-2-home (standaard ~/.runner2)")
    ap.add_argument("--no-mac-status", action="store_true")
    a = ap.parse_args(argv)
    env = environ if environ is not None else os.environ
    home = Path(a.home or env.get("R2_HOME") or Path.home() / ".runner2").expanduser()
    cmd = env.get("SSH_ORIGINAL_COMMAND", "")
    act = classify(cmd, a.code, a.python, not a.no_mac_status)
    if not act:
        log_denied(home, cmd)
        (stdout or sys.stderr).write("ssh-gate: dit commando is niet toegestaan voor deze sleutel\n")
        return 126
    kind, arg = act
    stdin = stdin or sys.stdin.buffer
    if kind == "rpc":
        e = dict(env, PYTHONPATH=a.code)
        exec_fn(a.python, [a.python, "-m", "runner2.rpc"], e)   # vervangt dit proces; stdin/stdout lopen direct door
        return 0
    if kind == "inbox-put":
        return inbox_put(arg, Path.home(), stdin)   # .runner2/inbox is relatief aan de loginmap, zoals handover.push het aanroept
    if kind == "inbox-check":
        return inbox_check(arg, Path.home(), stdout or sys.stdout)
    if kind == "role-read":
        try:
            (stdout or sys.stdout).write((Path.home() / ROLE_FILE).read_text())
            return 0
        except FileNotFoundError:
            (stdout or sys.stderr).write("cat: control-plane.json: No such file or directory\n")   # peer zonder rolbestand = legacy primary
            return 1
        except OSError:
            return 1
    if kind == "mac-status":
        p = (Path.home() / MAC_STATUS)
        try:
            (stdout or sys.stdout).write(p.read_text())
            return 0
        except OSError:
            return 1
    return 126


# --- authorized_keys-regel ---

def render_line(pub: str, from_ips: list, gate: str, code: str, python: str = "/usr/bin/python3", extra: str = "") -> str:
    pub = " ".join(pub.split())
    if not KEY_RE.match(pub):
        raise ValueError("geen geldige openbare sleutel")
    if not from_ips or not all(ALLOWED_FROM.fullmatch(i) for i in from_ips):
        raise ValueError("--from moet één of meer IP-adressen zijn (geen jokertekens)")
    for v in (gate, code, python):
        if any(c in v for c in '"\n\r') or not v.startswith("/"):
            raise ValueError(f"onveilig of niet-absoluut pad: {v!r}")
    cmd = f"{python} {gate} --code {code} --python {python}{(' ' + extra) if extra else ''}"
    return f'restrict,from="{",".join(from_ips)}",command="{cmd}" {pub}'


def install_line(line: str, authorized: Path, dry_run: bool = False) -> dict:
    """Voeg de regel toe aan authorized_keys: back-up, atomair, idempotent, rechten 0600. Wijzigt nooit een bestaande regel."""
    if not line.startswith("restrict,") or "command=" not in line or "from=" not in line:
        raise ValueError("regel mist restrict/from/command; geweigerd")
    m = re.search(r"(?:ssh-ed25519|ecdsa-sha2-nistp\d+|ssh-rsa) ([A-Za-z0-9+/=]{20,})", line)
    if not m:
        raise ValueError("geen openbare sleutel in de regel")
    blob = m.group(1)
    existing = authorized.read_text() if authorized.exists() else ""
    if any(blob in l for l in existing.splitlines()):
        return {"changed": False, "reason": "sleutel stond al in authorized_keys"}
    if dry_run:
        return {"changed": False, "dry_run": True, "would_add": True}
    backup = authorized.with_name(authorized.name + f".bak-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-voor-nuc")
    had = authorized.exists()
    if had:
        shutil.copy2(authorized, backup)
        os.chmod(backup, 0o600)
    tmp = authorized.with_name(authorized.name + f".tmp{os.getpid()}")
    tmp.write_text(existing + ("" if existing.endswith("\n") or not existing else "\n") + line + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, authorized)
    return {"changed": True, "backup": str(backup) if had else None, "lines": len(authorized.read_text().splitlines())}


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("render", "install"):
        ap = argparse.ArgumentParser(prog="ssh-gate " + argv[0])
        ap.add_argument("--pub", required=True)
        ap.add_argument("--from", dest="from_ips", required=True)
        ap.add_argument("--gate", required=True)
        ap.add_argument("--code", required=True)
        ap.add_argument("--python", default="/usr/bin/python3")
        ap.add_argument("--authorized", default="~/.ssh/authorized_keys")
        ap.add_argument("--dry-run", action="store_true")
        a = ap.parse_args(argv[1:])
        line = render_line(Path(a.pub).expanduser().read_text(), a.from_ips.split(","), a.gate, a.code, a.python)
        if argv[0] == "render":
            print(line)
            return 0
        import json
        print(json.dumps(install_line(line, Path(a.authorized).expanduser(), a.dry_run)))
        return 0
    return run(argv)


if __name__ == "__main__":
    sys.exit(main())
