"""Node-routing vanaf de controller: taken op een andere machine lopen via één ssh-aanroep van `runner2.rpc`.

Nodes staan in ~/.runner2/config/nodes.toml. Host, gebruiker, sleutel en poort komen uit de bestaande
Studio OBF-nodesconfig (nodes.json) als `nodes_json` is gezet; anders uit het bestand zelf.

    [LINUX]
    nodes_json = "ubuntu"
    code = "/home/operator/runner2-code"
    python = "/usr/bin/python3"

    [GERARD]
    nodes_json = "gerard"
    code = "/home/operator/runner2-code"
    python = "/usr/bin/python3"

Op GERARD zelf staat `node = "GERARD"` in machine.toml. De bestaande Linux-adapter verzorgt daar
de systemd-userdienst en de afzonderlijke transient cgroup per taakbeurt.

    [WINDOWS]
    host = "192.0.2.29"
    user = "operator"
    identity_file = "~/.ssh/obf_node_ed25519"
    os = "windows"                      # de standaardshell van sshd is daar cmd.exe, geen POSIX-shell
    code = 'C:\\Users\\operator\\runner2-code'
    python = 'C:\\Users\\operator\\AppData\\Local\\Programs\\Python\\Python314\\python.exe'

Tailscale: met `tailscale_name = "worker-mac"` (en `host` als vast tailnetadres of terugval) lost Runner 2 de
host op via `tailscale ip -4 <naam>`. Dat werkt zonder MagicDNS in het systeem (GERARD draait met --accept-dns=false)
en volgt het adres als het tailnet het wijzigt. Lukt de opzoeking niet, dan geldt `host`; Tailscale SSH wordt niet
gebruikt, het is gewoon ssh met de bestaande sleutel.

Een Windows-node krijgt een cmd.exe-opdracht (`set "PYTHONPATH=..." && "python" -m runner2.rpc`); de state
staat daar onder %USERPROFILE%\\.runner2 (worktrees in ...\\.runner2\\worktrees), tenzij R2_HOME anders zegt.
"""
import json
import os
import subprocess
import time
import tomllib
from pathlib import Path

from . import config
from .controller import ControllerError

NODES_JSON = Path("~/obf-runner-bridge/studio-obf-nodes/nodes.json").expanduser()
CONNECT_TIMEOUT_S = 5
CALL_TIMEOUT_S = 60
DOWN_BACKOFF_S = 8.0
_down: dict = {}   # node -> (tot wanneer we niet opnieuw proberen, laatste melding)


WINDOWS_UNSAFE = frozenset('"&|<>^%\r\n\0')   # binnen aanhalingstekens in cmd.exe niet veilig om letterlijk door te geven


def is_windows(spec: dict) -> bool:
    return str(spec.get("os", "")).lower() == "windows"


TAILSCALE_LOOKUP_TIMEOUT_S = 3
_ts_cache: dict = {}   # tailscale-naam -> (geldig tot, adres of None)


def tailscale_address(name: str) -> str | None:
    """Het tailnetadres (IPv4) van een peer via de lokale tailscale-client; None als dat niet lukt. 60 s gecached."""
    now = time.time()
    hit = _ts_cache.get(name)
    if hit and now < hit[0]:
        return hit[1]
    addr = None
    try:
        r = subprocess.run(["tailscale", "ip", "-4", name], capture_output=True, text=True, timeout=TAILSCALE_LOOKUP_TIMEOUT_S)
        line = (r.stdout.strip().splitlines() or [""])[0].strip()
        if r.returncode == 0 and line.count(".") == 3 and all(x.isdigit() for x in line.split(".")):
            addr = line
    except (OSError, subprocess.SubprocessError):
        pass
    _ts_cache[name] = (now + (60 if addr else 10), addr)
    return addr


def nodes() -> dict:
    """Geconfigureerde remote nodes: NAAM -> ssh-specificatie. Leeg als er geen nodes.toml is."""
    p = config.r2_home() / "config" / "nodes.toml"
    if not p.exists():
        return {}
    out = {}
    for name, spec in tomllib.loads(p.read_text()).items():
        spec = dict(spec)
        if spec.get("nodes_json"):
            base = json.loads(Path(os.environ.get("OBF_NODES_JSON") or NODES_JSON).read_text())["nodes"][spec["nodes_json"]]
            spec = {"host": base["host"], "user": base["user"], "identity_file": base.get("identity_file"),
                    "port": base.get("port", 22), **spec}
        if name.upper() == "WINDOWS":
            spec.setdefault("os", "windows")
        if spec.get("tailscale_name"):
            spec["host"] = tailscale_address(str(spec["tailscale_name"])) or spec.get("host")
        out[name.upper()] = spec
    return out


def _ssh_cmd(spec: dict) -> list:
    home = config.r2_home()
    pinned = bool(spec.get("known_hosts_file"))   # vastgepinde hostsleutel (bijv. de NUC als master): strikt, geen vertrouwen-bij-eerste-gebruik
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={CONNECT_TIMEOUT_S}", "-o", f"StrictHostKeyChecking={'yes'}",
           "-o", "ControlMaster=auto", "-o", f"ControlPath={home}/cm-%C", "-o", "ControlPersist=120",
           "-p", str(spec.get("port", 22))]
    if spec.get("identity_file"):
        cmd += ["-i", str(Path(spec["identity_file"]).expanduser()), "-o", "IdentitiesOnly=yes"]
    if pinned:
        cmd += ["-o", f"UserKnownHostsFile={Path(spec['known_hosts_file']).expanduser()}"]
    remote = windows_rpc_command(spec) if is_windows(spec) else \
        f"env PYTHONPATH={spec['code']} {spec.get('python', 'python3')} -m runner2.rpc"
    return cmd + [f"{spec['user']}@{spec['host']}", remote]


def windows_rpc_command(spec: dict) -> str:
    """De opdracht voor cmd.exe (standaardshell van OpenSSH op Windows): geen `env`, geen POSIX-quoting.

    Padwaarden komen uit nodes.toml maar gaan als tekst door cmd.exe; tekens die daar iets doen weigeren we hard.
    """
    code, python = str(spec["code"]), str(spec.get("python") or "python")
    for label, value in (("code", code), ("python", python)):
        if not value.strip() or WINDOWS_UNSAFE & set(value):
            raise ControllerError("unsupported_node", f"node-instelling {label}={value!r} bevat tekens die cmd.exe niet veilig doorgeeft")
    return f'set "PYTHONPATH={code}" && set "PYTHONUTF8=1" && "{python}" -m runner2.rpc'


def call(node: str, tool: str, args: dict | None = None) -> dict:
    """Voert `tool` uit op de remote node; geeft het resultaat of gooit dezelfde ControllerError als lokaal."""
    spec = nodes().get(node.upper())
    if not spec:
        raise ControllerError("unsupported_node", f"node {node!r} is niet geconfigureerd; er is geen route ernaartoe.")
    until, last = _down.get(node.upper(), (0, ""))
    if time.time() < until:
        raise ControllerError("node_unreachable", last)
    try:
        r = subprocess.run(_ssh_cmd(spec), input=json.dumps({"tool": tool, "args": args or {}}), capture_output=True,
                           text=True, timeout=CALL_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError) as e:
        return _unreachable(node, f"{type(e).__name__}: {e}")
    if r.returncode == 255:
        return _unreachable(node, (r.stderr.strip().splitlines() or ["ssh mislukt"])[-1][:200])
    try:
        reply = json.loads(r.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        raise ControllerError("runner2_unavailable", f"onbegrijpelijk antwoord van node {node}: "
                              f"{(r.stderr or r.stdout).strip()[-200:]}") from None
    if not reply.get("ok"):
        raise ControllerError(reply.get("error") or "runner2_unavailable", reply.get("message") or "", reply.get("data"))
    _down.pop(node.upper(), None)
    return reply["result"]


def _unreachable(node: str, why: str):
    msg = f"node {node} niet bereikbaar: {why}"
    _down[node.upper()] = (time.time() + DOWN_BACKOFF_S, msg)
    raise ControllerError("node_unreachable", msg)


def locate(tid) -> str | None:
    """Op welke node staat deze taak? None = lokaal. Taak-id's zijn uuid-achtig, dus uniek over nodes."""
    from . import store
    if not isinstance(tid, str) or (store.task_dir(tid) / "state.json").exists():
        return None
    err = None
    for name in nodes():
        try:
            call(name, "runner2_status", {"task_id": tid})
            return name
        except ControllerError as e:
            if e.code != "not_found":
                err = e
    if err:
        raise err
    return None


def overview() -> dict:
    """Compacte toestand van elke remote node voor het algemene statusoverzicht; nooit een fout naar buiten."""
    out = {}
    for name in nodes():
        try:
            s = call(name, "runner2_status")
            out[name] = {"reachable": True, "active_task": s.get("active_task"), "watch": s.get("watch"),
                         "count": s.get("count")}
        except ControllerError as e:
            out[name] = {"reachable": False, "error": e.message}
    return out
