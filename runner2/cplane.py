"""Control plane van Runner 2 op GERARD: rol/hek, gezondheid, diensten, geheimen en uitrol.

`python3 -m runner2.cplane <opdracht>`:
    monitor                          Monitor-2-momentopname (uit de notifier): ACTIE OPEN / WACHT OP ACTIE / supervisie-labels
    rotate-token [--finish]          nieuw MCP-token met overlap; --finish trekt het oude in
    status [--text] [--public URL]   gezondheid van alles wat de control plane draagt (exit 0 = gezond)
    role | promote | demote          rol van DEZE machine: primary (voert supervisie uit) of standby (niet)
    install-services [--dry-run]     schrijft en activeert de systemd --user-eenheden (overleven reboot via Linger)
    uninstall-services               zet de control-plane-eenheden uit; de watch blijft staan
    init-secrets                     maakt ~/.config/runner2/control.env (0600) als die ontbreekt; toont nooit een geheim
    connector-url                    schrijft de ChatGPT-connector-URL naar ~/.config/runner2/connector-url.txt (0600)
    deploy SRC                       zet SRC/runner2 atomair in ~/runner2-code (vorige versie blijft als .prev)

Het hek: staat er geen ~/.runner2/config/control-plane.json, dan werkt een machine als vroeger (compatibel). Staat er
role=standby, dan voert deze machine GEEN supervisie uit en weigert hij de supervisietools, zodat er nooit twee
control planes tegelijk beslissen. Er is altijd hooguit één primary; zie docs/runner2-control-plane.md.
"""
import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from . import config, osadapter, store

ROLE_FILE = "control-plane.json"
HOME = Path.home()
CONFIG_DIR = HOME / ".config" / "runner2"
ENV_FILE = CONFIG_DIR / "control.env"
UNIT_DIR = HOME / ".config" / "systemd" / "user"
CODE_DIR = HOME / "runner2-code"
SERVICES = ("runner2-watch.service", "runner2-mcp.service", "runner2-notifier.service", "runner2-tunnel.service")
TIMERS = ("runner2-backup.timer",)


# --- rol / hek ---

def role_path() -> Path:
    return config.r2_home() / "config" / ROLE_FILE


def role() -> dict | None:
    try:
        d = json.loads(role_path().read_text())
    except (OSError, ValueError):
        return None
    return d if isinstance(d, dict) else None


def is_primary() -> bool:
    r = role()
    if r is None:
        # Geen rolbestand = oude machine, geldt als primary (compatibel). Een bestand dat er WEL is maar niet te lezen valt: nooit primary
        # (fail-closed; anders zou een half geschreven of beschadigd bestand twee control planes opleveren).
        return not role_path().exists()
    return r.get("role") == "primary" and str(r.get("host", "")).upper() == config.local_node()


def require_primary() -> None:
    if not is_primary():
        from .supervision import SupervisionError
        r = role() or {}
        where = r.get("primary_host")
        # Geen advies om te promoveren: wie hier aanklopt (meestal ChatGPT via de verkeerde verbinding) hoort de primaire te gebruiken.
        here = config.local_node()
        if where and str(where).upper() != here:
            hint = (f"; de opdrachtwachtrij en supervisie draaien alleen op {where}: gebruik de MCP-verbinding van {where}"
                    ". Alleen bij een geplande wissel van control plane: handover import of 'cplane promote'.")
        else:   # rolbestand wijst niet naar een andere machine: een onafgemaakte overdracht
            hint = "; voer eerst de overdracht uit (handover import) of 'cplane promote'."
        raise SupervisionError("not_control_plane", f"deze machine ({here}) is (nog) niet de primaire control plane (rol {r.get('role')}"
                               + (f", primair: {where}" if where else "") + ")" + hint,
                               {"node": here, "role": r.get("role"), "primary": where})


CONTROL_PLANE_TOOLS = ("runner2_jobs", "runner2_discussions", "runner2_supervision_")   # alleen op de primaire control plane (require_primary)


def tools_for_role(tools: list) -> list:
    """tools/list: op een niet-primaire machine staan de control-plane-tools gemarkeerd, zodat een client met meerdere
    verbindingen (MAC standby naast GERARD) de juiste kiest. Zelfde namen en schema's; alleen de beschrijving verschilt."""
    if is_primary():
        return tools
    where = (role() or {}).get("primary_host") or "de primaire control plane"
    mark = f"NIET BESCHIKBAAR OP DEZE VERBINDING ({config.local_node()} is standby): gebruik de MCP-verbinding van {where}. "
    return [dict(t, description=mark + t["description"]) if t["name"].startswith(CONTROL_PLANE_TOOLS) else t for t in tools]


def set_role(new_role: str, epoch_floor: int = 0, **info) -> dict:
    if new_role not in ("primary", "standby"):
        raise ValueError(new_role)
    cur = role() or {}
    hist = (cur.get("history") or [])[-19:] + ([{"at": cur.get("at"), "role": cur.get("role"), "host": cur.get("host")}] if cur else [])
    doc = {"role": new_role, "host": config.local_node(), "at": store.now(), "epoch": max(int(cur.get("epoch", 0)) + 1, int(epoch_floor)),
           "history": hist, **info}
    store.atomic_write(role_path(), json.dumps(doc, indent=1))
    return doc


# --- systemd ---

def _sctl(*args, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(["systemctl", "--user", *args], capture_output=True, text=True, env=osadapter._bus_env(), timeout=60, check=check)


def _unit(desc: str, exec_start: str, env_files: list, extra_env: dict | None = None, after: str = "network-online.target",
          restart: str = "always") -> str:
    env = "".join(f"EnvironmentFile={f}\n" for f in env_files)
    env += "".join(f"Environment={k}={v}\n" for k, v in {"PYTHONPATH": str(CODE_DIR), **(extra_env or {})}.items())
    return (f"[Unit]\nDescription={desc}\nAfter={after}\nWants=network-online.target\n\n[Service]\nType=simple\n{env}"
            f"WorkingDirectory={config.r2_home()}\nExecStart={exec_start}\nRestart={restart}\nRestartSec=5\n"
            f"StandardOutput=append:{config.r2_home()}/logs/{{log}}.out\nStandardError=append:{config.r2_home()}/logs/{{log}}.err\n\n"
            f"[Install]\nWantedBy=default.target\n")


def unit_files() -> dict:
    py = "/usr/bin/python3"
    env = f"-{ENV_FILE}"   # '-' = ontbreken is geen fout (init-secrets maakt hem)
    home = config.r2_home()
    return {
        "runner2-watch.service": (
            f"[Unit]\nDescription=Studio OBF Runner 2 watch (control plane: supervisie-tik, ketens, herstel)\nAfter=network-online.target\n"
            f"Wants=network-online.target\n\n[Service]\nType=simple\nEnvironmentFile={env}\nEnvironment=PYTHONPATH={CODE_DIR}\n"
            f"ExecStart={py} -m runner2.watch\nWorkingDirectory={home}\nRestart=always\nRestartSec=10\n"
            f"StandardOutput=append:{home}/logs/watch.out\nStandardError=append:{home}/logs/watch.err\n\n[Install]\nWantedBy=default.target\n"),
        "runner2-mcp.service": _unit("Runner 2 control plane: controller-MCP over HTTP (127.0.0.1:8766)", f"{py} -m runner2.mcp_http",
                                     [env], after="network-online.target runner2-watch.service").replace("{log}", "mcp-http"),
        "runner2-notifier.service": _unit("Runner 2 control plane: notifier, wake-up en headless Monitor 2", f"{py} -m runner2.notifier",
                                          [env]).replace("{log}", "notifier"),
        "runner2-tunnel.service": _unit("Runner 2 control plane: Cloudflare Tunnel naar de controller-MCP",
                                        f"{HOME}/.local/bin/cloudflared tunnel --no-autoupdate --metrics 127.0.0.1:20242 run",
                                        [f"-{CONFIG_DIR}/cloudflared.env"], after="network-online.target runner2-mcp.service",
                                        restart="on-failure").replace("{log}", "tunnel"),
        "runner2-backup.service": (
            f"[Unit]\nDescription=Runner 2 control plane: lokale back-up (en off-site als ingesteld)\n\n[Service]\nType=oneshot\n"
            f"EnvironmentFile=-{CONFIG_DIR}/backup-r2.env\nEnvironment=PYTHONPATH={CODE_DIR}\nWorkingDirectory={home}\n"
            f"ExecStart={py} -m runner2.cpbackup run\nStandardOutput=append:{home}/logs/backup.out\nStandardError=append:{home}/logs/backup.err\n"),
        "runner2-backup.timer": ("[Unit]\nDescription=Runner 2 control plane: back-up elke 6 uur\n\n[Timer]\nOnCalendar=*-*-* 00/6:17:00\n"
                                 "Persistent=true\nRandomizedDelaySec=120\n\n[Install]\nWantedBy=timers.target\n"),
    }


def install_services(dry_run: bool = False, only: list | None = None) -> list:
    out = []
    (config.r2_home() / "logs").mkdir(parents=True, exist_ok=True)
    UNIT_DIR.mkdir(parents=True, exist_ok=True)
    for name, text in unit_files().items():
        if only and name not in only:
            continue
        path = UNIT_DIR / name
        same = path.exists() and path.read_text() == text
        out.append(f"{name}: {'ongewijzigd' if same else 'nieuw' if not path.exists() else 'bijgewerkt'}")
        if dry_run or same:
            continue
        if path.exists() and name == "runner2-watch.service" and not (UNIT_DIR / (name + ".pre-control-plane")).exists():
            shutil.copy2(path, UNIT_DIR / (name + ".pre-control-plane"))   # terugdraaien: dit bestand terugzetten
        path.write_text(text)
    if not dry_run:
        _sctl("daemon-reload")
    return out


def enable_services(names: list) -> list:
    out = []
    for n in names:
        r = _sctl("enable", "--now", n)
        out.append(f"{n}: {'actief' if r.returncode == 0 else 'mislukt: ' + r.stderr.strip()}")
    return out


def unit_state(name: str) -> dict:
    r = _sctl("show", name, "-p", "ActiveState", "-p", "SubState", "-p", "UnitFileState", "-p", "NRestarts", "-p", "ActiveEnterTimestamp")
    info = dict(l.partition("=")[::2] for l in r.stdout.splitlines())
    return {"active": info.get("ActiveState") == "active", "state": f"{info.get('ActiveState')}/{info.get('SubState')}",
            "enabled": info.get("UnitFileState") in ("enabled", "enabled-runtime", "static"), "restarts": info.get("NRestarts"),
            "since": info.get("ActiveEnterTimestamp")}


# --- geheimen ---

def _read_env(path: Path) -> dict:
    out = {}
    try:
        for line in path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, _, v = line.partition("=")
                out[k.strip()] = v.strip()
    except OSError:
        pass
    return out


def _write_env(cur: dict) -> None:
    tmp = ENV_FILE.with_name(ENV_FILE.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write("# Runner 2 control plane (mode 0600; nooit in git of in een back-up). Rotatie: zie docs/runner2-control-plane.md\n")
        f.write("".join(f"{k}={v}\n" for k, v in cur.items()))
    os.replace(tmp, ENV_FILE)


def rotate_token(finish: bool = False) -> str:
    """Nieuw MCP-token; het oude blijft tot `--finish` geldig (R2_MCP_PREVIOUS_TOKEN), zodat ChatGPT nog niet hoeft om te zetten."""
    cur = _read_env(ENV_FILE)
    if finish:
        cur.pop("R2_MCP_PREVIOUS_TOKEN", None)
        _write_env(cur)
        return "oud token ingetrokken (herstart runner2-mcp)"
    if cur.get("R2_MCP_TOKEN"):
        cur["R2_MCP_PREVIOUS_TOKEN"] = cur["R2_MCP_TOKEN"]
    cur["R2_MCP_TOKEN"] = secrets.token_hex(32)
    _write_env(cur)
    return "nieuw token actief na herstart van runner2-mcp; het oude werkt nog tot 'rotate-token --finish'; haal de nieuwe URL op met connector-url"


def init_secrets() -> list:
    """Maakt control.env (0600) met MCP-token en webhook-geheim als die er nog niet zijn; bestaande waarden blijven."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    cur = _read_env(ENV_FILE)
    made = []
    for key in ("R2_MCP_TOKEN", "R2_WEBHOOK_SECRET", "R2_NOTIFY_HMAC"):
        if len(cur.get(key, "")) < 32:
            cur[key] = secrets.token_hex(32)
            made.append(key)
    if made:
        _write_env(cur)
    return made


def write_connector_url(base: str) -> Path:
    tok = _read_env(ENV_FILE).get("R2_MCP_TOKEN", "")
    if len(tok) < 32:
        raise SystemExit("geen R2_MCP_TOKEN: eerst init-secrets")
    p = CONFIG_DIR / "connector-url.txt"
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{base.rstrip('/')}/s/{tok}/mcp\n")
    return p


# --- uitrol ---

def deploy(src: Path) -> str:
    src = Path(src).resolve()
    if not (src / "runner2" / "__init__.py").exists():
        raise SystemExit(f"{src}/runner2 niet gevonden")
    rev = subprocess.run(["git", "-C", str(src), "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip() or "unknown"
    new, prev = CODE_DIR.with_name(CODE_DIR.name + ".new"), CODE_DIR.with_name(CODE_DIR.name + ".prev")
    shutil.rmtree(new, ignore_errors=True)
    shutil.copytree(src / "runner2", new / "runner2", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    (new / "REVISION").write_text(rev + "\n")
    shutil.rmtree(prev, ignore_errors=True)
    if CODE_DIR.exists():
        os.rename(CODE_DIR, prev)
    os.rename(new, CODE_DIR)
    return rev


# --- status ---

def _http_ok(url: str, timeout: float = 3) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "runner2-cplane/1"})   # Cloudflare weigert de standaard Python-agent
        with urllib.request.urlopen(req, timeout=timeout) as r:   # noqa: S310 - alleen eigen localhost/ingestelde URL
            return r.status == 200
    except OSError:
        return False


def _age(path: Path) -> float | None:
    try:
        return round(time.time() - path.stat().st_mtime)
    except OSError:
        return None


def _newest(d: Path, pattern: str) -> Path | None:
    files = sorted(d.glob(pattern)) if d.exists() else []
    return files[-1] if files else None


def status(public: str | None = None) -> dict:
    from . import controller, remote, supervision
    cfg = config.load_config()
    home = config.r2_home()
    r = role()
    out = {"node": config.local_node(), "default_node": config.default_node(), "role": (r or {}).get("role", "legacy"),
           "primary": is_primary(), "checks": {}}
    ck = out["checks"]
    w = controller._watch()
    ck["watch"] = {"ok": w["healthy"], **w}
    ck["supervision"] = {"ok": bool(cfg.get("supervision_enabled", True)), "enabled": cfg.get("supervision_enabled", True),
                         "projects": len(supervision.dossier.project_ids()), "beacon_age_s": _age(home / "supervision" / "attention.json")}
    ck["services"] = {}
    for n in SERVICES + TIMERS:
        st = unit_state(n)
        ck["services"][n] = st
    ck["services"]["ok"] = all(v["active"] and v["enabled"] for k, v in ck["services"].items() if k != "ok" and (k != "runner2-tunnel.service"
                               or (CONFIG_DIR / "cloudflared.env").exists()))
    mcp_port = int(os.environ.get("R2_MCP_PORT", 8766))
    ck["mcp_http"] = {"ok": _http_ok(f"http://127.0.0.1:{mcp_port}/readyz"), "port": mcp_port}
    if public:
        ck["public"] = {"ok": _http_ok(public.rstrip("/") + "/readyz", 8), "url": public.rstrip("/") + "/readyz"}
    hb = _age(home / "control" / "notifier" / "heartbeat.json")
    ck["notifier"] = {"ok": hb is not None and hb < 120, "heartbeat_age_s": hb}
    ck["nodes"] = {"configured": sorted(remote.nodes()), "ok": True, "note": "alleen deze machine; geen remote nodes ingesteld" if not remote.nodes() else ""}
    last = _newest(home / "control" / "backups", "r2control-*.tar.gz")
    age = _age(last) if last else None
    ck["backup_local"] = {"ok": age is not None and age < 8 * 3600, "latest": last.name if last else None, "age_s": age}
    off = _read_json(home / "control" / "backups" / "offsite.json")
    ck["backup_offsite"] = {"ok": bool(off.get("ok")) and time.time() - off.get("epoch", 0) < 36 * 3600 if off else False,
                            "configured": (CONFIG_DIR / "backup-r2.env").exists(), "last": off or None}
    if (UNIT_DIR / "monitor2.service").exists():   # Monitor 2 (dashboard) draait als eigen service; zichtbaar, maar niet vereist voor 'ok'
        m2 = unit_state("monitor2.service")
        port = int(os.environ.get("OBF_MONITOR2_PORT", 8772))
        ck["monitor2"] = {"ok": bool(m2["active"] and m2["enabled"] and _http_ok(f"http://127.0.0.1:{port}/healthz")), "port": port,
                          "state": m2["state"], "restarts": m2["restarts"]}
    rev = (CODE_DIR / "REVISION").read_text().strip() if (CODE_DIR / "REVISION").exists() else None
    ck["code"] = {"ok": True, "revision": rev, "path": str(CODE_DIR)}
    # Alleen wat de control plane per se nodig heeft telt voor 'ok'; offsite en publieke URL zijn gescheiden, zichtbare onderdelen.
    required = ("watch", "services", "mcp_http", "notifier", "backup_local")
    out["ok"] = all(ck[k]["ok"] for k in required)   # gezondheid; of deze machine ook primair is staat apart in 'primary'/'role'
    out["degraded"] = [k for k, v in ck.items() if not v.get("ok")]
    return out


def _read_json(p: Path) -> dict:
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return {}


def render(s: dict) -> str:
    lines = [f"control plane {s['node']}  rol={s['role']}{'' if s['primary'] else ' (supervisie nog niet hier actief)'}  default-node={s['default_node']}  "
             f"{'GEZOND' if s['ok'] else 'AANDACHT'}"]
    for k, v in s["checks"].items():
        if k == "services":
            lines.append(f"  [{'ok' if v['ok'] else '!!'}] services: " + ", ".join(f"{n.replace('.service', '')}={'up' if x['active'] else 'DOWN'}" for n, x in v.items() if n != "ok"))
        else:
            detail = {a: b for a, b in v.items() if a not in ("ok", "last")}
            lines.append(f"  [{'ok' if v['ok'] else '!!'}] {k}: {json.dumps(detail, ensure_ascii=False)}")
    return "\n".join(lines)


def _tailscale_peers() -> dict:
    """Peers in het tailnet: korte naam -> {ip, online}. Leeg als tailscale niet antwoordt."""
    try:
        r = subprocess.run(["tailscale", "status", "--json"], capture_output=True, text=True, timeout=5)
        peers = json.loads(r.stdout).get("Peer") or {}
    except (OSError, subprocess.SubprocessError, ValueError):
        return {}
    return {(p.get("DNSName") or "").split(".")[0]: {"ip": (p.get("TailscaleIPs") or [None])[0], "online": bool(p.get("Online"))}
            for p in peers.values() if p.get("DNSName")}


def nodes_probe(names=("MAC", "LINUX", "WINDOWS")) -> list:
    """Per node wat GERARD echt kan: tailnetpeer, ssh-poort, en een onschadelijke Runner-2-lezing (runner2_status). Geen schrijfacties."""
    import socket
    from . import controller, remote
    peers, cfg, out = _tailscale_peers(), remote.nodes(), []
    for name in names:
        spec = cfg.get(name)
        row = {"node": name, "configured": bool(spec), "transport": None, "tailscale": None, "ssh_port": None, "runner2": None, "reason": ""}
        if not spec:
            row["reason"] = "geen route in nodes.toml (machine nog niet aan het tailnet gekoppeld of nog niet voorbereid)"
            out.append(row)
            continue
        peer = peers.get(str(spec.get("tailscale_name") or "").lower())
        row["transport"] = "tailscale+ssh" if spec.get("tailscale_name") else "ssh"
        row["tailscale"] = {"name": spec.get("tailscale_name"), "ip": spec["host"], "online": peer["online"] if peer else None}
        try:
            socket.create_connection((spec["host"], int(spec.get("port", 22))), 4).close()
            row["ssh_port"] = True
        except OSError as e:
            row["ssh_port"] = False
            row["reason"] = f"ssh-poort niet bereikbaar: {e}"
        if row["ssh_port"]:
            try:
                res = remote.call(name, "runner2_status", {})
                row["runner2"] = {"ok": True, "revision": res.get("revision"), "node": res.get("node")}
            except controller.ControllerError as e:
                row["runner2"] = {"ok": False, "error": e.code}
                row["reason"] = e.message[:200]
        out.append(row)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="runner2.cplane")
    sub = ap.add_subparsers(dest="cmd", required=True)
    st = sub.add_parser("status")
    st.add_argument("--text", action="store_true")
    st.add_argument("--public")
    sub.add_parser("role")
    pr = sub.add_parser("promote")
    pr.add_argument("--from-host", default="")
    dm = sub.add_parser("demote")
    dm.add_argument("--primary-host", default="", help="welke machine nu de primaire control plane is (anders blijft het veld leeg)")
    isv = sub.add_parser("install-services")
    isv.add_argument("--dry-run", action="store_true")
    isv.add_argument("--enable", action="store_true")
    sub.add_parser("uninstall-services")
    sub.add_parser("init-secrets")
    rt = sub.add_parser("rotate-token")
    rt.add_argument("--finish", action="store_true")
    sub.add_parser("monitor")
    cu = sub.add_parser("connector-url")
    cu.add_argument("base", help="publieke basis-URL van de tunnel, bijvoorbeeld https://r2.example.com")
    nd = sub.add_parser("nodes", help="onschadelijke probe van MAC/LINUX/WINDOWS vanaf deze machine (tailnet, ssh, runner2_status)")
    nd.add_argument("--json", action="store_true")
    dp = sub.add_parser("deploy")
    dp.add_argument("src")
    a = ap.parse_args(argv)
    if a.cmd == "status":
        s = status(a.public)
        print(render(s) if a.text else json.dumps(s, indent=1, ensure_ascii=False))
        return 0 if s["ok"] else 1
    if a.cmd == "role":
        print(json.dumps(role() or ({"role": "ONLEESBAAR (behandeld als niet-primary)"} if role_path().exists() else {"role": "legacy"}), indent=1))
    elif a.cmd == "promote":
        print(json.dumps(set_role("primary", primary_host=config.local_node(), promoted_from=a.from_host or None), indent=1))
    elif a.cmd == "demote":
        print(json.dumps(set_role("standby", primary_host=a.primary_host or None), indent=1))
    elif a.cmd == "install-services":
        print("\n".join(install_services(a.dry_run)))
        if a.enable and not a.dry_run:
            print("\n".join(enable_services([n for n in unit_files() if n.endswith(".timer") or n in SERVICES and n != "runner2-tunnel.service"])))
    elif a.cmd == "uninstall-services":
        for n in SERVICES[1:] + TIMERS:
            _sctl("disable", "--now", n)
            (UNIT_DIR / n).unlink(missing_ok=True)
        (UNIT_DIR / "runner2-backup.service").unlink(missing_ok=True)
        _sctl("daemon-reload")
        print("control-plane-eenheden verwijderd; runner2-watch blijft actief (zet pre-control-plane-eenheid terug indien gewenst)")
    elif a.cmd == "init-secrets":
        made = init_secrets()
        print("aangemaakt: " + ", ".join(made) if made else "alle geheimen bestonden al (niets gewijzigd)")
    elif a.cmd == "rotate-token":
        print(rotate_token(a.finish))
    elif a.cmd == "monitor":
        snap = _read_json(config.r2_home() / "control" / "monitor.json")
        print(f"[{snap.get('at', '?')}] {snap.get('headline', 'geen momentopname (draait runner2-notifier?)')}")
        for k, v in (snap.get("nodes") or {}).items():
            print(f"  node {k}: {'bereikbaar' if v.get('reachable') else 'ONBEREIKBAAR ' + str(v.get('error', ''))}")
        print(f"  supervisie: {snap.get('supervision')}  wacht op actie: {snap.get('waiting_for_action')}  loopt: {snap.get('running')}")
    elif a.cmd == "connector-url":
        print(f"geschreven naar {write_connector_url(a.base)} (0600)")
    elif a.cmd == "nodes":
        rows = nodes_probe()
        if a.json:
            print(json.dumps(rows, indent=1, ensure_ascii=False))
        for r in rows if not a.json else []:
            ts = r["tailscale"] or {}
            r2 = r["runner2"]
            print(f"{r['node']:8} transport={r['transport'] or '-':14} tailnet={ts.get('name') or '-'} {ts.get('ip') or ''} online={ts.get('online')} "
                  f"ssh22={r['ssh_port']} runner2={'OK' if r2 and r2['ok'] else ('GEBLOKKEERD' if r2 else '-')}"
                  + (f"  -- {r['reason']}" if r["reason"] else ""))
        return 0 if all(r["runner2"] and r["runner2"]["ok"] for r in rows if r["configured"]) else 1
    elif a.cmd == "deploy":
        print("gedeployd:", deploy(Path(a.src)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
