"""Handmatige noodovername van de Runner-2-control-plane: NUC is normaal master, GERARD is de reserve. Er is GEEN automatische failover,
GEEN quorum en GEEN automatische promotie: alleen een mens (Beheerder) kan de reserve promoveren, en alleen met een expliciete bevestiging.

Dit is een hulpmiddel dat nergens vanzelf draait. Niets hiervan is geïnstalleerd; de systemd-sjablonen (`units`) worden alleen getoond.

Op de MASTER (NUC, primary):
    snapshot [--out DIR]          passieve momentopname van ALLE toestand (wachtrij incl. held/order/requests, supervisie, ketens, supervisors,
                                  notifier-ledger/outbox, actiebeheer, tekst-backups, taakstaat + open acties, Monitor-2-toestand) ZONDER hek.
    sync [--to GERARD]            snapshot + push naar de reserve (ssh-gate, alleen de inbox) + sha256-controle; schrijft control/replica-sync.json.
Op de RESERVE (GERARD, standby):
    ingest                        neemt replica-bundels uit de inbox over in ~/.runner2-replica (een map die geen watch scant), met controles.
    status                        stand van de replica (versheid, integriteit, epoch) en van de peer; exit 0 gezond, 1 aandacht, 2 niet bruikbaar.
    promote [--apply ...]         de NOODOVERNAME. Standaard alleen een plan; --apply eist een bevestiging (zie hieronder). Start zelf geen dienst.
    verify-import BUNDEL          na een import (ook over machines heen): wachtrij, supervisie, ketens, ledger, open acties en rol tegen de bundel gecontroleerd
    reopen                        na het terugschakelen: de (gesloten) replica weer openen voor de herstelde master
    reconcile                     na promote, alleen lezen: vergelijkt de wachtrij met wat de nodes werkelijk draaien (geen dubbele opdrachten).
Op BEIDE:
    guard [--apply]               stale-master-bewaking: een machine die zichzelf primary noemt maar waarvan de peer een hoger epoch heeft, zet zichzelf
                                  op standby (alleen in de veilige richting; promoveert nooit).
    divergence OUD_HOME NIEUW     wat stond alleen op de oude master (bij terugschakelen: te redden werk).
    return-check ...              controle vóór het terugschakelen naar de herstelde master.
    units                         toont de systemd-sjablonen (niet geïnstalleerd).

Bevestiging bij promote --apply (de oude master is onbereikbaar, dus een mens moet instaan voor 'uit of geïsoleerd'):
    --oude-master NUC --methode stroom-uit|netwerk-los|diensten-gestopt --bevestigd-door NAAM --opdracht "wat Beheerder opdroeg" --token TOKEN
    Het token staat in het plan (promote zonder --apply) en hangt aan precies deze replica en deze epochs; een oud akkoord werkt dus niet.
"""
import argparse
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from . import handover

METHODS = {
    "stroom-uit": "de oude master heeft geen stroom (uitgeschakeld/uit het stopcontact); hij start niet vanzelf weer",
    "netwerk-los": "de oude master is van internet EN tailnet losgekoppeld (kabel/wifi/tailscale down) zodat geen tunnelverbinding en geen node-verkeer mogelijk is",
    "diensten-gestopt": "op de oude master zijn runner2-watch, -mcp, -notifier en -tunnel gestopt én uitgeschakeld (via console/ssh gezien), en zijn rol is standby",
}
WINDOW_PEER_STATES = ("primary", "legacy")   # de peer gedraagt zich als primary
SNAP_KEEP = 48
MAX_AGE_S = 900
SNAPSHOT_NAME = "handover-"
MONITOR2_STATE = Path("~/.local/state/monitor2").expanduser()
MONITOR2_FILES = ("notify-journal.json", "notifications/switch.json", "jobs-repos.json")   # GEEN usage/auth-keeper (token-afgeleid)
CONFIRM_FIELDS = ("oude_master", "methode", "bevestigd_door", "opdracht", "token")


ALIASES = {"NUC": "LINUX", "STUDIO": "LINUX"}   # de NUC is node LINUX


def node_name(n: str | None) -> str:
    n = str(n or "").strip().upper()
    return ALIASES.get(n, n)


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _parse_stamp(s: str) -> float:
    return datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).timestamp()


def replica_home(arg: str | None = None) -> Path:
    return Path(arg or os.environ.get("R2_REPLICA_HOME") or Path.home() / ".runner2-replica").expanduser()


def _read_json(p: Path):
    try:
        return json.loads(Path(p).read_text())
    except (OSError, ValueError):
        return None


def _write_json(p: Path, doc) -> None:
    handover._atomic(Path(p), json.dumps(doc, indent=1, ensure_ascii=False))


def _log(home: Path, kind: str, **data) -> None:
    p = home / "control" / "failover-log.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a") as f:
        f.write(json.dumps({"at": _stamp(), "kind": kind, **data}, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def check(level: str, name: str, detail="") -> dict:
    return {"level": level, "check": name, "detail": detail}


def worst(checks: list) -> int:
    levels = {c["level"] for c in checks}
    return 2 if "FAIL" in levels else 1 if "WARN" in levels else 0


# --- rol van de peer (alleen lezen) ---

def local_role(home: Path) -> dict:
    """{'state': primary|standby|legacy|unreadable, 'epoch', 'role', 'primary_host'} voor deze home."""
    p = home.joinpath(*handover.ROLE_FILE)
    if not p.exists():
        return {"state": "legacy", "epoch": 0}
    d = handover._read_role(home)
    if d is None:
        return {"state": "unreadable", "epoch": 0}
    ok = d.get("role") == "primary" and str(d.get("host", "")).upper() == handover.local_node(home)
    return {"state": "primary" if ok else "standby", "epoch": int(d.get("epoch") or 0), "role": d.get("role"), "host": d.get("host"),
            "primary_host": d.get("primary_host")}


PEER_ROLE_CMD = "cat ~/.runner2/config/control-plane.json"


def _ssh_base(spec: dict) -> list:
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-p", str(spec.get("port", 22))]
    if spec.get("identity_file"):
        ssh += ["-i", str(Path(spec["identity_file"]).expanduser()), "-o", "IdentitiesOnly=yes"]
    if spec.get("known_hosts_file"):
        ssh += ["-o", f"UserKnownHostsFile={Path(spec['known_hosts_file']).expanduser()}", "-o", "StrictHostKeyChecking=yes"]
    return ssh + [f"{spec['user']}@{spec['host']}"]


def probe_peer(home: Path, peer: str, run=subprocess.run) -> dict:
    """Leest alleen het rolbestand van de peer (ssh, vaste opdracht; de ssh-gate laat precies dit toe). Wijzigt niets.
    state: primary | standby | legacy (peer heeft geen rolbestand = werkt als primary) | unreadable | unreachable | unknown (gate/ssh-fout)."""
    try:
        spec = handover._gerard_spec(home, peer)
    except (OSError, ValueError, StopIteration, KeyError) as e:
        return {"state": "unknown", "detail": f"geen route naar {peer} in nodes.toml ({type(e).__name__})"}
    try:
        r = run(_ssh_base(spec) + [PEER_ROLE_CMD], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as e:
        return {"state": "unreachable", "detail": type(e).__name__}
    if r.returncode == 255:
        return {"state": "unreachable", "detail": (r.stderr or "").strip()[-120:]}
    if r.returncode == 126:
        return {"state": "unknown", "detail": "ssh-gate van de peer kent het rolcommando niet (gate verouderd)"}
    if r.returncode == 1 and "No such file" in (r.stderr or ""):
        return {"state": "legacy", "epoch": 0, "detail": "peer heeft geen rolbestand"}
    if r.returncode != 0:
        return {"state": "unknown", "detail": f"ssh exit {r.returncode}: {(r.stderr or '').strip()[-120:]}"}
    try:
        d = json.loads(r.stdout)
        assert isinstance(d, dict)
    except (ValueError, AssertionError):
        return {"state": "unreadable", "detail": "rolbestand van de peer is geen geldig JSON-object"}
    primary = d.get("role") == "primary" and str(d.get("host", "")).upper() == peer.upper()
    return {"state": "primary" if primary else "standby", "epoch": int(d.get("epoch") or 0), "role": d.get("role"), "host": d.get("host"),
            "primary_host": d.get("primary_host")}


# --- master: passieve momentopname + synchronisatie ---

def monitor2_extras(state_dir: Path = MONITOR2_STATE) -> dict:
    return {f"monitor2/{rel}": state_dir / rel for rel in MONITOR2_FILES if (state_dir / rel).is_file()}


def take_snapshot(home: Path, out_dir: Path | None = None, extras: dict | None = None) -> Path:
    """Passieve replica-bundel (kind=replica), zonder hek. Alleen vanaf een expliciete primary zonder wachtrijhek: een standby, een
    gehekte of een onduidelijke machine mag NOOIT een replica leveren (zou een nieuwere toestand op de reserve overschrijven)."""
    role = local_role(home)
    if role["state"] != "primary":
        raise ValueError(f"snapshot geweigerd: deze machine is geen expliciete primary (rol: {role['state']}); alleen de master levert een replica")
    if (home / "jobs" / handover.JOBS_FENCE).exists():
        raise ValueError("snapshot geweigerd: de wachtrij is gehekt (overgedragen); deze machine is niet meer de bron")
    sup = home / "supervision"
    has_sup = sup.exists() and any(p.is_dir() for p in sup.iterdir())
    out_dir = out_dir or home / "replica-out"
    bundle = handover.make_bundle(home, sup if has_sup else None, out_dir, kind="replica", extra_files=extras if extras is not None else monitor2_extras())
    handover.verify_bundle(bundle)
    return bundle


def sync_once(home: Path, to: str = "GERARD", out_dir: Path | None = None, keep_local: int = 3, push=handover.push, extras: dict | None = None) -> dict:
    """snapshot + push + registratie. Bij een fout blijft de vorige replica op de reserve gewoon staan."""
    status_file = home / "control" / "replica-sync.json"
    prev = _read_json(status_file) or {}
    try:
        bundle = take_snapshot(home, out_dir, extras)
        m = handover.verify_bundle(bundle)
        where = push(bundle, home, to=to)
        doc = {"ok": True, "at": _stamp(), "to": to, "bundle": bundle.name, "pushed_to": where, "source_epoch": m.get("source_epoch"),
               "queue": m.get("queue"), "files": len(m["files"]), "consecutive_failures": 0}
    except (ValueError, RuntimeError, OSError, tarfile.TarError, subprocess.SubprocessError) as e:
        doc = {"ok": False, "at": _stamp(), "to": to, "error": f"{type(e).__name__}: {e}"[:300], "last_ok": prev.get("last_ok") or (prev.get("at") if prev.get("ok") else None),
               "consecutive_failures": int(prev.get("consecutive_failures") or 0) + 1}
        _write_json(status_file, doc)
        return doc
    doc["last_ok"] = doc["at"]
    _write_json(status_file, doc)
    od = out_dir or home / "replica-out"
    for old in sorted(od.glob(f"{SNAPSHOT_NAME}*.tar.gz"))[:-keep_local]:
        old.unlink(missing_ok=True)
        old.with_name(old.name + ".sha256").unlink(missing_ok=True)
    return doc


# --- reserve: ingest en status ---

def _snap_dir(replica: Path) -> Path:
    return replica / "snapshots"


def current(replica: Path) -> dict | None:
    return _read_json(replica / "current.json")


def promoted(replica: Path) -> dict | None:
    return _read_json(replica / "PROMOTED.json")


def _key(m: dict) -> tuple:
    return (int(m.get("source_epoch") or 0), int(((m.get("queue") or {}).get("version")) or -1), m.get("created") or "")


def ingest(replica: Path, inbox: Path, home: Path, expect_source: str | None = None, keep: int = SNAP_KEEP) -> dict:
    """Controleert replica-bundels in de inbox en neemt ze over. Een gewone overdracht-bundel (kind != replica) blijft ongemoeid in de inbox.
    Geweigerd: als deze machine primary is of al gepromoveerd (de replica is dan gesloten), een bundel van een onverwachte bron, een lager
    epoch dan dat van deze machine, of een oudere/gelijke toestand dan de huidige replica."""
    res = {"accepted": [], "ignored": [], "rejected": [], "pending": []}
    role = local_role(home)
    if role["state"] in ("primary", "legacy", "unreadable"):
        res["error"] = f"inname gesloten: rol van deze machine is {role['state']} (alleen een expliciete standby neemt een replica in)"
        return res
    if promoted(replica):
        res["error"] = "inname gesloten: deze machine is gepromoveerd (PROMOTED.json); een late bundel van de oude master wordt niet meer aangenomen"
        return res
    expect = (expect_source or role.get("primary_host") or "").upper() or None
    snaps = _snap_dir(replica)
    for tar in sorted(inbox.glob(f"{SNAPSHOT_NAME}*.tar.gz")) if inbox.is_dir() else []:
        side = tar.with_name(tar.name + ".sha256")
        if not side.exists():
            res["pending"].append(tar.name)   # upload nog bezig (de sha256 komt als laatste)
            continue
        try:
            m = handover.verify_bundle(tar)
        except (ValueError, OSError, tarfile.TarError, KeyError) as e:
            res["rejected"].append({"bundle": tar.name, "reason": f"integriteit: {type(e).__name__}: {e}"[:200]})
            _quarantine_bad(replica, tar)
            continue
        if m.get("kind") != "replica":
            res["ignored"].append({"bundle": tar.name, "reason": "geen replica (gewone overdracht)"})
            continue
        why = None
        cur = current(replica)
        if expect and str(m.get("source_node", "")).upper() != expect:
            why = f"onverwachte bron {m.get('source_node')} (verwacht {expect})"
        elif int(m.get("source_epoch") or 0) < role["epoch"]:
            why = f"epoch {m.get('source_epoch')} is lager dan dat van deze machine ({role['epoch']}): bundel van een verouderde master"
        elif not m.get("queue"):
            why = "bundel bevat geen leesbare wachtrij"
        elif cur and _key(m) <= (int(cur.get("source_epoch") or 0), int(((cur.get("queue") or {}).get("version")) or -1), cur.get("created") or ""):
            why = "niet nieuwer dan de huidige replica"
        if why:
            res["rejected"].append({"bundle": tar.name, "reason": why})
            tar.unlink(missing_ok=True)
            side.unlink(missing_ok=True)
            continue
        snaps.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.replace(tar, snaps / tar.name)
        os.replace(side, snaps / side.name)
        doc = {"name": tar.name, "sha256": handover._sha(snaps / tar.name), "created": m.get("created"), "received_at": _stamp(),
               "source_node": m.get("source_node"), "source_epoch": m.get("source_epoch"), "format": m.get("format"), "kind": "replica",
               "queue": m.get("queue"), "active_runs": m.get("active_runs") or [], "sets": m.get("sets")}
        _write_json(replica / "current.json", doc)
        res["accepted"].append(tar.name)
        with open(replica / "ingest-log.jsonl", "a") as f:
            f.write(json.dumps({"at": _stamp(), "bundle": tar.name, "epoch": m.get("source_epoch"), "queue_version": (m.get("queue") or {}).get("version")}) + "\n")
    for old in sorted(snaps.glob(f"{SNAPSHOT_NAME}*.tar.gz"))[:-keep] if snaps.is_dir() else []:
        if current(replica) and old.name == current(replica)["name"]:
            continue
        old.unlink(missing_ok=True)
        old.with_name(old.name + ".sha256").unlink(missing_ok=True)
    res["current"] = (current(replica) or {}).get("name")
    return res


def reopen(replica: Path, home: Path, peer_state: dict) -> dict:
    """Na het terugschakelen: de replica weer openen voor de herstelde master. Alleen op een expliciete standby die eerder gepromoveerd was,
    en alleen als de peer (de herstelde master) aantoonbaar primary is met een hoger epoch; de oude replica en PROMOTED.json gaan naar
    history/ (niets wordt verwijderd)."""
    role = local_role(home)
    if role["state"] != "standby":
        raise ValueError(f"heropenen geweigerd: deze machine is {role['state']}; alleen een standby (na het terugschakelen) neemt weer een replica in")
    pr = promoted(replica)
    if not pr:
        raise ValueError("de replica is niet gesloten (geen PROMOTED.json); niets te heropenen")
    if peer_state.get("state") != "primary" or int(peer_state.get("epoch") or 0) <= role["epoch"]:
        raise ValueError(f"heropenen geweigerd: de peer is niet aantoonbaar primary met een hoger epoch dan dat van deze machine "
                         f"(peer: {peer_state.get('state')} epoch {peer_state.get('epoch')}, hier {role['epoch']}); het terugschakelen is nog niet voltooid")
    hist = replica / "history" / _stamp()
    hist.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name in ("PROMOTED.json", "current.json", "snapshots"):
        if (replica / name).exists():
            os.replace(replica / name, hist / name)
    _log(home, "replica-reopened", moved_to=str(hist))
    return {"reopened": True, "history": str(hist)}


def _quarantine_bad(replica: Path, tar: Path) -> None:
    bad = replica / "rejected"
    bad.mkdir(parents=True, exist_ok=True, mode=0o700)
    for f in sorted(bad.glob("*"))[:-9]:
        f.unlink(missing_ok=True)
    os.replace(tar, bad / tar.name)
    tar.with_name(tar.name + ".sha256").unlink(missing_ok=True)


def replica_status(replica: Path, max_age_s: int = MAX_AGE_S, deep: bool = False, now: float | None = None, home: Path | None = None) -> list:
    now = time.time() if now is None else now
    out = []
    cur = current(replica)
    if promoted(replica):
        out.append(check("WARN", "replica gesloten", f"deze machine is gepromoveerd op {promoted(replica).get('at')}; er komt geen nieuwe replica meer binnen"))
    if not cur:
        out.append(check("FAIL", "replica aanwezig", "er is nog geen replica ingenomen (current.json ontbreekt)"))
        return out
    snap = _snap_dir(replica) / cur["name"]
    if not snap.is_file():
        out.append(check("FAIL", "replica-bestand aanwezig", f"{cur['name']} ontbreekt"))
        return out
    try:
        same = handover._sha(snap) == cur["sha256"] == (snap.with_name(snap.name + ".sha256").read_text().split() or [""])[0]
    except OSError:
        same = False
    out.append(check("PASS" if same else "FAIL", "sha256 van de replica klopt (bestand, sidecar, current.json)"))
    if deep:
        try:
            m = handover.verify_bundle(snap)
            out.append(check("PASS", "volledige bundelcontrole", f"{len(m['files'])} bestanden, formaat {m.get('format')}"))
        except (ValueError, OSError, tarfile.TarError, KeyError) as e:
            out.append(check("FAIL", "volledige bundelcontrole", f"{type(e).__name__}: {e}"[:200]))
    try:
        age = now - _parse_stamp(cur["created"])
    except (KeyError, ValueError):
        age = float("inf")
    lvl = "PASS" if age <= max_age_s else "WARN" if age <= 3 * max_age_s else "FAIL"
    out.append(check(lvl, "versheid replica (RPO)", f"{int(age)} s oud (drempel {max_age_s} s)" if age != float("inf") else "onbekend"))
    q = cur.get("queue") or {}
    out.append(check("PASS" if q.get("jobs") else "FAIL", "wachtrij in de replica", f"versie {q.get('version')}, {q.get('jobs')} opdrachten, {q.get('held')} vastgehouden"))
    if cur.get("active_runs"):
        out.append(check("WARN", "actieve ketens/supervisors in de replica", f"{len(cur['active_runs'])}: komen bij promote in quarantaine, niet in de live map"))
    if home is not None:
        role = local_role(home)
        out.append(check("PASS" if role["state"] == "standby" else "FAIL", "rol van deze machine", role["state"]))
        out.append(check("PASS" if int(cur.get("source_epoch") or 0) >= role["epoch"] else "FAIL", "epoch replica ≥ epoch van deze machine",
                         f"{cur.get('source_epoch')} / {role['epoch']}"))
    n = len(list(_snap_dir(replica).glob(f"{SNAPSHOT_NAME}*.tar.gz")))
    out.append(check("PASS" if n >= 2 else "WARN", "bewaarde generaties", str(n)))
    return out


# --- promote (de noodovername) ---

def plan_token(replica_sha: str, source_epoch, local_epoch, source_node: str) -> str:
    """Hangt aan precies deze replica, deze epochs en deze bron; verandert er iets (nieuwe sync, andere rol), dan is het oude akkoord ongeldig."""
    return hashlib.sha256("|".join(map(str, (replica_sha, source_epoch, local_epoch, node_name(source_node)))).encode()).hexdigest()[:10]


def promote_checks(home: Path, replica: Path, *, peer_state: dict, max_age_s: int = MAX_AGE_S, accept_stale: bool = False, confirm: dict | None = None,
                   now: float | None = None, services_active=lambda names: []) -> tuple:
    """(checks, token). Alleen lezen. Elke FAIL blokkeert --apply."""
    checks = []
    role = local_role(home)
    checks.append(check("PASS" if role["state"] == "standby" else "FAIL", "deze machine is expliciete standby",
                        {"standby": "ja", "primary": "deze machine is al primary: niets te doen", "legacy": "geen rolbestand (= legacy primary): zet eerst expliciet standby",
                         "unreadable": "rolbestand onleesbaar"}[role["state"]]))
    cur = current(replica)
    if promoted(replica):
        checks.append(check("FAIL", "replica nog niet gebruikt", f"al gepromoveerd op {promoted(replica).get('at')}"))
    if not cur:
        checks.append(check("FAIL", "replica aanwezig", "geen replica ingenomen"))
        return checks, None
    checks += [c for c in replica_status(replica, max_age_s, deep=True, now=now, home=home) if c["check"] not in ("rol van deze machine", "bewaarde generaties")]
    for c in checks:
        if c["check"].startswith("versheid") and c["level"] != "PASS":
            if accept_stale and c["level"] == "WARN":
                c["detail"] += " — bewust geaccepteerd (--accept-stale): wijzigingen van de laatste minuten zijn verloren"
            else:
                c["level"] = "FAIL"
                c["detail"] += " — te oud; wacht op een nieuwe sync of accepteer het verlies met --accept-stale (alleen binnen 3× de drempel)"
    ps = peer_state.get("state")
    if ps in WINDOW_PEER_STATES:
        checks.append(check("FAIL", "oude master is uitgeschakeld/standby", f"de peer meldt zich als {ps} (epoch {peer_state.get('epoch')}) en is BEREIKBAAR: "
                            "dit is geen uitval maar een geplande wissel; volg de geplande procedure (peer eerst demoten/hekken)"))
    elif ps == "standby":
        checks.append(check("PASS", "oude master is uitgeschakeld/standby", "peer bereikbaar en standby (geplande wissel)"))
    else:
        checks.append(check("WARN", "oude master onbereikbaar of onduidelijk", f"{ps}: {peer_state.get('detail', '')} — een mens moet instaan dat hij uit of geïsoleerd is"))
    token = plan_token(cur["sha256"], cur.get("source_epoch"), role["epoch"], cur.get("source_node") or "")
    need_attest = ps not in ("standby",)
    if need_attest:
        c = confirm or {}
        missing = [f for f in CONFIRM_FIELDS if not c.get(f)]
        bad = []
        if c.get("methode") and c["methode"] not in METHODS:
            bad.append(f"methode moet een van {sorted(METHODS)} zijn")
        if c.get("oude_master") and node_name(c["oude_master"]) != node_name(cur.get("source_node")):
            bad.append(f"oude master {c['oude_master']} komt niet overeen met de bron van de replica ({cur.get('source_node')})")
        if c.get("token") and c["token"] != token:
            bad.append("token klopt niet met dit plan (opnieuw plannen: de replica of een epoch is veranderd)")
        if c.get("opdracht") and len(str(c["opdracht"]).strip()) < 8:
            bad.append("--opdracht moet de opdracht van Beheerder kort weergeven (≥ 8 tekens)")
        ok = not missing and not bad
        checks.append(check("PASS" if ok else "FAIL", "expliciete bevestiging van Beheerder (oude master uit of geïsoleerd)",
                            "gegeven" if ok else "; ".join(([f"ontbreekt: {', '.join(missing)}"] if missing else []) + bad)))
    if services_active:
        run = services_active(["runner2-mcp.service", "runner2-notifier.service", "runner2-tunnel.service"])
        checks.append(check("PASS" if not run else "FAIL", "control-plane-diensten op de reserve staan nog uit", ", ".join(run) if run else "ja"))
    for name, where in (("control.env", Path.home() / ".config/runner2/control.env"), ("cloudflared.env", Path.home() / ".config/runner2/cloudflared.env")):
        checks.append(check("PASS" if where.is_file() else "WARN", f"geheimen-voorbereiding: {name} aanwezig op de reserve",
                            "aanwezig" if where.is_file() else "ontbreekt: zonder dit kan de reserve de connector/tunnel niet overnemen (inhoud wordt nooit gelezen)"))
    return checks, token


def promote(home: Path, replica: Path, *, apply: bool = False, confirm: dict | None = None, peer_state: dict, max_age_s: int = MAX_AGE_S,
            accept_stale: bool = False, services_active=lambda names: [], backup=True, now: float | None = None) -> dict:
    checks, token = promote_checks(home, replica, peer_state=peer_state, max_age_s=max_age_s, accept_stale=accept_stale, confirm=confirm, now=now,
                                   services_active=services_active)
    cur = current(replica)
    plan = {"checks": checks, "token": token, "applied": False, "replica": (cur or {}).get("name"),
            "rpo": {"gemaakt": (cur or {}).get("created"), "wachtrijversie": ((cur or {}).get("queue") or {}).get("version")},
            "steps": ["herstelpunt van de reserve zelf (statebackup)", "replica sluiten (PROMOTED.json) — geen late bundel van de oude master meer",
                      "replica importeren met quarantaine voor actieve ketens/supervisors, lokale ketens blijven staan, oude reserve-toestand opzij",
                      "wachtrij HELEMAAL pauzeren (ALL) vóór promote", "rol primary met hogere epoch", "geen dienst starten (handmatig, zie draaiboek)"]}
    if worst(checks) >= 2:
        plan["error"] = "niet toegestaan: " + "; ".join(c["check"] for c in checks if c["level"] == "FAIL")
        return plan
    if not apply:
        return plan
    c = confirm or {}
    _log(home, "promote-start", token=token, replica=cur["name"], by=c.get("bevestigd_door"), order=c.get("opdracht"), old=c.get("oude_master"),
         method=c.get("methode"), peer=peer_state.get("state"))
    if backup:
        from . import statebackup
        res = statebackup.create(home, Path.home() / "obf-backups" / f"noodovername-{_stamp()}", with_repos=False)
        if not res.get("ok"):
            plan["error"] = "herstelpunt van de reserve mislukt; niets gewijzigd"
            return plan
        plan["herstelpunt"] = res["out"]
    _write_json(replica / "PROMOTED.json", {"at": _stamp(), "bundle": cur["name"], "sha256": cur["sha256"], "source_epoch": cur.get("source_epoch"),
                                           "old_master": c.get("oude_master"), "method": c.get("methode"), "by": c.get("bevestigd_door"), "order": c.get("opdracht"),
                                           "queue": cur.get("queue")})
    bundle = _snap_dir(replica) / cur["name"]
    info = {"failover": True, "old_master": c.get("oude_master"), "isolation": c.get("methode"), "confirmed_by": c.get("bevestigd_door")}
    try:
        with handover._env_home(home):
            imp = handover.import_bundle(bundle, home, apply=True, promote=True, replace_existing=True, allow_replica=True, quarantine_active=True,
                                         keep_local_runs=True, pause_reason="noodovername: wachtrij gepauzeerd tot reconcile en akkoord van Beheerder",
                                         promote_info=info)
    except BaseException as e:
        _abort_promote(home, replica, f"{type(e).__name__}: {e}"[:200])
        raise
    plan["import"] = {k: imp.get(k) for k in ("applied", "error", "promote", "epoch", "paused_all", "quarantined", "quarantine_dir", "kept_local_runs",
                                              "integrity_problems", "archive", "standby_set")}
    plan["applied"] = bool(imp.get("applied")) and imp.get("promote") == "primary"
    if plan["applied"]:
        pr = promoted(replica) or {}
        _write_json(replica / "PROMOTED.json", {**pr, "epoch_after": imp.get("epoch"), "done_at": _stamp()})
    _log(home, "promote-done" if plan["applied"] else "promote-incomplete", token=token, epoch=imp.get("epoch"), error=imp.get("error"))
    if not plan["applied"]:
        plan["error"] = imp.get("error") or "import onvolledig; de rol is niet primary geworden. Niets gestart. Zie failover-log.jsonl"
        if local_role(home)["state"] != "primary":
            _abort_promote(home, replica, plan["error"])
            plan["replica_heropend"] = True
        return plan
    plan["volgende_stappen"] = NEXT_STEPS_AFTER_PROMOTE
    return plan


def _abort_promote(home: Path, replica: Path, why: str) -> None:
    """Mislukte promote waarna de rol nog niet primary is: het PROMOTED-slot gaat terug (de reserve is nog een gewone standby met een bruikbare replica).
    Is de rol WEL primary geworden, dan blijft het slot staan."""
    if local_role(home)["state"] == "primary":
        return
    pr = replica / "PROMOTED.json"
    if pr.exists():
        hist = replica / "history"
        hist.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.replace(pr, hist / f"PROMOTE-MISLUKT-{_stamp()}.json")
    _log(home, "promote-aborted", why=why)


NEXT_STEPS_AFTER_PROMOTE = [
    "1. python3 -m runner2.noodovername reconcile        (alleen lezen: geen dubbele opdrachten, wezen, ontbrekende taken)",
    "2. runner2-mcp.service en runner2-notifier.service starten; lokaal /readyz en tools/list controleren (wachtrij staat nog GEPAUZEERD)",
    "3. NOOIT twee connectors: bevestig dat de oude master uit/geïsoleerd is, pas dan runner2-tunnel.service starten; publiek /readyz controleren",
    "4. monitor2.service starten; Monitor-2-toestand uit het archief (archive/*/extra/monitor2) alleen terugzetten als Monitor 2 gestopt is",
    "5. cplane connector-url, back-uptimer aan (runner2-backup.timer), eerste cpbackup run + verify",
    "6. met Beheerder: wachtrij vrijgeven (resume_queue) — held blijft held; eerst één proefopdracht per node",
]


def verify_import(bundle: Path, home: Path, *, paused_ok: bool = True) -> list:
    """Vergelijkt een (replica- of overdracht-)bundel met de lokale home na een import. Alleen lezen; werkt over machines heen (alleen de bundel nodig).
    De wachtrij wordt inhoudelijk vergeleken (opdrachten, volgorde, requests, held) omdat promote hem bewust pauzeert; de rest op sha256."""
    m = handover.verify_bundle(bundle)
    out = []
    files = {e["path"]: e for e in m["files"]}
    bq = json.load(tf_member(bundle, "files/jobs/queue.json")) if "jobs/queue.json" in files else None
    lq = _read_json(home / "jobs" / "queue.json")
    if bq is not None:
        same = lq is not None and all(lq.get(k) == bq.get(k) for k in ("jobs", "order", "requests")) and \
            (lq.get("paused") == bq.get("paused") or (paused_ok and bool((lq.get("paused") or {}).get("all"))))
        out.append(check("PASS" if same else "FAIL", "wachtrij inhoudelijk gelijk (opdrachten, volgorde, requests, held, pauzes)",
                         f"{len(bq['jobs'])} opdrachten" if same else "verschil met de bundel"))
    active = set(m.get("active_runs") or [])   # horen live (met --allow-active) of in failover/quarantine-*/ te staan
    bad = []
    for path, e in sorted(files.items()):
        top = path.split("/")[0]
        if top == "supervision" and path.count("/") >= 2:
            loc = home / path
        elif top in ("chains", "supervisors"):
            loc = home / path
            if path.split("/")[1] in active and not loc.is_file():
                if not list((home / "failover").glob(f"quarantine-*/{path}")):
                    bad.append(path + " (noch live noch in quarantaine)")
                continue
        else:
            continue
        if not loc.is_file():
            bad.append(path)
        elif handover._sha(loc) != e["sha256"] and not loc.name.endswith(("audit.jsonl",)):
            bad.append(path + " (ander)")
    out.append(check("PASS" if not bad else "FAIL", "supervisie, ketens en supervisors aanwezig en gelijk", f"{len(bad)} afwijkend: {bad[:3]}" if bad else "alles gelijk"))
    nl = (_read_json(home / "control" / "notifier" / "ledger.json") or {}).get("open", {})
    if "notifier/ledger.json" in files:
        bl = json.load(tf_member(bundle, "files/notifier/ledger.json")).get("open", {})
        out.append(check("PASS" if set(bl) <= set(nl) else "FAIL", "notifier-ledger: alle meldingen uit de bundel aanwezig", f"{len(bl)} / {len(nl)}"))
    arch = sorted((home / "archive").glob("*/open-actions.json"))
    if "reference/open-actions.json" in files:
        want = {i["action_id"] for i in json.load(tf_member(bundle, "files/reference/open-actions.json"))}
        have = {i["action_id"] for f in arch for i in (_read_json(f) or [])}
        out.append(check("PASS" if want <= have else "FAIL", "open acties als naslag aanwezig", f"{len(want)} / {len(have)}"))
    role = local_role(home)
    out.append(check("PASS" if role["state"] in ("primary", "standby") else "FAIL", "rolbestand expliciet en leesbaar", role["state"]))
    if m.get("source_epoch") is not None and role["state"] == "primary":
        out.append(check("PASS" if role["epoch"] > int(m["source_epoch"]) else "FAIL", "epoch hoger dan dat van de bron", f"{role['epoch']} > {m['source_epoch']}"))
    return out


def tf_member(bundle: Path, name: str):
    with tarfile.open(bundle, "r:gz") as tf:
        return io.BytesIO(tf.extractfile(name).read())


# --- reconcile (na promote, alleen lezen) ---

def reconcile(home: Path, call=None, snapshot_created: str | None = None) -> dict:
    """Vergelijkt de wachtrij met wat de nodes werkelijk draaien. Schrijft niets. Categorieën:
    - dubbel_risico: opdracht staat hier 'queued' maar de node kent de taak al (de start geeft dan already_started: veilig, maar controleer)
    - onbereikbaar: node of taak niet te bevragen (wachtrij kan hem niet bijwerken)
    - wezen: taken op een node met '· J-…' in de titel die hier ontbreken of een ander taak-id/run hebben (gestart door de oude master na de replica)
    - lopend: opdrachten die hier 'running/starting' staan met de waargenomen toestand."""
    from . import jobs
    call = call or jobs._call
    out = {"dubbel_risico": [], "onbereikbaar": [], "wezen": [], "lopend": [], "samenvatting": {}}
    with handover._env_home(home):
        q = jobs.load()
        by_id = q["jobs"]
        for jid, j in sorted(by_id.items()):
            if j.get("status") in ("starting", "running"):
                try:
                    s = call("runner2_status", {"task_id": j["task_id"], "node": j["node"]})
                    out["lopend"].append({"job": jid, "node": j["node"], "task": j["task_id"], "taak": s.get("status")})
                except Exception as e:   # noqa: BLE001
                    out["onbereikbaar" if getattr(e, "code", "") != "not_found" else "wezen"].append(
                        {"job": jid, "node": j["node"], "task": j["task_id"], "reden": f"{getattr(e, 'code', type(e).__name__)}: {str(getattr(e, 'message', e))[:120]}"})
        for node in jobs.NODES:
            head = next((by_id[x] for x in q["order"].get(node, []) if x in by_id and by_id[x]["status"] == "queued" and not by_id[x].get("held")), None)
            if head:
                try:
                    s = call("runner2_status", {"task_id": head["task_id"], "node": node})
                    out["dubbel_risico"].append({"job": head["id"], "node": node, "task": head["task_id"], "taak": s.get("status"),
                                                 "advies": "taak bestaat al op de node: de dispatcher krijgt already_started; controleer de uitkomst vóór vrijgeven"})
                except Exception as e:   # noqa: BLE001
                    if getattr(e, "code", "") != "not_found":
                        out["onbereikbaar"].append({"job": head["id"], "node": node, "reden": f"{getattr(e, 'code', type(e).__name__)}"})
            try:
                st = call("runner2_status", {"node": node})
            except Exception as e:   # noqa: BLE001
                out["onbereikbaar"].append({"node": node, "reden": f"{getattr(e, 'code', type(e).__name__)}"})
                continue
            for t in st.get("tasks") or []:
                title = t.get("title") or ""
                if "· J-" not in title:
                    continue
                jid = "J-" + title.rsplit("· J-", 1)[1].strip()[:12]
                j = by_id.get(jid)
                if j is None:
                    out["wezen"].append({"node": node, "task": t.get("task_id"), "job": jid, "reden": "opdracht staat niet in deze wachtrij (gestart na de replica)"})
                elif t.get("task_id") != j.get("task_id") and j.get("status") in ("queued", "starting", "running") and \
                        t.get("task_id") != jobs.task_id_for(jid, int(j.get("run") or 0)):
                    out["wezen"].append({"node": node, "task": t.get("task_id"), "job": jid, "reden": f"andere run dan hier (hier {j.get('task_id')}): oude master was verder"})
    s = out["samenvatting"]
    s.update({k: len(out[k]) for k in ("dubbel_risico", "onbereikbaar", "wezen", "lopend")})
    s["vrijgeven"] = "NEE: eerst wezen/dubbel_risico beoordelen" if (s["wezen"] or s["dubbel_risico"]) else "ja, als Beheerder akkoord geeft" if not s["onbereikbaar"] else "onbereikbare nodes eerst bekijken"
    return out


# --- guard: stale-master-bewaking (alleen de veilige richting) ---

def guard_check(home: Path, peer_state: dict, on_unknown: str = "closed", override: dict | None = None, now: float | None = None) -> dict:
    """Beslist wat deze machine moet doen. 'fence' = zet jezelf op standby (alleen als de peer bewijst dat jij verouderd bent);
    'hold' = niet starten (peer onduidelijk, beleid closed); 'ok' = doorgaan. Nooit 'promote'."""
    me = local_role(home)
    if me["state"] != "primary":
        return {"action": "ok", "why": f"deze machine is {me['state']}: niets te bewaken"}
    ps = peer_state.get("state")
    if ps in WINDOW_PEER_STATES:
        pe = int(peer_state.get("epoch") or 0)
        if pe > me["epoch"]:
            return {"action": "fence", "why": f"peer is primary met hoger epoch ({pe} > {me['epoch']}): deze machine is de verouderde master"}
        if pe == me["epoch"]:
            return {"action": "fence", "why": f"CONFLICT: beide primary met epoch {pe}; deze machine trekt zich terug (veilige kant), laat een mens kiezen"}
        return {"action": "ok", "why": f"peer is primary met lager epoch ({pe} < {me['epoch']}): peer is verouderd en hoort zichzelf te hekken", "alert": "peer-stale-primary"}
    if ps == "standby":
        return {"action": "ok", "why": "peer is standby"}
    now = time.time() if now is None else now
    if override and float(override.get("until_epoch") or 0) > now:
        return {"action": "ok", "why": f"peer onduidelijk ({ps}) maar een mens heeft dit tot {override.get('until')} toegestaan ({override.get('by')}: {override.get('reason')})", "alert": "guard-override"}
    if on_unknown == "open":
        return {"action": "ok", "why": f"peer onduidelijk ({ps}); beleid 'open': doorgaan", "alert": "peer-unknown"}
    return {"action": "hold", "why": f"peer onduidelijk ({ps}: {peer_state.get('detail', '')}); beleid 'closed': niet starten tot de peer bereikbaar is of een mens dit toestaat (guard --override)"}


def guard_apply(home: Path, peer: str, decision: dict) -> dict:
    if decision["action"] == "fence":
        handover.fence_jobs(home, peer)
        handover._mark_standby(home, peer)
        _log(home, "guard-fence", peer=peer, why=decision["why"])
        decision["fenced"] = True
    elif decision.get("alert") or decision["action"] == "hold":
        _log(home, f"guard-{decision['action']}", peer=peer, why=decision["why"])
    return decision


def write_override(home: Path, hours: float, by: str, reason: str) -> dict:
    if not by or len(reason.strip()) < 8 or not 0 < hours <= 24:
        raise ValueError("override vraagt --by, een reden (≥ 8 tekens) en --hours tussen 0 en 24")
    until = time.time() + hours * 3600
    doc = {"until": datetime.fromtimestamp(until, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "until_epoch": until, "by": by, "reason": reason, "at": _stamp()}
    _write_json(home / "control" / "guard-override.json", doc)
    _log(home, "guard-override", **{k: doc[k] for k in ("until", "by", "reason")})
    return doc


# --- divergentie en terugschakelen ---

def _queue_of(src: Path) -> dict | None:
    """queue.json uit een home-map of uit een bundel (.tar.gz)."""
    src = Path(src)
    if src.is_dir():
        return _read_json(src / "jobs" / "queue.json")
    try:
        with tarfile.open(src, "r:gz") as tf:
            return json.load(tf.extractfile("files/jobs/queue.json"))
    except (OSError, KeyError, ValueError, tarfile.TarError, AttributeError):
        return None


def divergence(old: Path, new: Path) -> dict:
    """Wat stond alleen op de OUDE master (`old`, bijv. de herstelde NUC) en zou verloren gaan als de NIEUWE toestand (`new`, bundel of home van
    de huidige master) eroverheen komt. Alleen lezen. 'redden' is de lijst opdrachten die een mens opnieuw moet beoordelen."""
    oq, nq = _queue_of(old), _queue_of(new)
    if oq is None or nq is None:
        raise ValueError("kan een van beide wachtrijen niet lezen")
    terminal = ("done", "failed", "incomplete", "stopped", "cancelled")
    only_old, ahead, behind = [], [], []
    for jid, j in sorted(oq["jobs"].items()):
        n = nq["jobs"].get(jid)
        brief = {"job": jid, "title": (j.get("title") or "")[:80], "node": j.get("node"), "status": j.get("status"), "run": j.get("run"), "created": j.get("created_at") or j.get("created")}
        if n is None:
            only_old.append(brief)
        elif (j.get("run") or 0) > (n.get("run") or 0) or (j.get("run") == n.get("run") and j.get("status") in terminal and n.get("status") not in terminal):
            ahead.append({**brief, "nieuwe_status": n.get("status"), "nieuwe_run": n.get("run")})
        elif (j.get("run") or 0) < (n.get("run") or 0) or (j.get("status") not in terminal and n.get("status") in terminal):
            behind.append({"job": jid, "oude_status": j.get("status"), "nieuwe_status": n.get("status")})
    reqs = sorted(set(oq.get("requests") or {}) - set(nq.get("requests") or {}))
    redden = [b for b in only_old + ahead if b["status"] not in ("done", "cancelled")]
    return {"oud": {"version": oq.get("version"), "jobs": len(oq["jobs"])}, "nieuw": {"version": nq.get("version"), "jobs": len(nq["jobs"])},
            "alleen_op_oud": only_old, "oud_verder": ahead, "nieuw_verder": len(behind), "requests_alleen_op_oud": len(reqs),
            "redden": redden, "verlies": bool(only_old or ahead or reqs),
            "advies": ("de oude toestand gaat bij het terugschakelen opzij (jobs.replaced-<tijd>); beoordeel 'redden' en voeg die opdrachten zo nodig opnieuw toe via runner2_jobs add "
                       "(zelfde request_id = idempotent)") if (only_old or ahead or reqs) else "geen verlies: de oude master heeft niets dat de nieuwe mist"}


def return_check(old_home: Path, bundle: Path | None, *, services_active=lambda names: [], divergence_ack: str | None = None, peer_state: dict | None = None,
                 backup_dir: Path | None = None, now: float | None = None) -> list:
    """Controle vóór het terugschakelen naar de HERSTELDE oude master (`old_home`). Alleen lezen. Ontvanger = deze machine (de herstelde NUC)."""
    now = time.time() if now is None else now
    out = []
    me = local_role(old_home)
    out.append(check("PASS" if me["state"] == "standby" else "FAIL", "herstelde machine staat op standby (niet primary)",
                     me["state"] + ("" if me["state"] == "standby" else " — eerst 'guard --apply' of cplane demote; nooit importeren op een primary")))
    run = services_active(["runner2-mcp.service", "runner2-notifier.service", "runner2-tunnel.service"])
    out.append(check("PASS" if not run else "FAIL", "control-plane-diensten (mcp, notifier, tunnel) op de herstelde machine staan uit", ", ".join(run) or "ja"))
    if peer_state is not None:
        ps = peer_state.get("state")
        out.append(check("PASS" if ps in WINDOW_PEER_STATES and (peer_state.get("epoch") or 0) > me["epoch"] else "FAIL",
                         "de huidige master (reserve) is bereikbaar, primary en heeft het hoogste epoch", f"{ps}, epoch {peer_state.get('epoch')} (hier {me['epoch']})"))
    if bundle is None:
        out.append(check("FAIL", "overdrachtsbundel van de huidige master", "geen bundel opgegeven (handover export --to <deze machine> op de huidige master)"))
        return out
    try:
        m = handover.verify_bundle(Path(bundle))
    except (ValueError, OSError, tarfile.TarError, KeyError) as e:
        out.append(check("FAIL", "bundel is heel (sha256 + inhoud)", f"{type(e).__name__}: {e}"[:200]))
        return out
    out.append(check("PASS", "bundel is heel (sha256 + inhoud)", f"formaat {m.get('format')}, {len(m['files'])} bestanden"))
    out.append(check("PASS" if m.get("kind", "handover") == "handover" else "FAIL", "bundel is een echte overdracht (met hek op de bron), geen replica", m.get("kind", "handover")))
    out.append(check("PASS" if int(m.get("source_epoch") or 0) > me["epoch"] else "FAIL", "bundel-epoch hoger dan dat van de herstelde machine", f"{m.get('source_epoch')} > {me['epoch']}"))
    out.append(check("PASS" if not m.get("active_runs") else "FAIL", "bron heeft geen actieve ketens/supervisors", ", ".join(m.get("active_runs") or []) or "geen"))
    try:
        d = divergence(old_home, Path(bundle))
        digest = hashlib.sha256(json.dumps({k: d[k] for k in ("alleen_op_oud", "oud_verder", "requests_alleen_op_oud")}, sort_keys=True).encode()).hexdigest()[:10]
        if not d["verlies"]:
            out.append(check("PASS", "divergentie: geen werk alleen op de herstelde machine", d["advies"]))
        else:
            ok = divergence_ack == digest
            out.append(check("PASS" if ok else "FAIL", "divergentie beoordeeld door een mens",
                             f"{len(d['alleen_op_oud'])} opdracht(en) alleen op de herstelde machine, {len(d['oud_verder'])} verder dan de huidige master, "
                             f"{d['requests_alleen_op_oud']} request-id(s); bevestig met --ack-divergentie {digest}" if not ok else "bevestigd"))
    except ValueError as e:
        out.append(check("FAIL", "divergentie bepaald", str(e)))
    if backup_dir is not None:
        recent = [p for p in backup_dir.glob("*") if p.is_dir() and now - p.stat().st_mtime < 3600]
        out.append(check("PASS" if recent else "WARN", "verse back-up van de herstelde machine (< 1 uur)", str(len(recent))))
    return out


# --- sjablonen (alleen tonen) ---

def unit_templates(role: str, peer: str, code: str, python: str = "/usr/bin/python3") -> dict:
    env = f"Environment=PYTHONPATH={code}\n"
    if role == "master":
        return {
            "runner2-replica-sync.service": f"[Unit]\nDescription=Runner 2: passieve replica naar de reserve ({peer})\n\n[Service]\nType=oneshot\n{env}"
                                            f"ExecStart={python} -m runner2.noodovername sync --to {peer}\nNice=19\nIOSchedulingClass=idle\n",
            "runner2-replica-sync.timer": "[Unit]\nDescription=Runner 2: replica elke 5 minuten\n\n[Timer]\nOnBootSec=3min\nOnUnitActiveSec=5min\nAccuracySec=30s\n\n[Install]\nWantedBy=timers.target\n",
        }
    if role == "reserve":
        return {
            "runner2-replica-ingest.service": f"[Unit]\nDescription=Runner 2: replica-bundels controleren en overnemen\n\n[Service]\nType=oneshot\n{env}"
                                              f"ExecStart={python} -m runner2.noodovername ingest\nNice=19\nIOSchedulingClass=idle\n",
            "runner2-replica-ingest.timer": "[Unit]\nDescription=Runner 2: replica-inname elke minuut\n\n[Timer]\nOnBootSec=2min\nOnUnitActiveSec=1min\nAccuracySec=10s\n\n[Install]\nWantedBy=timers.target\n",
        }
    if role == "guard":
        return {"masterguard.conf": f"# drop-in voor runner2-watch/-mcp/-notifier/-tunnel .service.d/masterguard.conf (NIET geïnstalleerd; besluit van Beheerder)\n"
                                    f"[Service]\n{env}ExecStartPre={python} -m runner2.noodovername guard --apply --peer {peer} --wait-s 90 --on-unknown closed\n"}
    raise ValueError("role moet master, reserve of guard zijn")


def services_active(names: list) -> list:
    out = []
    for n in names:
        try:
            r = subprocess.run(["systemctl", "--user", "is-active", n], capture_output=True, text=True, timeout=10)
            if r.stdout.strip() == "active":
                out.append(n)
        except (OSError, subprocess.SubprocessError):
            pass
    return out


# --- CLI ---

def _confirm(a) -> dict | None:
    c = {"oude_master": a.oude_master, "methode": a.methode, "bevestigd_door": a.bevestigd_door, "opdracht": a.opdracht, "token": a.token}
    return c if any(c.values()) else None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="noodovername", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add(name, **kw):
        p = sub.add_parser(name, **kw)
        p.add_argument("--home")
        p.add_argument("--json", action="store_true")
        return p

    p = add("snapshot")
    p.add_argument("--out")
    p = add("sync")
    p.add_argument("--to", default="GERARD")
    p = add("ingest")
    p.add_argument("--replica-home")
    p.add_argument("--inbox")
    p.add_argument("--expect-source")
    p = add("status")
    p.add_argument("--replica-home")
    p.add_argument("--max-age-s", type=int, default=MAX_AGE_S)
    p.add_argument("--peer")
    p = add("promote")
    p.add_argument("--replica-home")
    p.add_argument("--peer", default="LINUX", help="node van de oude master volgens nodes.toml (de NUC is LINUX)")
    p.add_argument("--max-age-s", type=int, default=MAX_AGE_S)
    p.add_argument("--accept-stale", action="store_true")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--no-backup", action="store_true")
    for f in ("oude-master", "methode", "bevestigd-door", "opdracht", "token"):
        p.add_argument("--" + f)
    p = add("verify-import")
    p.add_argument("bundle")
    p = add("reopen")
    p.add_argument("--replica-home")
    p.add_argument("--peer", default="LINUX")
    p = add("reconcile")
    p = add("guard")
    p.add_argument("--peer", required=True)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--wait-s", type=int, default=0)
    p.add_argument("--on-unknown", choices=("closed", "open"), default="closed")
    p.add_argument("--override", action="store_true")
    p.add_argument("--hours", type=float, default=2)
    p.add_argument("--by")
    p.add_argument("--reason", default="")
    p = add("divergence")
    p.add_argument("old")
    p.add_argument("new")
    p = add("return-check")
    p.add_argument("--bundle")
    p.add_argument("--peer")
    p.add_argument("--ack-divergentie")
    p = add("units")
    p.add_argument("role", choices=("master", "reserve", "guard"))
    p.add_argument("--peer", required=True)
    p.add_argument("--code", default=str(Path.home() / "runner2-code"))
    a = ap.parse_args(argv)
    home = handover.home_dir(a.home)

    def emit(doc, checks=None):
        if a.json or checks is None:
            print(json.dumps(doc, indent=1, ensure_ascii=False))
        else:
            for c in checks:
                print(f"{c['level']:4} {c['check']}" + (f" — {c['detail']}" if c["detail"] else ""))

    try:
        if a.cmd == "snapshot":
            b = take_snapshot(home, Path(a.out) if a.out else None)
            emit({"bundle": str(b)})
        elif a.cmd == "sync":
            doc = sync_once(home, node_name(a.to))
            emit(doc)
            return 0 if doc["ok"] else 1
        elif a.cmd == "ingest":
            rh = replica_home(a.replica_home)
            res = ingest(rh, Path(a.inbox).expanduser() if a.inbox else home / "inbox", home, a.expect_source)
            emit(res)
            return 1 if res.get("error") or res["rejected"] else 0
        elif a.cmd == "status":
            rh = replica_home(a.replica_home)
            checks = replica_status(rh, a.max_age_s, deep=True, home=home)
            if a.peer:
                ps = probe_peer(home, node_name(a.peer))
                checks.append(check("PASS" if ps["state"] in ("primary", "standby") else "WARN", f"peer {a.peer}", f"{ps['state']} epoch {ps.get('epoch')} {ps.get('detail', '')}".strip()))
            emit({"checks": checks}, checks)
            return worst(checks)
        elif a.cmd == "promote":
            rh = replica_home(a.replica_home)
            ps = probe_peer(home, node_name(a.peer))
            res = promote(home, rh, apply=a.apply, confirm=_confirm(a), peer_state=ps, max_age_s=a.max_age_s, accept_stale=a.accept_stale,
                          services_active=services_active, backup=not a.no_backup)
            if a.json or a.apply:
                print(json.dumps(res, indent=1, ensure_ascii=False))
            else:
                print(f"PLAN noodovername naar deze machine ({handover.local_node(home)}) — peer {a.peer}: {ps['state']} {ps.get('detail', '')}")
                for c in res["checks"]:
                    print(f"{c['level']:4} {c['check']}" + (f" — {c['detail']}" if c["detail"] else ""))
                print("stappen:\n  - " + "\n  - ".join(res["steps"]))
                if res.get("token"):
                    print(f"token voor --apply (hangt aan deze replica, epochs en methode): {res['token']}")
                print(res.get("error") or "geen blokkade: --apply kan (na expliciete opdracht van Beheerder)")
            return 1 if res.get("error") or (a.apply and not res["applied"]) else 0
        elif a.cmd == "verify-import":
            checks = verify_import(Path(a.bundle).expanduser(), home)
            emit({"checks": checks}, checks)
            return worst(checks)
        elif a.cmd == "reopen":
            emit(reopen(replica_home(a.replica_home), home, probe_peer(home, node_name(a.peer))))
        elif a.cmd == "reconcile":
            res = reconcile(home)
            emit(res)
            return 0
        elif a.cmd == "guard":
            if a.override:
                emit(write_override(home, a.hours, a.by or "", a.reason))
                return 0
            deadline = time.time() + a.wait_s
            while True:
                ps = probe_peer(home, node_name(a.peer))
                if ps["state"] not in ("unreachable", "unknown") or time.time() >= deadline:
                    break
                time.sleep(5)
            ov = _read_json(home / "control" / "guard-override.json")
            dec = guard_check(home, ps, a.on_unknown, ov)
            if a.apply:
                guard_apply(home, node_name(a.peer), dec)
            emit({**dec, "peer": ps})
            return {"ok": 0, "fence": 2, "hold": 3}[dec["action"]]
        elif a.cmd == "divergence":
            res = divergence(Path(a.old).expanduser(), Path(a.new).expanduser())
            emit(res)
            return 1 if res["verlies"] else 0
        elif a.cmd == "return-check":
            ps = probe_peer(home, node_name(a.peer)) if a.peer else None
            checks = return_check(home, Path(a.bundle).expanduser() if a.bundle else None, services_active=services_active, divergence_ack=a.ack_divergentie,
                                  peer_state=ps, backup_dir=Path.home() / "obf-backups")
            emit({"checks": checks}, checks)
            return worst(checks)
        elif a.cmd == "units":
            for name, text in unit_templates(a.role, node_name(a.peer), a.code).items():
                print(f"### {name}\n{text}")
    except (ValueError, RuntimeError, OSError, tarfile.TarError, subprocess.SubprocessError) as ex:
        print(f"FOUT: {type(ex).__name__}: {ex}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
