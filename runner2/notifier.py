"""Notifier / wake-up / headless Monitor 2 van de control plane (draait always-on op GERARD, onafhankelijk van Mac en chat).

Bouwt voort op de bestaande supervisiearchitectuur en maakt er geen tweede systeem naast: de dossiers en events zijn de
bron van waarheid en blijven in `supervision/`; dit proces leest ze lock-vrij (`supervision.overview`, `actions`,
`store`) en maakt van elke *nieuwe* aandachtsconditie één duurzame notificatie:

    ~/.runner2/control/notifier/outbox.jsonl   append-only, nooit verwijderd (rotatie: zie notifier_keep)
    ~/.runner2/control/notifier/ledger.json    welke condities nu open zijn (dedupe; een opgeloste conditie die terugkomt meldt opnieuw)
    ~/.runner2/control/notifier/heartbeat.json levensteken voor `cplane status`
    ~/.runner2/control/monitor.json            compacte toestand zoals Monitor 2 hem toont

Ook zonder enige externe afnemer blijft dus alles bewaard. Optionele afnemers (control.toml, [notify]): ntfy_url (https),
webhook_url (https, HMAC met R2_NOTIFY_HMAC) en command (argv, JSON op stdin). Meldingen bevatten alleen titel/label/korte
tekst, nooit dossierinhoud, tokens of paden naar geheimen.

Besluit 48 (Beheerder, 23 oktober 2026): DIRECT melden alleen bij een echte blokkade door een menselijke beslissing (klasse `blokkade`:
taak op waiting_for_human, supervisie WACHT OP BEHEERDER/LIMIET/GEBLOKKEERD), pas na een stabiele waarneming en één keer per blokkade;
alle overige menselijke punten (open acties) gaan NIET afzonderlijk maar in één dagoverzicht dat volgt op de dagelijkse herbeoordeling
(geen eigen timer). Regels, zuivering en klassen: `humanalerts.py`; dossier: `docs/meldingen-menselijke-blokkades.md`.

Wake-up: de watch roept (supervision_webhook_url) POST /wake aan op 127.0.0.1:8767 zodra er een supervisie-event wacht;
dat start direct een extra ronde. De ronde draait ook zonder wake-up elke notify.interval_s seconden.
"""
import hashlib
import hmac
import json
import os
import subprocess
import sys
import threading
import time
import tomllib
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import actionreassess, actions, config, humanalerts, store, supervision
from .log import log

LOGNAME = "notifier.log"
DEFAULT_PORT = 8767
ATTENTION_STATES = supervision.ATTENTION
KEEP_LINES = 20000
_wake = threading.Event()


def control_dir() -> Path:
    return config.r2_home() / "control"


def notifier_dir() -> Path:
    return control_dir() / "notifier"


def settings() -> dict:
    cfg = {"interval_s": 15, "node_probe_s": 60, "ntfy_url": "", "webhook_url": "", "command": [], "keep_lines": KEEP_LINES,
           "stable_s": humanalerts.STABLE_S, "stable_rounds": humanalerts.STABLE_ROUNDS, "resolve_grace_s": humanalerts.RESOLVE_GRACE_S,
           "max_direct": humanalerts.MAX_DIRECT_PER_ROUND, "supervisor_stale_h": humanalerts.SUPERVISOR_STALE_H}
    p = config.r2_home() / "config" / "control.toml"
    if p.exists():
        try:
            cfg.update(tomllib.loads(p.read_text()).get("notify", {}))
        except (OSError, ValueError) as e:
            log(f"control.toml onleesbaar: {e}", LOGNAME)
    return cfg


def _now() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def _clip(text, n: int = 200) -> str:
    text = " ".join(str(text or "").split())
    return text if len(text) <= n else text[: n - 1] + "…"


def _nid(kind: str, key: str) -> str:
    return "N-" + hashlib.sha1(f"{kind}:{key}".encode()).hexdigest()[:12]


# --- condities: wat vraagt nu aandacht ---

def conditions(nodes: dict | None = None, now: float | None = None, cfg: dict | None = None) -> list:
    """Alle open aandachtscondities als {kind, class, key, title, text}. Lock-vrij en zonder netwerk (nodes komt van de aanroeper).

    Open acties (`human_action`) zijn bewust GEEN conditie meer: ze blokkeren niets en gaan via het dagoverzicht (besluit 48)."""
    now = time.time() if now is None else now
    cfg = cfg or {}
    ov = supervision.overview() or {"projects": []}
    out, covered = humanalerts.supervision_conditions(ov["projects"], now, supervision._epoch,
                                                      float(cfg.get("supervisor_stale_h", humanalerts.SUPERVISOR_STALE_H)))
    out += humanalerts.task_conditions(store.all_tasks(), covered)
    for name, info in (nodes or {}).items():
        if not info.get("reachable"):
            out.append({"kind": "node_down", "class": humanalerts.INFRA, "key": name, "title": f"NODE ONBEREIKBAAR · {name}",
                        "text": humanalerts.redact(info.get("error"))})
    from . import workwatch, guardian
    for condition in workwatch.conditions() + guardian.conditions():
        out.append({**condition, "text": humanalerts.redact(condition.get("text"))})
    return out


# --- ledger en outbox (duurzaam) ---

def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def _append_outbox(entry: dict) -> None:
    d = notifier_dir()
    d.mkdir(parents=True, exist_ok=True)
    with open(d / "outbox.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _rotate_outbox(keep: int) -> None:
    p = notifier_dir() / "outbox.jsonl"
    try:
        if p.stat().st_size < keep * 400:
            return
        lines = p.read_text(encoding="utf-8").splitlines()
        if len(lines) <= keep:
            return
        old = p.with_name(f"outbox-{time.strftime('%Y%m%d%H%M%S')}.jsonl")
        old.write_text("\n".join(lines[:-keep]) + "\n", encoding="utf-8")   # oude regels blijven bestaan, alleen verplaatst
        store.atomic_write(p, "\n".join(lines[-keep:]) + "\n")
    except OSError:
        pass


def sync_ledger(conds: list, probe_complete: bool = True, now: float | None = None, cfg: dict | None = None) -> list:
    """Vergelijkt condities met het ledger; geeft de NIEUWE notificaties terug en legt ze vast. Opgeloste condities verdwijnen uit het ledger."""
    return sync_ledger_full(conds, probe_complete, now, cfg)[0]


def sync_ledger_full(conds: list, probe_complete: bool = True, now: float | None = None, cfg: dict | None = None) -> tuple:
    """(nieuw, opgelost). Een blokkade (klasse `blokkade`) wordt pas nieuw als ze stabiel is (stable_rounds opeenvolgende rondes én stable_s
    seconden); een flapper die eerder verdwijnt wordt nooit gemeld. Ze telt pas als opgelost als ze resolve_grace_s weg is; komt ze
    daarbinnen terug dan is het dezelfde melding (geen tweede). Niet-blokkades gedragen zich als vroeger. Het eerste ledger is baseline."""
    now = time.time() if now is None else now
    cfg = cfg or {}
    stable_s = float(cfg.get("stable_s", humanalerts.STABLE_S))
    rounds = int(cfg.get("stable_rounds", humanalerts.STABLE_ROUNDS))
    grace = float(cfg.get("resolve_grace_s", humanalerts.RESOLVE_GRACE_S))
    path = notifier_dir() / "ledger.json"
    first = not path.exists()
    ledger = _read_json(path, {"open": {}})
    pending = ledger.setdefault("pending", {})
    for nid in [n for n, e in ledger["open"].items() if e.get("kind") == "human_action"]:
        del ledger["open"][nid]   # oud: open acties zijn geen melding meer (besluit 48); stil opgeruimd, geen 'opgelost'-regel per punt
    current = {_nid(c["kind"], c["key"]): c for c in conds}
    new, resolved = [], []
    for nid in [n for n in pending if n not in current]:
        del pending[nid]   # niet meer zichtbaar voor ze stabiel was: nooit gemeld
    for nid, c in current.items():
        if nid in ledger["open"]:
            ledger["open"][nid].pop("missing_since", None)
            pending.pop(nid, None)
            continue
        if c.get("class") == humanalerts.BLOCK and not first:
            seen = pending.setdefault(nid, {"first": now, "seen": 0})
            seen["seen"] += 1
            if seen["seen"] < rounds or now - seen["first"] < stable_s:
                continue
        pending.pop(nid, None)
        entry = {"id": nid, "at": _now(), "baseline": first, **c}
        ledger["open"][nid] = {"first_at": entry["at"], "kind": c["kind"], "class": c.get("class", ""), "title": c.get("title", ""),
                               "baseline": first, "delivered": {}}
        _append_outbox(entry)
        new.append(entry)
    for nid in list(ledger["open"]):
        meta = ledger["open"][nid]
        if nid in current or not (probe_complete or meta["kind"] != "node_down"):
            continue
        if meta.get("class") == humanalerts.BLOCK and now - meta.setdefault("missing_since", now) < grace:
            continue
        rec = {"id": nid, "at": _now(), "resolved": True, "kind": meta["kind"]}
        _append_outbox(rec)
        resolved.append({**rec, "class": meta.get("class", ""), "title": meta.get("title", ""), "baseline": meta.get("baseline", False),
                         "delivered": meta.get("delivered") or {}})
        del ledger["open"][nid]
    store.atomic_write(path, json.dumps(ledger, indent=1))
    return new, resolved


# --- afnemers (optioneel, nooit van invloed op de toestand) ---

def _post(url: str, body: bytes, headers: dict) -> int:
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=8) as r:   # noqa: S310 - https afgedwongen door deliver()
        return r.status


def deliver(entry: dict, cfg: dict) -> dict:
    """Levert één notificatie aan de ingestelde afnemers; geeft {afnemer: 'ok'|foutklasse}."""
    res = {}
    if entry.get("baseline"):
        return res   # bestaande toestand bij de eerste start: wel bewaard, niet extern verstuurd
    body = json.dumps({k: entry[k] for k in ("id", "at", "kind", "title", "text")}, ensure_ascii=False).encode()
    if cfg.get("ntfy_url", "").startswith("https://"):
        try:
            res["ntfy"] = "ok" if 200 <= _post(cfg["ntfy_url"], f"{entry['title']}\n{entry['text']}".encode(),
                                                 {"Title": entry["title"].encode("ascii", "replace").decode()[:100]}) < 300 else "http"
        except Exception as e:   # noqa: BLE001
            res["ntfy"] = type(e).__name__
    if cfg.get("webhook_url", "").startswith("https://"):
        headers = {"Content-Type": "application/json"}
        if os.environ.get("R2_NOTIFY_HMAC"):
            headers["X-R2-Signature"] = hmac.new(os.environ["R2_NOTIFY_HMAC"].encode(), body, hashlib.sha256).hexdigest()
        try:
            res["webhook"] = "ok" if 200 <= _post(cfg["webhook_url"], body, headers) < 300 else "http"
        except Exception as e:   # noqa: BLE001
            res["webhook"] = type(e).__name__
    if cfg.get("command"):
        try:
            r = subprocess.run(list(cfg["command"]), input=body, capture_output=True, timeout=20)
            res["command"] = "ok" if r.returncode == 0 else f"exit{r.returncode}"
        except (OSError, subprocess.SubprocessError) as e:
            res["command"] = type(e).__name__
    return res


def record_delivery(nid: str, res: dict) -> None:
    if not res:
        return
    path = notifier_dir() / "ledger.json"
    ledger = _read_json(path, {"open": {}})
    if nid in ledger["open"]:
        ledger["open"][nid]["delivered"].update(res)
        store.atomic_write(path, json.dumps(ledger, indent=1))


def retry_failed(cfg: dict) -> None:
    """Afnemers die faalden worden elke ronde opnieuw geprobeerd tot ze lukken (open conditie), de outbox blijft de waarheid."""
    path = notifier_dir() / "ledger.json"
    ledger = _read_json(path, {"open": {}})
    if not any(v != "ok" for e in ledger["open"].values() for v in e["delivered"].values()):
        return
    entries = {}
    try:
        for line in (notifier_dir() / "outbox.jsonl").read_text(encoding="utf-8").splitlines()[-2000:]:
            e = json.loads(line)
            entries[e["id"]] = e
    except (OSError, ValueError):
        return
    for nid, meta in ledger["open"].items():
        bad = {k for k, v in meta["delivered"].items() if v != "ok"}
        if bad and nid in entries and "title" in entries[nid]:
            only = {k: v for k, v in cfg.items() if k in ("ntfy_url", "webhook_url", "command") and
                    {"ntfy_url": "ntfy", "webhook_url": "webhook", "command": "command"}[k] in bad}
            record_delivery(nid, deliver({**entries[nid], "baseline": False}, {**only}))


# --- monitor-momentopname ---

def human_blocked(conds: list | None = None) -> list:
    """Wat Runner 2 NU werkelijk tegenhoudt: alleen condities van klasse `blokkade`. Een open actie hoort hier nooit bij."""
    ledger = _read_json(notifier_dir() / "ledger.json", {"open": {}})
    out = []
    for nid, meta in ledger["open"].items():
        if meta.get("class") == humanalerts.BLOCK:
            out.append({"id": nid, "kind": meta["kind"], "title": meta.get("title", ""), "since": meta.get("first_at"),
                        "gemeld": any(v == "ok" for v in (meta.get("delivered") or {}).values())})
    return sorted(out, key=lambda x: x["since"] or "")


def monitor_snapshot(nodes: dict | None) -> dict:
    states = store.all_tasks()
    ov = supervision.overview() or {"projects": [], "attention": 0, "labels": []}
    acts = actions.open_items(states)
    waiting = [s["task_id"] for s in states if s["status"] == "waiting_for_human"]
    running = [s["task_id"] for s in states if s["status"] == "running"]
    blocked = human_blocked()
    from . import cplane
    return {"at": _now(), "node": config.local_node(), "role": (cplane.role() or {}).get("role", "legacy"),
            "supervision": {"projects": len(ov["projects"]), "attention": ov["attention"], "labels": ov["labels"]},
            "human_actions_open": len(acts), "waiting_for_action": waiting, "running": running,
            "human_blocked": blocked, "dagoverzicht": digest_status(),
            "nodes": nodes or {}, "headline": headline(ov, len(acts), waiting, len(blocked))}


def headline(ov: dict, actions_open: int, waiting: list, blocked: int = 0) -> str:
    """`GEBLOKKEERD DOOR MENS` komt alleen uit echte blokkades; `ACTIE OPEN` is een telling van punten die niets tegenhouden."""
    parts = list(ov["labels"])
    if blocked:
        parts.insert(0, f"GEBLOKKEERD DOOR MENS · {blocked}")
    if waiting:
        parts.append(f"WACHT OP ACTIE · {len(waiting)}")
    if actions_open:
        parts.append(f"ACTIE OPEN · {actions_open}")
    return " | ".join(parts) or "RUSTIG"


# --- dagoverzicht (besluit 48): volgt de dagelijkse herbeoordeling, geen eigen timer ---

def _digest_path() -> Path:
    return notifier_dir() / "digest.json"


def digest_status() -> dict:
    d = _read_json(_digest_path(), {})
    return {k: d.get(k) for k in ("laatste_dag", "laatste_at", "verstuurd", "bron_ronde", "bron_gegenereerd")}


def _epoch_of(ts: str | None) -> float | None:
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z").timestamp()
    except (TypeError, ValueError):
        return None


def digest_round(cfg: dict, now: float | None = None) -> dict | None:
    """Stuurt hoogstens één dagoverzicht per dag, zodra de dagelijkse herbeoordeling een nieuw rapport heeft bewaard.

    Dezelfde dag opnieuw (handmatige ronde, herstart van de notifier, dubbel rapport) geeft niets nieuws. Zonder rapport (herbeoordeling
    uit of nog niet uitgerold) gebeurt er niets: er komt geen eigen planning bij. Een leeg overzicht wordt bewaard maar niet verstuurd."""
    now = time.time() if now is None else now
    rep = actionreassess.latest()
    if not rep or not rep.get("generated_at"):
        return None
    state = _read_json(_digest_path(), {})
    gen = rep["generated_at"]
    if gen == state.get("bron_gegenereerd"):
        return _retry_digest(cfg, state)
    day = gen[:10]
    baseline = not state and (now - (_epoch_of(gen) or 0)) > humanalerts.DIGEST_BASELINE_S
    if state.get("laatste_dag") == day:
        state["bron_gegenereerd"] = gen   # tweede ronde dezelfde dag: onthouden, niet nogmaals melden
        store.atomic_write(_digest_path(), json.dumps(state, indent=1))
        return None
    blockers = human_blocked()
    title, text = humanalerts.digest_text(rep, actionreassess.KLASSEN, blockers)
    quiet = humanalerts.mens_total(rep.get("counts") or {}, actionreassess.KLASSEN) == 0 and not blockers
    entry = {"id": _nid("human_digest", day), "at": _now(), "kind": "human_digest", "class": "overzicht", "key": day, "title": title,
             "text": text if not quiet else "Geen open punten die een menselijke beslissing vragen.", "baseline": baseline,
             "bron": {"ronde": rep.get("ronde"), "gegenereerd": gen}}
    _append_outbox(entry)
    res = {} if (baseline or quiet) else deliver(entry, cfg)
    state.update(laatste_dag=day, laatste_at=entry["at"], bron_ronde=rep.get("ronde"), bron_gegenereerd=gen, entry=entry, delivered=res,
                 verstuurd=bool(res) and all(v == "ok" for v in res.values()), pogingen=1 if res else 0,
                 reden_niet_verstuurd="baseline" if baseline else ("leeg" if quiet else None))
    store.atomic_write(_digest_path(), json.dumps(state, indent=1))
    log(f"dagoverzicht {entry['id']} ({day}): {'baseline' if baseline else 'leeg' if quiet else res or 'bewaard, geen afnemer'}", LOGNAME)
    return entry


def _retry_digest(cfg: dict, state: dict) -> None:
    """Een afnemer die het dagoverzicht niet haalde wordt hoogstens 5 keer opnieuw geprobeerd; het overzicht staat altijd in de outbox."""
    bad = {k for k, v in (state.get("delivered") or {}).items() if v != "ok"}
    if not bad or int(state.get("pogingen") or 0) >= 5 or not state.get("entry"):
        return None
    names = {"ntfy_url": "ntfy", "webhook_url": "webhook", "command": "command"}
    only = {k: v for k, v in cfg.items() if names.get(k) in bad}
    state["delivered"].update(deliver({**state["entry"], "baseline": False}, only))
    state["pogingen"] = int(state.get("pogingen") or 0) + 1
    state["verstuurd"] = all(v == "ok" for v in state["delivered"].values())
    store.atomic_write(_digest_path(), json.dumps(state, indent=1))
    return None


# --- ronde en dienst ---

_probe = {"at": 0.0, "nodes": {}}


def probe_nodes(cfg: dict) -> tuple:
    """(nodes, complete). Remote nodes worden hoogstens elke node_probe_s gecontroleerd; een enkele misser telt pas bij de tweede."""
    from . import remote
    if time.time() - _probe["at"] < cfg["node_probe_s"]:
        return _probe["nodes"], True
    fresh = remote.overview()
    prev = _probe["nodes"]
    for name, info in fresh.items():
        info["misses"] = 0 if info.get("reachable") else prev.get(name, {}).get("misses", 0) + 1
    _probe.update(at=time.time(), nodes=fresh)
    shown = {n: i for n, i in fresh.items() if i.get("reachable") or i["misses"] >= 2}
    return shown, True


def _deliver_new(new: list, cfg: dict) -> None:
    """Directe levering van nieuwe meldingen. Intern = niet extern; meer dan `max_direct` nieuwe blokkades = één bundelmelding."""
    direct = [e for e in new if e.get("class") != humanalerts.INTERNAL]
    blocks = [e for e in direct if e.get("class") == humanalerts.BLOCK and not e.get("baseline")]
    cap = int(cfg.get("max_direct", humanalerts.MAX_DIRECT_PER_ROUND))
    if len(blocks) > cap:
        bundle = {"id": _nid("human_bundle", ",".join(sorted(e["id"] for e in blocks))), "at": _now(), "kind": "human_bundle", "baseline": False,
                  "title": f"{len(blocks)} NIEUWE BLOKKADES DOOR EEN MENSELIJKE BESLISSING",
                  "text": humanalerts.redact("; ".join(e["title"] for e in blocks), 300)}
        res = deliver(bundle, cfg)
        for e in blocks:
            record_delivery(e["id"], res)
        direct = [e for e in direct if e not in blocks]
        log(f"bundelmelding {bundle['id']} voor {len(blocks)} blokkades: {res}", LOGNAME)
    for entry in direct:
        record_delivery(entry["id"], deliver(entry, cfg))
        log(f"melding {entry['id']} {entry['kind']}: {entry.get('title')}", LOGNAME)
    for entry in new:
        if entry.get("class") == humanalerts.INTERNAL:
            log(f"bewaard (niet extern) {entry['id']} {entry['kind']}: {entry.get('title')}", LOGNAME)


def _deliver_recovery(resolved: list, cfg: dict) -> None:
    """Een herstelbericht alleen voor een blokkade die eerder echt is gemeld (een afnemer zei ok); nooit voor baseline of onbezorgde meldingen."""
    for r in resolved:
        if r.get("class") == humanalerts.BLOCK and not r.get("baseline") and any(v == "ok" for v in r["delivered"].values()):
            deliver({"id": r["id"] + "-herstel", "at": _now(), "kind": r["kind"], "baseline": False,
                     "title": "HERSTELD · " + humanalerts.redact(r.get("title"), 120).removeprefix("HERSTELD · "),
                     "text": "De blokkade door een menselijke beslissing is opgeheven."}, cfg)


def run_round(cfg: dict | None = None) -> dict:
    from . import guardian
    guardian.tick()
    cfg = cfg or settings()
    from . import cplane
    nodes = {}
    try:
        nodes, _ = probe_nodes(cfg)
    except Exception as e:   # noqa: BLE001
        log(f"node-probe mislukt: {type(e).__name__}: {e}", LOGNAME)
    new, resolved = sync_ledger_full(conditions(nodes, cfg=cfg), cfg=cfg)
    _deliver_new(new, cfg)
    _deliver_recovery(resolved, cfg)
    retry_failed(cfg)
    try:
        digest_round(cfg)
    except Exception as e:   # noqa: BLE001 - het dagoverzicht mag de directe meldingen nooit tegenhouden
        log(f"dagoverzicht mislukt: {type(e).__name__}: {e}", LOGNAME)
    _rotate_outbox(int(cfg["keep_lines"]))
    snap = monitor_snapshot({n: {k: v for k, v in i.items() if k != "misses"} for n, i in nodes.items()})
    store.atomic_write(control_dir() / "monitor.json", json.dumps(snap, ensure_ascii=False, indent=1))
    store.atomic_write(notifier_dir() / "heartbeat.json", json.dumps({"at": _now(), "epoch": time.time(), "pid": os.getpid(),
                                                                       "new": len(new), "role": snap["role"]}))
    return {"new": len(new), "headline": snap["headline"]}


class _Wake(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _reply(self, code: int, body: bytes = b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._reply(200 if self.path == "/readyz" else 404, b"ok" if self.path == "/readyz" else b"")

    def do_POST(self):
        if self.path != "/wake":
            return self._reply(404)
        n = min(int(self.headers.get("Content-Length", "0") or 0), 65536)
        body = self.rfile.read(n)
        secret = os.environ.get("R2_WEBHOOK_SECRET", "")
        if secret:
            want = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(want, self.headers.get("X-R2-Signature", "")):
                return self._reply(401)
        _wake.set()   # alleen wekken: geen inhoud wordt vertrouwd; de ronde leest zelf de dossiers
        self._reply(202)


def serve_wake(port: int) -> ThreadingHTTPServer:
    srv = ThreadingHTTPServer(("127.0.0.1", port), _Wake)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def main() -> int:
    port = int(os.environ.get("R2_NOTIFY_PORT", DEFAULT_PORT))
    notifier_dir().mkdir(parents=True, exist_ok=True)
    try:
        serve_wake(port)
    except OSError as e:
        log(f"wake-poort {port} niet beschikbaar: {e}", LOGNAME)
    log(f"notifier gestart (pid {os.getpid()}, wake 127.0.0.1:{port})", LOGNAME)
    while True:
        cfg = settings()
        try:
            run_round(cfg)
        except Exception as e:   # noqa: BLE001 - de notifier moet blijven leven
            log(f"ronde mislukt: {type(e).__name__}: {e}", LOGNAME)
        _wake.wait(cfg["interval_s"])
        _wake.clear()


if __name__ == "__main__":
    sys.exit(main())
