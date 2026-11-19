"""Externe supervisie: een ChatGPT-regisseur boven de Claude-runners, zonder afhankelijkheid van chatgeheugen.

Lokaal en deterministisch (dit module, in de watch): events uitgeven bij het einde van een run, claim/lease, validatie en
toepassen van beslissingen, starten met vooraf toegekende id's, reconcile met de Runner-werkelijkheid, limieten, stop/pauze.
Extern (ChatGPT via de MCP-tools): alleen het oordeel - doel bereikt / uitvoerbaar werk resteert / echte menselijke blokkade.
Het dossier (`dossier`) is de enige bron van waarheid; een run is een assignment die via de gewone controller-start loopt.
"""
import hashlib
import hmac
import json
import os
import re
import time
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime

from . import config, dossier, store
from .log import log

LABELS = {"blocked": "SUPERVISIE GEBLOKKEERD", "needed": "SUPERVISIE NODIG", "claimed": "SUPERVISIE BEZIG",
          "waiting_human": "WACHT OP BEHEERDER", "running": "RUN LOOPT", "paused": "GEPAUZEERD", "stopped": "GESTOPT",
          "limit": "LIMIET BEREIKT", "complete": "PROJECT KLAAR", "idle": "IDLE"}
ATTENTION = ("blocked", "needed", "waiting_human", "limit")
NODES = ("MAC", "LINUX", "GERARD", "WINDOWS")
DECISIONS = ("goal_complete", "work_remaining", "human_blocked")
OPEN_EVENT = ("pending", "claimed", "escalated")
BACKLOG_STATES = ("open", "in_progress", "done", "blocked")
ACTION_CLASSES = ("blocking", "non_blocking", "outdated", "resolved")
CHAIN_LIMIT_KEYS = ("max_tasks", "max_hours", "max_no_progress")
TEXT_CAP = 8000
_RETRY: dict = {}          # assignment_id -> epoch van de eerstvolgende startpoging (niet bewaard; na herstart direct opnieuw)
_last_tick = [0.0]


class SupervisionError(Exception):
    def __init__(self, code: str, message: str, data: dict | None = None):
        super().__init__(message)
        self.code, self.message, self.data = code, message, data


def _time() -> float:
    return time.time()


def _epoch(ts: str) -> float:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z").timestamp()


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def _clip(text, n: int) -> str:
    text = " ".join(str(text or "").split())
    return text if len(text) <= n else text[: n - 1] + "…"


def _short_id(*parts) -> str:
    return hashlib.sha1(":".join(parts).encode()).hexdigest()[:12]


def event_id(pid: str, key: str) -> str:
    return "E-" + _short_id(pid, key)


def _call(tool: str, args: dict) -> dict:
    """Enige poort naar de Runner-werkelijkheid (start/status/result/stop/actions op een node); tests vervangen deze."""
    from . import controller_mcp
    return controller_mcp.dispatch(tool, args)


def _code(e: Exception) -> str:
    return getattr(e, "code", None) or "runner2_unavailable"


# --- transacties ---

class Txn:
    def __init__(self, pid: str, d: dict, cfg: dict):
        self.pid, self.d, self.cfg, self.records = pid, d, cfg, []

    def audit(self, kind: str, summary: str, /, **data) -> None:
        self.records.append((kind, summary, data))

    def commit(self) -> None:
        if self.records:
            dossier.save(self.pid, self.d, self.records)
            self.records = []


@contextmanager
def txn(pid: str, ignore_integrity: bool = False):
    """Lock + laden + controleren. Een geblokkeerd project geeft SupervisionError(supervision_blocked); er verandert niets."""
    cfg = config.load_config()
    with dossier.lock(pid):
        d, problems = dossier.load_checked(pid)
        if problems:
            raise SupervisionError("supervision_blocked", "SUPERVISIE GEBLOKKEERD: " + problems[0]["message"])
        if d["integrity"]["status"] == "blocked" and not ignore_integrity:
            raise SupervisionError("supervision_blocked", "SUPERVISIE GEBLOKKEERD: " + d["integrity"]["message"])
        t = Txn(pid, d, cfg)
        yield t
        t.commit()


def _event(d: dict, eid: str) -> dict | None:
    return next((e for e in d["events"] if e["id"] == eid), None)


def _assignment(d: dict, aid: str) -> dict | None:
    return next((a for a in d["assignments"] if a["assignment_id"] == aid), None)


def _live(d: dict) -> dict | None:
    return next((a for a in d["assignments"] if a["status"] in dossier.LIVE), None)


def _emit(t: Txn, kind: str, key: str, summary: str, assignment_id: str | None = None, **extra) -> dict:
    """Legt een supervisie-event vast; dezelfde sleutel geeft nooit een tweede event (idempotent bij replay/herstart)."""
    d = t.d
    eid = event_id(t.pid, key)
    if old := _event(d, eid):
        return old
    d["counters"]["event_seq"] += 1
    hook = "pending" if _webhook_url(t.cfg) else "off"
    ev = {"id": eid, "kind": kind, "key": key, "seq": d["counters"]["event_seq"], "created_at": dossier.now(), "state": "pending",
          "summary": dossier.redact(_clip(summary, 400)), "assignment_id": assignment_id, "claims_total": 0, "claim": None,
          "decision_n": None, "webhook": {"state": hook, "attempts": 0, "next_at": 0}, **extra}
    d["events"].append(ev)
    t.audit("event", f"{kind}: {ev['summary'][:120]}", event_id=eid, event_kind=kind, assignment_id=assignment_id)
    return ev


def _next_decision_n(d: dict) -> int:
    return (d["decisions"][-1]["n"] if d["decisions"] else 0) + 1


def _auto_decision(t: Txn, ev: dict, kind: str, reason: str) -> dict:
    """Deterministische beslissing van Runner 2 zelf (geen ChatGPT nodig): pauze/limiet/stop."""
    d = t.d
    rec = {"n": _next_decision_n(d), "at": dossier.now(), "event_id": ev["id"], "kind": kind, "reason": dossier.redact(_clip(reason, 400)),
           "why_next": "", "by": "runner2", "payload_sha": dossier.sha({"auto": kind, "event": ev["id"]}), "assignment_id": None}
    d["decisions"].append(rec)
    ev.update(state="applied", decision_n=rec["n"], applied_at=dossier.now())
    t.audit("decision", f"automatisch: {kind}: {rec['reason'][:120]}", event_id=ev["id"], n=rec["n"], kind=kind, by="runner2")
    return rec


# --- webhook (wekt alleen een always-on trigger; geen inhoud, nooit van invloed op de toestand) ---

def _webhook_url(cfg: dict) -> str:
    url = (cfg.get("supervision_webhook_url") or "").strip()
    if not url:
        return ""
    p = urllib.parse.urlparse(url)
    if p.scheme == "https" or (p.scheme == "http" and p.hostname in ("127.0.0.1", "localhost", "::1")):
        return url
    log(f"supervisie: webhook-url genegeerd (alleen https, of http op localhost): {p.scheme}://{p.hostname}")
    return ""


def _post(url: str, body: bytes, headers: dict, timeout: float) -> int:
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:   # noqa: S310 - schema is hierboven beperkt
        return r.status


def _deliver_webhooks(cfg: dict) -> None:
    url = _webhook_url(cfg)
    if not url:
        return
    secret = os.environ.get(cfg.get("supervision_webhook_secret_env") or "", "") if cfg.get("supervision_webhook_secret_env") else ""
    now = _time()
    for pid in dossier.project_ids():
        d, problems = dossier.read(pid)
        if problems or not d:
            continue
        for ev in d["events"]:
            hook = ev.get("webhook") or {}
            if ev["state"] not in ("pending", "claimed") or hook.get("state") != "pending" or hook.get("next_at", 0) > now:
                continue
            body = json.dumps({"type": "supervision_needed", "project_id": pid, "event_id": ev["id"], "kind": ev["kind"],
                               "at": dossier.now()}, separators=(",", ":")).encode()
            headers = {"Content-Type": "application/json"}
            if secret:
                headers["X-R2-Signature"] = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
            try:
                ok = 200 <= _post(url, body, headers, cfg["supervision_webhook_timeout_s"]) < 300
                err = "" if ok else "http-status"
            except Exception as e:   # noqa: BLE001 - een webhookfout raakt de toestand nooit
                ok, err = False, type(e).__name__
            try:
                with txn(pid) as t:
                    cur = _event(t.d, ev["id"])
                    if not cur or cur["state"] not in ("pending", "claimed"):
                        continue
                    h = cur["webhook"]
                    h["attempts"] += 1
                    if ok:
                        h.update(state="delivered", delivered_at=dossier.now())
                    elif h["attempts"] >= cfg["supervision_webhook_max_attempts"]:
                        h.update(state="failed", last_error=err)
                    else:
                        h.update(next_at=now + min(30 * 2 ** h["attempts"], 3600), last_error=err)
                    t.audit("webhook", f"webhook {h['state']} voor {ev['id']}", event_id=ev["id"], attempts=h["attempts"], error=err)
            except SupervisionError:
                continue


# --- limieten en vasthoudregels ---

def hold_reason(d: dict, cfg: dict) -> tuple | None:
    """(code, tekst) als er nu geen nieuwe opdracht gestart mag worden; None = vrij."""
    if not cfg.get("supervision_enabled", True):
        return "disabled", "supervisie staat uit (supervision_enabled=false)"
    c = d["control"]
    if c["state"] != "active":
        return c["state"], f"project is {c['state']}" + (f": {c['reason']}" if c.get("reason") else "")
    lim = d["limits"]
    if d["counters"]["assignments_total"] >= lim["max_assignments"]:
        return "limit_assignments", f"maximaal aantal opdrachten ({lim['max_assignments']}) bereikt"
    if (_time() - _epoch(d["created_at"])) / 3600 >= lim["max_hours"]:
        return "limit_hours", f"maximale looptijd ({lim['max_hours']} uur) bereikt"
    if d.get("deadline") and _time() >= _epoch(d["deadline"]):
        return "limit_deadline", "deadline van het project verstreken"
    if d["counters"]["no_progress"] >= lim["max_no_progress"]:
        return "limit_no_progress", f"{d['counters']['no_progress']} opdrachten zonder nieuwe commits (maximaal {lim['max_no_progress']})"
    return None


def _limits(cfg: dict, raw: dict | None) -> dict:
    raw = raw or {}
    out = {}
    for key, cap_key, kind in (("max_assignments", "supervision_cap_max_assignments", int), ("max_hours", "supervision_cap_max_hours", float),
                               ("max_no_progress", "supervision_cap_max_no_progress", int)):
        v = raw.get(key, cfg[f"supervision_default_{key}"])
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not 0 < v <= cfg[cap_key] or (kind is int and int(v) != v):
            raise SupervisionError("invalid_input", f"limits.{key} moet groter dan 0 en hoogstens {cfg[cap_key]} zijn")
        out[key] = kind(v)
    chain = raw.get("chain") or {}
    if not isinstance(chain, dict) or set(chain) - set(CHAIN_LIMIT_KEYS):
        raise SupervisionError("invalid_input", f"limits.chain mag alleen {CHAIN_LIMIT_KEYS} bevatten")
    out["chain"] = {k: chain[k] for k in CHAIN_LIMIT_KEYS if k in chain}
    return out


# --- invoer normaliseren ---

def _text(v, name: str, required: bool = True, cap: int = TEXT_CAP) -> str:
    if v is None or (isinstance(v, str) and not v.strip()):
        if required:
            raise SupervisionError("invalid_input", f"{name} ontbreekt")
        return ""
    if not isinstance(v, str):
        raise SupervisionError("invalid_input", f"{name} moet tekst zijn")
    return dossier.redact(v.strip())[:cap]


def _text_list(v, name: str, cap: int = 20) -> list:
    if v is None:
        return []
    if not isinstance(v, list) or not all(isinstance(i, str) for i in v):
        raise SupervisionError("invalid_input", f"{name} moet een lijst van teksten zijn")
    return [dossier.redact(i.strip())[:1000] for i in v if i.strip()][:cap]


def _targets(raw) -> list:
    if not isinstance(raw, list) or not raw:
        raise SupervisionError("invalid_input", "targets moet een niet-lege lijst van {node, repository} zijn")
    out = []
    for t in raw:
        if not isinstance(t, dict) or str(t.get("node", "")).upper() not in NODES or not isinstance(t.get("repository"), str) or not t["repository"].strip():
            raise SupervisionError("invalid_input", f"elk target heeft node (een van {NODES}) en repository nodig")
        out.append({"node": t["node"].upper(), "repository": t["repository"].strip()})
    return out


def _backlog_item(raw: dict, n: int) -> dict:
    if not isinstance(raw, dict):
        raise SupervisionError("invalid_input", "backlog-item moet een object zijn")
    prio = raw.get("priority", 3)
    if isinstance(prio, bool) or not isinstance(prio, int) or not 1 <= prio <= 9:
        raise SupervisionError("invalid_input", "backlog priority moet een geheel getal van 1 (hoogste) t/m 9 zijn")
    state = raw.get("state", "open")
    if state not in BACKLOG_STATES:
        raise SupervisionError("invalid_input", f"backlog state moet een van {BACKLOG_STATES} zijn")
    return {"id": _text(raw.get("id") or f"T{n}", "backlog id", cap=40), "title": _text(raw.get("title"), "backlog title", cap=300),
            "priority": prio, "depends_on": _text_list(raw.get("depends_on"), "depends_on", 40), "state": state,
            "blocked_by": _text_list(raw.get("blocked_by"), "blocked_by", 10), "note": _text(raw.get("note"), "note", False, 500)}


# --- registreren ---

def register(title, goal, targets, master_list=None, backlog=None, knowledge=None, limits=None, deadline_hours=None,
             idempotency_key=None) -> dict:
    cfg = config.load_config()
    title, goal = _text(title, "title", cap=200), _text(goal, "goal", cap=4000)
    if idempotency_key is not None and not (isinstance(idempotency_key, str) and idempotency_key.strip()):
        raise SupervisionError("invalid_input", "idempotency_key moet tekst zijn")
    pid = "P-" + (_short_id("register", idempotency_key.strip()) if idempotency_key else uuid.uuid4().hex[:12])
    if dossier.dossier_path(pid).exists():
        d, problems = dossier.read(pid)
        if d and not problems and d["goal"]["text"] == goal:
            return {"project_id": pid, "created": False, "project": summarize(pid)}
        raise SupervisionError("exists", f"idempotency_key hoort al bij een ander project ({pid})")
    ml = master_list or {}
    if not isinstance(ml, dict):
        raise SupervisionError("invalid_input", "master_list moet een object {refs, text} zijn")
    ml = {"refs": _text_list(ml.get("refs"), "master_list.refs", 40), "text": _text(ml.get("text"), "master_list.text", False, 12000)}
    items = [_backlog_item(b, i + 1) for i, b in enumerate(backlog or [])]
    kn = knowledge or {}
    kn = {"architecture": _text_list(kn.get("architecture"), "knowledge.architecture", 100),
          "beheerder_decisions": _text_list(kn.get("beheerder_decisions"), "knowledge.beheerder_decisions", 100)}
    deadline = None
    if deadline_hours is not None:
        if isinstance(deadline_hours, bool) or not isinstance(deadline_hours, (int, float)) or not 0 < deadline_hours <= cfg["supervision_cap_max_hours"]:
            raise SupervisionError("invalid_input", f"deadline_hours moet groter dan 0 en hoogstens {cfg['supervision_cap_max_hours']} zijn")
        deadline = _iso(_time() + deadline_hours * 3600)
    d = dossier.blank(pid, title, goal, _targets(targets), _limits(cfg, limits), ml, items, kn, deadline)
    with dossier.lock(pid):
        dossier.create(pid, d)
    with txn(pid) as t:
        _emit(t, "project_started", "project_started", f"project {title} geregistreerd; eerste opdracht formuleren")
    return {"project_id": pid, "created": True, "project": summarize(pid)}


# --- toestand en weergave ---

def _open_events(d: dict) -> list:
    return [e for e in d["events"] if e["state"] in OPEN_EVENT]


def _lease_expired(ev: dict, now: float) -> bool:
    return ev["state"] == "claimed" and (ev.get("claim") or {}).get("lease_until", 0) <= now


def state_of(d: dict, cfg: dict, now: float | None = None) -> str:
    now = _time() if now is None else now
    if d["integrity"]["status"] == "blocked":
        return "blocked"
    opened = _open_events(d)
    if any(e["state"] == "escalated" for e in opened):
        return "blocked"
    if any(e["state"] == "pending" or _lease_expired(e, now) for e in opened):
        return "needed"
    if any(e["state"] == "claimed" for e in opened):
        return "claimed"
    live = _live(d)
    if any(b["state"] == "open" for b in d["blockers"]) or (live and (live.get("observed") or {}).get("task_status") == "waiting_for_human"):
        return "waiting_human"
    if live:
        return "running"
    c = d["control"]["state"]
    if c in ("paused", "stopped"):
        return c
    if c == "complete":
        return "complete"
    if hold_reason(d, cfg):
        return "limit"
    return "idle"


def _executable(d: dict) -> list:
    """Open backlogitems waarvan alle afhankelijkheden klaar zijn en die niet op een open blokkade wachten."""
    done = {b["id"] for b in d["backlog"] if b["state"] == "done"}
    open_blockers = {b["id"] for b in d["blockers"] if b["state"] == "open"}
    return sorted((b for b in d["backlog"] if b["state"] in ("open", "in_progress") and set(b["depends_on"]) <= done
                   and not set(b["blocked_by"]) & open_blockers), key=lambda b: (b["priority"], b["id"]))


def _next_step(d: dict, state: str, cfg: dict) -> str:
    live = _live(d)
    ev = next(iter(_open_events(d)), None)
    if state == "blocked":
        if d["integrity"]["status"] == "blocked":
            return d["integrity"]["message"]
        return f"event {ev['id']} is geëscaleerd na {ev['claims_total']} claims; runner2_supervision_admin action=release_event"
    if state in ("needed", "claimed") and ev:
        return f"ChatGPT moet beslissen over {ev['kind']} ({ev['id']}): {ev['summary'][:160]}"
    if state == "waiting_human":
        bl = next((b for b in d["blockers"] if b["state"] == "open"), None)
        if bl:
            return f"wacht op Beheerder: {bl['question'][:200]}"
        return f"opdracht {live['assignment_id']} wacht op een antwoord van Beheerder" if live else "wacht op Beheerder"
    if state == "running" and live:
        return f"opdracht {live['assignment_id']} ({live['task_id']} op {live['node']}) {'start nog' if live['status'] == 'planned' else 'loopt'}"
    if state in ("paused", "stopped"):
        return f"project is {state}: {d['control'].get('reason') or 'geen reden vermeld'}; hervatten kan via admin action=resume"
    if state == "limit":
        return (hold_reason(d, cfg) or ("", ""))[1] + "; Beheerder moet limieten verruimen of het project afsluiten"
    if state == "complete":
        return "doel bevestigd door de supervisor"
    nxt = _executable(d)
    return f"volgende backlogitem: {nxt[0]['id']} {nxt[0]['title'][:120]}" if nxt else "geen open event; wacht op een nieuw event"


def _assignment_brief(a: dict) -> dict:
    out = {k: a.get(k) for k in ("assignment_id", "task_id", "chain_id", "node", "status", "outcome", "created_at", "started_at", "ended_at")}
    out["goal"] = _clip(a.get("goal"), 200)
    if a.get("observed"):
        out["observed"] = a["observed"]
    if a.get("last_error"):
        out["last_error"] = a["last_error"]
    return out


EVENT_REASONS = {
    "project_started": "nieuw project: de supervisor moet de eerste opdracht formuleren",
    "run_ended": "opdracht {aid} is afgelopen ({outcome}): de supervisor moet beoordelen of het projectdoel bereikt is of welke opdracht volgt",
    "goal_amended": "het projectdoel is gewijzigd: de supervisor moet het project opnieuw beoordelen",
    "project_resumed": "het project is hervat: de supervisor moet de volgende stap bepalen",
    "request_review": "er is om een herbeoordeling gevraagd: de supervisor moet het project opnieuw beoordelen",
    "blocker_resolved": "een menselijke blokkade is opgelost: de supervisor moet de volgende stap bepalen",
    "manual_assign": "handmatige opdracht van de beheerder"}
POINT_EVENTS = 5


def _ended_report(a: dict | None) -> dict | None:
    """Waar het eindrapport van een afgelopen opdracht staat: de laatste taak van de keten op de node van de opdracht."""
    if not a or a["status"] != "ended" or a.get("outcome") == "start_failed":
        return None
    return {"node": a["node"], "task_id": (a.get("result") or {}).get("tip_task") or a["task_id"]}


def _event_reason(d: dict, ev: dict) -> str:
    a = _assignment(d, ev["assignment_id"]) if ev.get("assignment_id") else None
    text = EVENT_REASONS.get(ev["kind"], ev["kind"] + ": de supervisor moet beslissen")
    return text.format(aid=ev.get("assignment_id") or "?", outcome=(a or {}).get("outcome") or ev.get("outcome") or "onbekend")


def _point_assignment(d: dict, opened: list) -> dict | None:
    """De opdracht waar het supervisiepunt over gaat: die van het nieuwste open event, anders de lopende, anders de laatste."""
    for ev in reversed(opened):
        if ev.get("assignment_id") and (a := _assignment(d, ev["assignment_id"])):
            return a
    return _live(d) or (d["assignments"][-1] if d["assignments"] else None)


def _action(aid: str, who: str, label: str, tool: str, args: dict, **extra) -> dict:
    return {"id": aid, "who": who, "label": label, "tool": tool, "args": args, **extra}


def _point_actions(d: dict, st: str, opened: list, cfg: dict) -> list:
    """Bestaande Runner 2-acties die dit supervisiepunt oplossen (alleen beschrijving; Monitor 2 of ChatGPT voert ze uit)."""
    pid, out = d["project_id"], []
    if d["integrity"]["status"] == "blocked":
        code = d["integrity"].get("code")
        out.append(_action("repair_accept" if code else "repair_restore_prev", "beheerder",
                           "Controleer de melding en hef de integriteitsblokkade op" if code else "Zet het dossier terug naar de vorige geldige versie",
                           "runner2_supervision_admin", {"project_id": pid, "action": "repair", "mode": "accept" if code else "restore_prev"}))
        return out
    if esc := next((e for e in opened if e["state"] == "escalated"), None):
        out.append(_action("release_event", "beheerder", f"Geef event {esc['id']} vrij zodat de supervisor het opnieuw kan claimen",
                           "runner2_supervision_admin", {"project_id": pid, "action": "release_event", "event_id": esc["id"]}))
        return out
    if st == "needed":
        ev = next(e for e in opened if e["state"] in ("pending", "claimed"))
        note = "Beslissen gaat per event, oudste eerst" + (f"; er staan {len(opened)} events open" if len(opened) > 1 else "")
        if ex := _executable(d):
            note += f". goal_complete wordt geweigerd zolang uitvoerbare backlogitems open staan ({', '.join(b['id'] for b in ex[:5])}): " \
                    "sluit die in dezelfde beslissing met backlog_updates"
        h = hold_reason(d, cfg)
        if h:
            note += f". Let op: een nieuwe opdracht (work_remaining) kan nu niet starten: {h[1]}"
        out.append(_action("decide", "supervisor", "Claim het event en beslis: doel bereikt, nieuwe opdracht of menselijke blokkade",
                           "runner2_supervision_claim", {"project_id": pid}, then="runner2_supervision_decide",
                           then_args={"project_id": pid, "event_id": ev["id"], "token": "<uit de claim>", "decision": {"kind": "…"}},
                           options=list(DECISIONS), note=note + "."))
        out.append(_action("context", "supervisor", "Lees eerst de volledige dossiercontext (alleen lezen)",
                           "runner2_supervision_context", {"project_id": pid, "event_id": ev["id"]}))
        if h and h[0].startswith("limit_"):
            out.append(_action("set_limits", "beheerder", "Verruim eerst de limiet als er nog een nieuwe opdracht moet volgen",
                               "runner2_supervision_admin", {"project_id": pid, "action": "set_limits", "limits": {},
                                                             "reset_no_progress": h[0] == "limit_no_progress",
                                                             **({"deadline_hours": "<uren vanaf nu>"} if h[0] == "limit_deadline" else {})}))
    elif st == "waiting_human":
        bl = [b for b in d["blockers"] if b["state"] == "open"]
        if bl:
            out.append(_action("resolve_blocker", "beheerder", "Beantwoord de vraag en markeer de blokkade als opgelost",
                               "runner2_supervision_admin", {"project_id": pid, "action": "resolve_blocker",
                                                             "blocker_ids": [b["id"] for b in bl[:20]], "note": "<antwoord>"}))
        live = _live(d)
        if live and (live.get("observed") or {}).get("task_status") == "waiting_for_human":
            tid = (live.get("observed") or {}).get("current_task") or live["task_id"]
            out.append(_action("answer_task", "beheerder", f"Beantwoord de vraag van taak {tid} op {live['node']}",
                               "runner2_answer", {"task_id": tid, "node": live["node"], "text": "<antwoord>"}))
    elif st == "limit":
        dl = (hold_reason(d, cfg) or ("",))[0] == "limit_deadline"
        out.append(_action("set_limits", "beheerder", "Verruim de limiet (of zet de teller zonder voortgang terug)",
                           "runner2_supervision_admin", {"project_id": pid, "action": "set_limits", "limits": {}, "reset_no_progress": False,
                                                         **({"deadline_hours": "<uren vanaf nu>"} if dl else {})}))
        out.append(_action("stop", "beheerder", "Sluit het project af als er niets meer moet gebeuren",
                           "runner2_supervision_admin", {"project_id": pid, "action": "stop", "reason": "<reden>"}))
    return out


def _point_blockade(d: dict, st: str, opened: list, cfg: dict, now: float) -> str | None:
    if d["integrity"]["status"] == "blocked":
        return d["integrity"]["message"]
    if esc := next((e for e in opened if e["state"] == "escalated"), None):
        return f"event {esc['id']} is geëscaleerd na {esc['claims_total']} claims zonder beslissing"
    if st == "waiting_human":
        bl = next((b for b in d["blockers"] if b["state"] == "open"), None)
        return f"wacht op Beheerder: {_clip(bl['question'], 300)}" if bl else "de lopende taak wacht op een antwoord van Beheerder"
    if st == "limit":
        return (hold_reason(d, cfg) or ("", ""))[1] or None
    if st == "needed":
        ev = opened[0]
        days = (now - _epoch(ev["created_at"])) / 86400
        claims = ev["claims_total"]
        parts = [f"geen technische blokkade; de beslissing van de supervisor ontbreekt al {days:.1f} dagen"
                 + (f" ({claims} claims zonder beslissing)" if claims else " (nog nooit geclaimd)")]
        if ev["state"] == "claimed" and _lease_expired(ev, now):
            parts.append(f"de claim van {(ev.get('claim') or {}).get('by', '?')} is verlopen")
        if h := hold_reason(d, cfg):
            parts.append(f"nieuwe opdrachten kunnen niet starten: {h[1]}")
        return "; ".join(parts)
    return None


def _point(d: dict, st: str, opened: list, cfg: dict) -> dict:
    """Eén supervisiepunt voor Monitor 2: Runner, opdracht, taak, reden, status/blokkade, acties en het eindrapport.
    Alles volgt uit het dossier op het moment van lezen; er is geen aparte administratie."""
    now = _time()
    a = _point_assignment(d, opened)
    ev = opened[0] if opened else None
    nodes = sorted({x["node"] for x in d["targets"]})
    res = (a or {}).get("result") or {}
    obs = (a or {}).get("observed") or {}
    if ev:
        reason = {"code": "event_" + ev["state"], "event_id": ev["id"], "event_kind": ev["kind"], "text": _event_reason(d, ev),
                  "summary": _clip(ev["summary"], 300), "since": ev["created_at"]}
    elif d["integrity"]["status"] == "blocked":
        reason = {"code": "integrity", "text": "het dossier is geblokkeerd: " + _clip(d["integrity"]["message"], 300)}
    elif st == "waiting_human":
        reason = {"code": "waiting_human", "text": "er wacht een vraag op een antwoord van Beheerder"}
    else:
        reason = {"code": st, "text": _clip((hold_reason(d, cfg) or ("", LABELS[st]))[1], 300)}
    return {
        "runner": (a or {}).get("node") or (nodes[0] if len(nodes) == 1 else None), "runners": nodes,
        "repository": (a or {}).get("repo") or (d["targets"][0]["repository"] if len(d["targets"]) == 1 else None),
        "assignment": ({"assignment_id": a["assignment_id"], "goal": _clip(a.get("goal"), 300), "status": a["status"],
                        "outcome": a.get("outcome"), "started_at": a.get("started_at"), "ended_at": a.get("ended_at"),
                        "summary": _clip((res.get("stopped_because") or {}).get("message") or res.get("summary"), 400) or None}
                       if a else None),
        "task_id": (a or {}).get("task_id"), "tip_task": res.get("tip_task") or obs.get("current_task") or (a or {}).get("task_id"),
        "reason": reason,
        "events": [_point_event(d, e) for e in opened[:POINT_EVENTS]],
        "status": {"state": st, "label": LABELS[st], "control": d["control"]["state"], "assignment_status": (a or {}).get("status"),
                   "task_status": (res.get("final_status") if (a or {}).get("status") == "ended" else None) or obs.get("task_status"), "blockade": _point_blockade(d, st, opened, cfg, now)},
        "actions": _point_actions(d, st, opened, cfg),
        "runner_actions_open": sum(1 for v in d["runner_actions"].values() if v.get("class") in ("blocking", "non_blocking")),
        "report": _ended_report(a)}


def _point_event(d: dict, e: dict) -> dict:
    a = _assignment(d, e["assignment_id"]) if e.get("assignment_id") else None
    return {"id": e["id"], "kind": e["kind"], "state": e["state"], "since": e["created_at"], "text": _event_reason(d, e),
            "assignment_id": e.get("assignment_id"), "task_id": (a or {}).get("task_id"), "report": _ended_report(a)}


def _broken_point(pid: str, msg: str) -> dict:
    return {"runner": None, "runners": [], "repository": None, "assignment": None, "task_id": None, "tip_task": None,
            "reason": {"code": "integrity", "text": "het dossier is onleesbaar of beschadigd: " + _clip(msg, 300)}, "events": [],
            "status": {"state": "blocked", "label": LABELS["blocked"], "control": None, "assignment_status": None, "task_status": None,
                       "blockade": msg},
            "actions": [_action("repair_restore_prev", "beheerder", "Zet het dossier terug naar de vorige geldige versie",
                                "runner2_supervision_admin", {"project_id": pid, "action": "repair", "mode": "restore_prev"})],
            "runner_actions_open": 0, "report": None}


def summarize(pid: str, d: dict | None = None, problems: list | None = None) -> dict:
    """Compacte projectweergave voor status/Monitor 2; lock-vrij en zonder netwerk."""
    cfg = config.load_config()
    if d is None:
        d, problems = dossier.read(pid)
    if not d or problems:
        msg = (problems or [{"message": "dossier onleesbaar"}])[0]["message"]
        return {"project_id": pid, "title": "", "state": "blocked", "label": LABELS["blocked"], "attention": True,
                "next_step": msg, "integrity": {"status": "blocked", "message": msg, "problems": problems or []},
                "point": _broken_point(pid, msg)}
    st = state_of(d, cfg)
    live = _live(d)
    opened = _open_events(d)
    last = d["decisions"][-1] if d["decisions"] else None
    c = d["counters"]
    return {"project_id": pid, "title": d["title"], "state": st, "label": LABELS[st], "attention": st in ATTENTION,
            "next_step": _next_step(d, st, cfg), "control": d["control"], "integrity": d["integrity"],
            "checkpoint": {k: d["checkpoint"][k] for k in ("revision", "at", "event_id", "decision_n", "assignment_id")},
            "goal": _clip(d["goal"]["text"], 200), "goal_version": d["goal"]["version"],
            "assignment": _assignment_brief(live) if live else None,
            "events_open": len(opened), "event": ({k: opened[0][k] for k in ("id", "kind", "state", "summary", "created_at")} if opened else None),
            "blockers_open": [{"id": b["id"], "question": _clip(b["question"], 200)} for b in d["blockers"] if b["state"] == "open"][:5],
            "backlog": {"open": sum(1 for b in d["backlog"] if b["state"] in ("open", "in_progress")),
                        "done": sum(1 for b in d["backlog"] if b["state"] == "done"),
                        "blocked": sum(1 for b in d["backlog"] if b["state"] == "blocked")},
            "usage": {"assignments": f"{c['assignments_total']}/{d['limits']['max_assignments']}",
                      "no_progress": f"{c['no_progress']}/{d['limits']['max_no_progress']}"},
            "last_decision": ({"n": last["n"], "kind": last["kind"], "at": last["at"], "by": last["by"], "reason": _clip(last["reason"], 160)}
                              if last else None),
            "point": _point(d, st, opened, cfg) if st in ATTENTION else None}


def overview() -> dict | None:
    """Alle projecten compact (voor runner2_status/Monitor 2); None als er geen enkel project is."""
    pids = dossier.project_ids()
    if not pids:
        return None
    items = [summarize(p) for p in pids]
    return {"projects": items, "attention": sum(1 for i in items if i["attention"]),
            "labels": sorted({i["label"] for i in items if i["attention"]}), "at": dossier.now()}


def reference(s: dict) -> dict | None:
    """Blok voor status/result van een taak die bij een supervisieproject hoort (via task- of chain-id)."""
    for pid in dossier.project_ids():
        d, problems = dossier.read(pid)
        if problems or not d:
            continue
        for a in d["assignments"]:
            if a.get("task_id") == s["task_id"] or (s.get("chain_id") and a.get("chain_id") == s.get("chain_id")):
                sm = summarize(pid, d, problems)
                return {"project_id": pid, "title": d["title"], "assignment_id": a["assignment_id"], "state": sm["state"],
                        "label": sm["label"], "assignment_status": a["status"]}
    return None


def status(project_id: str | None = None) -> dict:
    if not project_id:
        return overview() or {"projects": [], "attention": 0, "labels": []}
    d, problems = dossier.read(_pid(project_id))
    out = summarize(project_id, d, problems)
    if d and not problems:
        out["recent_decisions"] = [{k: x.get(k) for k in ("n", "kind", "at", "by", "reason", "why_next")} for x in d["decisions"][-5:]]
        out["recent_assignments"] = [_assignment_brief(a) for a in d["assignments"][-5:]]
        out["events"] = [{k: e.get(k) for k in ("id", "kind", "state", "created_at", "claims_total")} for e in d["events"][-8:]]
    return out


def _pid(pid) -> str:
    try:
        dossier.project_dir(pid)
    except dossier.DossierError as e:
        raise SupervisionError(e.code, e.message) from None
    if not dossier.dossier_path(pid).exists() and not dossier.project_dir(pid).exists():
        raise SupervisionError("not_found", f"project {pid} bestaat niet")
    return pid


def context(project_id: str, event_id_: str | None = None) -> dict:
    """Alles wat een verse ChatGPT-sessie nodig heeft, compact, uit het dossier (nooit uit chatgeheugen)."""
    d, problems = dossier.read(_pid(project_id))
    if problems or not d:
        raise SupervisionError("supervision_blocked", "SUPERVISIE GEBLOKKEERD: " + (problems or [{"message": "dossier onleesbaar"}])[0]["message"])
    return _context(d, event_id_)


def _context(d: dict, event_id_: str | None = None) -> dict:
    cfg = config.load_config()
    ev = _event(d, event_id_) if event_id_ else next(iter(_open_events(d)), None)
    sm = summarize(d["project_id"], d, [])
    out = {"project": {k: sm[k] for k in ("project_id", "title", "state", "label", "next_step", "usage")} | {
               "control": d["control"], "limits": d["limits"], "deadline": d["deadline"], "checkpoint": d["checkpoint"],
               "schema_version": d["schema_version"]},
           "goal": {"version": d["goal"]["version"], "text": d["goal"]["text"], "sha256": d["goal"]["sha256"],
                    "amendments": [{k: a.get(k) for k in ("version", "at", "by", "reason")} for a in d["goal"]["amendments"][-3:]]},
           "master_list": d["master_list"], "targets": d["targets"], "knowledge": d["knowledge"], "status_items": d["status_items"][-30:],
           "backlog": [b if b["state"] != "done" else {"id": b["id"], "state": "done", "title": _clip(b["title"], 80)} for b in d["backlog"]],
           "executable_now": [b["id"] for b in _executable(d)],
           "blockers": [b for b in d["blockers"] if b["state"] == "open"],
           "runner_actions": {k: v for k, v in d["runner_actions"].items() if v.get("class") in ("blocking", "non_blocking")},
           "assignments": {"total": len(d["assignments"]), "recent": [_assignment_brief(a) | {"result": _clip(json.dumps(a.get("result") or {}, ensure_ascii=False), 700)}
                                                                      for a in d["assignments"][-4:]]},
           "decisions": [{k: x.get(k) for k in ("n", "kind", "at", "by", "reason", "why_next", "assignment_id")} for x in d["decisions"][-5:]],
           "hold": (lambda h: {"code": h[0], "message": h[1]} if h else None)(hold_reason(d, cfg)),
           "decision_help": {
               "goal_complete": "evidence[] verplicht; geweigerd zolang uitvoerbare backlogitems open staan (sluit ze in dezelfde beslissing met backlog_updates)",
               "work_remaining": "reason, why_next, next_goal, next_prompt verplicht; node/repository uit targets; optioneel backlog_ids, chain_limits",
               "human_blocked": "human_blockers[{title, question, blocks[]}] of question; geweigerd zolang uitvoerbaar werk resteert",
               "optioneel": "backlog_updates, status_updates, action_classes, resolve_actions, knowledge_add"}}
    if ev:
        out["event"] = {k: ev.get(k) for k in ("id", "kind", "state", "summary", "created_at", "assignment_id", "claims_total")}
        a = _assignment(d, ev["assignment_id"]) if ev.get("assignment_id") else None
        if a:
            out["run"] = _assignment_brief(a) | {"prompt": _clip(a.get("prompt"), 1500), "result": a.get("result"), "backlog_ids": a.get("backlog_ids")}
    return out


# --- claim / lease ---

def _claim_candidate(d: dict, now: float) -> dict | None:
    ev = next(iter(_open_events(d)), None)
    if not ev or ev["state"] == "escalated":
        return None
    if ev["state"] == "claimed" and not _lease_expired(ev, now):
        return None
    return ev


def claim(project_id: str | None = None, claimer: str = "chatgpt", lease_s: int | None = None) -> dict:
    """Geeft het oudste openstaande event + token + context, of {"claimed": false}. Een verlopen lease mag worden overgenomen."""
    cfg = config.load_config()
    lease = cfg["supervision_lease_s"] if lease_s is None else lease_s
    if isinstance(lease, bool) or not isinstance(lease, int) or lease < 30:
        raise SupervisionError("invalid_input", "lease_s moet een geheel aantal seconden van minstens 30 zijn")
    lease = min(lease, cfg["supervision_lease_cap_s"])
    claimer = _text(claimer or "chatgpt", "claimer", cap=80)
    pids = [_pid(project_id)] if project_id else dossier.project_ids()
    now = _time()
    cands = []
    blocked = []
    for pid in pids:
        d, problems = dossier.read(pid)
        if problems or not d or d["integrity"]["status"] == "blocked":
            blocked.append(pid)
            continue
        if ev := _claim_candidate(d, now):
            cands.append((ev["created_at"], ev["seq"], pid))
    for _, _, pid in sorted(cands):
        try:
            with txn(pid) as t:
                ev = _claim_candidate(t.d, _time())
                if not ev:
                    continue
                if ev["claims_total"] >= cfg["supervision_max_claims"]:
                    ev["state"] = "escalated"
                    t.audit("escalated", f"event {ev['id']} geëscaleerd na {ev['claims_total']} claims zonder beslissing", event_id=ev["id"])
                    continue
                token = uuid.uuid4().hex
                ev["claims_total"] += 1
                ev["state"] = "claimed"
                ev["claim"] = {"by": claimer, "token_sha": hashlib.sha256(token.encode()).hexdigest(), "leased_at": dossier.now(),
                               "lease_until": _time() + lease, "lease_until_iso": _iso(_time() + lease)}
                t.audit("claim", f"event {ev['id']} geclaimd door {claimer} (poging {ev['claims_total']})", event_id=ev["id"], by=claimer)
                return {"claimed": True, "project_id": pid, "event_id": ev["id"], "token": token,
                        "lease_until": ev["claim"]["lease_until_iso"], "attempt": ev["claims_total"], "context": _context(t.d, ev["id"])}
        except SupervisionError:
            blocked.append(pid)
    return {"claimed": False, "blocked_projects": blocked, "note": "geen openstaand event" if not blocked else
            "geen claimbaar event; geblokkeerde projecten: " + ", ".join(blocked)}


# --- beslissen ---

def _normalize_decision(raw) -> dict:
    if not isinstance(raw, dict):
        raise SupervisionError("invalid_decision", "decision moet een object zijn")
    kind = raw.get("kind")
    if kind not in DECISIONS:
        raise SupervisionError("invalid_decision", f"decision.kind moet een van {DECISIONS} zijn")
    known = {"kind", "reason", "evidence", "why_next", "next_goal", "next_prompt", "node", "repository", "backlog_ids", "chain_limits",
             "question", "human_blockers", "backlog_updates", "status_updates", "action_classes", "resolve_actions", "knowledge_add"}
    if extra := set(raw) - known:
        raise SupervisionError("invalid_decision", f"onbekende velden in decision: {sorted(extra)}")
    dec = {"kind": kind, "reason": _text(raw.get("reason"), "reason", cap=1500)}
    dec["evidence"] = _text_list(raw.get("evidence"), "evidence")
    dec["backlog_updates"] = []
    for i, b in enumerate(raw.get("backlog_updates") or []):
        if not isinstance(b, dict) or not b.get("id"):
            raise SupervisionError("invalid_decision", "elk backlog_updates-item heeft een id")
        if "state" in b and b["state"] not in BACKLOG_STATES:
            raise SupervisionError("invalid_decision", f"backlog state moet een van {BACKLOG_STATES} zijn")
        dec["backlog_updates"].append(dossier.redact_deep(b))
    dec["status_updates"] = []
    for s in raw.get("status_updates") or []:
        if not isinstance(s, dict) or not s.get("item") or s.get("state") not in ("done", "partial", "open"):
            raise SupervisionError("invalid_decision", "status_updates: {item, state: done|partial|open, evidence}")
        dec["status_updates"].append({"item": _text(s["item"], "status item", cap=200), "state": s["state"],
                                      "evidence": _text(s.get("evidence"), "status evidence", False, 600)})
    ac = raw.get("action_classes") or {}
    if not isinstance(ac, dict) or any(v not in ACTION_CLASSES for v in ac.values()):
        raise SupervisionError("invalid_decision", f"action_classes: {{action_id: een van {ACTION_CLASSES}}}")
    dec["action_classes"] = {str(k): v for k, v in ac.items()}
    dec["resolve_actions"] = _text_list(raw.get("resolve_actions"), "resolve_actions", 30)
    kn = raw.get("knowledge_add") or {}
    if not isinstance(kn, dict) or set(kn) - {"architecture", "beheerder_decisions"}:
        raise SupervisionError("invalid_decision", "knowledge_add: {architecture[], beheerder_decisions[]}")
    dec["knowledge_add"] = {k: _text_list(v, f"knowledge_add.{k}", 20) for k, v in kn.items()}
    if kind == "goal_complete":
        if not dec["evidence"]:
            raise SupervisionError("invalid_decision", "goal_complete vereist evidence[] (wat is geverifieerd, waar)")
    elif kind == "work_remaining":
        for k in ("why_next", "next_goal", "next_prompt"):
            dec[k] = _text(raw.get(k), k, cap=TEXT_CAP if k == "next_prompt" else 1500)
        dec["node"] = str(raw["node"]).upper() if raw.get("node") else None
        dec["repository"] = _text(raw.get("repository"), "repository", False, 500) or None
        dec["backlog_ids"] = _text_list(raw.get("backlog_ids"), "backlog_ids", 40)
        cl = raw.get("chain_limits") or {}
        if not isinstance(cl, dict) or set(cl) - set(CHAIN_LIMIT_KEYS):
            raise SupervisionError("invalid_decision", f"chain_limits mag alleen {CHAIN_LIMIT_KEYS} bevatten")
        dec["chain_limits"] = cl
    else:
        dec["question"] = _text(raw.get("question"), "question", False, 1000)
        blockers = []
        for b in raw.get("human_blockers") or []:
            if not isinstance(b, dict) or not (b.get("question") or b.get("title")):
                raise SupervisionError("invalid_decision", "human_blockers: {title, question, blocks[]}")
            blockers.append({"title": _text(b.get("title") or b.get("question"), "blocker title", cap=200),
                             "question": _text(b.get("question") or b.get("title"), "blocker question", cap=1000),
                             "blocks": _text_list(b.get("blocks"), "blocks", 40)})
        if not blockers and dec["question"]:
            blockers = [{"title": _clip(dec["question"], 100), "question": dec["question"], "blocks": []}]
        if not blockers:
            raise SupervisionError("invalid_decision", "human_blocked vereist human_blockers[] of question")
        dec["human_blockers"] = blockers
    return dec


def _apply_updates(t: Txn, dec: dict) -> None:
    d = t.d
    by_id = {b["id"]: b for b in d["backlog"]}
    for upd in dec["backlog_updates"]:
        cur = by_id.get(upd["id"])
        if cur is None:
            cur = _backlog_item({**upd, "state": upd.get("state", "open")}, len(d["backlog"]) + 1)
            d["backlog"].append(cur)
            by_id[cur["id"]] = cur
            continue
        if "title" in upd:
            cur["title"] = _text(upd["title"], "title", cap=300)
        if "priority" in upd:
            cur["priority"] = _backlog_item({"title": "x", "priority": upd["priority"]}, 0)["priority"]
        for k in ("depends_on", "blocked_by"):
            if k in upd:
                cur[k] = _text_list(upd[k], k, 40)
        if "note" in upd:
            cur["note"] = _text(upd["note"], "note", False, 500)
        if "state" in upd:
            cur["state"] = upd["state"]
    for s in dec["status_updates"]:
        d["status_items"] = [x for x in d["status_items"] if x["item"] != s["item"]] + [{**s, "at": dossier.now()}]
    for k, items in dec["knowledge_add"].items():
        d["knowledge"][k] = d["knowledge"].get(k, []) + [i for i in items if i not in d["knowledge"].get(k, [])]
    for aid, cls in dec["action_classes"].items():
        cur = d["runner_actions"].setdefault(aid, {"first_seen": dossier.now()})
        cur["class"] = cls


def _check_target(d: dict, node: str | None, repo: str | None) -> tuple:
    targets = d["targets"]
    if not node:
        nodes = {x["node"] for x in targets}
        if len(nodes) != 1:
            raise SupervisionError("invalid_decision", f"node ontbreekt; kies uit {sorted(nodes)}")
        node = next(iter(nodes))
    cands = [x for x in targets if x["node"] == node]
    if not cands:
        raise SupervisionError("target_not_allowed", f"node {node} is geen toegestaan target van dit project")
    if repo is None:
        if len(cands) != 1:
            raise SupervisionError("invalid_decision", "repository ontbreekt; meerdere toegestane repositories op deze node")
        repo = cands[0]["repository"]
    if not any(x["repository"] == repo for x in cands):
        raise SupervisionError("target_not_allowed", f"repository {repo} is geen toegestaan target op {node}")
    return node, repo


def _fingerprint(node: str, repo: str, goal: str, prompt: str) -> str:
    return dossier.sha(" ".join(f"{node}|{repo}|{goal}|{prompt}".lower().split()))


def _plan_assignment(t: Txn, ev: dict, decision_n: int, node: str | None, repo: str | None, goal: str, prompt: str,
                     backlog_ids: list, chain_limits: dict) -> dict:
    """Controleert de harde regels en legt de opdracht (status planned, met vooraf toegekende id's) vast. Start niets."""
    d = t.d
    if h := hold_reason(d, t.cfg):
        raise SupervisionError("hold_" + h[0], f"geen nieuwe opdracht mogelijk: {h[1]}")
    if _live(d):
        raise SupervisionError("assignment_live", f"opdracht {_live(d)['assignment_id']} loopt nog; één lopende opdracht per project")
    node, repo = _check_target(d, node, repo)
    fp = _fingerprint(node, repo, goal, prompt)
    if any(a.get("fingerprint") == fp for a in d["assignments"][-3:]):
        raise SupervisionError("duplicate_assignment", "dezelfde opdracht is zojuist al gegeven; formuleer een andere of kies human_blocked")
    known = {b["id"] for b in d["backlog"]}
    if missing := [b for b in backlog_ids if b not in known]:
        raise SupervisionError("invalid_decision", f"onbekende backlog_ids: {missing}")
    n = d["counters"]["assignments_total"] + 1
    tid = "R2-" + _short_id(ev["id"], "task")
    a = {"assignment_id": f"A{n}", "n": n, "event_id": ev["id"], "decision_n": decision_n, "task_id": tid, "chain_id": "C-" + tid[3:],
         "node": node, "repo": repo, "goal": goal, "prompt": prompt, "prompt_sha": dossier.sha(prompt), "fingerprint": fp,
         "backlog_ids": backlog_ids, "chain_limits": chain_limits, "status": "planned", "outcome": None,
         "created_at": dossier.now(), "created_epoch": _time(), "started_at": None, "ended_at": None, "start_failures": 0,
         "last_error": None, "observed": None, "result": None}
    d["assignments"].append(a)
    d["counters"]["assignments_total"] = n
    for b in d["backlog"]:
        if b["id"] in backlog_ids and b["state"] == "open":
            b["state"] = "in_progress"
    t.audit("assignment", f"opdracht {a['assignment_id']} gepland op {node}: {goal[:100]}", assignment_id=a["assignment_id"],
            task_id=tid, node=node, repo=repo, event_id=ev["id"])
    return a


def _result_of(d: dict, rec: dict, replay: bool = False) -> dict:
    out = {"recorded": True, "replayed": replay, "decision_n": rec["n"], "kind": rec["kind"], "event_id": rec["event_id"]}
    if rec.get("assignment_id"):
        a = _assignment(d, rec["assignment_id"])
        out["assignment"] = _assignment_brief(a) if a else None
        out["started"] = bool(a and a["status"] in ("started", "ended"))
        out["queued"] = bool(a and a["status"] == "planned")
    out["project"] = {k: v for k, v in summarize(d["project_id"], d, []).items() if k in ("project_id", "state", "label", "next_step")}
    return out


def decide(project_id: str, event_id_: str, token: str, decision: dict) -> dict:
    cfg = config.load_config()
    try:
        dec = _normalize_decision(decision)
    except SupervisionError as e:
        raise SupervisionError("invalid_decision", e.message) from None
    psha = dossier.sha(dec)
    pid = _pid(project_id)
    resolve_ids: list = []
    with txn(pid) as t:
        d = t.d
        ev = _event(d, event_id_)
        if not ev:
            raise SupervisionError("not_found", f"event {event_id_} bestaat niet in project {pid}")
        if ev["state"] in ("decided", "applied"):
            prior = next((x for x in d["decisions"] if x["n"] == ev["decision_n"]), None)
            if prior and prior["payload_sha"] == psha:
                return _result_of(d, prior, replay=True)
            raise SupervisionError("already_decided", f"event {ev['id']} is al beslist (beslissing {ev['decision_n']}) met andere inhoud")
        if ev["state"] == "escalated":
            raise SupervisionError("event_escalated", "dit event is geëscaleerd; een mens moet het eerst vrijgeven (admin action=release_event)")
        claim_ = ev.get("claim") or {}
        if ev["state"] != "claimed" or hashlib.sha256(str(token).encode()).hexdigest() != claim_.get("token_sha"):
            raise SupervisionError("lease_lost", "token hoort niet bij de huidige claim van dit event (verlopen en overgenomen, of onjuist)")
        _apply_updates(t, dec)
        rec = {"n": _next_decision_n(d), "at": dossier.now(), "event_id": ev["id"], "kind": dec["kind"], "reason": dec["reason"],
               "why_next": dec.get("why_next", ""), "by": claim_.get("by", "chatgpt"), "payload_sha": psha, "assignment_id": None,
               "evidence": dec["evidence"]}
        assignment = None
        if dec["kind"] == "goal_complete":
            if _live(d):
                raise SupervisionError("assignment_live", "er loopt nog een opdracht; het project kan nog niet klaar zijn")
            if open_ := _executable(d):
                raise SupervisionError("work_remains", "goal_complete geweigerd: uitvoerbaar werk staat nog open in de backlog "
                                       f"({', '.join(b['id'] for b in open_[:5])}); sluit het (backlog_updates) of kies work_remaining")
            d["control"] = {"state": "complete", "reason": _clip(dec["reason"], 300), "at": dossier.now(), "by": rec["by"]}
        elif dec["kind"] == "human_blocked":
            ids = []
            for b in dec["human_blockers"]:
                d["counters"]["blocker_seq"] += 1
                bid = f"B{d['counters']['blocker_seq']}"
                ids.append(bid)
                d["blockers"].append({"id": bid, "title": b["title"], "question": b["question"], "state": "open", "created_at": dossier.now(),
                                      "event_id": ev["id"], "blocks": b["blocks"]})
                for it in d["backlog"]:
                    if it["id"] in b["blocks"]:
                        it["blocked_by"] = sorted(set(it["blocked_by"]) | {bid})
                        if it["state"] in ("open", "in_progress"):
                            it["state"] = "blocked"
            if not hold_reason(d, cfg) and (ex := _executable(d)):
                raise SupervisionError("work_remains", "human_blocked geweigerd: uitvoerbaar werk zonder menselijke blokkade resteert "
                                       f"({', '.join(b['id'] for b in ex[:5])}); kies work_remaining of koppel die items aan de blokkade (blocks)")
            rec["blockers"] = ids
        else:
            assignment = _plan_assignment(t, ev, rec["n"], dec["node"], dec["repository"], dec["next_goal"], dec["next_prompt"],
                                          dec["backlog_ids"], dec["chain_limits"])
            rec["assignment_id"] = assignment["assignment_id"]
        d["decisions"].append(rec)
        ev.update(state="decided" if assignment else "applied", decision_n=rec["n"], decided_at=dossier.now())
        t.audit("decision", f"{rec['kind']}: {rec['reason'][:140]}", event_id=ev["id"], n=rec["n"], kind=rec["kind"], by=rec["by"],
                assignment_id=rec["assignment_id"], payload_sha=psha)
        t.commit()    # beslissing + geplande opdracht (met vooraf toegekende task-id) zijn nu duurzaam, voordat er iets gestart wordt
        resolve_ids = dec["resolve_actions"]
        if assignment:
            _start_assignment(t, assignment)
        out = _result_of(d, rec)
        node_for_resolve = (assignment or {}).get("node")
    if resolve_ids:
        _resolve_runner_actions(pid, resolve_ids, node_for_resolve)
    return out


def _resolve_runner_actions(pid: str, ids: list, node: str | None) -> None:
    """Sluit runneracties (best effort, nooit blokkerend); legt het resultaat in het dossier vast."""
    d, problems = dossier.read(pid)
    if problems or not d:
        return
    nodes = [node] if node else sorted({t["node"] for t in d["targets"]})
    done: list = []
    for n in nodes:
        try:
            r = _call("runner2_actions", {"node": n, "resolve": ids, "note": "afgehandeld door de supervisor (dossier " + pid + ")"})
            done += r.get("resolved", []) + r.get("already_resolved", [])
        except Exception as e:   # noqa: BLE001
            log(f"supervisie {pid}: acties sluiten op {n} mislukt: {_code(e)}")
    try:
        with txn(pid) as t:
            for aid in done:
                t.d["runner_actions"].setdefault(aid, {"first_seen": dossier.now()})["class"] = "resolved"
            if done:
                t.audit("actions_resolved", f"{len(done)} runneractie(s) gesloten", ids=done)
    except SupervisionError:
        pass


# --- opdrachten starten en observeren ---

def _start_args(t: Txn, a: dict) -> dict:
    d = t.d
    cl = {**d["limits"].get("chain", {}), **a.get("chain_limits", {})}
    return {"repository": a["repo"], "prompt": a["prompt"], "node": a["node"], "title": _clip(f"{d['title']} · {a['assignment_id']}", 80),
            "continuation": True, "goal": a["goal"], "task_id": a["task_id"], **cl}


def _start_assignment(t: Txn, a: dict) -> bool:
    """Eén startpoging (idempotent dankzij het vooraf toegekende task-id). Bezet/onbereikbaar = blijft planned en wordt opnieuw geprobeerd."""
    if not t.cfg.get("supervision_enabled", True):
        return False
    try:
        out = _call("runner2_start", _start_args(t, a))
    except Exception as e:   # noqa: BLE001
        code = _code(e)
        msg = dossier.redact(str(getattr(e, "message", e)))[:300]
        _RETRY[a["assignment_id"] + t.pid] = _time() + t.cfg["supervision_start_retry_s"]
        prev = (a.get("last_error") or {}).get("code")
        a["last_error"] = {"code": code, "message": msg, "at": dossier.now()}
        if code in ("busy", "node_unreachable"):
            if prev != code:
                t.audit("start_deferred", f"opdracht {a['assignment_id']} wacht: {code}", assignment_id=a["assignment_id"], code=code)
            return False
        a["start_failures"] += 1
        t.audit("start_failed", f"opdracht {a['assignment_id']} startpoging {a['start_failures']} mislukt: {code}",
                assignment_id=a["assignment_id"], code=code, message=msg)
        if a["start_failures"] >= t.cfg["supervision_max_start_failures"]:
            _end_assignment(t, a, "start_failed", {"error": {"code": code, "message": msg}})
        return False
    a.update(status="started", started_at=dossier.now(), last_error=None,
             start_result={"status": out.get("status"), "already_started": bool(out.get("already_started"))})
    if ev := _event(t.d, a["event_id"]):
        ev.update(state="applied", applied_at=dossier.now())
    t.audit("assignment_started", f"opdracht {a['assignment_id']} gestart als {a['task_id']} op {a['node']}", assignment_id=a["assignment_id"],
            task_id=a["task_id"], node=a["node"])
    return True


def _end_assignment(t: Txn, a: dict, outcome: str, result: dict) -> None:
    d = t.d
    a.update(status="ended", outcome=outcome, ended_at=dossier.now(), result=dossier.redact_deep(result))
    commits = result.get("commits_total")
    if outcome in ("stopped_by_user",):
        pass
    elif commits == 0 or commits is None and outcome == "start_failed":
        d["counters"]["no_progress"] += 1
    elif commits:
        d["counters"]["no_progress"] = 0
    for item in result.get("human_actions") or []:
        d["runner_actions"].setdefault(item["id"], {"first_seen": dossier.now(), "class": "non_blocking", "text": _clip(item.get("text"), 300),
                                                    "reason": _clip(item.get("reason"), 200), "node": a["node"]})
    for b in d["backlog"]:
        if b["id"] in a.get("backlog_ids", []) and b["state"] == "in_progress":
            b["state"] = "open"   # ChatGPT zet het op done of laat het open op basis van bewijs, niet de runner
    t.audit("assignment_ended", f"opdracht {a['assignment_id']} geëindigd: {outcome}", assignment_id=a["assignment_id"], outcome=outcome,
            commits=commits)
    ev = _emit(t, "run_ended", f"run_ended:{a['assignment_id']}", f"opdracht {a['assignment_id']} ({a['goal'][:80]}) eindigde: {outcome}",
               assignment_id=a["assignment_id"], outcome=outcome)
    if outcome == "stopped_by_user" and d["control"]["state"] == "active":
        d["control"] = {"state": "paused", "reason": f"run {a['assignment_id']} is door een mens gestopt", "at": dossier.now(), "by": "runner2"}
        _auto_decision(t, ev, "auto_pause", "de run is bewust gestopt; project gepauzeerd, geen automatische herstart")


def _compact_result(st: dict, res: dict | None) -> dict:
    chain = (st.get("chain") or {})
    out = {"chain_state": chain.get("state"), "links_total": chain.get("links_total"), "tip_task": chain.get("current_task") or st.get("task_id")}
    if chain.get("stopped_because"):
        out["stopped_because"] = {k: _clip(v, 300) for k, v in chain["stopped_because"].items() if k in ("kind", "message")}
    if gate := chain.get("completion_gate"):
        out["completion_gate"] = {k: gate.get(k) for k in ("checks", "accepted", "refused")} | {"last": (gate.get("last") or {}).get("summary")}
    if res:
        out.update(final_status=res.get("final_status"), summary=_clip(res.get("summary"), 700), tests=(res.get("tests") or {}).get("run", [])[:8],
                   commits_total=(res.get("chain") or {}).get("commits_total", len(res.get("commits") or [])),
                   attention=[_clip(x, 200) for x in (res.get("attention") or [])[:5]])
        if res.get("error"):
            out["error"] = res["error"]
        ha = (res.get("human_actions") or {}).get("items") or []
        out["human_actions"] = [{"id": i["id"], "text": _clip(i.get("text"), 300), "reason": _clip(i.get("reason"), 200)}
                                for i in ha if i.get("status") == "open"][:10]
    return out


def _ended_outcome(st: dict) -> str | None:
    chain = st.get("chain")
    if chain:
        if chain["state"] == "active":
            return None
        stop = chain.get("stopped_because") or {}
        if chain["state"] == "completed":
            return "goal_reached"
        if chain["state"] == "limit_reached":
            return "limit_reached"
        return "stopped_by_user" if stop.get("kind") in ("stopped_by_user", "task_stopped") else "stopped:" + str(stop.get("kind") or "unknown")
    return {"completed": "task_completed", "failed": "failed", "stopped": "stopped_by_user"}.get(st.get("status"))


def _observe(t: Txn, a: dict) -> bool:
    """Vergelijkt een levende opdracht met de Runner-werkelijkheid. True als het dossier veranderde."""
    try:
        st = _call("runner2_status", {"task_id": a["task_id"], "node": a["node"]})
    except Exception as e:   # noqa: BLE001
        code = _code(e)
        if code == "not_found":
            if a["status"] == "started":
                t.d["integrity"] = {"status": "blocked", "code": "task_missing", "at": dossier.now(),
                                    "message": f"taak {a['task_id']} van opdracht {a['assignment_id']} bestaat niet meer op {a['node']}; "
                                               "controleer de Runner-opslag en heropen via admin action=repair mode=accept"}
                t.audit("integrity_blocked", f"opdracht {a['assignment_id']}: taak ontbreekt op de node", assignment_id=a["assignment_id"])
                return True
            return False
        return False   # node onbereikbaar/onbegrijpelijk: deze tik overslaan, nooit gokken
    changed = False
    if a["status"] == "planned":
        a.update(status="started", started_at=a["started_at"] or dossier.now(), last_error=None)
        if ev := _event(t.d, a["event_id"]):
            ev.update(state="applied", applied_at=dossier.now())
        t.audit("assignment_started", f"opdracht {a['assignment_id']} bleek al te draaien als {a['task_id']} (herstel na crash)",
                assignment_id=a["assignment_id"], reconciled=True)
        changed = True
    if (outcome := _ended_outcome(st)) is None:
        phase = (st.get("chain") or {}).get("phase") or st.get("status")
        obs = {"task_status": st.get("status"), "phase": phase, "current_task": (st.get("chain") or {}).get("current_task")}
        if {k: (a.get("observed") or {}).get(k) for k in obs} != obs:
            a["observed"] = obs | {"at": dossier.now()}
            t.audit("observe", f"opdracht {a['assignment_id']}: {phase}", assignment_id=a["assignment_id"], phase=phase)
            changed = True
        return changed
    tip = (st.get("chain") or {}).get("current_task") or a["task_id"]
    try:
        res = _call("runner2_result", {"task_id": tip, "node": a["node"]})
    except Exception:   # noqa: BLE001 - zonder result gaan we door met de status
        res = None
    _end_assignment(t, a, outcome, _compact_result(st, res))
    return True


def _check_assignments(t: Txn) -> None:
    d = t.d
    for a in [x for x in d["assignments"] if x["status"] in dossier.LIVE]:
        if a["status"] == "planned" and _RETRY.get(a["assignment_id"] + t.pid, 0) > _time():
            continue
        if a["status"] == "planned" and _time() - a.get("created_epoch", _time()) > t.cfg["supervision_planned_max_h"] * 3600:
            _end_assignment(t, a, "start_failed", {"error": {"code": "start_timeout", "message": "opdracht kon niet binnen de maximale wachttijd starten"}})
            continue
        if a["status"] == "planned":
            if _start_assignment(t, a):
                t.commit()
            elif a["status"] == "planned":
                _observe(t, a)   # misschien draait hij al (crash na start, voor het checkpoint)
            t.commit()
        else:
            _observe(t, a)
            t.commit()


def advance_project(pid: str, cfg: dict | None = None) -> dict:
    """Eén deterministische tik voor één project: reconcile met de Runner, vasthoudregels, escalaties. Start alleen geplande opdrachten."""
    cfg = cfg or config.load_config()
    if not cfg.get("supervision_enabled", True):
        return {"skipped": "supervision_disabled"}
    try:
        with txn(pid) as t:
            _check_assignments(t)
            d, now = t.d, _time()
            for ev in list(_open_events(d)):
                if ev["state"] == "claimed" and _lease_expired(ev, now) and ev["claims_total"] >= cfg["supervision_max_claims"]:
                    ev["state"] = "escalated"
                    t.audit("escalated", f"event {ev['id']} geëscaleerd na {ev['claims_total']} claims zonder beslissing", event_id=ev["id"])
                elif (ev["state"] == "pending" or _lease_expired(ev, now)) and ev["kind"] != "request_review" \
                        and d["control"]["state"] in ("paused", "stopped"):
                    # alleen bij bewuste pauze/stop beslist Runner 2 zelf; bij een limiet blijft het oordeel (klaar? blokkade?) aan de supervisor
                    _auto_decision(t, ev, "auto_hold", f"project is {d['control']['state']}; geen nieuwe opdracht, wachten op een mens")
            return {"state": state_of(d, cfg)}
    except SupervisionError as e:
        return {"blocked": e.message}


def advance_all(cfg: dict | None = None, force: bool = False) -> None:
    """Watch-hook: reconcile alle projecten, lever webhooks, schrijf de aandachtsbaken. Fouten van één project stoppen de rest niet."""
    cfg = cfg or config.load_config()
    if not force and _time() - _last_tick[0] < cfg["supervision_poll_s"]:
        return
    _last_tick[0] = _time()
    from . import cplane
    if not cplane.is_primary():   # hek: een standby-machine voert nooit supervisie uit (nooit twee control planes tegelijk)
        return
    for pid in dossier.project_ids():
        try:
            advance_project(pid, cfg)
        except Exception as e:   # noqa: BLE001
            log(f"supervisie {pid}: tikfout {type(e).__name__}: {e}")
    try:
        _deliver_webhooks(cfg)
        _write_beacon()
    except Exception as e:   # noqa: BLE001
        log(f"supervisie: bijwerken baken/webhook mislukt {type(e).__name__}: {e}")


def _write_beacon() -> None:
    """attention.json: wat een toekomstige notifier/always-on trigger moet weten, zonder het dossier te hoeven lezen."""
    ov = overview()
    items = [{k: i.get(k) for k in ("project_id", "title", "state", "label", "next_step")} for i in (ov or {"projects": []})["projects"] if i["attention"]]
    path = config.supervision_dir() / "attention.json"
    try:
        if path.exists() and json.loads(path.read_text()).get("projects") == items:
            return
    except (OSError, ValueError):
        pass
    store.atomic_write(path, json.dumps({"at": dossier.now(), "projects": items}, ensure_ascii=False))


# --- beheer (Beheerder via ChatGPT) ---

def admin(project_id: str | None, action: str, **a) -> dict:
    cfg = config.load_config()
    if action == "register":
        return register(a.get("title"), a.get("goal"), a.get("targets"), a.get("master_list"), a.get("backlog"), a.get("knowledge"),
                        a.get("limits"), a.get("deadline_hours"), a.get("idempotency_key"))
    pid = _pid(project_id) if project_id else None
    if not pid:
        raise SupervisionError("invalid_input", f"action {action} vereist project_id")
    if action == "repair":
        try:
            mode = a.get("mode")
            if mode == "accept":
                with txn(pid, ignore_integrity=True) as t:
                    if t.d["integrity"]["status"] != "blocked":
                        return {"changed": False, "note": "geen vastgelegde blokkade"}
                    t.d["integrity"] = {"status": "ok", "code": "", "message": "", "at": dossier.now()}
                    t.audit("integrity_accepted", "integriteitsblokkade opgeheven na beoordeling: " + _text(a.get("note"), "note", False, 300),
                            by=_text(a.get("by") or "beheerder", "by", cap=60))
                return {"changed": True, "project": summarize(pid)}
            out = dossier.repair(pid, mode, bool(a.get("confirm")))
        except dossier.DossierError as e:
            raise SupervisionError(e.code, e.message) from None
        return out | {"project": summarize(pid)}
    with txn(pid, ignore_integrity=action == "repair") as t:
        d = t.d
        by = _text(a.get("by") or "beheerder", "by", cap=60)
        if action in ("pause", "stop"):
            state = "paused" if action == "pause" else "stopped"
            d["control"] = {"state": state, "reason": _text(a.get("reason"), "reason", False, 300), "at": dossier.now(), "by": by}
            t.audit(action, f"project {state}: {d['control']['reason'][:100]}", by=by)
            live = _live(d)
            t.commit()
            if action == "stop" and live:
                try:
                    _call("runner2_stop", {"task_id": live["task_id"], "node": live["node"], "reason": "project gestopt door de supervisie"})
                except Exception as e:   # noqa: BLE001
                    log(f"supervisie {pid}: stop van {live['task_id']} mislukt: {_code(e)}")
        elif action == "resume":
            if d["control"]["state"] == "active":
                return {"changed": False, "project": summarize(pid)}
            d["control"] = {"state": "active", "reason": "", "at": dossier.now(), "by": by}
            t.audit("resume", "project hervat", by=by)
            _emit(t, "project_resumed", f"project_resumed:{d['revision'] + 1}", "project hervat; bepaal de volgende stap")
        elif action == "set_limits":
            merged = {**{k: d["limits"][k] for k in ("max_assignments", "max_hours", "max_no_progress")}, **{k: v for k, v in (a.get("limits") or {}).items() if k != "chain"}}
            merged["chain"] = {**d["limits"].get("chain", {}), **(a.get("limits") or {}).get("chain", {})}
            d["limits"] = _limits(cfg, merged)
            if a.get("reset_no_progress"):
                d["counters"]["no_progress"] = 0
            if (dh := a.get("deadline_hours")) is not None:   # een verstreken deadline was anders alleen met een dossierreparatie op te heffen
                if isinstance(dh, bool) or not isinstance(dh, (int, float)) or not 0 < dh <= cfg["supervision_cap_max_hours"]:
                    raise SupervisionError("invalid_input", f"deadline_hours moet groter dan 0 en hoogstens {cfg['supervision_cap_max_hours']} zijn")
                d["deadline"] = _iso(_time() + dh * 3600)
            t.audit("set_limits", "limieten aangepast", by=by, limits=d["limits"], deadline=d.get("deadline"))
        elif action == "amend_goal":
            text, reason = _text(a.get("goal"), "goal", cap=4000), _text(a.get("reason"), "reason", cap=500)
            g = d["goal"]
            g["amendments"].append({"version": g["version"], "text": g["text"], "sha256": g["sha256"], "at": dossier.now(), "by": by, "reason": reason})
            g.update(version=g["version"] + 1, text=text, sha256=dossier.sha(text))
            t.audit("amend_goal", f"projectdoel aangepast naar versie {g['version']}: {reason[:100]}", by=by, goal_sha=g["sha256"])
            _emit(t, "goal_amended", f"goal_amended:{g['version']}", f"projectdoel gewijzigd (versie {g['version']}): {reason[:120]}")
        elif action == "note":
            kind = a.get("kind") or "beheerder_decisions"
            if kind not in ("architecture", "beheerder_decisions"):
                raise SupervisionError("invalid_input", "kind moet architecture of beheerder_decisions zijn")
            d["knowledge"][kind].append(_text(a.get("text"), "text", cap=1000))
            t.audit("note", f"{kind} toegevoegd", by=by, kind=kind)
        elif action == "backlog":
            for i, b in enumerate(a.get("backlog") or []):
                item = _backlog_item(b, len(d["backlog"]) + 1)
                if any(x["id"] == item["id"] for x in d["backlog"]):
                    raise SupervisionError("invalid_input", f"backlog-id {item['id']} bestaat al")
                d["backlog"].append(item)
            t.audit("backlog", "backlogitems toegevoegd", by=by)
        elif action == "request_review":
            _emit(t, "request_review", f"request_review:{d['counters']['event_seq'] + 1}", _text(a.get("reason") or "herbeoordeling gevraagd", "reason", cap=300))
        elif action == "release_event":
            ev = _event(d, a.get("event_id"))
            if not ev or ev["state"] not in ("escalated", "claimed"):
                raise SupervisionError("not_found", "event bestaat niet of is niet geëscaleerd/geclaimd")
            ev.update(state="pending", claims_total=0, claim=None)
            ev["webhook"] = {"state": "pending" if _webhook_url(cfg) else "off", "attempts": 0, "next_at": 0}
            t.audit("release_event", f"event {ev['id']} vrijgegeven", event_id=ev["id"], by=by)
        elif action == "resolve_blocker":
            ids = _text_list(a.get("blocker_ids"), "blocker_ids", 20)
            hit = [b for b in d["blockers"] if b["id"] in ids and b["state"] == "open"]
            if not hit:
                return {"changed": False, "project": summarize(pid)}
            for b in hit:
                b.update(state="resolved", resolved_at=dossier.now(), resolved_by=by, resolved_note=_text(a.get("note"), "note", False, 600))
                for it in d["backlog"]:
                    if b["id"] in it["blocked_by"]:
                        it["blocked_by"] = [x for x in it["blocked_by"] if x != b["id"]]
                        if it["state"] == "blocked" and not it["blocked_by"]:
                            it["state"] = "open"
            if a.get("note"):
                d["knowledge"]["beheerder_decisions"].append(_text(a["note"], "note", cap=600))
            t.audit("resolve_blocker", f"blokkade(s) opgelost: {', '.join(b['id'] for b in hit)}", by=by, ids=[b["id"] for b in hit])
            _emit(t, "blocker_resolved", "blocker_resolved:" + ",".join(sorted(b["id"] for b in hit)), "menselijke blokkade opgelost; bepaal de volgende stap")
        elif action == "assign":
            cfg_limits = a.get("chain_limits") or {}
            ev = _emit(t, "manual_assign", f"manual_assign:{d['counters']['event_seq'] + 1}", "handmatige opdracht door de beheerder")
            ev_n = _next_decision_n(d)
            goal, prompt = _text(a.get("goal"), "goal", cap=1500), _text(a.get("prompt"), "prompt")
            asg = _plan_assignment(t, ev, ev_n, (a.get("node") or "").upper() or None, a.get("repository"), goal, prompt,
                                   _text_list(a.get("backlog_ids"), "backlog_ids", 40), cfg_limits)
            d["decisions"].append({"n": ev_n, "at": dossier.now(), "event_id": ev["id"], "kind": "work_remaining", "reason": "handmatige opdracht",
                                   "why_next": _text(a.get("reason"), "reason", False, 500), "by": by, "assignment_id": asg["assignment_id"],
                                   "payload_sha": dossier.sha({"assign": asg["fingerprint"]}), "evidence": []})
            ev.update(state="decided", decision_n=ev_n)
            t.audit("decision", f"handmatig: opdracht {asg['assignment_id']}", event_id=ev["id"], n=ev_n, by=by)
            t.commit()
            _start_assignment(t, asg)
        elif action == "reconcile":
            pass
        else:
            raise SupervisionError("invalid_input", f"onbekende action {action!r}")
        t.commit()
    if action == "reconcile":
        advance_project(pid, cfg)
    return {"ok": True, "action": action, "project": summarize(pid)}
