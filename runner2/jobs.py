"""Opdrachtwachtrij van Runner 2: één invoer voor opdrachten, een persistente wachtrij per Runner (MAC, GERARD, LINUX,
WINDOWS) en een dispatcher die de volgende opdracht start zodra een Runner vrij is.

Draait op de primaire control plane (de watch roept `tick` aan; `cplane.is_primary()`), wordt bediend via de
controller-MCP-tool `runner2_jobs` (Monitor 2, ChatGPT of de CLI) en start opdrachten uitsluitend via de gewone
controllerroute (`controller_mcp.dispatch` -> lokaal of via het node-transport). Er is geen eigen procesbeheer.

Zekerheden
- Persistent: `~/.runner2/jobs/queue.json` (atomair vervangen, vorige generatie in `queue.prev.json`) plus een
  append-only `audit.jsonl` (fsync per regel). Alle wijzigingen gaan onder één bestandslock (`.lock`).
- Geen dubbele start: elke run van een opdracht heeft een vooraf toegekend taak-id (`R2-` + sha1(opdracht:run)).
  `controller.start(task_id=...)` is idempotent, dus een herhaalde startpoging na een crash, time-out of verloren
  antwoord geeft de bestaande taak terug (`already_started`) en start niets een tweede keer.
- Geen racecondities: de dispatcher legt een startclaim (status `starting`, poging, tijd) vast vóór de netwerkaanroep
  en verwerkt het antwoord daarna onder de lock alleen als de opdracht nog in dezelfde toestand en run is.
  Netwerkaanroepen gebeuren nooit onder de lock.
- Per Runner hooguit één opdracht in `starting`/`running`; Runner 2 zelf houdt daarnaast één actieve taak per machine
  aan (een handmatige taak geeft `busy`, de opdracht wacht dan zichtbaar).
- Geen verloren opdrachten: een opdracht verdwijnt nooit uit de state; afgeronde opdrachten blijven met hun
  uitkomst staan (archiveren verbergt ze alleen). Een onleesbare state wordt nooit overschreven: de dispatcher stopt
  en meldt `queue_corrupt` tot iemand `repair` doet.

Statussen (opgeslagen): queued, starting, running, done, failed, incomplete, stopped, cancelled. Wat de gebruiker
ziet (WACHT OP VOORGANGER, WACHT OP GOEDKEURING, GEBLOKKEERD, CLAUDE KLAAR, CHATGPT GECONTROLEERD, ...) wordt telkens
afgeleid (`label`), zodat afhankelijkheden en goedkeuringen nooit uit de pas lopen.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from . import config, filelock, riskpolicy, store

SCHEMA = 1
NODES = ("MAC", "GERARD", "LINUX", "WINDOWS")
QUEUED, STARTING, RUNNING, DONE, FAILED, INCOMPLETE, STOPPED, CANCELLED = (
    "queued", "starting", "running", "done", "failed", "incomplete", "stopped", "cancelled")
ACTIVE_STATES = (STARTING, RUNNING)
END_STATES = (DONE, FAILED, INCOMPLETE, STOPPED, CANCELLED)
APPROVAL_KINDS = ("start", "integratie", "publicatie", "risico")
CHECK_SOURCES = ("chatgpt", "mens")
MODEL_CHOICES = ("standaard", "opus", "sonnet")
DEFAULT_MODELS = {"opus": "claude-opus-5-5", "sonnet": "claude-sonnet-5-5"}

MAX_PROMPT = 60_000
MAX_TITLE = 120
MAX_REPO = 300
MAX_NOTE = 2000
MAX_DEPS = 10
MAX_OPEN_JOBS = 300
MAX_JOB_LOG = 80
MAX_REQUESTS = 1000
NAME_RE = re.compile(r"^[\w .'-]{1,40}$", re.UNICODE)
JOB_RE = re.compile(r"^J-[0-9a-f]{12}$")

LABELS = {
    "in_wachtrij": "IN WACHTRIJ", "vastgehouden": "VASTGEHOUDEN", "wacht_op_goedkeuring": "WACHT OP GOEDKEURING",
    "wacht_op_voorganger": "WACHT OP VOORGANGER", "geblokkeerd": "GEBLOKKEERD", "wachtrij_gepauzeerd": "WACHTRIJ GEPAUZEERD",
    "start": "START", "wacht_op_runner": "WACHT OP RUNNER", "bezig": "BEZIG", "wacht_op_actie": "WACHT OP ACTIE",
    "gepauzeerd": "GEPAUZEERD", "claude_klaar": "CLAUDE KLAAR", "chatgpt_gecontroleerd": "CHATGPT GECONTROLEERD",
    "gecontroleerd": "GECONTROLEERD", "afgekeurd": "AFGEKEURD", "mislukt": "MISLUKT", "limiet": "LIMIET BEREIKT",
    "gestopt": "GESTOPT", "geannuleerd": "GEANNULEERD", "wordt_geannuleerd": "WORDT GEANNULEERD",
}


class JobsError(Exception):
    def __init__(self, code: str, message: str, data: dict | None = None):
        super().__init__(message)
        self.code, self.message, self.data = code, message, data   # data: gestructureerde context (bijv. een repositoryvraag)


# --- paden, lock, opslag ---

def jobs_dir() -> Path:
    return config.r2_home() / "jobs"


def _paths() -> tuple[Path, Path, Path, Path]:
    d = jobs_dir()
    return d / "queue.json", d / "queue.prev.json", d / "audit.jsonl", d / ".lock"


FENCE = "HANDED_OVER.json"   # geschreven door `handover export`: de wachtrij is naar een andere control plane


def handed_over() -> dict | None:
    try:
        return json.loads((jobs_dir() / FENCE).read_text())
    except (OSError, ValueError):
        return None


def _check_fence() -> None:
    h = handed_over()
    if h:
        raise JobsError("handed_over", f"de opdrachtwachtrij is op {h.get('at', '?')} overgedragen aan {h.get('to', '?')}; "
                        "bedien hem daar (of draai de overdracht terug met 'handover unfence').")


@contextmanager
def _locked():
    _q, _p, _a, lock = _paths()
    lock.parent.mkdir(parents=True, exist_ok=True)
    with open(lock, "a+") as f:
        filelock.lock_exclusive(f)
        yield


@contextmanager
def _launch_lock(node):
    """Serialize start admission without holding the queue lock over transport."""
    path = jobs_dir() / ('.launch-' + node + '.lock')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+') as handle:
        filelock.lock_exclusive(handle)
        yield


def _empty() -> dict:
    return {"schema": SCHEMA, "version": 0, "updated": store.now(), "paused": {"all": None, "nodes": {}},
            "jobs": {}, "order": {n: [] for n in NODES}, "requests": {}}


_JOB_KEYS = ("id", "prompt", "repo", "node", "status", "run", "task_id", "created_at")


def _read(path: Path) -> dict:
    """Leest en controleert de hele structuur; alles wat niet klopt is ValueError (-> queue_corrupt), nooit een KeyError later."""
    d = json.loads(path.read_text())
    if not isinstance(d, dict) or d.get("schema") != SCHEMA or not isinstance(d.get("jobs"), dict):
        raise ValueError("onbekend schema")
    paused, order, requests = d.get("paused"), d.get("order"), d.get("requests")
    if not isinstance(paused, dict) or "all" not in paused or not isinstance(paused.get("nodes"), dict) \
            or not isinstance(order, dict) or not isinstance(requests, dict):
        raise ValueError("onvolledige wachtrij")
    for n in NODES:
        order.setdefault(n, [])
        if not isinstance(order[n], list) or any(j not in d["jobs"] for j in order[n]):
            raise ValueError(f"volgorde van {n} verwijst naar een onbekende opdracht")
    for jid, job in d["jobs"].items():
        if not isinstance(job, dict) or any(k not in job for k in _JOB_KEYS) or job["id"] != jid \
                or job["status"] not in (QUEUED, *ACTIVE_STATES, *END_STATES) or job["node"] not in NODES:
            raise ValueError(f"opdracht {jid} is onvolledig")
    return d


def load() -> dict:
    """De huidige state (alleen lezen). Ontbreekt hij, dan een lege wachtrij; onleesbaar = JobsError(queue_corrupt)."""
    q, _p, _a, _l = _paths()
    if not q.exists():
        return _empty()
    try:
        return _read(q)
    except (OSError, ValueError, KeyError, TypeError) as e:
        raise JobsError("queue_corrupt", f"de opdrachtwachtrij ({q}) is onleesbaar ({type(e).__name__}); er wordt niets gestart. "
                        "Herstel met runner2_jobs action=repair mode=restore_prev (vorige generatie) na controle.") from None


def _save(d: dict) -> None:
    q, prev, _a, _l = _paths()
    d["version"] = int(d.get("version", 0)) + 1
    d["updated"] = store.now()
    if q.exists():
        try:
            _read(q)   # alleen een geldige generatie wordt de vorige
            store.atomic_write(prev, q.read_text())
        except (OSError, ValueError, KeyError, TypeError):
            pass
    store.atomic_write(q, json.dumps(d, indent=1, ensure_ascii=False))


_tx = threading.local()


def _audit(kind: str, text: str, **data) -> None:
    """Binnen een transactie pas na het opslaan geschreven (het log meldt nooit iets dat niet bestaat)."""
    pending = getattr(_tx, "pending", None)
    if pending is not None:
        pending.append((kind, text, data))
        return
    _write_audit(kind, text, **data)


def _write_audit(kind: str, text: str, **data) -> None:
    _q, _p, audit, _l = _paths()
    audit.parent.mkdir(parents=True, exist_ok=True)
    from . import gerard_queue
    actor = gerard_queue.metadata()
    if actor is not None:
        data["subject"] = actor["subject"]
    line = json.dumps({"at": store.now(), "kind": kind, "text": text, **data}, ensure_ascii=False)
    with open(audit, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


@contextmanager
def _txn():
    """Lees-wijzig-schrijf onder de lock. Zet de body `box["dirty"] = True`, dan wordt opgeslagen en daarna geaudit."""
    with _locked():
        _check_fence()   # onder de lock: na het hek wijzigt deze machine de wachtrij niet meer
        d = load()
        from . import gerard_queue
        gerard_queue.check(d)
        box = {"d": d, "dirty": False}
        _tx.pending = []
        try:
            yield box
            if box["dirty"]:
                _save(box["d"])
                for kind, text, data in _tx.pending:
                    _write_audit(kind, text, **data)
        finally:
            _tx.pending = None


def _log(job: dict, by: str, text: str) -> None:
    job["log"] = (job.get("log") or [])[-(MAX_JOB_LOG - 1):] + [{"at": store.now(), "by": by, "text": text[:500]}]


# --- validatie ---

def _name(by) -> str:
    if not isinstance(by, str) or not NAME_RE.match(by.strip()):
        raise JobsError("invalid_input", "geef een naam (wie doet dit?): 1-40 letters, cijfers, spaties, punt of streepje")
    return by.strip()


def _text(value, field: str, limit: int, required: bool = False) -> str:
    if value is None and not required:
        return ""
    if not isinstance(value, str) or (required and not value.strip()):
        raise JobsError("invalid_input", f"{field} ontbreekt of is geen tekst")
    if len(value) > limit:
        raise JobsError("invalid_input", f"{field} is te lang ({len(value)} tekens, maximaal {limit})")
    return value


def _node(value) -> str:
    n = str(value or "").upper()
    if n not in NODES:
        raise JobsError("invalid_input", f"kies een Runner: {', '.join(NODES)}")
    return n


def _job(d: dict, jid) -> dict:
    if not isinstance(jid, str) or not JOB_RE.match(jid) or jid not in d["jobs"]:
        raise JobsError("not_found", f"opdracht {jid!r} bestaat niet")
    return d["jobs"][jid]


def task_id_for(jid: str, run: int) -> str:
    return "R2-" + hashlib.sha1(f"{jid}:{run}".encode()).hexdigest()[:12]


def _cycle(d: dict, jid: str, deps: list) -> bool:
    seen, todo = set(), list(deps)
    while todo:
        x = todo.pop()
        if x == jid:
            return True
        if x in seen or x not in d["jobs"]:
            continue
        seen.add(x)
        todo.extend(d["jobs"][x].get("depends_on") or [])
    return False


def _deps(d: dict, jid: str, deps) -> list:
    if deps is None:
        return []
    if not isinstance(deps, list) or len(deps) > MAX_DEPS or not all(isinstance(x, str) for x in deps):
        raise JobsError("invalid_input", f"depends_on is een lijst van hoogstens {MAX_DEPS} opdracht-id's")
    out = []
    for x in deps:
        if x not in d["jobs"]:
            raise JobsError("invalid_input", f"voorganger {x} bestaat niet")
        if x not in out:
            out.append(x)
    if jid in out or _cycle(d, jid, out):
        raise JobsError("invalid_input", "deze afhankelijkheid maakt een kring; een opdracht kan niet op zichzelf wachten")
    return out


def _models(cfg: dict) -> dict:
    return {"opus": cfg.get("jobs_model_opus") or DEFAULT_MODELS["opus"],
            "sonnet": cfg.get("jobs_model_sonnet") or DEFAULT_MODELS["sonnet"]}


# --- afgeleide toestand ---

def _dep_state(d: dict, job: dict) -> tuple[str, list]:
    """('ok'|'wait'|'dead', toelichting per voorganger)."""
    from . import integration_check
    worst, notes = "ok", []
    from . import integration_check
    for previous in d['jobs'].values():
        if (previous['id'] != job['id'] and (previous.get('integration_obligation') or {}).get('node', previous.get('node')) == job['node']
                and (previous.get('integration_obligation') or (previous.get('integration_workflow') and previous.get('started_at')))
                and not integration_check.satisfied(previous)):
            worst = 'wait'
            notes.append(f"{previous['id']}: wacht op bewezen integratie en geslaagde nacontroles")
    for x in job.get("depends_on") or []:
        p = d["jobs"].get(x)
        if not p:
            worst, notes = "dead", notes + [f"{x}: bestaat niet meer"]
            continue
        chk = (p.get("check") or {}).get("state")
        if p["status"] == DONE and chk == "afgekeurd":
            worst = "dead"
            notes.append(f"{x}: afgekeurd bij controle")
        elif p["status"] == DONE and integration_check.required(p) and not integration_check.satisfied(p):
            worst = "wait" if worst == "ok" else worst
            notes.append(f"{x}: wacht op bewezen integratie en geslaagde nacontroles")
        elif p["status"] == DONE and (not p.get("require_check") or chk == "ok"):
            continue
        elif p["status"] == DONE:
            worst = "wait" if worst == "ok" else worst
            notes.append(f"{x}: Claude klaar, wacht op controle")
        elif p["status"] in (FAILED, INCOMPLETE, STOPPED, CANCELLED):
            worst = "dead"
            notes.append(f"{x}: {p['status']}")
        else:
            worst = "wait" if worst == "ok" else worst
            notes.append(f"{x}: nog niet klaar")
    return worst, notes


def _paused(d: dict, node: str) -> dict | None:
    return d["paused"].get("all") or d["paused"]["nodes"].get(node)


def label(d: dict, job: dict) -> tuple[str, str]:
    """(sleutel, toelichting) voor de gebruiker."""
    st = job["status"]
    if st == QUEUED:
        dep, notes = _dep_state(d, job)
        appr = job.get("approval") or {}
        if dep == "dead":
            return "geblokkeerd", "voorganger niet geslaagd: " + "; ".join(notes)
        if job.get("held"):
            return "vastgehouden", "deze opdracht wordt vastgehouden tot iemand haar vrijgeeft"
        if appr.get("required") and appr.get("state") != "approved":
            return "wacht_op_goedkeuring", f"goedkeuring nodig ({appr.get('kind')}) voordat deze opdracht start"
        if dep == "wait":
            return "wacht_op_voorganger", "; ".join(notes)
        if p := _paused(d, job["node"]):
            return "wachtrij_gepauzeerd", f"wachtrij gepauzeerd door {p.get('by')}" + (f": {p['reason']}" if p.get("reason") else "")
        return "in_wachtrij", f"positie {d['order'][job['node']].index(job['id']) + 1 if job['id'] in d['order'][job['node']] else '?'} op {job['node']}"
    if job.get("cancel_requested") and st in ACTIVE_STATES:
        return "wordt_geannuleerd", "de taak wordt gestopt"
    if st == STARTING:
        appr = job.get("approval") or {}
        if appr.get("required") and appr.get("state") != "approved":
            return "wacht_op_goedkeuring", "goedkeuring nodig; een eerdere start wordt gecontroleerd"
        err = job.get("last_error") or {}
        if err.get("code") == "busy":
            return "wacht_op_runner", f"{job['node']} is bezet ({err.get('message') or 'andere taak'}); start zodra de Runner vrij is"
        if err.get("code") == "node_unreachable":
            return "wacht_op_runner", f"{job['node']} is niet bereikbaar; nieuwe poging volgt"
        return "start", "wordt gestart"
    if st == RUNNING:
        sub = (job.get("progress") or {}).get("task_status")
        if sub == "waiting_for_human":
            return "wacht_op_actie", "Claude stelt een vraag (beantwoorden via de kaart van de Runner)"
        if sub == "paused":
            return "gepauzeerd", "de taak is gepauzeerd"
        return "bezig", (job.get("progress") or {}).get("text") or "loopt"
    if st == DONE:
        from . import integration_check
        chk = job.get("check") or {}
        if ((job.get("integration_disposition") or {}).get("kind") == "not_applicable_no_changes"
                and job.get("require_check") is False and not chk
                and not job.get("integration_workflow") and not job.get("integration_obligation")):
            return "claude_klaar", "Geen wijzigingen; geen samenvoeging nodig. Eventuele open acties blijven open."
        if chk.get("state") != "afgekeurd" and integration_check.required(job) and not integration_check.satisfied(job):
            return "claude_klaar", (job.get("last_error") or {}).get("message") or "Wacht op bewezen integratie en geslaagde nacontroles"
        if chk.get("state") == "ok":
            return ("chatgpt_gecontroleerd" if chk.get("source") == "chatgpt" else "gecontroleerd",
                    f"gecontroleerd door {chk.get('by')}" + (f": {chk['note']}" if chk.get("note") else ""))
        if chk.get("state") == "afgekeurd":
            return "afgekeurd", f"afgekeurd door {chk.get('by')}" + (f": {chk['note']}" if chk.get("note") else "")
        if (job.get("last_error") or {}).get("code") == "integration_check_blocked":
            return "claude_klaar", job["last_error"]["message"]
        return "claude_klaar", "Claude is klaar; controle staat nog open"
    return {FAILED: "mislukt", INCOMPLETE: "limiet", STOPPED: "gestopt", CANCELLED: "geannuleerd"}[st], \
        (job.get("outcome") or {}).get("text") or ""


def view(d: dict, job: dict) -> dict:
    key, why = label(d, job)
    out = {k: copy.deepcopy(job.get(k)) for k in (
        "id", "title", "node", "repo", "model_choice", "model", "supervisor", "continuation", "depends_on", "require_check",
        "approval", "held", "status", "task_id", "run", "created_by", "created_at", "started_at", "ended_at", "outcome",
        "progress", "check", "last_error", "attempts", "archived", "cancel_requested", "repo_choice", "siblings")}
    out["prompt_preview"] = job["prompt"][:300]
    out["state"], out["label"], out["why"] = key, LABELS[key], why
    out["log"] = (job.get("log") or [])[-20:]
    return out


def overview(include_archived: bool = False, job_id: str | None = None) -> dict:
    d = load()
    if job_id:
        job = _job(d, job_id)
        v = view(d, job)
        v["prompt"], v["log"] = job["prompt"], job.get("log") or []
        return {"job": v}
    nodes = {}
    for n in NODES:
        ordered = [d["jobs"][j] for j in d["order"][n] if j in d["jobs"]]
        rest = sorted((j for j in d["jobs"].values() if j["node"] == n and j["id"] not in d["order"][n]
                       and (include_archived or not j.get("archived"))),
                      key=lambda j: j.get("ended_at") or j.get("started_at") or j["created_at"], reverse=True)
        active = [j for j in ordered if j["status"] in ACTIVE_STATES]
        waiting = [j for j in ordered if j["status"] == QUEUED]
        nodes[n] = {"paused": d["paused"]["nodes"].get(n), "active": [view(d, j) for j in active],
                    "queue": [view(d, j) for j in waiting], "finished": [view(d, j) for j in rest[:25]],
                    "counts": {"queued": len(waiting), "active": len(active), "finished": len(rest)}}
    return {"schema": SCHEMA, "version": d["version"], "updated": d["updated"], "paused_all": d["paused"].get("all"),
            "handed_over": handed_over(),
            "nodes": nodes, "models": list(MODEL_CHOICES), "approval_kinds": list(APPROVAL_KINDS)}


# --- handelingen (bediening) ---

def _resolve_repo(args: dict, node: str, prompt: str) -> tuple[list[dict], dict | None]:
    """De opdracht(en) die `add` moet maken: [{repo, prompt, repo_choice}] plus het besluit (None bij een expliciet pad).

    `repository` is een pad (ongewijzigd gedrag) of "auto": dan bepaalt `repos.resolve` de repository uit de tekst.
    Meerdere kandidaten, geen kandidaat of een gesplitste opdracht zonder `split=true` is een JobsError met `data`
    (de vraag of het voorstel) en er wordt niets aangemaakt.
    """
    from . import repos
    raw = args.get("repository")
    split_ok, choices = args.get("split", False), args.get("project_repos")
    if not isinstance(split_ok, bool):
        raise JobsError("invalid_input", "split moet true of false zijn")
    if choices is not None and (not isinstance(choices, dict) or any(not isinstance(k, str) or not isinstance(v, str)
                                                                   for k, v in choices.items()) or len(choices) > 10):
        raise JobsError("invalid_input", "project_repos moet een object {project: repository} zijn")
    if not (isinstance(raw, str) and raw.strip().lower() == repos.AUTO):
        repo = _text(raw, "repository", MAX_REPO, required=True).strip()
        res = {"decision": "explicit", "node": node, "groups": [{"node": node, "repository": repo}],
               "warnings": repos.explicit_warnings(prompt, node, repo)}
        return [{"repo": repo, "prompt": prompt, "repo_choice": {"mode": "expliciet", "projects": []}}], res
    res = repos.resolve(prompt, node, choices=choices)
    if res["decision"] == "choice_needed":
        raise JobsError("repo_choice_needed", res["question"]["text"], {"resolution": res})
    if res["decision"] != "resolved":
        raise JobsError("repo_unknown", res.get("message") or "geen repository bepaald", {"resolution": res})
    groups = res["groups"]
    if len(groups) > 1 and not split_ok:
        raise JobsError("repo_split_confirm", res["message"] + " Bevestig het splitsen (split=true) of kies zelf een repository.",
                        {"resolution": res})
    out = []
    for g in groups:
        others = [o for o in groups if o is not g]
        out.append({"repo": g["repository"], "prompt": repos.scope_header(g, others) + prompt,
                    "repo_choice": {"mode": "auto", "projects": [{k: p[k] for k in ("id", "name", "component")} for p in g["projects"]],
                                    "verified": g["verified"], "split": len(groups) > 1},
                    "label": ", ".join(p["name"] for p in g["projects"])})
    return out, res


def plan(args: dict) -> dict:
    """Alleen lezen: wat `add` met deze invoer zou doen (geen opdracht aangemaakt)."""
    from . import repos
    node = _node(args.get("node"))
    prompt = _text(args.get("prompt"), "prompt", MAX_PROMPT, required=True)
    raw = args.get("repository")
    if isinstance(raw, str) and raw.strip() and raw.strip().lower() != repos.AUTO:
        return {"plan": {"decision": "explicit", "node": node, "groups": [{"node": node, "repository": raw.strip()}],
                         "warnings": repos.explicit_warnings(prompt, node, raw.strip())}}
    choices = args.get("project_repos")
    if choices is not None and (not isinstance(choices, dict) or any(not isinstance(k, str) or not isinstance(v, str)
                                                                    for k, v in choices.items())):
        raise JobsError("invalid_input", "project_repos moet een object {project: repository} zijn")
    return {"plan": repos.resolve(prompt, node, choices=choices)}


def add(args: dict, by: str, channel: str = "lokaal") -> dict:
    by = _name(by)
    cfg = config.load_config()
    gate_enabled = riskpolicy.enabled(cfg)
    title = _text(args.get("title"), "title", MAX_TITLE)
    prompt = _text(args.get("prompt"), "prompt", MAX_PROMPT, required=True)
    node = _node(args.get("node"))
    choice = args.get("model") or "standaard"
    if choice not in MODEL_CHOICES:
        raise JobsError("invalid_input", f"model moet een van {MODEL_CHOICES} zijn")
    supervisor = args.get("supervisor", False)
    continuation = args.get("continuation", False)
    require_check = args.get("require_check", False)
    held = args.get("held", False)
    for k, v in (("supervisor", supervisor), ("continuation", continuation), ("require_check", require_check), ("held", held)):
        if not isinstance(v, bool):
            raise JobsError("invalid_input", f"{k} moet true of false zijn")
    approval = None
    if args.get("approval") not in (None, "", "geen"):
        if args["approval"] not in APPROVAL_KINDS:
            raise JobsError("invalid_input", f"approval moet een van {APPROVAL_KINDS} zijn (of leeg)")
        approval = {"required": True, "kind": args["approval"], "state": "open"}
    sup_opts = {}
    for k in ("project_goal", "max_chains", "supervisor_max_hours"):
        if args.get(k) is not None:
            if not supervisor:
                raise JobsError("invalid_input", f"{k} hoort bij supervisor=true")
            sup_opts[k] = args[k]
    request_id = args.get("request_id")
    if request_id is not None and (not isinstance(request_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{8,64}", request_id)):
        raise JobsError("invalid_input", "request_id: 8-64 letters, cijfers, - of _")
    from . import gerard_queue
    if request_id and not gerard_queue.active():   # restricted duplicates are checked under the lock
        d = load()
        if request_id in d["requests"] and d["requests"][request_id] in d["jobs"]:
            return _duplicate(d, d["jobs"][d["requests"][request_id]])
    plans, resolution = _resolve_repo(args, node, prompt)   # kan een netwerkaanroep zijn: nooit onder de lock
    with _txn() as box:
        d = box["d"]
        if request_id and request_id in d["requests"]:
            return _duplicate(d, d["jobs"][d["requests"][request_id]])
        if sum(1 for j in d["jobs"].values() if j["status"] not in END_STATES) + len(plans) > MAX_OPEN_JOBS:
            raise JobsError("limit", f"er staan al {MAX_OPEN_JOBS} openstaande opdrachten")
        made = []
        for p in plans:
            jid = "J-" + uuid.uuid4().hex[:12]
            deps = _deps(d, jid, args.get("depends_on"))
            jtitle = title.strip() or prompt.strip().splitlines()[0][:MAX_TITLE]
            if len(plans) > 1:
                jtitle = (jtitle[:MAX_TITLE - len(p["label"]) - 3] + f" · {p['label']}")[:MAX_TITLE]
            verdict = riskpolicy.assess(channel, [jtitle, p["prompt"], args.get("project_goal")], p["repo"], cfg) if gate_enabled else None
            japproval = copy.deepcopy(approval)
            if verdict and verdict["risky"] and not japproval:
                japproval = {"required": True, "kind": verdict["kind"], "state": "open", "by_policy": verdict["reasons"]}
            job = {"id": jid, "title": jtitle, "prompt": p["prompt"], "repo": p["repo"],
                   "node": node, "model_choice": choice, "model": _models(cfg).get(choice), "supervisor": supervisor,
                   "continuation": continuation or supervisor, "supervisor_opts": sup_opts, "depends_on": deps,
                   "require_check": require_check, "approval": japproval, "held": held, "status": QUEUED, "run": 1,
                   "task_id": task_id_for(jid, 1), "created_by": by, "created_at": store.now(), "attempts": 0,
                   "start_failures": 0, "next_try": 0, "last_error": None, "progress": None, "outcome": None, "check": None,
                   "log": [], "repo_choice": p["repo_choice"]}
            job["origin_channel"] = channel if channel in ("chatgpt", "lokaal", "rpc") else "unknown"
            if verdict is not None:
                job["policy"] = riskpolicy.policy_record(channel, p["prompt"], verdict)
            from . import integration_workflow
            try:
                workflow = integration_workflow.for_job(job, cfg)
            except ValueError as exc:
                raise JobsError('integration_workflow_config', str(exc)) from exc
            if workflow:
                job['integration_workflow'] = workflow
                job['require_check'] = True
            _log(job, by, f"aangemaakt voor {node}" + (f", wacht op {', '.join(deps)}" if deps else "")
                 + (f", goedkeuring nodig ({japproval['kind']})" if japproval else "")
                 + (f" [beleid: {'; '.join(verdict['reasons'])}]" if verdict and verdict["risky"] else "")
                 + (f"; repository automatisch bepaald: {p['repo']}" if p["repo_choice"]["mode"] == "auto" else ""))
            d["jobs"][jid] = job
            made.append(job)
        ids = [j["id"] for j in made]
        if len(made) > 1:
            for j in made:
                j["siblings"] = [i for i in ids if i != j["id"]]
                _log(j, by, "gesplitst opdracht: " + ", ".join(ids))
        for job in made:
            if args.get("position") == "top":
                d["order"][node].insert(_first_queued_index(d, node), job["id"])
            else:
                d["order"][node].append(job["id"])
        if request_id:
            d["requests"][request_id] = ids[0]
            if len(d["requests"]) > MAX_REQUESTS:
                for k in list(d["requests"])[:len(d["requests"]) - MAX_REQUESTS]:
                    del d["requests"][k]
        box["dirty"] = True
        _audit("add", f"{', '.join(ids)} toegevoegd door {by} voor {node}", job_id=ids[0], job_ids=ids, node=node, by=by)
        out = {"job": view(d, made[0])}
        if len(made) > 1:
            out.update({"jobs": [view(d, j) for j in made], "split": True})
        if resolution is not None and (resolution.get("warnings") or resolution.get("decision") != "explicit"):
            out["repo_resolution"] = {k: resolution.get(k) for k in ("decision", "message", "warnings", "mentions", "checked")}
        return out


def _duplicate(d: dict, job: dict) -> dict:
    sib = [d["jobs"][j] for j in job.get("siblings") or [] if j in d["jobs"]]
    out = {"job": view(d, job), "duplicate": True}
    if sib:
        out.update({"jobs": [view(d, j) for j in [job, *sib]], "split": True})
    return out


def _first_queued_index(d: dict, node: str) -> int:
    order = d["order"][node]
    for i, j in enumerate(order):
        if d["jobs"][j]["status"] == QUEUED:
            return i
    return len(order)


def _queued(job: dict, what: str) -> None:
    if job["status"] != QUEUED:
        raise JobsError("wrong_state", f"{what} kan alleen bij een opdracht die nog in de wachtrij staat (nu: {job['status']})")


def _approvers(cfg: dict) -> list:
    v = cfg.get("jobs_approvers", [])
    return [str(x).strip() for x in v if str(x).strip()]


def _reset_approval(job: dict, by: str, why: str) -> None:
    appr = job.get("approval") or {}
    if appr.get("required") and appr.get("state") == "approved":
        appr.update(state="open", by=None, at=None, note=None)
        _log(job, by, f"goedkeuring vervalt: {why}")


def act(action: str, args: dict, by: str, channel: str = "lokaal") -> dict:
    result = _act(action, args, by, channel)
    # Persist pause/decision first, then drain any already admitted request.
    # A subsequent start must see that decision before crossing admission.
    if action == 'pause_queue':
        node = args.get('node')
        nodes = NODES if node in (None, '', 'ALL', 'all') else [_node(node)]
    elif action in ('check', 'reject', 'cancel', 'hold', 'set_deps', 'set_node'):
        node = (result.get('job') or {}).get('node')
        nodes = [node] if node else []
    else:
        nodes = []
    for node in nodes:
        with _launch_lock(node):
            pass
    if action == 'check':
        _hook_reassess('opdracht_controle', f"{(result.get('job') or {}).get('id')}:{((result.get('job') or {}).get('check') or {}).get('at')}")
    return result


def _hook_reassess(soort: str, ref) -> None:
    try:
        from . import actionreassess
        actionreassess.hook_event(soort, ref)
    except Exception:   # noqa: BLE001 -- herbeoordeling mag de wachtrij nooit raken
        pass


def _act(action: str, args: dict, by: str, channel: str = "lokaal") -> dict:
    """Bediening. Elke handeling is een korte transactie; netwerkwerk (stoppen van een lopende taak) doet de dispatcher.

    `channel` is hoe de handeling binnenkwam: "chatgpt" (de controller-MCP van ChatGPT), "rpc" (node-transport, o.a.
    Monitor 2 op een andere machine) of "lokaal" (Monitor 2 of CLI op de control plane). Goedkeuren en een menselijke
    controle kunnen niet via het ChatGPT-kanaal, en alleen door een naam uit `jobs_approvers`."""
    if action == "add":
        return add(args, by, channel)
    if action == "plan":
        return plan(args)
    if action == "list":
        return overview(bool(args.get("include_archived")), args.get("job_id"))
    if action == "repair":
        gated = riskpolicy.enabled(config.load_config())
        if (not riskpolicy.is_human_channel(channel) if gated else channel == "chatgpt") and _readable(_paths()[0]):
            # Op een gezonde wachtrij zet repair een generatie terug (kan een annulering of goedkeuring ongedaan maken): mens.
            raise JobsError("forbidden", "repair via ChatGPT alleen bij een onleesbare wachtrij; anders doet een mens dit in Monitor 2 (of met r2 jobs)")
        return repair(args.get("mode"), _name(by))
    by = _name(by)
    cfg = config.load_config()
    gated = riskpolicy.enabled(cfg)
    non_human = not riskpolicy.is_human_channel(channel) if gated else channel == "chatgpt"
    if gated and action in ("resume", "retry") and non_human:
        verdict = riskpolicy.classify(args.get("note"))
        if verdict["risky"]:
            raise JobsError("approval_required", "Deze notitie vraagt menselijke goedkeuring: " + "; ".join(verdict["reasons"]))
    if action in ("approve", "reject"):
        if non_human:
            raise JobsError("forbidden", "goedkeuren of weigeren doet een mens in Monitor 2 (of met r2 jobs), niet ChatGPT")
        if by.lower() not in [a.lower() for a in _approvers(cfg)]:
            raise JobsError("forbidden", f"alleen {', '.join(_approvers(cfg))} kunnen goedkeuren of weigeren")
    with _txn() as box:
        d = box["d"]
        if action in ("pause_queue", "resume_queue"):
            node = args.get("node")
            entry = {"by": by, "at": store.now(), "reason": _text(args.get("reason"), "reason", 300)} if action == "pause_queue" else None
            if node in (None, "", "ALL", "all"):
                d["paused"]["all"] = entry
                scope = "alle Runners"
            else:
                node = _node(node)
                if entry:
                    d["paused"]["nodes"][node] = entry
                else:
                    d["paused"]["nodes"].pop(node, None)
                scope = node
            box["dirty"] = True
            _audit(action, f"wachtrij {scope} {'gepauzeerd' if entry else 'hervat'} door {by}", node=scope, by=by)
            return {"ok": True, "paused_all": d["paused"]["all"], "paused_nodes": d["paused"]["nodes"]}
        job = _job(d, args.get("job_id"))
        jid, note = job["id"], _text(args.get("note"), "note", MAX_NOTE)
        if action == "move":
            _queued(job, "verplaatsen")
            order = [j for j in d["order"][job["node"]]]
            queued = [j for j in order if d["jobs"][j]["status"] == QUEUED]
            i = queued.index(jid)
            to = args.get("to")
            if isinstance(to, str) and to.isdigit():
                to = int(to)
            target = {"top": 0, "bottom": len(queued) - 1, "up": max(0, i - 1), "down": min(len(queued) - 1, i + 1)}.get(to)
            if target is None:
                if not isinstance(to, int) or isinstance(to, bool) or not 0 <= to < len(queued):
                    raise JobsError("invalid_input", "to: top, bottom, up, down of een positie (0 = eerst)")
                target = to
            queued.remove(jid)
            queued.insert(target, jid)
            it = iter(queued)
            d["order"][job["node"]] = [next(it) if d["jobs"][j]["status"] == QUEUED else j for j in order]
            _log(job, by, f"verplaatst naar positie {target + 1}")
        elif action == "set_node":
            _queued(job, "naar een andere Runner verplaatsen")
            new = _node(args.get("node"))
            if new != job["node"]:
                d["order"][job["node"]].remove(jid)
                from . import integration_check, integration_workflow
                if job.get('integration_obligation') and not integration_check.satisfied(job):
                    raise JobsError('integration_pending', 'Deze opdracht houdt haar oorspronkelijke node vast tot integratie bewezen is')
                try:
                    workflow = integration_workflow.for_job({**job, 'node': new}, cfg)
                except ValueError as exc:
                    raise JobsError('integration_workflow_config', str(exc)) from exc
                d["order"][new].append(jid)
                _log(job, by, f"verplaatst van {job['node']} naar {new}")
                job["node"] = new
                if workflow:
                    job['integration_workflow'] = workflow
                    job['require_check'] = True
                else:
                    job.pop('integration_workflow', None)
                _reset_approval(job, by, "andere Runner gekozen")
        elif action == "set_deps":
            _queued(job, "afhankelijkheden wijzigen")
            new_deps = _deps(d, jid, args.get("depends_on"))
            if new_deps != job["depends_on"]:
                job["depends_on"] = new_deps
                _reset_approval(job, by, "voorgangers gewijzigd")
            _log(job, by, "voorgangers: " + (", ".join(job["depends_on"]) or "geen"))
        elif action in ("hold", "release"):
            _queued(job, "vasthouden of vrijgeven")
            job["held"] = action == "hold"
            _log(job, by, "vastgehouden" if job["held"] else "vrijgegeven")
        elif action in ("approve", "reject"):
            appr = job.get("approval") or {}
            if not appr.get("required") or appr.get("state") != "open":
                raise JobsError("wrong_state", "voor deze opdracht staat geen goedkeuring open")
            if job["status"] not in (QUEUED, STARTING):
                raise JobsError("wrong_state", "goedkeuren kan alleen voor een wachtende of startende opdracht")
            appr.update(state="approved" if action == "approve" else "rejected", by=by, at=store.now(), note=note)
            if action == "reject":
                if job["status"] == STARTING:
                    job["cancel_requested"] = {"by": by, "at": store.now(), "reason": note, "approval_rejected": True}
                else:
                    job.update(status=CANCELLED, ended_at=store.now(), outcome={"kind": "rejected", "text": f"goedkeuring geweigerd door {by}"})
                    _remove_from_order(d, job)
            _log(job, by, ("goedgekeurd" if action == "approve" else "goedkeuring geweigerd") + (f": {note}" if note else ""))
        elif action == "cancel":
            if job["status"] == QUEUED:
                job.update(status=CANCELLED, ended_at=store.now(), outcome={"kind": "cancelled", "text": f"geannuleerd door {by}"})
                _remove_from_order(d, job)
            elif job["status"] in ACTIVE_STATES:
                job["cancel_requested"] = {"by": by, "at": store.now(), "reason": note}   # de dispatcher stopt de taak
            else:
                raise JobsError("wrong_state", f"opdracht {jid} is al afgerond ({job['status']})")
            _log(job, by, "annuleren gevraagd" + (f": {note}" if note else ""))
        elif action == "check":
            if job["status"] != DONE:
                raise JobsError("wrong_state", "controleren kan pas als Claude klaar is")
            verdict, source = args.get("verdict", "ok"), args.get("source", "chatgpt" if channel == "chatgpt" else "mens")
            if verdict not in ("ok", "afgekeurd") or source not in CHECK_SOURCES:
                raise JobsError("invalid_input", "verdict ok|afgekeurd, source chatgpt|mens")
            if non_human and source != "chatgpt":
                raise JobsError("forbidden", "via ChatGPT is een controle altijd 'chatgpt'; een menselijke controle doe je in Monitor 2")
            job["check"] = {"state": verdict, "source": source, "by": by, "at": store.now(), "note": note}
            _log(job, by, ("gecontroleerd" if verdict == "ok" else "afgekeurd") + f" ({source})" + (f": {note}" if note else ""))
        elif action == "retry":
            if job["status"] not in (FAILED, INCOMPLETE, STOPPED, CANCELLED) and not (job["status"] == DONE and (job.get("check") or {}).get("state") == "afgekeurd"):
                raise JobsError("wrong_state", "opnieuw kan alleen na mislukt, limiet, gestopt, geannuleerd of afgekeurd")
            job["run"] += 1
            job.update(status=QUEUED, task_id=task_id_for(jid, job["run"]), attempts=0, start_failures=0, next_try=0,
                       last_error=None, progress=None, outcome=None, check=None, started_at=None, ended_at=None,
                       archived=False, cancel_requested=None)
            if job.get("approval"):
                job["approval"]["state"] = "open"   # opnieuw uitvoeren vraagt opnieuw goedkeuring
            _remove_from_order(d, job)
            d["order"][job["node"]].insert(_first_queued_index(d, job["node"]), jid)
            _log(job, by, f"opnieuw in de wachtrij (run {job['run']}, nieuwe taak {job['task_id']})")
        elif action == "resume":
            if 'guardian_expected' in args:
                from .guardian import admission
                if _paused(d, job['node']) or admission(job) != args['guardian_expected']:
                    raise JobsError('wrong_state', 'wachtrij of opdracht gewijzigd vóór zelfstandig herstel')
            # Hervatten van de bestaande eenmalige taak (werk in de worktree blijft); de dispatcher roept runner2_resume aan.
            # Een keten of supervisorrun die op een limiet stopte is af; daarvoor is retry (nieuwe run) bedoeld.
            if job["status"] != FAILED or not job.get("started_at") or job.get("continuation") or job.get("supervisor"):
                raise JobsError("wrong_state", "hervatten kan alleen bij een gestarte eenmalige taak die mislukt is; gebruik anders 'opnieuw'")
            others = [j for j in d["order"][job["node"]] if j != jid and d["jobs"][j]["status"] in ACTIVE_STATES]
            if others:
                raise JobsError("wrong_state", f"op {job['node']} loopt al opdracht {others[0]}; hervat als die klaar is")
            job.update(status=RUNNING, resume_requested={"by": by, "at": store.now(), "note": note, "task_id": job["task_id"]},
                       ended_at=None, outcome=None, missing=0)
            if 'guardian_context' in args:
                from .guardian import admission
                job['resume_requested']['guardian_context'] = {'expected': args['guardian_context'], 'admission': admission(job)}
            if job["id"] not in d["order"][job["node"]]:
                d["order"][job["node"]].insert(0, jid)
            _log(job, by, "hervatten gevraagd" + (f": {note}" if note else ""))
        elif action == "archive":
            if job["status"] not in END_STATES:
                raise JobsError("wrong_state", "alleen afgeronde opdrachten kunnen worden gearchiveerd")
            from . import integration_check
            if job.get('integration_obligation') and not integration_check.satisfied(job):
                raise JobsError('integration_pending', 'Archiveren kan pas nadat de verplichte integratiecontrole is geslaagd')
            job["archived"] = True
            _log(job, by, "gearchiveerd")
        else:
            raise JobsError("invalid_input", f"onbekende handeling {action!r}")
        box["dirty"] = True
        _audit(action, f"{jid}: {action} door {by}", job_id=jid, by=by)
        return {"job": view(d, job)}


def _remove_from_order(d: dict, job: dict) -> None:
    for n in NODES:
        if job["id"] in d["order"][n]:
            d["order"][n].remove(job["id"])


def _readable(path: Path) -> bool:
    try:
        _read(path)
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


def repair(mode, by: str) -> dict:
    q, prev, _a, _l = _paths()
    if mode != "restore_prev":
        raise JobsError("invalid_input", "repair: mode=restore_prev")
    with _locked():
        _check_fence()
        try:
            d = _read(prev)
        except (OSError, ValueError, KeyError, TypeError):
            raise JobsError("queue_corrupt", "ook de vorige generatie is onleesbaar; herstel handmatig uit de back-up") from None
        if q.exists():
            store.atomic_write(q.with_name(f"queue.corrupt-{int(time.time())}.json"), q.read_text(errors="replace"))
        store.atomic_write(q, json.dumps(d, indent=1, ensure_ascii=False))
    _audit("repair", f"vorige generatie teruggezet door {by}", by=by)
    return {"ok": True, "version": d["version"]}


# --- dispatcher (alleen op de primaire control plane, aangeroepen door de watch) ---

def _call(tool: str, args: dict) -> dict:
    """Enige poort naar de Runners (start/status/stop op een node); tests vervangen deze."""
    from . import controller_mcp
    return controller_mcp.dispatch(tool, args)


def _code(e: Exception) -> str:
    return getattr(e, "code", None) or "runner2_unavailable"


def enabled(cfg: dict) -> bool:
    if not cfg.get("jobs_enabled", True):
        return False
    from . import cplane
    return cplane.is_primary()


def start_args(job: dict) -> dict:
    a = {"repository": job["repo"], "prompt": job["prompt"], "node": job["node"], "task_id": job["task_id"],
         "title": (job["title"] or "")[:80] + f" · {job['id']}"}
    if job.get('integration_workflow'):
        from .integration_workflow import instructions
        a['prompt'] += instructions(job, job['integration_workflow'])
    if job.get("model"):
        a["model"] = job["model"]
    if job.get("supervisor"):
        a["supervisor"] = True
        a.update(job.get("supervisor_opts") or {})
    elif job.get("continuation"):
        a["continuation"] = True
    return a


RECOVERABLE = {"interrupted", "api_error", "stalled", "auth_error"}   # gelijk aan watch.RECOVERABLE: zulke fouten herstelt de Runner zelf


def _guardian_resume(job):
    from . import guardian
    request = job['resume_requested']
    context = request['guardian_context']
    with _launch_lock(job['node']):
        queue = load();current = queue['jobs'].get(job['id'])
        if (not current or _paused(queue, job['node']) or current.get('resume_requested') != request
                or guardian.admission(current) != context['admission']):
            raise JobsError('wrong_state', 'herstel gepauzeerd: actuele wachtrij/toestemming is gewijzigd')
        actual = guardian.call_node(job['node'], {'action': 'probe', 'job': current})
        before, after = context['expected']['identity'], actual['identity']
        if (all(before.get(k) == after.get(k) for k in ('task_id', 'session_id'))
                and after.get('turn', 0) > before.get('turn', 0)):
            return {'state': 'continuation_already_observed', 'current': actual}
        result = guardian.call_node(job['node'], {'action': 'recover', 'job': current,
                  'expected': context['expected'], 'mode': 'resume_technical'})
        if not guardian.confirmed('resume_technical', context['expected'], result.get('current', {})):
            raise JobsError('recovery_unconfirmed', 'hervatting nog niet door een echte nieuwe beurt bevestigd')
        return result


def _observe(job: dict, cfg: dict | None = None) -> dict:
    """Wat Runner 2 nu van de taak van deze opdracht zegt -> {'kind': running|done|failed|incomplete|stopped|missing|error, ...}."""
    cfg = cfg or {}
    try:
        s = _call("runner2_status", {"task_id": job["task_id"], "node": job["node"]})
    except Exception as e:   # noqa: BLE001
        if _code(e) == "not_found":
            return {"kind": "missing"}
        return {"kind": "error", "code": _code(e), "message": str(getattr(e, "message", e))[:300]}
    task_status = s.get("status")
    prog = {"task_status": task_status, "elapsed_s": s.get("elapsed_s"), "summary": (s.get("summary") or "")[:2000] or None,
            "current_task": job["task_id"]}
    text = {"running": "Claude werkt", "waiting_for_human": "wacht op antwoord", "paused": "gepauzeerd", "created": "start",
            "failed": "taak mislukt", "completed": "taak klaar", "stopped": "taak gestopt"}.get(task_status, task_status or "")
    sup, ch = s.get("supervisor"), s.get("chain")
    if job.get("supervisor") and sup:
        prog["supervisor"] = {k: sup.get(k) for k in ("state", "phase", "chain", "max_chains", "commits_total", "current_task")}
        text = f"keten {sup.get('chain')} van {sup.get('max_chains')} · {sup.get('phase') or ''}".strip(" ·")
        kind = {"active": "running", "project_complete": "done", "limit_reached": "incomplete", "blocked": "incomplete",
                "failed": "failed", "stopped": "stopped"}.get(sup.get("state"), "running")
        cur = sup.get("current_task")
        if cur:
            prog["current_task"] = cur
        if kind == "running" and cur and cur != job["task_id"]:
            try:   # de lopende schakel/judge bepaalt of er een vraag openstaat
                prog["task_status"] = _call("runner2_status", {"task_id": cur, "node": job["node"]}).get("status")
            except Exception:   # noqa: BLE001
                pass
    elif job.get("continuation") and ch:
        prog["chain"] = {k: ch.get(k) for k in ("state", "phase", "link", "links_total", "max_tasks", "current_task")}
        if ch.get("current_task"):
            prog["current_task"] = ch["current_task"]
        text = f"schakel {ch.get('links_total')} van max {ch.get('max_tasks')} · {ch.get('phase') or ''}".strip(" ·")
        kind = {"active": "running", "completed": "done", "limit_reached": "incomplete", "stopped": "stopped"}.get(ch.get("state"), "running")
        if kind == "running" and ch.get("phase") in ("waiting_for_human", "paused"):
            prog["task_status"] = ch["phase"]
    else:
        kind = {"completed": "done", "failed": "failed", "stopped": "stopped"}.get(task_status, "running")
        err, rec = s.get("last_error") or {}, s.get("recovery") or {}
        will = rec.get("will_retry")   # oordeel van de Runner zelf (zijn eigen max_recovery_attempts); oudere Runners: schatting
        if will is None:
            will = int(rec.get("attempts") or 0) < int(cfg.get("max_recovery_attempts", 5))
        if kind == "failed" and err.get("kind") in RECOVERABLE and will:
            # De Runner-watch herstelt dit zelf (interrupted/api_error/stalled): geen eindstand, de Runner blijft van deze opdracht.
            kind, text = "running", f"herstel loopt na {err.get('kind')} (poging {int(rec.get('attempts') or 0)})"
            prog["task_status"] = "recovering"
    prog["text"] = text
    out = {"kind": kind, "progress": prog}
    if kind in ("failed", "stopped", "incomplete"):
        stop = (sup or {}).get("stopped_because") or (ch or {}).get("stopped_because") or s.get("stop") or s.get("last_error") or {}
        out["why"] = (stop.get("message") or stop.get("kind") or stop.get("reason") or "")[:300] if isinstance(stop, dict) else str(stop)[:300]
    return out


def _foreign_work(job: dict) -> str | None:
    """Loopt er op de Runner een keten of supervisorrun die niet van deze opdracht is? Tussen twee schakels is de
    machinelock even vrij; dan mag de wachtrij er niet tussendoor. Geeft een korte reden, of None."""
    from .discussion_dispatch import pending_on_node
    try:
        if pending_on_node(job['node']):
            return 'Overleg loopt of de eerdere overlegstart wordt nog gecontroleerd.'
    except (ValueError, OSError, KeyError, TypeError):
        return 'Overleggegevens ontbreken of zijn beschadigd; veilige blokkering op deze machine, controleer de overleggeschiedenis.'
    try:
        s = _call("runner2_status", {"node": job["node"]})
    except Exception:   # noqa: BLE001 -- de start zelf meldt onbereikbaar/bezet
        return None
    hexpart = job["task_id"][3:]
    for ch in s.get("chains") or []:
        if ch.get("state") == "active" and ch.get("id") != "C-" + hexpart:
            return f"keten {ch.get('id')} op {job['node']} loopt nog (tussen twee schakels)"
    for sup in s.get("supervisors") or []:
        if sup.get("state") == "active" and sup.get("id") != "S-" + hexpart:
            return f"supervisorrun {sup.get('id')} op {job['node']} loopt nog"
    return None


def tick(cfg: dict | None = None, now: float | None = None) -> dict:
    """Eén ronde: lopende opdrachten volgen, annuleringen uitvoeren, per vrije Runner de volgende opdracht starten.

    Volgorde per ronde: (1) korte lees-snapshot, (2) netwerk zonder lock, (3) verwerken onder de lock met controle dat de
    opdracht nog in dezelfde toestand en run is. Geeft een korte samenvatting (voor tests en logs)."""
    cfg = cfg or config.load_config()
    if not enabled(cfg):
        return {"skipped": "disabled"}
    now = time.time() if now is None else now
    if handed_over():
        return {"skipped": "handed_over"}
    try:
        snap = load()
    except JobsError as e:
        return {"skipped": e.code}
    try:
        return _tick(snap, cfg, now)
    except JobsError as e:
        if e.code == "handed_over":   # hek kwam midden in deze ronde: niets meer verwerken
            return {"skipped": "handed_over"}
        raise


def enforce_policy(d: dict, cfg: dict) -> list:
    """Goedkeuringspoort bij het starten (defence in depth, ook voor oude opdrachten zonder `policy`).

    Een wachtende opdracht van ChatGPT die risicovol is of waarvan de prompt na het beoordelen is gewijzigd, krijgt een
    open goedkeuring en start dus niet voordat een mens goedkeurt. Geeft de aangepaste opdracht-id's terug."""
    changed = []
    for job in d["jobs"].values():
        if job["status"] not in (QUEUED, STARTING):
            continue
        pol = job.get("policy")
        if not isinstance(pol, dict):   # legacy: kanaal onbekend, dus afgeleid van de indiener
            # Missing provenance cannot establish a human channel; classify it conservatively.
            origin = job.get("origin_channel") or "unknown"
            verdict = riskpolicy.assess(origin, [job.get("title"), job.get("prompt")], job.get("repo"), cfg)
            pol = job["policy"] = {**riskpolicy.policy_record("legacy", job.get("prompt"), verdict), "legacy": True}
            changed.append(job["id"])
        if not pol.get("gated"):
            continue
        tampered = pol.get("prompt_sha") != riskpolicy.prompt_hash(job.get("prompt"))
        appr = job.get("approval") or {}
        if tampered or (pol.get("risky") and not appr.get("required")):
            kind = riskpolicy.approval_kind(pol.get("categories"))
            job["approval"] = {"required": True, "kind": kind, "state": "open",
                               "by_policy": ["prompt gewijzigd na beoordeling"] if tampered else pol.get("reasons")}
            if tampered:
                # One reset per changed prompt; approval always binds the current text.
                pol["prompt_sha"] = riskpolicy.prompt_hash(job.get("prompt"))
            _log(job, "runner2", f"beleid: goedkeuring vereist ({kind}); " + ("prompt gewijzigd" if tampered else "; ".join(pol.get("reasons") or [])))
            if job["id"] not in changed:
                changed.append(job["id"])
    return changed



def _tick(snap: dict, cfg: dict, now: float) -> dict:
    summary = {"observed": 0, "started": [], "ended": [], "deferred": []}
    retry_s = float(cfg.get("jobs_start_retry_s", 20))
    max_fail = int(cfg.get("jobs_max_start_failures", 6))
    missing_max = int(cfg.get("jobs_missing_max", 3))
    created_grace = float(cfg.get("jobs_created_grace_s", 90))

    # 0) na annuleren tijdens het starten: verschijnt de taak alsnog (start liep na een time-out door), dan stoppen
    for job in [j for j in snap["jobs"].values() if j["status"] == CANCELLED and (j.get("orphan_check_until") or 0) > now]:
        obs = _observe(job, cfg)
        if obs["kind"] == "running":
            _stop_task(job)
            with _txn() as box:
                cur = box["d"]["jobs"].get(job["id"])
                if cur:
                    _log(cur, "runner2", f"taak {job['task_id']} verscheen na het annuleren alsnog en is gestopt")
                    box["dirty"] = True

    # A lost acknowledgement may leave a real task STARTING. A pause must
    # still drain it, without attempting execution of an absent task.
    for job in [j for j in snap['jobs'].values() if j['status'] == STARTING and not j.get('cancel_requested')
                and ((j.get('attempts', 0) > 0 and _paused(snap, j['node']))
                    or (riskpolicy.enabled(cfg) and (j.get('approval') or {}).get('required')
                        and (j.get('approval') or {}).get('state') != 'approved'))]:
        obs = _observe(job, cfg)
        if obs['kind'] in ('missing', 'error'):
            continue
        with _txn() as box:
            cur = box['d']['jobs'].get(job['id'])
            if not cur or any(cur.get(k) != job.get(k) for k in ('status','run','task_id','node')):
                continue
            cur['progress'] = obs.get('progress')
            if obs['kind'] == 'running':
                cur.update(status=RUNNING, started_at=cur.get('started_at') or cur.get('claimed_at') or store.now())
            else:
                _finish(box['d'], cur, obs)
                summary['ended'].append(cur['id'])
            box['dirty'] = True

    # 1) lopende opdrachten volgen (en annuleren / hervatten)
    for job in [j for j in snap["jobs"].values() if j["status"] == RUNNING]:
        if job.get("cancel_requested"):
            _stop_task(job)
        resumed = None
        if job.get("resume_requested") and not job.get("cancel_requested"):
            target = job["resume_requested"].get("task_id") or job["task_id"]
            try:
                if job['resume_requested'].get('guardian_context'):
                    _guardian_resume(job)
                else:
                    _call("runner2_resume", {"task_id": target, "node": job["node"],
                                             "note": (job["resume_requested"].get("note") or "")[:500]})
                resumed = "ok"
            except Exception as e:   # noqa: BLE001
                resumed = f"{_code(e)}: {str(getattr(e, 'message', e))[:200]}"
        obs = _observe(job, cfg)
        respawn = obs["kind"] == "running" and (obs.get("progress") or {}).get("task_status") == "created" \
            and now - _epoch(job.get("started_at")) > created_grace
        if respawn:   # taakproces kwam nooit op gang (bv. machinelock tussendoor bezet): idempotent opnieuw starten
            try:
                _call("runner2_start", start_args(job))
            except Exception:   # noqa: BLE001 -- busy/onbereikbaar: volgende ronde opnieuw
                pass
        summary["observed"] += 1
        with _txn() as box:
            cur = box["d"]["jobs"].get(job["id"])
            if not cur or cur["status"] != RUNNING or cur["run"] != job["run"]:
                continue
            box["dirty"] = True
            if resumed is not None and cur.get("resume_requested"):
                cur["resume_requested"] = None
                _log(cur, "runner2", "taak hervat" if resumed == "ok" else f"hervatten mislukt: {resumed}")
            if respawn:
                _log(cur, "runner2", "taak stond nog op 'created'; opnieuw (idempotent) gestart")
            if obs["kind"] == "error":
                cur["progress"] = {**(cur.get("progress") or {}), "stale": obs["message"]}
                continue
            if obs["kind"] == "missing":
                cur["missing"] = int(cur.get("missing") or 0) + 1
                cur["progress"] = {**(cur.get("progress") or {}), "stale": "taak niet gevonden op de Runner"}
                if cur.get("cancel_requested") or cur["missing"] >= missing_max:
                    _finish(box["d"], cur, {"kind": "cancelled" if cur.get("cancel_requested") else "failed",
                                            "why": f"taak {cur['task_id']} bestaat niet (meer) op {cur['node']}"})
                    summary["ended"].append(cur["id"])
                continue
            cur["missing"] = 0
            cur["progress"] = obs["progress"]
            if obs["kind"] != "running":
                _finish(box["d"], cur, obs)
                summary["ended"].append(cur["id"])

    # Completed standalone integrations can satisfy the required completion check.
    from . import integration_check
    integration_check.reconcile(load(), cfg)

    # 2) startclaims: per Runner hooguit één actieve opdracht, nooit op een gepauzeerde Runner
    with _txn() as box:
        d = box["d"]
        if riskpolicy.enabled(cfg) and enforce_policy(d, cfg):
            box["dirty"] = True
        for node in NODES:
            order = d["order"][node]
            if any(d["jobs"][j]["status"] in ACTIVE_STATES for j in order if j in d["jobs"]):
                continue
            if _paused(d, node):
                continue
            for jid in order:
                job = d["jobs"].get(jid)
                if job and job["status"] == QUEUED and label(d, job)[0] == "in_wachtrij":
                    job.update(status=STARTING, claimed_at=store.now(), next_try=0)
                    _log(job, "runner2", f"start op {node} (taak {job['task_id']})")
                    box["dirty"] = True
                    break
        claims = [copy.deepcopy(j) for j in d["jobs"].values() if j["status"] == STARTING and j.get("next_try", 0) <= now
                  and (j.get("cancel_requested") or (not (_paused(d, j["node"]) or j.get("held")) and _dep_state(d, j)[0] == "ok"
                       and not (riskpolicy.enabled(cfg) and (j.get("approval") or {}).get("required")
                                and (j.get("approval") or {}).get("state") != "approved")))]

    # 3) starten (idempotent) en verwerken
    for job in claims:
        if job.get("cancel_requested"):
            obs = _observe(job, cfg)   # eerst kijken: een al voltooide taak niet meer stoppen
            if obs["kind"] == "running":
                _stop_task(job)
                obs = _observe(job, cfg)
            with _txn() as box:
                cur = box["d"]["jobs"].get(job["id"])
                if cur and cur["status"] == STARTING and cur["run"] == job["run"]:
                    if obs["kind"] == "done":   # het werk was al af (antwoord eerder verloren): niet als geannuleerd boeken
                        _finish(box["d"], cur, obs)
                        _log(cur, "runner2", "annuleren kwam te laat: de taak was al klaar")
                    elif obs["kind"] == "missing" and (cur.get("approval") or {}).get("state") == "rejected":
                        # A start acknowledgement may be lost; keep the node reserved until its outcome is known.
                        cur["last_error"] = {"code": "uncertain_start", "message": "geweigerde start nog niet aantoonbaar beëindigd", "at": store.now()}
                    elif obs["kind"] in ("missing", "stopped", "failed", "incomplete"):
                        _finish(box["d"], cur, {"kind": "cancelled"})
                        if obs["kind"] == "missing":   # een doorlopende start na een time-out wordt nog even bewaakt
                            cur["orphan_check_until"] = now + float(cfg.get("jobs_orphan_watch_s", 900))
                    box["dirty"] = True
            continue
        if reason := _foreign_work(job):
            with _txn() as box:
                cur = box["d"]["jobs"].get(job["id"])
                if cur and cur["status"] == STARTING and cur["run"] == job["run"]:
                    if (cur.get("last_error") or {}).get("message") != reason:
                        _log(cur, "runner2", f"wacht: {reason}")
                    cur.update(last_error={"code": "busy", "message": reason, "at": store.now()}, next_try=now + retry_s)
                    box["dirty"] = True
            summary["deferred"].append(job["id"])
            continue
        with _launch_lock(job['node']):
            with _txn() as box:
                current = box['d']
                cur = current['jobs'].get(job['id'])
                if (not cur or any(cur.get(k) != job.get(k) for k in
                        ('status','run','task_id','node','repo','prompt','integration_workflow','approval','depends_on'))
                        or cur.get('cancel_requested') or cur.get('held') or _paused(current, job['node'])
                        or _dep_state(current, cur)[0] != 'ok' or not enabled(cfg)):
                    continue
                if cur.get('integration_workflow') or integration_check.required(cur, cfg):
                    obligation = cur.setdefault('integration_obligation', {'node': cur['node'], 'task_ids': []})
                    if cur['task_id'] not in obligation['task_ids']:
                        obligation['task_ids'].append(cur['task_id'])
                        box['dirty'] = True
            try:
                out = _call("runner2_start", start_args(job))
                err = None
            except Exception as e:   # noqa: BLE001
                out, err = None, {"code": _code(e), "message": str(getattr(e, "message", e))[:300], "at": store.now()}
        with _txn() as box:
            cur = box["d"]["jobs"].get(job["id"])
            if not cur or cur["status"] != STARTING or cur["run"] != job["run"]:
                continue
            cur["attempts"] = int(cur.get("attempts", 0)) + 1
            if err is None:
                cur.update(status=RUNNING, started_at=cur.get("started_at") or store.now(), last_error=None, next_try=0,
                           progress={"task_status": out.get("status"), "text": "gestart"})
                _log(cur, "runner2", f"gestart op {cur['node']} als {out.get('task_id')}" + (" (bestond al)" if out.get("already_started") else ""))
                _audit("started", f"{cur['id']} gestart als {cur['task_id']} op {cur['node']}", job_id=cur["id"],
                       task_id=cur["task_id"], node=cur["node"], already=bool(out.get("already_started")))
                summary["started"].append(cur["id"])
            elif err["code"] in ("busy", "node_unreachable"):
                if (cur.get("last_error") or {}).get("code") != err["code"]:
                    _log(cur, "runner2", f"wacht: {err['message'][:200]}")
                cur.update(last_error=err, next_try=now + retry_s)
                summary["deferred"].append(cur["id"])
            else:
                cur["start_failures"] = int(cur.get("start_failures", 0)) + 1
                cur.update(last_error=err, next_try=now + retry_s)
                _log(cur, "runner2", f"startpoging {cur['start_failures']} mislukt: {err['code']}: {err['message'][:200]}")
                if cur["start_failures"] >= max_fail or err["code"] in ("invalid_input", "repo_invalid", "unsupported_node"):
                    _finish(box["d"], cur, {"kind": "failed", "why": f"starten mislukt: {err['code']}: {err['message']}"})
                    summary["ended"].append(cur["id"])
            box["dirty"] = True
    return summary


def _epoch(ts) -> float:
    try:
        return time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z"))
    except (TypeError, ValueError):
        return time.time()


def _stop_task(job: dict) -> None:
    """Vraagt Runner 2 de taak (en daarmee keten/supervisor) te stoppen. Bestaat de taak niet (meer), dan is er niets te doen."""
    req = job.get("cancel_requested") or {}
    try:
        _call("runner2_stop", {"task_id": job["task_id"], "node": job["node"],
                               "reason": f"opdracht {job['id']} geannuleerd door {req.get('by', '?')}"})
    except Exception:   # noqa: BLE001 -- volgende ronde opnieuw; not_found / al gestopt is goed
        pass


def _has_started_audit(job_id: str) -> bool:
    audit = _paths()[2]
    try:
        with open(audit, encoding="utf-8") as f:
            for line in f:
                if job_id in line:
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    if e.get("job_id") == job_id and e.get("kind") in ("started", "started_hersteld"):
                        return True
    except OSError:
        pass
    return False


def _finish(d: dict, job: dict, obs: dict) -> None:
    kind = obs["kind"]
    if job.get("cancel_requested") and kind in ("stopped", "cancelled", "missing"):
        status, text = CANCELLED, f"geannuleerd door {job['cancel_requested'].get('by')}"
    else:
        status = {"done": DONE, "failed": FAILED, "incomplete": INCOMPLETE, "stopped": STOPPED, "cancelled": CANCELLED}[kind]
        text = {"done": "Claude klaar", "failed": "mislukt", "incomplete": "gestopt op een limiet of blokkade",
                "stopped": "gestopt", "cancelled": "geannuleerd"}[kind]
        if obs.get("why"):
            text += f": {obs['why']}"
    job.update(status=status, ended_at=store.now(), outcome={"kind": kind, "text": text[:500]})
    _remove_from_order(d, job)
    _log(job, "runner2", text)
    if job.get("started_at") and not _has_started_audit(job["id"]):
        # De start werd opgeslagen, maar de harde crash kwam vóór de auditregel: vul die aan, zonder de volgorde te breken.
        _audit("started_hersteld", f"{job['id']} gestart als {job.get('task_id')} (auditregel na herstart aangevuld)",
               job_id=job["id"], task_id=job.get("task_id"), node=job.get("node"))
    _audit("ended", f"{job['id']} {status}", job_id=job["id"], status=status, task_id=job.get("task_id"))


def discussion_admission(node: str, cfg: dict) -> dict:
    """Fresh primary queue admission; caller holds the launch lock at the final check."""
    from . import integration_check
    from .discussion_dispatch import pending_on_node
    if not enabled(cfg):return {'allowed': False, 'reason': 'De wachtrij is niet actief op deze primary.'}
    d = load()
    if _paused(d, node):return {'allowed': False, 'reason': 'De wachtrij is bewust gepauzeerd.'}
    try:
        if pending_on_node(node):return {'allowed': False, 'reason': 'Een eerdere overlegvraag loopt of de start wordt nog gecontroleerd.'}
    except (ValueError, OSError, KeyError, TypeError):
        return {'allowed': False, 'reason': 'Overleggegevens ontbreken of zijn beschadigd; veilige blokkering blijft staan.'}
    if any(j.get('node') == node and j['status'] in ACTIVE_STATES for j in d['jobs'].values()):
        return {'allowed': False, 'reason': 'De machine programmeert of start nog een opdracht.'}
    for j in d['jobs'].values():
        bound = (j.get('integration_obligation') or {}).get('node', j.get('node'))
        if bound == node and (j.get('integration_obligation') or (j.get('started_at') and integration_check.required(j, cfg))) and not integration_check.satisfied(j):
            return {'allowed': False, 'reason': 'Wacht op bewezen integratie en geslaagde nacontroles van de vorige opdracht.'}
    for jid in d['order'].get(node, []):
        j = d['jobs'].get(jid)
        if not j:continue
        if j['status'] in ACTIVE_STATES:return {'allowed': False, 'reason': 'De machine programmeert of start nog een opdracht.'}
        if j['status'] == QUEUED and label(d, j)[0] == 'in_wachtrij':
            return {'allowed': False, 'reason': 'Een gereedstaande programmeeropdracht gaat eerst.'}
    return {'allowed': True, 'reason': ''}

