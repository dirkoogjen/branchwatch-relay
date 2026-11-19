"""Supervisor (autopilot) boven autonome ketens: opt-in, meerdere ketens achter elkaar naar één projectdoel.

Een keten mag gewoon `goal_reached` melden. Daarna beoordeelt een aparte judge-Claude het ketenresultaat op
projectniveau en beslist: continue_new_chain | project_complete | needs_human | blocked. Bij `continue_new_chain`
start de supervisor een NIEUWE keten op de tip (laatste commit) van de vorige; er wordt nooit gepusht of naar de
basisbranch geïntegreerd.

De judge is een gewone Runner 2-taak (rol `review`, bestaande modelroutering, eigen conversatie en worktree op de tip),
dus machinelock, herstel, stop, pauze en waiting_for_human werken zonder nieuwe infrastructuur. De supervisor bewaart
alleen zijn eigen toestand in ~/.runner2/supervisors/<id>/supervisor.json. De watch roept `advance_all` aan; alle
overgangen zijn idempotent: id's van keten, eerste taak en judge-taak worden vóór het aanmaken vastgelegd, en de
beslissing + de nieuwe ketenintentie worden in één atomaire schrijfactie opgeslagen.
"""
import json
import time
import uuid
from contextlib import contextmanager

from . import actions, chains, config, gitops, quality, store
from .filelock import lock_exclusive
from .log import log

ACTIVE, COMPLETE, BLOCKED, LIMIT, STOPPED, FAILED = ("active", "project_complete", "blocked", "limit_reached",
                                                      "stopped", "failed")
ENDED = (COMPLETE, BLOCKED, LIMIT, STOPPED, FAILED)
DECISIONS = ("continue_new_chain", "project_complete", "needs_human", "blocked")
PROGRESS = ("substantive", "cosmetic", "none")
# Ketenuitkomsten waarover een beoordeling zinvol is; al het andere is een storing of een stop door een mens.
JUDGED_KINDS = ("goal_reached", "max_tasks", "no_progress", "stagnation", "time_limit")
USER_STOP_KINDS = ("stopped_by_user", "task_stopped")
SPAWN_GRACE_S = 30
MAX_SPAWN_ATTEMPTS = 5
GOAL_CHARS = 2000
PROMPT_CHARS = 8000
REASON_CHARS = 1500
JUDGE_PROMPT_CHARS = 30000
MAX_STATUS_FILES = 10

SAFETY_FOOTER = (
    "Vaste regels voor elke keten onder de supervisor (de supervisor kan ze niet wijzigen): push nooit en integreer "
    "niets naar main of een andere branch; gebruik of vraag geen secrets of credentials buiten wat de opdracht "
    "expliciet geeft; voer geen destructieve, onomkeerbare of infrastructuuracties buiten de worktree uit; bij twijfel "
    "of een ontbrekende toegang: mcp__r2__runner_ask."
)


def new_id() -> str:
    return "S-" + uuid.uuid4().hex[:12]


def _task_id() -> str:
    return "R2-" + uuid.uuid4().hex[:12]


def _dir(sid: str):
    return config.supervisors_dir() / sid


def _path(sid: str):
    return _dir(sid) / "supervisor.json"


@contextmanager
def _flock(sid: str):
    _dir(sid).mkdir(parents=True, exist_ok=True)
    with open(_dir(sid) / ".lock", "w") as f:
        lock_exclusive(f)
        yield


def load(sid: str) -> dict | None:
    try:
        return json.loads(_path(sid).read_text())
    except (OSError, ValueError):
        return None


def save(sup: dict) -> None:
    sup["updated"] = store.now()
    store.atomic_write(_path(sup["supervisor_id"]), json.dumps(sup, indent=1))


def all_supervisors() -> list:
    d = config.supervisors_dir()
    out = [s for p in sorted(d.glob("S-*/supervisor.json")) if (s := load(p.parent.name))] if d.exists() else []
    return sorted(out, key=lambda s: s["created"])


def recent(sup: dict, days: float = 7) -> bool:
    """Beëindigde supervisor die nog in overzichten (en voor de monitor) zichtbaar blijft."""
    try:
        return time.time() - chains._epoch(sup["updated"]) < days * 86400
    except (KeyError, ValueError):
        return False


def for_task(state: dict) -> dict | None:
    """De supervisor van een taak: een judge-taak draagt `supervisor_id`, een ketenschakel zit via zijn keten eronder."""
    sid = state.get("supervisor_id")
    if not sid and state.get("chain_id"):
        sid = ((chains.load(state["chain_id"]) or {}).get("supervisor") or {}).get("id")
    return load(sid) if sid else None


def _clip(text, n) -> str:
    return chains._clip(text, n)


def _norm(text) -> str:
    return " ".join(str(text or "").lower().split())


def create(sid: str, repo: str, goal: str, prompt: str, role: str, model: str | None, title: str, limits: dict,
           chain_limits: dict, status_files: list, first_chain: dict, base: dict, quality_limits: dict | None = None) -> dict:
    now = time.time()
    sup = {"supervisor_id": sid, "repo": repo, "goal": goal[:GOAL_CHARS], "prompt": prompt[:PROMPT_CHARS], "role": role,
           "model": model, "title": title, "limits": limits, "chain_limits": chain_limits,
           "status_files": status_files, "state": ACTIVE, "stop": None, "phase": "chain", "created": store.now(),
           "updated": store.now(), "deadline": chains._iso(now + limits["max_hours"] * 3600),
           "root_ref": base["ref"], "root_commit": base["commit"], "no_progress": 0, "commits_total": 0,
           "goals": [_norm(first_chain["goal"])], "judge": None, "decisions": [], "start_failures": 0,
           "next_retry_at": 0, "quality_limits": quality_limits or quality.limits_from_config(config.load_config()),
           "quality": quality.fresh_state(base["commit"]),
           "chains": [{"n": 1, "chain_id": first_chain["chain_id"], "task_id": first_chain["task_id"],
                       "goal": first_chain["goal"][:GOAL_CHARS], "prompt": prompt[:PROMPT_CHARS],
                       "start_commit": base["commit"], "created": True}]}
    with _flock(sid):
        save(sup)
    return sup


def _end(sup: dict, state: str, kind: str, message: str) -> None:
    sup["state"], sup["stop"] = state, {"kind": kind, "message": message[:600], "at": store.now()}
    log(f"{sup['supervisor_id']}: supervisor {state} ({kind}): {message[:200]}")


def stop_supervisor(sid: str, kind: str, message: str) -> bool:
    """Beëindigt een actieve supervisor (idempotent). Geeft True als er iets veranderde."""
    with _flock(sid):
        sup = load(sid)
        if not sup or sup["state"] != ACTIVE:
            return False
        _end(sup, STOPPED, kind, message)
        save(sup)
        return True


def live_tasks(sup: dict) -> list:
    """Taken van deze supervisor die nog niet terminaal zijn (judge en de taak van een actieve keten)."""
    out = []
    j = sup.get("judge")
    if j and j.get("provisioned"):
        out.append(j["task_id"])
    ch = chains.load(sup["chains"][-1]["chain_id"])
    if ch and ch["state"] == chains.ACTIVE:
        out.append(ch["links"][-1]["task_id"])
    live = []
    for tid in out:
        try:
            if store.load(tid)["status"] not in store.TERMINAL:
                live.append(tid)
        except (OSError, ValueError):
            pass
    return live


# --- beslissing van de judge ---

def run_open_actions(sup: dict) -> list:
    """Open menselijke actiepunten van alle taken (ketenschakels en beoordelingen) onder deze supervisor."""
    sid = sup["supervisor_id"]
    return actions.open_items([s for s in store.all_tasks() if s.get("supervisor_id") == sid])


def validate_decision(args: dict, sup: dict | None = None, check_actions: bool = True) -> str | None:
    """Foutmelding (voor Claude, in het Nederlands) of None. Gebruikt door de judge-tool én de supervisor zelf.

    Met `sup` komen de toestandsgebonden kwaliteitsregels erbij (budget, geen opeenvolgende audits, gewijzigd oppervlak)."""
    def text(k):
        return isinstance(args.get(k), str) and bool(args[k].strip())
    d = args.get("decision")
    if d not in DECISIONS:
        return f"decision moet een van {', '.join(DECISIONS)} zijn"
    if not text("reason"):
        return "reason (onderbouwing) is verplicht"
    if args.get("chain_progress") not in PROGRESS:
        return f"chain_progress moet een van {', '.join(PROGRESS)} zijn"
    if d == "continue_new_chain" and not (text("next_goal") and text("next_prompt")):
        return "continue_new_chain vraagt een concrete next_goal én next_prompt"
    if d == "continue_new_chain":
        err = quality.validate_plan(args, sup, quality.changed_paths(sup) if sup and (args.get("chain_kind") == "quality") else None)
        if err:
            return err
    if d == "project_complete":
        ev = args.get("evidence")
        if not (isinstance(ev, list) and any(isinstance(e, str) and e.strip() for e in ev)):
            return "project_complete vraagt evidence: een lijst van wat je zelf controleerde en waaruit blijkt dat het projectdoel af is"
    if d == "needs_human" and not text("question"):
        return "needs_human vraagt een concrete question aan de mens"
    err = actions.validate(args.get("open_actions"))
    if err:
        return err
    if d == "needs_human" and args.get("open_actions"):
        return "needs_human blokkeert al en is zelf de menselijke actie: laat open_actions weg (alleen voor niet-blokkerende punten)"
    res = args.get("resolve_actions")
    if res is not None:
        if not (isinstance(res, list) and all(isinstance(i, str) and i.strip() for i in res)):
            return "resolve_actions moet een lijst van id's van open actiepunten zijn"
        if check_actions and sup:
            known = {i["id"] for i in run_open_actions(sup)}
            unknown = [i for i in res if i not in known]
            if unknown:
                return f"resolve_actions bevat onbekende of al gesloten id's: {', '.join(unknown[:5])}"
    return None


# --- prompts ---

def _tests_of(task_id: str) -> dict:
    try:
        f = store.load(task_id).get("finish") or {}
    except (OSError, ValueError):
        return {}
    return {"summary": f.get("summary"), "tests_run": f.get("tests_run") or [], "test_results": f.get("test_results"),
            "attention": f.get("attention") or []}


def _chain_block(entry: dict, detailed: bool) -> str:
    out = entry.get("outcome") or {}
    kind = quality.kind_of(entry)
    head = (f"Keten {entry['n']} ({entry['chain_id']})" + (f" [{kind}, controle {quality.scope_of(entry)}]" if kind != "feature" or quality.scope_of(entry) != "targeted" else "")
            + f": doel: {_clip(entry['goal'], 300)}\n"
            f"  uitkomst: {out.get('kind', '?')} — {_clip(out.get('message'), 200)}; commits: {entry.get('commits', '?')}")
    if entry.get("decision"):
        d = entry["decision"]
        head += f"\n  beslissing erna: {d['decision']} — {_clip(d.get('reason'), 200)}"
    if not detailed:
        return head
    ch = chains.load(entry["chain_id"]) or {"links": []}
    lines = []
    for l in ch["links"]:
        t = _tests_of(l["task_id"])
        tests = f"; tests: {_clip(', '.join(t.get('tests_run') or []), 160)} → {_clip(t.get('test_results'), 120)}" if t.get("tests_run") else "; geen tests gemeld"
        lines.append(f"  - schakel {l['link']} ({l['task_id']}, {l.get('commits', '?')} commits, {l.get('chain_status')}): "
                     f"{_clip(l.get('summary'), 400)}{tests}")
    return head + "\n" + "\n".join(lines)


def judge_prompt(sup: dict) -> str:
    cur = sup["chains"][-1]
    lim = sup["limits"]
    remaining_h = max(0.0, (chains._epoch(sup["deadline"]) - time.time()) / 3600)
    last_tip = chains.load(cur["chain_id"]) or {"links": [{}]}
    tip_link = last_tip["links"][-1]
    try:
        tip_state = store.load(tip_link["task_id"])
    except (OSError, ValueError, KeyError):
        tip_state = {}
    integ = tip_state.get("integration") or {}
    fin = _tests_of(tip_link.get("task_id", ""))
    parts = [
        f"[Supervisor {sup['supervisor_id']} — beoordeling na keten {cur['n']}]",
        f"PROJECTDOEL (overkoepelend, wat uiteindelijk af moet):\n{sup['goal']}",
        f"ORIGINELE OPDRACHT aan de eerste keten:\n{_clip(sup['prompt'], 3000)}",
        f"BUDGET: keten {cur['n']} van maximaal {lim['max_chains']} (nog {max(0, lim['max_chains'] - cur['n'])} nieuwe ketens mogelijk); "
        f"nog ca. {remaining_h:.1f} uur van {lim['max_hours']} uur; opeenvolgende ketens zonder echte vooruitgang: "
        f"{sup['no_progress']} van maximaal {lim['max_no_progress']}.",
        "VERLOOP TOT NU TOE:\n" + "\n".join(
            _chain_block(e, detailed=e is cur or i >= len(sup["chains"]) - 3) for i, e in enumerate(sup["chains"])),
        f"TIP (laatste commit van de laatste keten): {cur.get('end_commit')}; je worktree staat precies op die tip "
        f"(alleen gecommit werk). Totaal commits sinds de start van de supervisor: {sup['commits_total']}.",
        f"Integratie-droogtest van de tip t.o.v. {sup['root_ref']} (informatief, er wordt niets geïntegreerd): "
        f"fast_forward={integ.get('fast_forward')} conflicten={integ.get('conflicts')} commits_voor={integ.get('commits_ahead')}",
    ]
    if tip_link.get("clean") is False:
        parts.append("LET OP: de laatste schakel liet ongecommitte wijzigingen achter; die gaan NIET mee naar een volgende keten.")
    if fin.get("attention"):
        parts.append("Aandachtspunten uit de laatste schakel: " + "; ".join(_clip(a, 200) for a in fin["attention"][:6]))
    if sup["decisions"]:
        parts.append("EERDERE BESLISSINGEN:\n" + "\n".join(
            f"- na keten {d['n']}: {d['decision']} ({d.get('chain_progress', '?')}): {_clip(d.get('reason'), 250)}"
            for d in sup["decisions"][-4:]))
    if sup.get("status_files"):
        parts.append("Lees ook deze project-/statusbestanden in de repository (pad relatief aan je worktree) en weeg ze mee: "
                     + ", ".join(sup["status_files"]))
    parts.append(quality.judge_block(sup))
    open_items = run_open_actions(sup)
    if open_items:
        parts.append("OPENSTAANDE MENSELIJKE ACTIEPUNTEN (eerder niet-blokkerend achtergelaten; id — omschrijving):\n" + "\n".join(
            f"- {i['id']} — {_clip(i.get('text'), 200)}" + (f" ({_clip(i.get('reason'), 150)})" if i.get("reason") else "")
            for i in open_items[:15]))
    if decided := actions.decisions_block(sup["supervisor_id"], include_open=False):
        parts.append(decided)
    parts.append(
        "TAAK: beoordeel of het PROJECTDOEL bereikt is (niet alleen het doel van de laatste keten) en beslis. Controleer "
        "claims steekproefsgewijs zelf (git log/diff, bestanden, gerichte tests) en roep dan mcp__r2__runner_finish aan met "
        "decision, reason, chain_progress en de velden die bij je beslissing horen. Bij continue_new_chain bepaal je ook het "
        "soort keten (chain_kind: feature, quality of repair) en de controle (verification) volgens het KWALITEITSBELEID. "
        "Staan er OPENSTAANDE MENSELIJKE ACTIEPUNTEN en is een punt inmiddels beslist of vervallen (bijvoorbeeld door een "
        "antwoord van de mens of omdat het werk het afdekt), sluit het dan met resolve_actions; laat een nog openstaand punt "
        "staan en meld nieuwe niet-blokkerende beslispunten in open_actions.")
    text = "\n\n".join(parts)
    return text if len(text) <= JUDGE_PROMPT_CHARS else text[:JUDGE_PROMPT_CHARS - 40] + "\n[…prompt ingekort…]"


def chain_assignment(sup: dict, entry: dict) -> str:
    """Opdracht van de eerste schakel van een vervolgketen: de next_prompt van de judge plus vaste context en regels."""
    prior = "\n".join(f"- keten {e['n']}: {_clip(e['goal'], 200)} → {(e.get('outcome') or {}).get('kind', '?')}, "
                      f"{e.get('commits', '?')} commits" for e in sup["chains"][:-1][-5:])
    decided = actions.decisions_block(sup["supervisor_id"])
    return (f"{entry['prompt']}\n\n--- Context van de supervisor (keten {entry['n']} van maximaal {sup['limits']['max_chains']}) ---\n"
            f"Projectdoel: {sup['goal']}\nEerdere ketens:\n{prior}\n"
            "Deze worktree begint op de laatste commit van de vorige keten; dat werk staat er al in.\n"
            + (decided + "\n" if decided else "")
            + quality.assignment_block(entry) + "\n" + SAFETY_FOOTER)


# --- weergave ---

def phase(sup: dict) -> str:
    if sup["state"] in ENDED:
        return "ended"
    if sup["phase"] == "judge":
        j = sup["judge"] or {}
        try:
            return {"created": "judging", "running": "judging", "completed": "between_chains",
                    "waiting_for_human": "waiting_for_human", "paused": "paused", "failed": "failed",
                    "stopped": "stopped"}.get(store.load(j["task_id"])["status"], "judging")
        except (OSError, ValueError, KeyError):
            return "judging"
    ch = chains.load(sup["chains"][-1]["chain_id"])
    if not ch:
        return "starting_chain"
    if ch["state"] != chains.ACTIVE:
        return "between_chains"
    try:
        tip = store.load(ch["links"][-1]["task_id"])
    except (OSError, ValueError):
        tip = None
    p = chains.phase(ch, tip)
    return {"between_links": "chain_running", "starting": "chain_running", "running": "chain_running"}.get(p, p)


def current_task(sup: dict) -> str | None:
    if sup["phase"] == "judge" and sup.get("judge"):
        return sup["judge"]["task_id"]
    ch = chains.load(sup["chains"][-1]["chain_id"])
    return ch["links"][-1]["task_id"] if ch else sup["chains"][-1].get("task_id")


def describe(sup: dict, task_id: str | None = None, detail: bool = False) -> dict:
    cur = sup["chains"][-1]
    last = next((d for d in reversed(sup["decisions"]) if d.get("decision") != "needs_human"), None) or \
        (sup["decisions"][-1] if sup["decisions"] else None)
    out = {"id": sup["supervisor_id"], "state": sup["state"], "phase": phase(sup), "goal": _clip(sup["goal"], 300),
           "chain": cur["n"], "chains_total": len(sup["chains"]), "max_chains": sup["limits"]["max_chains"],
           "current_chain": cur["chain_id"], "current_task": current_task(sup), "commits_total": sup["commits_total"],
           "no_progress": sup["no_progress"], "max_no_progress_chains": sup["limits"]["max_no_progress"],
           "deadline": sup["deadline"], "max_hours": sup["limits"]["max_hours"], "started": sup["created"],
           "chain_kind": quality.kind_of(cur), "quality": quality.describe_state(sup)}
    if sup["decisions"]:
        d = sup["decisions"][-1]
        out["last_decision"] = {"decision": d["decision"], "chain": d["n"], "reason": _clip(d.get("reason"), 400),
                                "chain_progress": d.get("chain_progress"), "at": d["at"]}
        if d["decision"] == "continue_new_chain" and sup["state"] == ACTIVE:
            out["next_goal"] = _clip(d.get("next_goal"), 400)
            out["next_chain"] = quality.describe_plan({"kind": d.get("chain_kind"), "verification": d.get("verification"),
                                                       "findings": d.get("findings")})
            if out["next_chain"]["kind"] == "quality":
                out["quality_milestone_next"] = True
    if sup["state"] == ACTIVE and out["phase"] == "waiting_for_human" and sup["phase"] == "judge":
        out["waiting_for"] = "runner2_answer op de beoordelingstaak: de supervisor wacht op een mens"
    if sup["stop"]:
        out["stopped_because"] = sup["stop"]
    if sup.get("last_error") and sup["state"] == ACTIVE:
        out["last_error"] = sup["last_error"]
    if detail:
        out["chains"] = [{"n": e["n"], "chain_id": e["chain_id"], "goal": _clip(e["goal"], 300), "commits": e.get("commits"),
                          "outcome": e.get("outcome"), "start_commit": e.get("start_commit"), "end_commit": e.get("end_commit"),
                          "decision": (e.get("decision") or {}).get("decision"),
                          "kind": quality.kind_of(e), "verification_scope": quality.scope_of(e)} for e in sup["chains"]]
        out["decisions"] = [{k: d.get(k) for k in ("n", "decision", "chain_progress", "at", "judge_task", "chain_kind")}
                            | {"reason": _clip(d.get("reason"), 500), "next_goal": _clip(d.get("next_goal"), 300)} for d in sup["decisions"]]
        out["root_commit"] = sup["root_commit"]
        out["tip_commit"] = cur.get("end_commit")
        out["integration"] = ("Er wordt niets gepusht of geïntegreerd. De tip bevat alle ketens lineair; integreren is "
                              "een aparte, bewuste handeling (eerst integration_dry_run van de laatste taak bekijken).")
    return out


# --- de aandrijving ---

def _count_failure(sup: dict, cfg: dict, e) -> None:
    sup["start_failures"] += 1
    sup["next_retry_at"] = time.time() + cfg["chain_start_retry_s"]
    sup["last_error"] = {"code": e.code, "message": e.message[:300], "at": store.now()}
    if sup["start_failures"] >= cfg["chain_max_start_failures"]:
        _end(sup, FAILED, "start_failed", f"starten mislukte {sup['start_failures']}x: {e.message[:200]}")


def _clear_failures(sup: dict) -> None:
    sup["start_failures"] = 0
    sup.pop("last_error", None)


def _commits_total(sup: dict, ch: dict) -> int:
    try:
        st = store.load(ch["links"][-1]["task_id"])
        return gitops.commits_between(st["worktree"], sup["root_commit"])
    except (OSError, ValueError, RuntimeError, KeyError):
        return sup["commits_total"] + sum(l.get("commits") or 0 for l in ch["links"])


def _advance_chain(sup: dict, cfg: dict) -> None:
    entry = sup["chains"][-1]
    ch = chains.load(entry["chain_id"])
    if ch is None:
        _ensure_chain(sup, entry, cfg)
        return
    if ch["state"] == chains.ACTIVE:
        return
    stop = ch["stop"] or {}
    kind = stop.get("kind", "")
    last = ch["links"][-1]
    entry.update(outcome={"state": ch["state"], "kind": kind, "message": str(stop.get("message") or "")[:300],
                          "at": stop.get("at")},
                 links=len(ch["links"]), end_commit=last.get("end_commit"),
                 commits=sum(l.get("commits") or 0 for l in ch["links"]))
    sup["commits_total"] = _commits_total(sup, ch)
    quality.record_chain_end(sup, entry)
    if kind in USER_STOP_KINDS:
        _end(sup, STOPPED, kind, f"keten {entry['n']} is door een mens gestopt")
    elif kind not in JUDGED_KINDS or not entry["end_commit"]:
        _end(sup, FAILED, f"chain_{kind or 'unknown'}", f"keten {entry['n']} eindigde zonder beoordeelbaar resultaat: "
             f"{kind}: {entry['outcome']['message']}")
    elif time.time() > chains._epoch(sup["deadline"]):
        _end(sup, LIMIT, "time_limit", f"totale tijdslimiet van {sup['limits']['max_hours']} uur bereikt na keten {entry['n']}")
    else:
        sup["phase"] = "judge"
        sup["judge"] = {"n": entry["n"], "task_id": _task_id(), "provisioned": False, "spawned_at": 0, "spawn_attempts": 0}
        save(sup)   # intentie en vooraf toegekend judge-id vastleggen vóór het aanmaken
        _ensure_judge(sup, cfg)


def _ensure_judge(sup: dict, cfg: dict) -> None:
    from . import controller, taskproc
    j, entry = sup["judge"], sup["chains"][-1]
    if controller.busy() or time.time() < sup["next_retry_at"]:
        return
    try:
        controller.preflight(cfg)
        controller.provision_judge(sup, j["task_id"], judge_prompt(sup), entry["end_commit"])
    except controller.ControllerError as e:
        _count_failure(sup, cfg, e)
        return
    _clear_failures(sup)
    j.update(provisioned=True, spawned_at=time.time(), spawn_attempts=1)
    save(sup)
    taskproc.spawn(j["task_id"])
    log(f"{sup['supervisor_id']}: judge {j['task_id']} gestart na keten {entry['n']}")


def _retry_judge_spawn(sup: dict) -> None:
    from . import controller, taskproc
    j = sup["judge"]
    if time.time() - j["spawned_at"] < SPAWN_GRACE_S or controller.busy():
        return
    if j["spawn_attempts"] >= MAX_SPAWN_ATTEMPTS:
        _end(sup, FAILED, "start_failed", f"judge {j['task_id']} kwam na {j['spawn_attempts']} pogingen niet op gang")
        return
    j["spawn_attempts"] += 1
    j["spawned_at"] = time.time()
    save(sup)
    taskproc.spawn(j["task_id"])


def _advance_judge(sup: dict, cfg: dict) -> None:
    j = sup["judge"]
    if not j["provisioned"]:
        _ensure_judge(sup, cfg)
        return
    try:
        st = store.load(j["task_id"])
    except (OSError, ValueError):
        _end(sup, FAILED, "task_missing", f"state van beoordelingstaak {j['task_id']} ontbreekt")
        return
    status = st["status"]
    if status == "created":
        _retry_judge_spawn(sup)
    elif status == "stopped":
        _end(sup, STOPPED, "task_stopped", f"beoordelingstaak {j['task_id']} is gestopt")
    elif status == "waiting_for_human":
        q = st.get("question") or {}
        key = f"{j['task_id']}:{st['turn']}"
        if q.get("supervisor_decision") == "needs_human" and not any(d.get("key") == key for d in sup["decisions"]):
            sup["decisions"].append({"n": j["n"], "key": key, "decision": "needs_human", "at": store.now(),
                                     "reason": str(q.get("why_needed") or "")[:REASON_CHARS], "judge_task": j["task_id"],
                                     "question": str(q.get("question") or "")[:REASON_CHARS],
                                     "chain_progress": q.get("chain_progress")})
    elif status == "completed":
        _apply_decision(sup, st)
    # running / paused / failed: de supervisor wacht; herstel, hervatten of stoppen gaat via de gewone taaktools


def _apply_decision(sup: dict, st: dict) -> None:
    j, entry = sup["judge"], sup["chains"][-1]
    fin = st.get("finish") or {}
    err = validate_decision(fin, sup, check_actions=False)   # sluiten is idempotent; een herhaling na herstart mag niet falen
    if err or fin.get("decision") == "needs_human":
        _end(sup, BLOCKED, "judge_invalid", f"beoordeling van keten {entry['n']} bruikbaar? nee: {err or 'needs_human is geen eindbesluit'}")
        return
    d = fin["decision"]
    rec = {"n": entry["n"], "decision": d, "reason": str(fin["reason"]).strip()[:REASON_CHARS],
           "chain_progress": fin["chain_progress"], "at": store.now(), "judge_task": j["task_id"]}
    if fin.get("resolve_actions"):
        sid = sup["supervisor_id"]
        closed = actions.resolve(fin["resolve_actions"], note=rec["reason"], by=f"supervisor {sid}",
                                 candidates=[s for s in store.all_tasks() if s.get("supervisor_id") == sid])
        rec["resolved_actions"] = closed["resolved"]
    plan = None
    if d == "continue_new_chain":
        plan = quality.normalize(fin)
        rec.update(next_goal=str(fin["next_goal"]).strip()[:GOAL_CHARS], next_prompt=str(fin["next_prompt"]).strip()[:PROMPT_CHARS],
                   chain_kind=plan["kind"], verification=plan["verification"], **({"findings": plan["findings"]} if "findings" in plan else {}))
    if d == "project_complete":
        rec["evidence"] = [str(e)[:300] for e in fin["evidence"] if isinstance(e, str)][:10]
    sup["decisions"].append(rec)
    entry["decision"] = {k: rec[k] for k in ("decision", "reason", "chain_progress")}
    substantive = fin["chain_progress"] == "substantive" and (entry.get("commits") or 0) > 0
    sup["no_progress"] = 0 if substantive else sup["no_progress"] + 1
    lim = sup["limits"]
    if d == "project_complete":
        _end(sup, COMPLETE, "project_complete", rec["reason"])
    elif d == "blocked":
        _end(sup, BLOCKED, "blocked", rec["reason"])
    elif sup["no_progress"] >= lim["max_no_progress"]:
        _end(sup, LIMIT, "no_progress", f"{sup['no_progress']} opeenvolgende ketens zonder echte vooruitgang")
    elif len(sup["chains"]) >= lim["max_chains"]:
        _end(sup, LIMIT, "max_chains", f"maximum van {lim['max_chains']} ketens bereikt; projectdoel nog niet bevestigd: {rec['next_goal'][:200]}")
    elif time.time() > chains._epoch(sup["deadline"]):
        _end(sup, LIMIT, "time_limit", f"totale tijdslimiet van {lim['max_hours']} uur bereikt")
    elif _norm(rec["next_goal"]) in sup["goals"][-3:]:
        _end(sup, LIMIT, "stagnation", "het voorgestelde vervolgdoel is gelijk aan dat van een recente keten")
    else:
        sup["chains"].append({"n": entry["n"] + 1, "chain_id": chains.new_id(), "task_id": _task_id(),
                              "goal": rec["next_goal"], "prompt": rec["next_prompt"], "start_commit": entry["end_commit"],
                              "created": False, "kind": plan["kind"], "verification": plan["verification"],
                              **({"findings": plan["findings"]} if "findings" in plan else {})})
        quality.record_planned(sup, plan)
        sup["goals"] = (sup["goals"] + [_norm(rec["next_goal"])])[-10:]
        sup["phase"], sup["judge"] = "chain", None
        _clear_failures(sup)
    save(sup)   # beslissing + nieuwe ketenintentie in één atomaire schrijfactie: een herstart herhaalt dit niet


def _ensure_chain(sup: dict, entry: dict, cfg: dict) -> None:
    from . import controller, taskproc
    if controller.busy() or time.time() < sup["next_retry_at"]:
        return
    remaining_h = (chains._epoch(sup["deadline"]) - time.time()) / 3600
    if remaining_h <= 0:
        _end(sup, LIMIT, "time_limit", f"totale tijdslimiet van {sup['limits']['max_hours']} uur bereikt")
        return
    limits = quality.chain_limits(sup, entry, dict(sup["chain_limits"], max_hours=min(sup["chain_limits"]["max_hours"], remaining_h)))
    try:
        controller.preflight(cfg)
        st = controller.start_supervised_chain(sup, entry, chain_assignment(sup, entry), limits)
    except controller.ControllerError as e:
        _count_failure(sup, cfg, e)
        return
    _clear_failures(sup)
    entry["created"] = True
    save(sup)
    taskproc.spawn(st["task_id"])
    log(f"{sup['supervisor_id']}: keten {entry['n']} ({entry['chain_id']}) gestart op tip {entry['start_commit'][:10]}")


def advance(sid: str, cfg: dict) -> None:
    with _flock(sid):
        sup = load(sid)
        if not sup or sup["state"] != ACTIVE:
            return
        before = json.dumps(sup, sort_keys=True)
        if sup["phase"] == "judge":
            _advance_judge(sup, cfg)
        else:
            _advance_chain(sup, cfg)
        if json.dumps(sup, sort_keys=True) != before:
            save(sup)


def advance_all(cfg: dict) -> None:
    for sup in all_supervisors():
        if sup["state"] == ACTIVE:
            try:
                advance(sup["supervisor_id"], cfg)
            except Exception as e:  # noqa: BLE001 -- één kapotte supervisor mag de watch niet raken
                log(f"{sup['supervisor_id']}: advance-fout: {type(e).__name__}: {e}")
