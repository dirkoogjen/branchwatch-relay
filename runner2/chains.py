"""Autonome ketens: opt-in doorwerken van schakel naar schakel bovenop gewone Runner 2-taken.

Een keten is geen nieuw soort taak. Elke schakel is een normale taak (eigen conversatie, worktree, branch, resultaat);
de keten bewaart alleen doel, limieten en de lijst schakels in ~/.runner2/chains/<id>/chain.json. De watch roept
`advance_all` aan; die start pas een volgende schakel als de laatste schakel `completed` is én de keten alle controles
haalt. Alles wat niet `completed` is (waiting_for_human, paused, failed, stopped) laat de keten wachten of stoppen.
Er wordt nooit gepusht of naar de basisbranch geïntegreerd: de keten levert één lineaire branch op (de tip).
"""
import json
import time
from contextlib import contextmanager
from datetime import datetime

from . import actions, config, gitops, store
from . import gate as gate_module
from .filelock import lock_exclusive
from .log import log

ACTIVE, COMPLETED, LIMIT, STOPPED = "active", "completed", "limit_reached", "stopped"
ENDED = (COMPLETED, LIMIT, STOPPED)
SPAWN_GRACE_S = 30       # zolang mag een net gespawnd taakproces nodig hebben om de machinelock te nemen
MAX_SPAWN_ATTEMPTS = 5
SUMMARY_CHARS = 600
NEXT_STEP_CHARS = 1500
GOAL_CHARS = 2000
REASON_CHARS = 1500
GATE_LOG_KEEP = 30


def _dir(cid: str):
    return config.chains_dir() / cid


def _path(cid: str):
    return _dir(cid) / "chain.json"


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def _epoch(ts: str) -> float:
    return time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z"))


@contextmanager
def _flock(cid: str):
    _dir(cid).mkdir(parents=True, exist_ok=True)
    with open(_dir(cid) / ".lock", "w") as f:
        lock_exclusive(f)
        yield


def load(cid: str) -> dict | None:
    try:
        return json.loads(_path(cid).read_text())
    except (OSError, ValueError):
        return None


def save(ch: dict) -> None:
    ch["updated"] = store.now()
    store.atomic_write(_path(ch["chain_id"]), json.dumps(ch, indent=1))


def all_chains() -> list:
    d = config.chains_dir()
    out = [c for p in sorted(d.glob("C-*/chain.json")) if (c := load(p.parent.name))] if d.exists() else []
    return sorted(out, key=lambda c: c["created"])


def for_task(state: dict) -> dict | None:
    cid = state.get("chain_id")
    return load(cid) if cid else None


def new_id() -> str:
    import uuid
    return "C-" + uuid.uuid4().hex[:12]


def create(repo: str, goal: str, first_task: str, base: dict, cfg_role: str, model: str | None, title: str,
           limits: dict, chain_id: str | None = None, supervisor: dict | None = None) -> dict:
    """`supervisor` ({"id", "n"}) markeert een keten die onder een supervisor draait; `chain_id` is dan vooraf toegekend."""
    now = time.time()
    ch = {"chain_id": chain_id or new_id(), "repo": repo, "goal": goal[:GOAL_CHARS], "role": cfg_role,
          "model": model, "title": title, "limits": limits, "state": ACTIVE, "stop": None, "created": store.now(),
          "updated": store.now(), "deadline": _iso(now + limits["max_hours"] * 3600),
          "root_ref": base["ref"], "root_commit": base["commit"], "no_progress": 0, "steps": [],
          "start_failures": 0, "next_retry_at": 0, "supervisor": supervisor, "gate_log": [],
          "links": [{"link": 1, "task_id": first_task, "base_commit": base["commit"], "spawned_at": now,
                     "spawn_attempts": 1, "evaluated": False}]}
    with _flock(ch["chain_id"]):
        save(ch)
    return ch


def _end(ch: dict, state: str, kind: str, message: str) -> None:
    ch["state"], ch["stop"] = state, {"kind": kind, "message": message, "at": store.now()}
    log(f"{ch['chain_id']}: keten {state} ({kind}): {message}")


def stop_chain(cid: str, kind: str, message: str) -> bool:
    """Beëindigt een actieve keten (idempotent). Geeft True als er iets veranderde."""
    with _flock(cid):
        ch = load(cid)
        if not ch or ch["state"] != ACTIVE:
            return False
        _end(ch, STOPPED, kind, message)
        save(ch)
        return True


# --- weergave (status/result) ---

def phase(ch: dict, tip: dict | None) -> str:
    if ch["state"] in ENDED:
        return "ended"
    if not tip:
        return "unknown"
    g = (ch["links"][-1].get("gate") or {})
    if tip["status"] == "completed" and g.get("state") == "pending":
        gate_task = _gate_task(g)
        if not gate_task or gate_task["status"] in ("created", "running", "completed"):
            return "completion_check"
        return {"waiting_for_human": "waiting_for_human", "paused": "paused", "failed": "failed",
                "stopped": "stopped"}.get(gate_task["status"], "completion_check")
    return {"created": "starting", "running": "running", "waiting_for_human": "waiting_for_human", "paused": "paused",
            "failed": "failed", "stopped": "stopped", "completed": "between_links"}.get(tip["status"], tip["status"])


def _clip(text, n) -> str:
    text = " ".join(str(text or "").split())
    return text if len(text) <= n else text[: n - 1] + "…"


def describe(ch: dict, task_id: str | None = None, detail: bool = False) -> dict:
    """Compact ketenblok voor status/result. `task_id` markeert welke schakel de aangevraagde taak is."""
    links = ch["links"]
    tip = links[-1]
    try:
        tip_state = store.load(tip["task_id"])
    except (OSError, ValueError):
        tip_state = None
    out = {"id": ch["chain_id"], "state": ch["state"], "phase": phase(ch, tip_state), "goal": _clip(ch["goal"], 300),
           "link": next((l["link"] for l in links if l["task_id"] == task_id), tip["link"]),
           "links_total": len(links), "max_tasks": ch["limits"]["max_tasks"], "current_task": tip["task_id"],
           "no_progress": ch["no_progress"], "max_no_progress": ch["limits"]["max_no_progress"],
           "deadline": ch["deadline"], "started": ch["created"]}
    if ch.get("supervisor"):
        out["supervisor_id"] = ch["supervisor"]["id"]
    if ch["state"] == ACTIVE and tip.get("next_step"):
        out["next_step"] = _clip(tip["next_step"], 300)
    if gate := describe_gate(ch, detail):
        out["completion_gate"] = gate
    if ch["stop"]:
        out["stopped_because"] = ch["stop"]
    if ch["state"] == ACTIVE and tip_state and tip_state["status"] == "failed":
        out["waiting_for"] = "runner2_resume of runner2_stop: de laatste schakel is mislukt; de keten wacht"
    if ch["state"] == ACTIVE and out["phase"] in ("failed", "paused", "waiting_for_human") and tip.get("gate", {}).get("state") == "pending":
        out["waiting_for"] = f"de voltooiingscontrole (taak {tip['gate']['gate_id']}) wacht: {out['phase']}"
    if detail:
        out["tasks"] = [{k: l.get(k) for k in ("link", "task_id", "commits", "chain_status", "tests_reported")
                         if l.get(k) is not None} | {"summary": _clip(l.get("summary"), 200)} for l in links]
        out["integration"] = ("Er wordt niets gepusht of geïntegreerd. De tip-branch bevat alle schakels lineair; "
                              "integreren is een aparte, bewuste handeling (eerst integration_dry_run bekijken).")
        out["branch"] = tip_state["branch"] if tip_state else None
    return out


def _gate_task(g: dict) -> dict | None:
    try:
        return store.load(g["gate_id"])
    except (OSError, ValueError, KeyError):
        return None


def describe_gate(ch: dict, detail: bool = False) -> dict | None:
    """Begrijpelijk blok over de voltooiingscontrole: wat nu loopt en waarom een 'klaar' is geaccepteerd of geweigerd."""
    log = ch.get("gate_log") or []
    tip = ch["links"][-1]
    g = tip.get("gate") or {}
    if not log and not g:
        return None
    out = {"checks": len(log), "refused": sum(1 for e in log if e["decision"] == "refused"),
           "accepted": sum(1 for e in log if e["decision"] == "accepted")}
    if g.get("state") == "pending":
        gt = _gate_task(g)
        out["now"] = {"gate_task": g["gate_id"], "after_link": tip["link"], "status": gt["status"] if gt else "unknown",
                      "note": "Schakel " + str(tip["link"]) + " meldde 'doel bereikt'; Runner 2 laat dat eerst onafhankelijk controleren."}
        if gt and gt["status"] == "waiting_for_human":
            out["now"]["blocked_on"] = _clip((gt.get("question") or {}).get("question"), 300)
    if log:
        last = log[-1]
        out["last"] = {k: last.get(k) for k in ("link", "decision", "verdict", "reason", "at", "gate_task", "remaining", "next_step", "limit")
                       if last.get(k) not in (None, [])}
        out["last"]["summary"] = last["summary"]
    if detail:
        out["history"] = [{k: e.get(k) for k in ("link", "decision", "verdict", "at", "gate_task", "reason", "summary", "remaining",
                                                  "evidence", "human_blockers", "backlog_open", "limit") if e.get(k) not in (None, [])}
                          for e in log]
    return out


def _gate_summary(entry: dict) -> str:
    link, d = entry["link"], entry["decision"]
    if d == "accepted":
        return f"Na schakel {link}: 'doel bereikt' bevestigd door de onafhankelijke controle. {_clip(entry.get('reason'), 300)}"
    if d == "refused":
        n = len(entry.get("remaining") or [])
        tail = f" Keten stopt toch door limiet ({entry['limit']})." if entry.get("limit") else " De keten gaat door met de volgende schakel."
        return (f"Na schakel {link}: 'doel bereikt' geweigerd; er resteert uitvoerbaar werk ({n} punt(en)): "
                f"{_clip('; '.join(entry.get('remaining') or []), 300)}.{tail}")
    if d == "blocked":
        return f"Na schakel {link}: geen uitvoerbaar werk meer door een menselijke blokkade; de keten wacht op een antwoord. {_clip(entry.get('reason'), 300)}"
    return f"Na schakel {link}: voltooiingscontrole {d}. {_clip(entry.get('reason'), 300)}"


def commits_total(ch: dict) -> int | None:
    try:
        st = store.load(ch["links"][-1]["task_id"])
        return gitops.commits_between(st["worktree"], ch["root_commit"])
    except (OSError, ValueError, RuntimeError, KeyError):
        return None


# --- prompts ---

def link_prompt(ch: dict, link_no: int, assignment: str, previous: list) -> str:
    head = (f"[Autonome keten {ch['chain_id']} — schakel {link_no} van maximaal {ch['limits']['max_tasks']}]\n"
            f"Overkoepelend doel: {ch['goal']}\n")
    if not previous:
        return f"{head}\nOpdracht van deze schakel:\n{assignment}"
    lines = []
    for l in previous[-5:]:
        lines.append(f"- schakel {l['link']} ({l['task_id']}, {l.get('commits', '?')} commits): {_clip(l.get('summary'), 400)}")
    older = len(previous) - 5
    history = (f"({older} eerdere schakels weggelaten)\n" if older > 0 else "") + "\n".join(lines)
    decided = actions.decisions_block((ch.get("supervisor") or {}).get("id"), ch.get("chain_id"))
    return (f"{head}\nEerder in deze keten:\n{history}\n\n"
            + (decided + "\n\n" if decided else "") +
            "Deze worktree begint op de laatste commit van de vorige schakel; dat werk staat er al in. "
            "Controleer kort de actuele toestand.\n\n"
            f"Voorgestelde opdracht voor deze schakel (van de vorige schakel):\n{assignment}\n\n"
            "Bepaal zelf wat binnen het overkoepelende doel nu de meest logische stap is en volg het voorstel "
            "tenzij de toestand iets anders eist.")


# --- de aandrijving ---

def _evaluate(ch: dict, link: dict, st: dict, cfg: dict) -> tuple | None:
    """Verwerkt het resultaat van de voltooide laatste schakel. Geeft (state, kind, message) als de keten eindigt.

    `goal_reached` is een bewering van Claude: bij een keten zonder supervisor opent dat een voltooiingscontrole (de gate)
    in plaats van de keten te beëindigen."""
    fin = st.get("finish") or {}
    link.update(summary=_clip(fin.get("summary"), SUMMARY_CHARS), tests_reported=bool(fin.get("tests_run")),
                chain_status=fin.get("chain_status"), next_step=str(fin.get("next_step") or "")[:NEXT_STEP_CHARS],
                evaluated=True, completed=st["updated"])
    try:
        link.update(end_commit=gitops.head_commit(st["worktree"]), clean=gitops.is_clean(st["worktree"]),
                    commits=gitops.commits_between(st["worktree"], link["base_commit"]))
    except RuntimeError as e:
        return STOPPED, "git_error", f"worktree van schakel {link['link']} niet leesbaar: {str(e)[:200]}"
    if link["chain_status"] == "goal_reached":
        if gate_enabled(ch, cfg):
            _open_gate(ch, link, st)
            return None
        return COMPLETED, "goal_reached", "Claude meldt dat het overkoepelende doel bereikt is"
    if link["chain_status"] != "continue":
        return STOPPED, "missing_verdict", "runner_finish gaf geen geldige chain_status (continue of goal_reached)"
    if not link["next_step"].strip():
        return STOPPED, "missing_verdict", "chain_status continue zonder next_step"
    return _continue_checks(ch, link)


def _continue_checks(ch: dict, link: dict) -> tuple | None:
    """Harde grenzen en stagnatie vóór een volgende schakel: gelden voor 'continue' én voor een geweigerde 'goal_reached'."""
    if not link["clean"]:
        return STOPPED, "dirty_worktree", (f"schakel {link['link']} liet ongecommitte wijzigingen achter; alleen commits "
                                            "worden overgenomen, dus de keten stopt in plaats van werk stil te verliezen")
    ch["no_progress"] = 0 if link["commits"] > 0 else ch["no_progress"] + 1
    if ch["no_progress"] >= ch["limits"]["max_no_progress"]:
        return LIMIT, "no_progress", f"{ch['no_progress']} opeenvolgende schakels zonder nieuwe commits"
    norm = " ".join(link["next_step"].lower().split())
    if norm in ch["steps"][-3:]:
        return STOPPED, "stagnation", "de voorgestelde volgende stap is gelijk aan een eerdere stap"
    ch["steps"] = (ch["steps"] + [norm])[-10:]
    if len(ch["links"]) >= ch["limits"]["max_tasks"]:
        return LIMIT, "max_tasks", f"maximum van {ch['limits']['max_tasks']} schakels bereikt"
    if time.time() > _epoch(ch["deadline"]):
        return LIMIT, "time_limit", f"tijdslimiet van {ch['limits']['max_hours']} uur bereikt"
    return None


# --- voltooiingscontrole (completion gate) ---

def gate_enabled(ch: dict, cfg: dict) -> bool:
    """Alleen ketens zonder supervisor (die heeft zijn eigen onafhankelijke judge) en met de instelling aan."""
    return bool(cfg.get("chain_completion_gate", True)) and not ch.get("supervisor")


def _gate_task_id() -> str:
    import uuid
    return "R2-" + uuid.uuid4().hex[:12]


def _open_gate(ch: dict, link: dict, st: dict) -> None:
    """Legt de controle vast (id vooraf toegekend, scan op exact de eindcommit): een herstart gebruikt dezelfde id's."""
    scan = gate_module.scan_backlog(st["worktree"], link["end_commit"])
    link["gate"] = {"state": "pending", "gate_id": _gate_task_id(), "round": len(ch.get("gate_log") or []) + 1,
                    "opened": store.now(), "provisioned": False, "spawn_attempts": 0, "spawned_at": 0,
                    "scan": {"files": scan["files"], "items": scan["items"], "total_open": scan["total_open"],
                             "truncated": scan["truncated"], **({"error": scan["error"]} if scan.get("error") else {})}}
    log(f"{ch['chain_id']}: schakel {link['link']} meldt goal_reached; voltooiingscontrole {link['gate']['gate_id']} wordt gestart")


def _links_text(ch: dict) -> str:
    lines = [f"- schakel {l['link']} ({l.get('commits', '?')} commits, {l.get('chain_status')}): {_clip(l.get('summary'), 300)}"
             for l in ch["links"][-8:]]
    return (f"({len(ch['links']) - 8} eerdere schakels weggelaten)\n" if len(ch["links"]) > 8 else "") + "\n".join(lines)


def _prior_gates_text(ch: dict) -> str:
    log = [e for e in ch.get("gate_log") or [] if e["decision"] == "refused"]
    if not log:
        return ""
    return ("EERDERE CONTROLES IN DEZE KETEN (daarna is verder gewerkt):\n"
            + "\n".join(f"- na schakel {e['link']}: geweigerd, resterend: {_clip('; '.join(e.get('remaining') or []), 300)}" for e in log[-4:]))


def _ensure_gate(ch: dict, link: dict, cfg: dict) -> None:
    from . import controller, taskproc
    g = link["gate"]
    if controller.busy() or time.time() < ch["next_retry_at"]:
        return
    open_human = actions.open_items([s for s in store.all_tasks() if s.get("chain_id") == ch["chain_id"]])
    prompt = gate_module.gate_prompt(ch, link, g["scan"], _links_text(ch), open_human)
    if prior := _prior_gates_text(ch):
        prompt += "\n\n" + prior
    try:
        controller.preflight(cfg)
        controller.provision_gate(ch, link, g["gate_id"], prompt)
    except controller.ControllerError as e:
        ch["start_failures"] += 1
        ch["next_retry_at"] = time.time() + cfg["chain_start_retry_s"]
        ch["last_error"] = {"code": e.code, "message": e.message[:300], "at": store.now()}
        if ch["start_failures"] >= cfg["chain_max_start_failures"]:
            _end(ch, STOPPED, "start_failed", f"voltooiingscontrole starten mislukte {ch['start_failures']}x: {e.message[:200]}")
        return
    ch["start_failures"] = 0
    ch.pop("last_error", None)
    g.update(provisioned=True, spawned_at=time.time(), spawn_attempts=1)
    save(ch)   # eerst vastleggen, dan spawnen: een crash daartussen wordt door de respawn-route opgevangen
    taskproc.spawn(g["gate_id"])
    log(f"{ch['chain_id']}: voltooiingscontrole {g['gate_id']} gestart na schakel {link['link']}")


def _retry_gate_spawn(ch: dict, g: dict) -> None:
    from . import controller, taskproc
    if time.time() - g["spawned_at"] < SPAWN_GRACE_S or controller.busy():
        return
    if g["spawn_attempts"] >= MAX_SPAWN_ATTEMPTS:
        _end(ch, STOPPED, "start_failed", f"voltooiingscontrole {g['gate_id']} kwam na {g['spawn_attempts']} pogingen niet op gang")
        return
    g["spawn_attempts"] += 1
    g["spawned_at"] = time.time()
    save(ch)
    taskproc.spawn(g["gate_id"])


def _log_gate(ch: dict, entry: dict) -> None:
    entry["summary"] = _gate_summary(entry)
    ch["gate_log"] = ((ch.get("gate_log") or []) + [entry])[-GATE_LOG_KEEP:]
    log(f"{ch['chain_id']}: {entry['summary']}")


def _advance_gate(ch: dict, link: dict, cfg: dict) -> None:
    g = link["gate"]
    if not g["provisioned"]:
        _ensure_gate(ch, link, cfg)
        return
    gt = _gate_task(g)
    if gt is None:
        _end(ch, STOPPED, "task_missing", f"state van voltooiingscontrole {g['gate_id']} ontbreekt")
        return
    status = gt["status"]
    if status == "created":
        _retry_gate_spawn(ch, g)
    elif status == "stopped":
        _end(ch, STOPPED, "task_stopped", f"voltooiingscontrole {g['gate_id']} is gestopt")
    elif status == "waiting_for_human":
        q = gt.get("question") or {}
        key = f"{g['gate_id']}:{gt['turn']}"
        if q.get("gate_decision") == "human_blocked" and g.get("blocked_key") != key:
            g["blocked_key"] = key
            _log_gate(ch, {"link": link["link"], "round": g["round"], "at": store.now(), "gate_task": g["gate_id"], "decision": "blocked",
                           "verdict": "human_blocked", "reason": _clip(q.get("why_needed"), REASON_CHARS),
                           "human_blockers": q.get("human_blockers") or [], "question": _clip(q.get("question"), 500),
                           "backlog_open": g["scan"]["total_open"]})
    elif status == "completed":
        _apply_gate(ch, link, gt)
    # running / paused / failed: de keten wacht; herstel, hervatten of stoppen gaat via de gewone taaktools


def _apply_gate(ch: dict, link: dict, gt: dict) -> None:
    """Past het oordeel toe in één atomaire schrijfactie (zie advance): een herstart herhaalt dit niet."""
    g = link["gate"]
    fin = gt.get("finish") or {}
    scan_ids = [i["id"] for i in g["scan"]["items"]]
    err = gate_module.validate_decision(fin, scan_ids)
    if err or fin.get("verdict") == "human_blocked":
        _log_gate(ch, {"link": link["link"], "round": g["round"], "at": store.now(), "gate_task": g["gate_id"], "decision": "invalid",
                       "reason": _clip(err or "human_blocked is geen eindbesluit", 300), "backlog_open": g["scan"]["total_open"]})
        g["state"] = "decided"
        _end(ch, STOPPED, "gate_invalid", f"oordeel van de voltooiingscontrole bruikbaar? nee: {err or 'human_blocked is geen eindbesluit'}")
        return
    verdict = fin["verdict"]
    entry = {"link": link["link"], "round": g["round"], "at": store.now(), "gate_task": g["gate_id"], "verdict": verdict,
             "reason": _clip(fin["reason"], REASON_CHARS), "backlog_open": g["scan"]["total_open"]}
    g["state"] = "decided"
    if verdict == "goal_complete":
        entry.update(decision="accepted", evidence=[_clip(e, 300) for e in fin["evidence"] if isinstance(e, str)][:10])
        g["decision"] = "accepted"
        _log_gate(ch, entry)
        _end(ch, COMPLETED, "goal_reached", "Doel bevestigd door onafhankelijke voltooiingscontrole: " + entry["reason"][:300])
        return
    remaining = [_clip(t["title"], 200) for t in fin["remaining_tasks"]][:10]
    fp = gate_module.fingerprint(fin)
    entry.update(decision="refused", remaining=remaining, next_step=_clip(fin["next_step"], 300), fingerprint=fp)
    g["decision"] = "refused"
    link["next_step"] = str(fin["next_step"]).strip()[:NEXT_STEP_CHARS]
    prev = [e for e in ch.get("gate_log") or [] if e["decision"] == "refused"]
    verdict_end = None
    if prev and prev[-1].get("fingerprint") == fp:
        verdict_end = STOPPED, "gate_stagnation", ("de voltooiingscontrole weigert 'doel bereikt' voor hetzelfde resterende werk als "
                                                   "bij de vorige controle: de keten komt niet vooruit")
    else:
        verdict_end = _continue_checks(ch, link)
    if verdict_end:
        entry["limit"] = verdict_end[1]
    _log_gate(ch, entry)
    if verdict_end:
        _end(ch, *verdict_end)


def _start_next(ch: dict, cfg: dict) -> None:
    """Maakt en start de volgende schakel op de tip van de vorige. Onder de ketenlock; herstelbaar bij een crash."""
    from . import controller, taskproc
    prev = ch["links"][-1]
    if controller.busy():
        return  # één actieve taak per node: wachten, niets tellen
    if time.time() < ch["next_retry_at"]:
        return
    try:
        controller.preflight(cfg)
        link_no = prev["link"] + 1
        prompt = link_prompt(ch, link_no, prev["next_step"], ch["links"])
        st = controller.provision_link(ch, link_no, prompt, prev["next_step"], prev["end_commit"])
    except controller.ControllerError as e:
        ch["start_failures"] += 1
        ch["next_retry_at"] = time.time() + cfg["chain_start_retry_s"]
        ch["last_error"] = {"code": e.code, "message": e.message[:300], "at": store.now()}
        if ch["start_failures"] >= cfg["chain_max_start_failures"]:
            _end(ch, STOPPED, "start_failed", f"volgende schakel starten mislukte {ch['start_failures']}x: {e.message[:200]}")
        return
    ch["start_failures"] = 0
    ch.pop("last_error", None)
    ch["links"].append({"link": link_no, "task_id": st["task_id"], "base_commit": prev["end_commit"],
                        "spawned_at": time.time(), "spawn_attempts": 1, "evaluated": False})
    save(ch)   # link vastleggen vóór de spawn: een crash daarna is herstelbaar
    taskproc.spawn(st["task_id"])
    log(f"{ch['chain_id']}: schakel {link_no} gestart als {st['task_id']}")


def _retry_spawn(ch: dict, link: dict, cfg: dict) -> None:
    """Laatste schakel bleef `created` (spawn verloren of node bezet): opnieuw spawnen, begrensd."""
    from . import controller, taskproc
    if time.time() - link["spawned_at"] < SPAWN_GRACE_S or controller.busy():
        return
    if link["spawn_attempts"] >= MAX_SPAWN_ATTEMPTS:
        _end(ch, STOPPED, "start_failed", f"taak {link['task_id']} kwam na {link['spawn_attempts']} pogingen niet op gang")
        return
    link["spawn_attempts"] += 1
    link["spawned_at"] = time.time()
    save(ch)
    taskproc.spawn(link["task_id"])


def advance(cid: str, cfg: dict) -> None:
    with _flock(cid):
        ch = load(cid)
        if not ch or ch["state"] != ACTIVE:
            return
        link = ch["links"][-1]
        try:
            st = store.load(link["task_id"])
        except (OSError, ValueError):
            _end(ch, STOPPED, "task_missing", f"state van taak {link['task_id']} ontbreekt")
            save(ch)
            return
        status = st["status"]
        if status == "created":
            if st.get("worktree"):
                _retry_spawn(ch, link, cfg)
                save(ch)
            elif time.time() - link["spawned_at"] > SPAWN_GRACE_S:
                _end(ch, STOPPED, "start_failed", f"taak {link['task_id']} kreeg nooit een worktree")
                save(ch)
            return
        if status == "stopped":
            _end(ch, STOPPED, "task_stopped", f"taak {link['task_id']} is gestopt")
            save(ch)
            return
        if status != "completed":
            return   # running / waiting_for_human / paused / failed: de keten wacht, er wordt niets omzeild
        if not link["evaluated"]:
            verdict = _evaluate(ch, link, st, cfg)
            if verdict:
                _end(ch, *verdict)
            save(ch)
            if verdict:
                return
        if (link.get("gate") or {}).get("state") == "pending":
            _advance_gate(ch, link, cfg)
            save(ch)   # beslissing + logregel + nieuwe stand in één atomaire schrijfactie
            if ch["state"] != ACTIVE or link["gate"]["state"] != "decided":
                return
        _start_next(ch, cfg)
        save(ch)


def advance_all(cfg: dict) -> None:
    for ch in all_chains():
        if ch["state"] == ACTIVE:
            try:
                advance(ch["chain_id"], cfg)
            except Exception as e:  # noqa: BLE001 -- één kapotte keten mag de watch niet raken
                log(f"{ch['chain_id']}: advance-fout: {type(e).__name__}: {e}")
