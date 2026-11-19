"""Controller-API van Runner 2: de enige plek met start/antwoord/pauze/hervat/stop/status/resultaat.

CLI (`r2`) en de externe controller-MCP (`controller_mcp`) roepen deze functies aan; er is geen eigen state.
"""
import json
import os
import re
import shutil
import sys
import subprocess
import time
from pathlib import Path
from contextlib import contextmanager

from . import actions, chains, config, gitops, osadapter, session, store, supervision, taskproc, watch
from . import quality as quality_module
from . import supervisor as supervisor_module
from .filelock import lock_exclusive
from .procs import alive, descendants, force_kill, protected_pids, snapshot, stop_group, terminate

ROLES = ("implement", "audit", "review")
START_WAIT_S = 6.0


class ControllerError(Exception):
    def __init__(self, code: str, message: str, data: dict | None = None):
        super().__init__(message)
        self.code, self.message, self.data = code, message, data


def _epoch(ts: str) -> float:
    return time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z"))


def busy() -> dict | None:
    return store.MachineLock("-")._holder()


def _load(tid) -> dict:
    if not isinstance(tid, str) or not tid.startswith("R2-"):
        raise ControllerError("invalid_input", "task_id ontbreekt of is ongeldig")
    try:
        return store.load(tid)
    except (OSError, ValueError):
        raise ControllerError("not_found", f"taak {tid} bestaat niet") from None


def _require_free() -> None:
    from .discussion_dispatch import active_local
    if active_local():
        raise ControllerError('busy', 'Er loopt een overleg; één modeluitvoering per machine.')
    if h := busy():
        raise ControllerError("busy", f"taak {h['task_id']} draait al (pid {h['pid']}); één actieve taak per machine")


def _spawn_and_wait(tid: str, turn_before: int) -> dict:
    """Start een taakproces en wacht kort tot de beurt echt loopt, zodat het antwoord de werkelijke status toont."""
    taskproc.spawn(tid)
    end = time.time() + START_WAIT_S
    while time.time() < end:
        s = store.load(tid)
        if s["turn"] > turn_before or s["status"] in ("failed", "stopped"):
            return s
        time.sleep(0.1)
    return store.load(tid)


def model_for(cfg: dict, role: str, override: str | None = None) -> str:
    return override or cfg[f"model_{role}"]


def preflight(cfg: dict) -> str:
    """Bestaat de gepinde Claude Code en klopt de versie? Anders is Runner 2 niet beschikbaar."""
    if cfg.get("allow_unattended_execution") is not True:
        raise ControllerError("runner2_unavailable", "Unattended execution has not been enabled.")
    try:
        env = session.build_env(cfg, {"task_id": "-"}, 0)
        found = session.executable_version(session.claude_binary(cfg, env), env)
    except (OSError, subprocess.SubprocessError) as e:
        raise ControllerError("runner2_unavailable", f"Claude Code niet uitvoerbaar: {e}") from None
    if not cfg["expected_claude_version"] or found != cfg["expected_claude_version"]:
        raise ControllerError("runner2_unavailable",
                              f"Claude Code versie {found!r}, verwacht {cfg['expected_claude_version']!r}")
    return found


def _int_limit(name: str, value, default: int, cap: int) -> int:
    v = default if value is None else value
    if isinstance(v, bool) or not isinstance(v, int) or not 1 <= v <= cap:
        raise ControllerError("invalid_input", f"{name} moet een geheel getal van 1 t/m {cap} zijn")
    return v


def chain_limits(cfg: dict, max_tasks=None, max_hours=None, max_no_progress=None) -> dict:
    hours = cfg["chain_default_max_hours"] if max_hours is None else max_hours
    if isinstance(hours, bool) or not isinstance(hours, (int, float)) or not 0 < hours <= cfg["chain_cap_max_hours"]:
        raise ControllerError("invalid_input", f"max_hours moet groter dan 0 en hoogstens {cfg['chain_cap_max_hours']} zijn")
    return {"max_tasks": _int_limit("max_tasks", max_tasks, cfg["chain_default_max_tasks"], cfg["chain_cap_max_tasks"]),
            "max_hours": hours,
            "max_no_progress": _int_limit("max_no_progress", max_no_progress, cfg["chain_default_max_no_progress"],
                                          cfg["chain_cap_max_tasks"])}


def supervisor_limits(cfg: dict, max_chains=None, max_hours=None, max_no_progress=None) -> dict:
    hours = cfg["supervisor_default_max_hours"] if max_hours is None else max_hours
    cap = cfg["supervisor_cap_max_hours"]
    if isinstance(hours, bool) or not isinstance(hours, (int, float)) or not 0 < hours <= cap:
        raise ControllerError("invalid_input", f"supervisor_max_hours moet groter dan 0 en hoogstens {cap} zijn")
    return {"max_chains": _int_limit("max_chains", max_chains, cfg["supervisor_default_max_chains"], cfg["supervisor_cap_max_chains"]),
            "max_hours": hours,
            "max_no_progress": _int_limit("supervisor_max_no_progress", max_no_progress,
                                          cfg["supervisor_default_max_no_progress"], cfg["supervisor_cap_max_no_progress"])}


def quality_limits(cfg: dict, max_quality_chains=None, full_verification=None) -> dict:
    cap = cfg["quality_cap_max_chains"]
    n = cfg["quality_default_max_chains"] if max_quality_chains is None else max_quality_chains
    if isinstance(n, bool) or not isinstance(n, int) or not 0 <= n <= cap:
        raise ControllerError("invalid_input", f"max_quality_chains moet een geheel getal van 0 t/m {cap} zijn (0 = geen aparte reviewketens)")
    return quality_module.limits_from_config(cfg, n, bool(full_verification))


def _provision(repo: str, prompt: str, cfg: dict, model: str, role: str, title: str | None, start: str | None = None,
               ref: str | None = None, tid: str | None = None, extra: dict | None = None) -> dict:
    """Taakstate + worktree (op HEAD, of op `start`). Ruimt bij een gitfout alles op en gooit repo_invalid.

    Met een vooraf toegekende `tid` is dit idempotent: een eerder volledig aangemaakte taak (worktree + `extra`
    zijn dan in één schrijfactie vastgelegd) wordt teruggegeven; een half aangemaakte wordt opgeruimd en opnieuw gemaakt."""
    if tid:
        try:
            old = store.load(tid)
        except (OSError, ValueError):
            old = None
        if old and old.get("worktree"):
            return old
        if old:
            gitops.discard_worktree(repo, config.worktrees_dir() / tid, f"r2/{tid}")
    s = store.new_task(repo, prompt, "", "", {}, model, tid=tid)
    tid, wt = s["task_id"], config.worktrees_dir() / s["task_id"]
    try:
        base = gitops.create_worktree(repo, wt, f"r2/{tid}", start=start, ref=ref)
    except RuntimeError as e:
        shutil.rmtree(store.task_dir(tid), ignore_errors=True)
        raise ControllerError("repo_invalid", str(e)) from None
    warn = ["hoofdcheckout heeft oncommitted wijzigingen; die zijn niet meegenomen"] if base["main_checkout_dirty"] and not start else []
    return store.update(tid, worktree=str(wt), branch=f"r2/{tid}", base=base, warnings=warn, title=title or "", role=role,
                        **(extra or {}))


def provision_link(ch: dict, link_no: int, prompt: str, assignment: str, start_commit: str) -> dict:
    """Maakt een volgende ketenschakel aan (nog niet gestart) op de commit waarmee de vorige eindigde."""
    cfg = config.load_config()
    title = f"{ch['title']} · schakel {link_no}" if ch["title"] else f"schakel {link_no}"
    s = _provision(ch["repo"], assignment, cfg, model_for(cfg, ch["role"], ch["model"]), ch["role"], title,
                   start=start_commit, ref=ch["root_ref"])
    sup = ch.get("supervisor")
    return store.update(s["task_id"], chain_id=ch["chain_id"], chain_link=link_no, inbox=[prompt],
                        **({"supervisor_id": sup["id"], "supervisor_chain_n": sup["n"]} if sup else {}))


def provision_judge(sup: dict, tid: str, prompt: str, tip_commit: str) -> dict:
    """Beoordelingstaak van de supervisor: gewone review-taak op de tip van de laatste keten (idempotent op `tid`)."""
    cfg = config.load_config()
    n = sup["chains"][-1]["n"]
    title = f"{sup['title']} · beoordeling na keten {n}" if sup["title"] else f"beoordeling na keten {n}"
    return _provision(sup["repo"], prompt, cfg, model_for(cfg, "review"), "review", title, start=tip_commit,
                      ref=sup["root_ref"], tid=tid, extra={"supervisor_id": sup["supervisor_id"], "supervisor_role": "judge",
                                                         "supervisor_chain_n": n})


def provision_gate(ch: dict, link: dict, tid: str, prompt: str) -> dict:
    """Voltooiingscontrole van een keten: gewone review-taak op de eindcommit van de claimende schakel (idempotent op `tid`).

    Hoort bij de keten (chain_id, zodat menselijke actiepunten en stop meelopen) maar is zelf geen schakel."""
    cfg = config.load_config()
    title = f"{ch['title']} · voltooiingscontrole na schakel {link['link']}" if ch["title"] else f"voltooiingscontrole na schakel {link['link']}"
    return _provision(ch["repo"], prompt, cfg, model_for(cfg, "review"), "review", title, start=link["end_commit"],
                      ref=ch["root_ref"], tid=tid, extra={"chain_id": ch["chain_id"], "gate_role": "gate", "gate_link": link["link"],
                                                         "gate_scan_ids": [i["id"] for i in link["gate"]["scan"]["items"]]})


def start_supervised_chain(sup: dict, entry: dict, assignment: str, limits: dict) -> dict:
    """Vervolgketen van een supervisor: eerste taak op de tip van de vorige keten. Idempotent op de vooraf toegekende id's."""
    cfg = config.load_config()
    title = f"{sup['title']} · keten {entry['n']}" if sup["title"] else f"keten {entry['n']}"
    s = _provision(sup["repo"], assignment, cfg, model_for(cfg, sup["role"], sup["model"]), sup["role"], title,
                   start=entry["start_commit"], ref=sup["root_ref"], tid=entry["task_id"],
                   extra={"supervisor_id": sup["supervisor_id"], "supervisor_chain_n": entry["n"]})
    if not chains.load(entry["chain_id"]):
        head = {"chain_id": entry["chain_id"], "goal": entry["goal"], "limits": limits}
        store.update(s["task_id"], chain_id=entry["chain_id"], chain_link=1,
                     inbox=[chains.link_prompt(head, 1, assignment, [])])
        chains.create(sup["repo"], entry["goal"], s["task_id"], s["base"], sup["role"], sup["model"], title, limits,
                      chain_id=entry["chain_id"], supervisor={"id": sup["supervisor_id"], "n": entry["n"]})
    return s


def start(repo: str, prompt: str, node: str | None = None, title: str | None = None, role: str = "implement",
          model: str | None = None, continuation: bool = False, goal: str | None = None, max_tasks=None,
          max_hours=None, max_no_progress=None, supervisor: bool = False, project_goal: str | None = None,
          max_chains=None, supervisor_max_hours=None, supervisor_max_no_progress=None,
          status_files: list | None = None, max_quality_chains=None, full_verification=None, task_id: str | None = None) -> dict:
    node = node or config.local_node()
    if str(node).upper() != config.local_node():
        raise ControllerError("unsupported_node", f"node {node!r} is niet deze machine ({config.local_node()}) en er is geen "
                              "route ernaartoe. Er is bewust geen terugval op Runner 1.")
    if not isinstance(prompt, str) or not prompt.strip():
        raise ControllerError("invalid_input", "prompt ontbreekt")
    if role not in ROLES:
        raise ControllerError("invalid_input", f"role moet een van {ROLES} zijn")
    if continuation is not True and continuation is not False:
        raise ControllerError("invalid_input", "continuation moet true of false zijn")
    if supervisor is not True and supervisor is not False:
        raise ControllerError("invalid_input", "supervisor moet true of false zijn")
    sup_args = (project_goal, max_chains, supervisor_max_hours, supervisor_max_no_progress, status_files, max_quality_chains,
                full_verification)
    if not supervisor and any(v is not None for v in sup_args):
        raise ControllerError("invalid_input", "project_goal, max_chains, supervisor_max_hours, supervisor_max_no_progress, "
                              "status_files, max_quality_chains en full_verification horen bij supervisor=true")
    if full_verification is not None and not isinstance(full_verification, bool):
        raise ControllerError("invalid_input", "full_verification moet true of false zijn")
    continuation = continuation or supervisor
    if not continuation and any(v is not None for v in (goal, max_tasks, max_hours, max_no_progress)):
        raise ControllerError("invalid_input", "goal, max_tasks, max_hours en max_no_progress horen bij continuation=true; "
                              "zonder continuation is de start een gewone eenmalige taak")
    if goal is not None and (not isinstance(goal, str) or not goal.strip()):
        raise ControllerError("invalid_input", "goal moet een niet-lege tekst zijn")
    if project_goal is not None and (not isinstance(project_goal, str) or not project_goal.strip()):
        raise ControllerError("invalid_input", "project_goal moet een niet-lege tekst zijn")
    if status_files is not None and (not isinstance(status_files, list) or len(status_files) > supervisor_module.MAX_STATUS_FILES
                                     or not all(isinstance(f, str) and f.strip() and len(f) < 300 for f in status_files)):
        raise ControllerError("invalid_input", f"status_files moet een lijst van hoogstens {supervisor_module.MAX_STATUS_FILES} paden zijn")
    repo_path = Path(str(repo)).expanduser()
    if not repo_path.is_dir():
        raise ControllerError("repo_invalid", f"repository {repo} bestaat niet")
    repo = str(repo_path.resolve())
    cfg = config.load_config()
    limits = chain_limits(cfg, max_tasks, max_hours, max_no_progress) if continuation else None
    sup_limits = supervisor_limits(cfg, max_chains, supervisor_max_hours, supervisor_max_no_progress) if supervisor else None
    q_limits = quality_limits(cfg, max_quality_chains, full_verification) if supervisor else None
    if supervisor:
        limits["max_hours"] = min(limits["max_hours"], sup_limits["max_hours"])
    if task_id is not None:
        # Vooraf toegekend id (supervisie, opdrachtwachtrij): idempotent voor elke soort start. Bij een supervisorrun
        # liggen ook supervisor- en keten-id vast (S-/C- plus dezelfde hex), zodat een herhaalde start niets dubbel maakt.
        if not (isinstance(task_id, str) and re.fullmatch(r"R2-[0-9a-f]{12}", task_id)):
            raise ControllerError("invalid_input", "task_id moet een vooraf toegekend id zijn: R2- plus 12 hexcijfers")
        # Eén start tegelijk op deze machine: een herhaalde start (bv. na een ssh-time-out aan de kant van de control
        # plane, terwijl de eerste hier nog doorloopt) wacht tot de eerste klaar is en ziet dan de bestaande taak.
        with _start_lock():
            return _start_body(repo, prompt, cfg, role, model, title, continuation, supervisor, goal, project_goal,
                               status_files, limits, sup_limits, q_limits, task_id)
    return _start_body(repo, prompt, cfg, role, model, title, continuation, supervisor, goal, project_goal,
                       status_files, limits, sup_limits, q_limits, None)


START_LOCK_WAIT_S = 40.0


@contextmanager
def _start_lock():
    path = config.r2_home() / "start.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+") as f:
        end = time.time() + START_LOCK_WAIT_S
        while True:
            try:
                lock_exclusive(f, blocking=False)
                break
            except OSError:
                if time.time() > end:
                    raise ControllerError("busy", "er loopt op deze machine al een start; probeer het zo opnieuw") from None
                time.sleep(0.1)
        yield   # de lock vervalt bij het sluiten van het bestand (ook als dit proces sterft)


def _start_body(repo, prompt, cfg, role, model, title, continuation, supervisor, goal, project_goal, status_files,
                limits, sup_limits, q_limits, task_id):
    if task_id is not None:
        done = _existing_start(task_id, repo, continuation=continuation, supervisor=supervisor)
        if done:
            return done
    _require_free()   # een bestaande taak (task_id) is hierboven al idempotent teruggegeven
    preflight(cfg)
    sid = ("S-" + task_id[3:] if task_id else supervisor_module.new_id()) if supervisor else None
    chain_id = ("C-" + task_id[3:] if task_id else chains.new_id()) if supervisor else ("C-" + task_id[3:] if task_id else None)
    s = _provision(repo, prompt, cfg, model_for(cfg, role, model), role, title, tid=task_id,
                   extra={"supervisor_id": sid, "supervisor_chain_n": 1} if sid else None)
    tid, wt = s["task_id"], s["worktree"]
    chain = sup = None
    if continuation:
        chain = chains.load(chain_id) if task_id else None
        if not chain:
            # eerst de taak (keten-id + schakelprompt), dan pas de keten: een crash ertussen laat de start opnieuw lopen
            head = {"chain_id": chain_id or chains.new_id(), "goal": (goal or prompt)[:chains.GOAL_CHARS], "limits": limits}
            if s["status"] == "created":
                s = store.update(tid, chain_id=head["chain_id"], chain_link=1, inbox=[chains.link_prompt(head, 1, prompt, [])])
            chain = chains.create(repo, goal or prompt, tid, s["base"], role, model, title or "", limits, chain_id=head["chain_id"],
                                  supervisor={"id": sid, "n": 1} if sid else None)
    if supervisor and task_id and (sup := supervisor_module.load(sid)):
        pass   # half afgemaakte idempotente start: de supervisor bestond al
    elif supervisor:
        first_goal = goal or prompt
        sup = supervisor_module.create(
            sid, repo, project_goal or first_goal, prompt, role, model, title or "", sup_limits, limits,
            [f.strip() for f in status_files or []], {"chain_id": chain_id, "task_id": tid, "goal": first_goal}, s["base"],
            quality_limits=q_limits)
    s = _spawn_and_wait(tid, 0) if s["status"] == "created" else store.load(tid)
    out = {"task_id": tid, "node": config.local_node(), "status": s["status"], "repository": repo, "worktree": wt,
           "session_id": s["session_id"], "started": s["created"], "model": s["model"]}
    if chain:
        out["chain"] = chains.describe(chain, tid)
    if sup:
        out["supervisor"] = supervisor_module.describe(sup, tid)
    return out


def _existing_start(task_id: str, repo: str, continuation: bool = True, supervisor: bool = False) -> dict | None:
    """Idempotente start met vooraf toegekende id: bestaat de taak al volledig (worktree, plus keten/supervisor waar die
    bij hoort), dan geven we haar terug zonder iets aan te maken of de machinelock te eisen. Half aangemaakt = verder aanmaken.

    Een volledig aangemaakte maar nooit gestarte taak (status created, bv. crash vóór het starten van het taakproces)
    wordt alsnog gestart als de machine vrij is; anders volgt `busy` en probeert de aanroeper het later opnieuw."""
    try:
        s = store.load(task_id)
    except (OSError, ValueError):
        return None
    chain = chains.load("C-" + task_id[3:]) if continuation or supervisor else None
    sup = supervisor_module.load("S-" + task_id[3:]) if supervisor else None
    if not s.get("worktree") or ((continuation or supervisor) and not chain) or (supervisor and not sup):
        return None
    if s["repo"] != repo:
        raise ControllerError("invalid_input", f"taak {task_id} bestaat al voor een andere repository ({s['repo']})")
    if s["status"] == "created" and not (continuation and not supervisor):   # ketens: ongewijzigd oud gedrag
        _require_free()
        s = _spawn_and_wait(task_id, 0)
    out = {"task_id": task_id, "node": config.local_node(), "status": s["status"], "repository": repo, "worktree": s["worktree"],
           "session_id": s["session_id"], "started": s["created"], "model": s["model"], "already_started": True}
    if chain:
        out["chain"] = chains.describe(chain, task_id)
    if sup:
        out["supervisor"] = supervisor_module.describe(sup, task_id)
    return out


def send(tid: str, message: str, allowed: tuple, action: str) -> dict:
    s = _load(tid)
    if s["status"] not in allowed:
        raise ControllerError("wrong_state", f"{action} kan niet: taak {tid} heeft status {s['status']} "
                              f"(toegestaan: {', '.join(allowed)})")
    q = s.get("question") or {}
    # Na een aanmeldfout staat de nooit afgeleverde opdracht nog in de inbox; het antwoord ('aangemeld') mag erbij.
    pending = int(q.get("pending_inbox") or 0) if q.get("kind") == "auth_required" else 0
    if len(s["inbox"]) > pending and s["status"] == "waiting_for_human":
        raise ControllerError("wrong_state", f"taak {tid} heeft al een bericht in de wachtrij; beurt start")
    _require_free()
    if message:
        store.update(tid, inbox=s["inbox"] + [message])
    s2 = _spawn_and_wait(tid, s["turn"])
    return {"task_id": tid, "status": s2["status"], "session_id": s2["session_id"], "turn": s2["turn"]}


def answer(tid: str, text: str) -> dict:
    if not isinstance(text, str) or not text.strip():
        raise ControllerError("invalid_input", "answer ontbreekt")
    return send(tid, text, ("waiting_for_human",), "antwoorden")


def resume(tid: str, note: str = "") -> dict:
    return send(tid, note or "", ("paused", "failed"), "hervatten")


def pause(tid: str, *, recovery_owner: str | None = None) -> dict:
    # A later explicit user pause always revokes automatic resume ownership,
    # including an already-paused task. The normal pause response stays unchanged.
    _load(tid)  # Preserve normal unknown-task validation before opening its directory.
    directory = store.task_dir(tid)
    with (directory / '.pause-request.lock').open('a+') as handle:
        lock_exclusive(handle)
        owner = directory / 'guardian-pause.json'
        if recovery_owner is None:
            owner.unlink(missing_ok=True)
        s = _load(tid)
        if s["status"] != "running":
            raise ControllerError("wrong_state", f"pauzeren kan niet: taak {tid} heeft status {s['status']}")
        if recovery_owner is not None:
            if (directory / 'pause').exists():
                raise ControllerError('wrong_state', 'bestaande pauzeaanvraag blijft behouden')
            if not re.fullmatch(r'[0-9a-f]{64}', recovery_owner):
                raise ControllerError('invalid_input', 'ongeldige herstelidentiteit')
            store.atomic_write(owner, json.dumps({'owner': recovery_owner, 'session_id': s['session_id'],
                                                  'turn': s['turn'], 'at': store.now()}))
        (directory / "pause").write_text(store.now())
    return {"task_id": tid, "status": "running", "pausing": True,
            "note": "pauze aangevraagd; de beurt wordt bij de eerstvolgende controle onderbroken (status wordt paused)"}


def stop(tid: str, reason: str = "") -> dict:
    """Stopt de taak, zijn keten en — bij een supervisor — de hele supervisorrun inclusief een lopende beoordelingstaak."""
    s = _load(tid)
    sup = supervisor_module.for_task(s)
    if not sup:
        return _stop_task(tid, reason)
    sid = sup["supervisor_id"]
    # Eerst de supervisor zelf beëindigen: daarna start de watch geen keten of beoordeling meer.
    changed = supervisor_module.stop_supervisor(sid, "stopped_by_user", f"runner2_stop op taak {tid}" + (f": {reason}" if reason else ""))
    others = [t for t in supervisor_module.live_tasks(supervisor_module.load(sid)) if t != tid]
    for other in others:
        _stop_task(other, reason or f"supervisor {sid} gestopt")
    out = _stop_task(tid, reason)
    out["supervisor"] = {"id": sid, "state": "stopped", "changed": changed, "also_stopped": others}
    return out


def _stop_task(tid: str, reason: str = "") -> dict:
    s = _load(tid)
    # Een stop beëindigt eerst de keten (ook tussen twee schakels), zodat de watch daarna niets meer start.
    chain_stopped = bool(s.get("chain_id")) and chains.stop_chain(
        s["chain_id"], "stopped_by_user", f"runner2_stop op taak {tid}" + (f": {reason}" if reason else ""))
    extra = {"chain": {"id": s["chain_id"], "state": "stopped", "changed": chain_stopped}} if s.get("chain_id") else {}
    if s["status"] == "stopped":
        return {"task_id": tid, "status": "stopped", "changed": False, "note": "taak was al gestopt", **extra}
    if s["status"] == "completed":
        return {"task_id": tid, "status": "completed", "changed": False, "note": "taak is al voltooid; niets gestopt", **extra}
    # Eerst de status: vanaf nu wijzigt geen taakproces de status meer en herstelt de watch niets.
    s = store.update(tid, status="stopped", stop={"reason": reason or "", "at": store.now(), "from": s["status"]})
    killed = _kill_all(s)
    (store.task_dir(tid) / "pause").unlink(missing_ok=True)
    store.MachineLock("-").clear_stale()  # stale door het gedode taakproces
    backup = session.backup_transcript(config.load_config(), s)  # transcript blijft ook los van Claude's eigen map bewaard
    store.update(tid, transcript_backup=backup, transcript_path=s.get("transcript_path"),
                 pid=None, pid_start=None, claude_pid=None, claude_pid_start=None, children=[], orphans=[],
                 active_tools=[], in_flight=[], resume_note=None, recovery={})
    return {"task_id": tid, "status": "stopped", "changed": True, "stopped_from": s["stop"]["from"],
            "processes_stopped": killed, "worktree": s["worktree"], **extra}


def _kill_all(s: dict) -> int:
    """Taakproces, Claude en alle bekende/afstammende processen. Geeft het aantal gestopte processen."""
    victims = {c["pid"]: c for c in s.get("children", []) + s.get("orphans", [])}
    unit = s.get("unit")
    if s.get("claude_pid"):
        victims[s["claude_pid"]] = {"pid": s["claude_pid"], "pgid": s["claude_pid"], "start": s["claude_pid_start"]}
        rows = snapshot()
        keep = protected_pids(rows)   # nooit de watch of een ander taakproces, ook niet via een hergebruikte pid
        for k in descendants(rows, s["claude_pid"], s.get("claude_pid_start")):
            if k["pid"] not in keep:
                victims[k["pid"]] = k
    n = sum(1 for v in victims.values() if alive(v["pid"], v["start"]))
    osadapter.stop_unit(unit)  # Linux: de hele cgroup van de taakeenheid in één keer
    if s.get("pid") and alive(s["pid"], s["pid_start"]):
        terminate(s["pid"], s["pid_start"])  # geen handler: sterft direct; caffeinate volgt via -w
    for v in victims.values():
        if not alive(v["pid"], v["start"]):
            continue
        stop_group(v["pgid"], start=v["start"] if v.get("pgid") == v["pid"] else None)
        force_kill(v["pid"], v["start"])
    return n


# --- status en resultaat ---

def _watch() -> dict:
    try:
        age = time.time() - json.loads((config.r2_home() / "watch.json").read_text())["epoch"]
    except (OSError, ValueError, KeyError):
        return {"healthy": False, "last_scan_s_ago": None}
    return {"healthy": age < 30, "last_scan_s_ago": round(age)}


def _elapsed(s: dict) -> int:
    end = _epoch(s["updated"]) if s["status"] in store.TERMINAL else time.time()
    return max(0, int(end - _epoch(s["created"])))


def _brief(s: dict) -> dict:
    out = {"task_id": s["task_id"], "title": s.get("title") or "", "status": s["status"], "elapsed_s": _elapsed(s),
           "repository": s["repo"], "turn": s["turn"]}
    if s.get("chain_id"):
        out.update(chain_id=s["chain_id"], chain_link=s.get("chain_link"))
        if s.get("gate_role"):
            out.update(gate_role=s["gate_role"], gate_link=s.get("gate_link"))
    if s.get("supervisor_id"):
        out["supervisor_id"] = s["supervisor_id"]
        if s.get("supervisor_role"):
            out["supervisor_role"] = s["supervisor_role"]
    return out


def status(tid: str | None = None) -> dict:
    if tid is None:
        tasks = list(reversed(store.all_tasks()))
        h = busy()
        from .discussion_dispatch import active_local
        discussion = active_local()
        if h and str(h.get('task_id', '')).startswith('DT-'):h = None
        out = {"node": config.local_node(), "active_task": h["task_id"] if h else None, "watch": _watch(), "count": len(tasks),
               "tasks": [_brief(s) for s in tasks[:10]]}
        if discussion:out['discussion'] = discussion
        active = [chains.describe(c) for c in chains.all_chains() if c["state"] == chains.ACTIVE]
        if active:
            out["chains"] = active
        sups = [supervisor_module.describe(x) for x in supervisor_module.all_supervisors()
                if x["state"] == supervisor_module.ACTIVE or supervisor_module.recent(x)]
        if sups:
            out["supervisors"] = sups
        out["human_actions"] = _actions_overview(store.all_tasks())
        if sv := supervision.overview():
            out["supervision"] = sv
        return out
    s = _load(tid)
    claude = bool(s["claude_pid"]) and alive(s["claude_pid"], s["claude_pid_start"])
    out = dict(_brief(s), node=config.local_node(), active=bool(s["pid"]) and alive(s["pid"], s["pid_start"]),
               claude_active=claude, session_id=s["session_id"], worktree=s["worktree"], model=s["model"],
               pausing=s["status"] == "running" and (store.task_dir(tid) / "pause").exists())
    if s["turn_started"] and s["status"] == "running":
        out["turn_elapsed_s"] = max(0, int(time.time() - _epoch(s["turn_started"])))
        out["activity"] = (s["active_tools"] or [])[-3:]
    if s["status"] == "waiting_for_human" and s["question"]:
        q = s["question"]
        out["question"] = {"text": q.get("question"), "reason": q.get("why_needed"), "kind": q.get("kind"),
                           "recommendation": q.get("recommendation"), "alternatives": q.get("alternatives") or [],
                           "asked_at": s["updated"]}
        if q.get("last_message"):
            out["question"]["last_message"] = q["last_message"]
    if s["failure"]:
        out["last_error"] = {"kind": s["failure"]["kind"], "message": s["failure"]["message"][:500]}
    if s["finish"]:
        f = s["finish"]
        out["summary"] = f.get("summary")
        out["tests"] = {"run": f.get("tests_run") or [], "result": f.get("test_results")}
    if s.get("stop"):
        out["stop"] = s["stop"]
    rec = s.get("recovery") or {}
    cfg = config.load_config()
    recoverable = watch.recoverable_failure(s, cfg)
    if rec.get("attempts") or recoverable:
        top = int(cfg["max_recovery_attempts"])
        # will_retry: herstelt de watch van DEZE machine de taak nog zelf? (de wachtrij op de control plane leest dit)
        out["recovery"] = {"attempts": int(rec.get("attempts") or 0), "max": top,
                           "will_retry": recoverable and int(rec.get("attempts") or 0) < top}
        if rec.get("cause"):   # stale_running (taakproces niet meer gevonden), interrupted, api_error, stalled, auth_error
            out["recovery"]["cause"] = rec["cause"]
    if s["warnings"]:
        out["warnings"] = s["warnings"][-3:]
    if ch := chains.for_task(s):
        out["chain"] = chains.describe(ch, tid)
        if not chains_watch_ok(ch):
            out["chain"]["warning"] = "watch draait niet; de keten gaat pas verder als de watch weer draait"
    if sup := supervisor_module.for_task(s):
        out["supervisor"] = supervisor_module.describe(sup, tid)
        if sup["state"] == supervisor_module.ACTIVE and not _watch()["healthy"]:
            out["supervisor"]["warning"] = "watch draait niet; de supervisor gaat pas verder als de watch weer draait"
    if blok := _run_actions(s):
        out["human_actions"] = blok
    if ref := supervision.reference(s):
        out["supervision"] = ref
    return out


def _actions_overview(states: list) -> dict:
    """Node-breed: aantal openstaande menselijke actiepunten (plus de punten zelf, begrensd). Oude taken zonder veld tellen nul."""
    items = actions.open_items(states)
    return {"open": len(items), "items": [actions.brief(i) for i in items[:20]]}


def _run_actions(s: dict) -> dict | None:
    """Actiepunten van de run waartoe deze taak behoort; None als er (nooit) iets was."""
    if not (s.get("supervisor_id") or s.get("chain_id")):
        states = [s]
    else:
        states = actions.run_states(s, store.all_tasks())
    block = actions.describe(states, include_resolved=True)
    return block if block["items"] else None


def list_actions(task_id: str | None = None, include_resolved: bool = False) -> dict:
    """Open menselijke actiepunten: van de run van `task_id`, anders node-breed."""
    if task_id:
        states = actions.run_states(_load(task_id), store.all_tasks())
    else:
        states = store.all_tasks()
    return dict(actions.describe(states, include_resolved), node=config.local_node(), scope=task_id or "node")


def resolve_actions(ids: list, note: str = "", by: str = "controller") -> dict:
    """Sluit open actiepunten op id (idempotent); het bijbehorende telwerk daalt direct."""
    if not isinstance(ids, list) or not ids:
        raise ControllerError("bad_request", "geef een niet-lege lijst action_ids")
    out = actions.resolve(ids, note=note, by=by)
    out.update(node=config.local_node(), open_remaining=len(actions.open_items(store.all_tasks())))
    return out


def review_actions() -> dict:
    """DRY RUN van het actiebeheer op deze node: welke open punten zouden sluiten en welke een beslissing vragen. Wijzigt niets."""
    from . import actionreview
    return actionreview.local_plan()


def closure_report() -> dict:
    """Bewijsgerichte afsluiting (besluit 43), alleen rapport: per gekoppeld open punt de dimensies besluit/uitvoering/controle/integratie."""
    from . import actionclosure
    res = actionclosure.run(apply=False)
    return dict(res["report"], mode="report", summary=actionclosure.summary(res["report"]))


def reassess_report(live: bool = False) -> dict:
    """Herbeoordeling van open acties (besluit 46), alleen lezen. Standaard het laatst bewaarde rapport van de periodieke ronde;
    `live` rekent nu opnieuw (ook andere nodes, via ssh) zonder iets te bewaren of als voorgelegd te markeren."""
    from . import actionreassess
    if live:
        rep = actionreassess.run(persist=False)
    elif not (rep := actionreassess.latest()):
        raise ControllerError("not_found", "er is nog geen herbeoordeling bewaard (de eerste ronde volgt binnen action_reassess_interval_h uur)")
    return dict(rep, mode="live" if live else "bewaard", summary=actionreassess.summary(rep))


def claim_action(action_id: str, job_id: str, besluit=None, soort: str | None = None, note: str = "", by: str = "controller") -> dict:
    """Koppelt een open actiepunt aan de opdracht die het uitvoert. Sluit niets: sluiten kan alleen de afsluitronde, met bewijs."""
    from . import actionclosure
    try:
        return actionclosure.claim(action_id, job_id, by, besluit=besluit, soort=soort, note=note)
    except actionclosure.ClosureError as e:
        raise ControllerError("invalid_input" if e.code == "invalid_input" else e.code, e.message) from e


def chains_watch_ok(ch: dict) -> bool:
    return ch["state"] != chains.ACTIVE or _watch()["healthy"]


def result(tid: str) -> dict:
    s = _load(tid)
    if s["status"] not in store.TERMINAL:
        raise ControllerError("not_finished", f"taak {tid} is nog niet klaar (status {s['status']})")
    out = {"task_id": tid, "node": config.local_node(), "title": s.get("title") or "", "final_status": s["status"], "repository": s["repo"],
           "worktree": s["worktree"], "branch": s["branch"], "session_id": s["session_id"], "model": s["model"],
           "claude_code_version": s["claude_version"], "elapsed_s": _elapsed(s)}
    f = s["finish"] or {}
    out.update(summary=f.get("summary"), attention=f.get("attention") or [],
               tests={"run": f.get("tests_run") or [], "result": f.get("test_results")},
               reported_changes=f.get("changes") or [])
    base = (s.get("base") or {}).get("commit")
    if base and Path(s["worktree"]).is_dir():
        try:
            log = gitops.git(s["worktree"], "log", "--format=%h %s", f"{base}..HEAD").stdout.splitlines()
            files = gitops.git(s["worktree"], "diff", "--name-only", base, "HEAD").stdout.splitlines()
            out["commits"] = log[:20]
            out["files_changed"] = files[:50]
            out["uncommitted"] = bool(gitops.git(s["worktree"], "status", "--porcelain").stdout.strip())
        except RuntimeError as e:
            out["git_error"] = str(e)[:200]
    out["integration_dry_run"] = s["integration"]
    if ch := chains.for_task(s):
        out["chain"] = chains.describe(ch, tid, detail=True)
        out["chain"]["commits_total"] = chains.commits_total(ch)
    if sup := supervisor_module.for_task(s):
        out["supervisor"] = supervisor_module.describe(sup, tid, detail=True)
    if blok := _run_actions(s):
        out["human_actions"] = blok
    if ref := supervision.reference(s):
        out["supervision"] = ref
    if s["failure"]:
        out["error"] = {"kind": s["failure"]["kind"], "message": s["failure"]["message"][:500]}
    if s.get("stop"):
        out["stop"] = s["stop"]
    turns = s.get("turns") or []
    out["timings"] = {"turns": len(turns), "claude_s": round(sum(t["claude_s"] for t in turns), 1),
                      "runner_overhead_s": round(sum(t["overhead_s"] for t in turns), 1)}
    return out


def bundle() -> dict:
    """Alles wat een monitor per poll nodig heeft in één aanroep (voor een remote node: één ssh-aanroep)."""
    listing = status()
    states, details, recovering = {}, {}, {}
    for b in listing["tasks"]:
        try:
            st = store.load(b["task_id"])
            states[b["task_id"]], details[b["task_id"]] = st, status(b["task_id"])
            recovering[b["task_id"]] = watch.needs_recovery(st)
        except (OSError, ValueError, KeyError):
            continue
    cfg = config.load_config()
    return {"listing": listing, "states": states, "details": details, "recovering": recovering,
            "config": {"max_recovery_attempts": cfg["max_recovery_attempts"]}}


# --- health (controller-MCP start, toolset, watch, tunnel) ---

TUNNEL_READY_URL = "http://127.0.0.1:8766/readyz"   # Runner-2-tunnel; de Runner-1-tunnel gebruikt 8765


def health(tunnel_url: str = TUNNEL_READY_URL) -> dict:
    """Minimale gezondheidscontrole; geen taak wordt aangeraakt."""
    import urllib.request
    from . import controller_mcp
    out = {"watch": _watch()}
    try:
        p = subprocess.run([sys.executable, "-m", "runner2.controller_mcp"], capture_output=True, text=True, timeout=15,
                           input='{"jsonrpc":"2.0","id":1,"method":"tools/list"}\n',
                           env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parent.parent)))
        names = [t["name"] for t in json.loads(p.stdout.splitlines()[0])["result"]["tools"]]
        out["tools"] = {"ok": names == [t["name"] for t in controller_mcp.TOOLS], "names": names}
    except (OSError, subprocess.SubprocessError, ValueError, IndexError, KeyError) as e:
        out["tools"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    try:
        with urllib.request.urlopen(tunnel_url, timeout=3) as r:
            out["tunnel"] = {"ready": r.status == 200}
    except OSError:
        out["tunnel"] = {"ready": False}
    out["ok"] = out["watch"]["healthy"] and out["tools"]["ok"]
    return out

