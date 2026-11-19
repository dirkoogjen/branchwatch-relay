"""De watch: scant ~/.runner2/tasks en herstelt onderbroken taken. Bezit nooit een Claude-beurt.

Taakprocessen draaien in een eigen processessie en overleven een herstart van de watch. De watch
start hoogstens een nieuw taakproces (dat --resume gebruikt) voor een taak die aantoonbaar onderbroken is.
"""
import json
import os
import signal
import sys
import threading
import time

from . import actionclosure, actionreassess, actionreview, actions, chains, config, cplane, jobs, store, supervision, supervisor, taskproc
from .filelock import lock_exclusive
from .log import log
from .procs import alive, proc_start

RECOVERABLE = {"interrupted", "api_error", "stalled"}   # failure.kind; config_error e.d. blijft failed
# auth_error (niet of niet meer aangemeld) is alleen herstelbaar als 'niet aangemeld' binnen auth_retry_attempts: op de Mac
# kan een ssh-start de inlogsleutelhanger niet lezen en lukt het vanuit de watch wel. Een verlopen login herstelt nooit
# door te herhalen; die gaat meteen als vraag naar de gebruiker (taskproc._finish_turn).


def taskproc_alive(st: dict) -> bool:
    """Zelfde instantie: pid én starttijd moeten kloppen (een hergebruikte pid telt niet)."""
    return bool(st.get("pid")) and alive(st["pid"], st["pid_start"])


def recoverable_failure(st: dict, cfg: dict | None = None) -> bool:
    """Herstelt de watch deze mislukte taak zelf? (zelfde regel voor de watch, de status en de wachtrij)"""
    f = st.get("failure") or {}
    if st.get("status") != "failed":
        return False
    if f.get("kind") in RECOVERABLE:
        return True
    if f.get("kind") == "auth_error" and f.get("auth") == "not_logged_in":
        cfg = cfg or config.load_config()
        return int((st.get("recovery") or {}).get("auth_attempts") or 0) < int(cfg.get("auth_retry_attempts", 0))
    return False


def needs_recovery(st: dict, cfg: dict | None = None) -> bool:
    if st["status"] == "running":
        return not taskproc_alive(st)
    return recoverable_failure(st, cfg)


def _finalize_from_signals(st: dict) -> bool:
    """Taakproces stierf nadat Claude al een signaal gaf: dat signaal alsnog verwerken, niet opnieuw werken."""
    sigs = store.read_signals(store.task_dir(st["task_id"]), st["turn"])
    ask = next((s for s in sigs if s["kind"] == "ask"), None)
    fin = next((s for s in sigs if s["kind"] == "finish"), None)
    if not (ask or fin):
        return False
    clean = dict(pid=None, pid_start=None, claude_pid=None, claude_pid_start=None, children=[], active_tools=[],
                 in_flight=[], resume_note=None, recovery={})
    if ask:
        store.update(st["task_id"], status="waiting_for_human", question=ask["payload"], **clean)
    else:
        store.update(st["task_id"], status="completed", finish=fin["payload"], **clean)
        actions.record(st["task_id"], fin["payload"])
    log(f"{st['task_id']}: signaal {'ask' if ask else 'finish'} alsnog verwerkt (taakproces verdwenen)")
    return True


def handle_spawn_request(st: dict) -> None:
    """Startverzoek van een ssh-aanroep (zie taskproc.spawn): de watch start het taakproces in de inlogsessie."""
    tid = st["task_id"]
    store.update(tid, spawn_request=None)
    if st["status"] == "stopped" or taskproc_alive(st):
        log(f"{tid}: startverzoek vervallen (status {st['status']})")
        return
    log(f"{tid}: startverzoek van {st['spawn_request']} uitgevoerd door de watch")
    taskproc.spawn(tid, handoff=False)


def recover(st: dict, cfg: dict, now: float) -> None:
    tid, rec = st["task_id"], st.get("recovery") or {}
    if rec.get("next_retry_at", 0) > now:
        return
    if store.MachineLock("-")._holder():
        return  # een andere taak draait; wachten zonder poging te tellen
    if st["status"] == "running" and _finalize_from_signals(st):
        return
    attempts = rec.get("attempts", 0)
    if attempts >= cfg["max_recovery_attempts"]:
        store.update(tid, status="failed", failure={"kind": "retry_exhausted", "at": store.now(),
                     "message": f"{attempts} herstelpogingen mislukt; laatste fout: {(st.get('failure') or {}).get('message')}"})
        log(f"{tid}: herstelpogingen op ({attempts}), blijft failed")
        return
    delay = min(cfg["retry_base_s"] * 2 ** attempts, cfg["retry_max_s"])
    cause = "stale_running" if st["status"] == "running" else (st.get("failure") or {}).get("kind")
    new = {"attempts": attempts + 1, "last_at": now, "next_retry_at": now + delay, "cause": cause}
    if cause == "auth_error":
        new["auth_attempts"] = int(rec.get("auth_attempts") or 0) + 1
    store.update(tid, recovery=new)
    log(f"{tid}: herstel gestart (poging {attempts + 1}, oorzaak {cause}, volgende poging vroegst over {delay}s)")
    taskproc.spawn(tid)


def scan_once(cfg: dict, seen: dict, quiet: dict | None = None) -> int:
    """Eén scan. `quiet` onthoudt mtime van taken waar niets aan te doen valt (scheelt parsen)."""
    quiet = {} if quiet is None else quiet
    n = 0
    tdir = config.tasks_dir()
    for p in sorted(tdir.glob("R2-*/state.json")) if tdir.exists() else []:
        mtime = p.stat().st_mtime
        if quiet.get(p) == mtime:
            continue
        try:
            st = json.loads(p.read_text())
        except (OSError, ValueError):
            log(f"{p.parent.name}: state.json onleesbaar")
            continue
        n += 1
        tid = st["task_id"]
        if tid not in seen:
            log(f"{tid}: ontdekt, status {st['status']}")
        elif seen[tid] != st["status"]:
            log(f"{tid}: status {seen[tid]} -> {st['status']}")
        seen[tid] = st["status"]
        if st.get("spawn_request"):
            handle_spawn_request(st)
            continue
        if needs_recovery(st, cfg):
            if st["status"] == "running":
                log(f"{tid}: taakproces pid {st.get('pid')} bestaat niet meer (stale running)")
            recover(st, cfg, time.time())
        else:
            quiet[p] = mtime if st["status"] != "running" and not recoverable_failure(st, cfg) else None
    return n


def housekeeping(cfg: dict) -> None:
    """Half geschreven state-tijdbestanden van niet-lopende taken opruimen; stale machine.lock verwijderen."""
    lock = store.MachineLock("-")
    if lock.path.exists() and not lock._holder() and lock.clear_stale():
        log("stale machine.lock verwijderd")
    for tmp in config.tasks_dir().glob("R2-*/*.tmp*") if config.tasks_dir().exists() else []:
        if time.time() - tmp.stat().st_mtime > 60:
            tmp.unlink(missing_ok=True)


def _cpu_s() -> float:
    try:
        import resource
    except ImportError:   # Windows
        return round(time.process_time(), 2)
    return round(sum(resource.getrusage(resource.RUSAGE_SELF)[:2]), 2)


def _review_tick(cfg: dict) -> None:
    """Actiebeheer: alleen een DRY RUN-rapport als `action_review_interval_h` > 0 (standaard 0 = uit); sluit nooit iets."""
    try:
        if path := actionreview.tick(cfg):
            log(f"actiebeheer: dry run bewaard in {path}")
    except Exception as e:   # noqa: BLE001 -- mag de watch nooit stilleggen
        log(f"actiebeheer: {type(e).__name__}: {e}")


def _closure_tick(cfg: dict) -> None:
    """Bewijsgerichte afsluiting (besluit 43): sluit alleen punten met een claim en sluitend bewijs; zonder claims verandert er niets."""
    try:
        if (res := actionclosure.tick(cfg)) and (res["closed"] or res["breaker"]):
            log(f"actieafsluiting: gesloten {res['closed']}" + (f"; {res['breaker']}" if res["breaker"] else ""))
    except Exception as e:   # noqa: BLE001 -- mag de watch nooit stilleggen
        log(f"actieafsluiting: {type(e).__name__}: {e}")


def _reassess_tick(cfg: dict) -> None:
    """Herbeoordeling van open acties (besluit 46): hoogstens elke `action_reassess_interval_h` uur, in een eigen thread; alleen lezen en rapporteren."""
    try:
        actionreassess.tick(cfg)
    except Exception as e:   # noqa: BLE001 -- mag de watch nooit stilleggen
        log(f"actieherbeoordeling: {type(e).__name__}: {e}")


def _jobs_tick(cfg: dict) -> None:
    try:
        jobs.tick(cfg)
        from . import discussion_dispatch
        discussion_dispatch.tick(cfg)
    except Exception as e:   # noqa: BLE001 -- de wachtrij mag de watch nooit stilleggen
        log(f"opdrachtwachtrij: {type(e).__name__}: {e}")


def _jobs_loop(cfg: dict, stop: list) -> None:
    """Opdrachtwachtrij (alleen actief op de primaire control plane; `jobs.tick` controleert dat zelf)."""
    while not stop:
        t0 = time.time()
        _jobs_tick(cfg)
        while time.time() < t0 + float(cfg.get("jobs_poll_s", 10)) and not stop:
            time.sleep(0.2)


_standby_warned = [0.0]


def _advance_runs(cfg: dict) -> None:
    """Ketens en supervisors schakelen alleen door op de primaire control plane (of een machine zonder rolbestand, zoals vroeger).
    Een expliciete standby (of een rolbestand dat niet te lezen is) laat ze staan: een kopie van ketenstate op de verkeerde machine
    mag nooit dubbel taken starten (masterverhuizing, split-brain). `chains_on_standby = true` zet het oude gedrag terug."""
    if cplane.is_primary() or cfg.get("chains_on_standby"):
        chains.advance_all(cfg)
        supervisor.advance_all(cfg)
        return
    if time.time() - _standby_warned[0] > 600:
        _standby_warned[0] = time.time()
        waiting = [c["chain_id"] for c in chains.all_chains() if c.get("state") == "active"] + \
                  [s["supervisor_id"] for s in supervisor.all_supervisors() if s.get("state") == "active"]
        if waiting:
            log(f"watch: standby; {len(waiting)} actieve keten(s)/supervisor(s) blijven staan (alleen de primaire control plane schakelt ze door): {', '.join(waiting[:5])}")


def run(once: bool = False) -> int:
    cfg = config.load_config()
    home = config.r2_home()
    home.mkdir(parents=True, exist_ok=True)
    guard = open(home / "watch.lock", "w")
    try:
        lock_exclusive(guard, blocking=False)
    except OSError:
        log("watch: er draait al een watch, deze stopt")
        return 3
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    signal.signal(signal.SIGINT, lambda *_: stop.append(1))
    me = proc_start(os.getpid())   # in watch.json, zodat een taakproces de watch exact herkent en nooit stopt
    log(f"watch gestart (pid {os.getpid()}, interval {cfg['watch_poll_s']}s)")
    seen: dict = {}
    quiet: dict = {}
    if not once:   # eigen thread: trage ssh-aanroepen van de wachtrij houden herstel en hartslag van de watch niet op
        threading.Thread(target=_jobs_loop, args=(cfg, stop), name="opdrachtwachtrij", daemon=True).start()
    while not stop:
        t0 = time.time()
        try:
            scan_once(cfg, seen, quiet)
            _advance_runs(cfg)
            supervision.advance_all(cfg)
            if once:
                _jobs_tick(cfg)
            housekeeping(cfg)
            _review_tick(cfg)
            _closure_tick(cfg)
            _reassess_tick(cfg)
            store.atomic_write(home / "watch.json", json.dumps({
                "pid": os.getpid(), "start": me, "last_scan": store.now(), "epoch": time.time(),
                "cpu_s": _cpu_s()}))
        except Exception as e:  # de watch moet blijven leven
            log(f"scanfout: {type(e).__name__}: {e}")
        if once:
            break
        end = t0 + cfg["watch_poll_s"]
        while time.time() < end and not stop:
            time.sleep(min(0.2, max(0, end - time.time())))
    log("watch gestopt")
    return 0


if __name__ == "__main__":
    sys.exit(run("--once" in sys.argv))


