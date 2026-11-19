"""Het taakproces: één Claude-beurt van start tot procesexit, met eigen pid, bewaking en classificatie."""
import json
import os
import queue
import re
import selectors
import signal
import subprocess
import sys
import threading
import time

from . import actions, config, gitops, osadapter, session, store
from .log import log
from .procs import INFRA, alive, descendants, force_kill, proc_start, protected_pids, snapshot, stop_group

PAUSE_KILL_AFTER_S = 30
USER_RECORD = re.compile(rb'"type":\s*"user"')
# Echte aanmeldfouten van Claude Code (resultaattekst of stderr van een mislukte beurt). Herhalen zonder nieuwe login
# helpt niet; zie watch.recoverable_failure.
AUTH_EXPIRED = re.compile(r"oauth (?:session|token)[^\n]{0,60}?(?:expired|could not be refreshed|revoked)|"
                          r"could not be refreshed|token has expired|invalid api key|invalid x-api-key", re.I)
AUTH_NOT_LOGGED_IN = re.compile(r"not logged in|please run /login", re.I)
AUTH_FAILED = re.compile(r"failed to authenticate|authentication_failed|authentication_error|\b401\b[^\n]{0,40}unauthori", re.I)


def auth_problem(text: str | None) -> str | None:
    """'expired' | 'not_logged_in' | 'failed' als de fouttekst op een aanmeldprobleem wijst, anders None."""
    t = (text or "")[:4000]
    if AUTH_EXPIRED.search(t):
        return "expired"
    if AUTH_NOT_LOGGED_IN.search(t):
        return "not_logged_in"
    if AUTH_FAILED.search(t):
        return "failed"
    return None


def auth_question(auth: str, message: str) -> dict:
    node = config.local_node()
    why = {"expired": "De aanmelding van Claude Code is verlopen en kon niet automatisch worden vernieuwd.",
           "not_logged_in": "Claude Code is hier niet aangemeld.",
           "failed": "Claude Code kon zich niet aanmelden bij Anthropic."}[auth]
    return {"kind": "auth_required", "auth": auth,
            "question": f"Claude Code op {node} moet opnieuw worden aangemeld. Meld Claude Code op {node} opnieuw aan "
                        "en antwoord daarna (bijvoorbeeld 'aangemeld, ga verder'); dan gaat de taak verder.",
            "why_needed": why + " Opnieuw proberen zonder nieuwe aanmelding helpt niet, dus Runner 2 herhaalt de beurt "
                          "niet automatisch.",
            "last_message": str(message or "")[:1000]}


def _stderr_tail(tdir, turn: int) -> str:
    try:
        with open(tdir / "turns" / f"{turn}.stderr", "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - 4000))
            return f.read().decode(errors="replace")
    except OSError:
        return ""


def _user_records(cfg: dict, state: dict) -> int:
    p = session.find_transcript(cfg, state["session_id"])
    return len(USER_RECORD.findall(p.read_bytes())) if p else 0


def settle_orphans(state: dict, cfg: dict, grace: float) -> list:
    """Wezen na een onderbreking: laten aflopen tot `grace`, daarna stoppen; geeft regels voor het hervatbericht.

    Elk wees wordt vlak voor het stoppen opnieuw op pid + starttijd gecontroleerd; de watch en taakprocessen van
    Runner 2 (protected_pids) worden nooit gestopt, ook niet als ze door een hergebruikte pid als kind leken te tellen."""
    keep = protected_pids(snapshot())
    report = []
    for o in state["orphans"]:
        if o["pid"] in keep or INFRA.search(o.get("cmd") or ""):
            log(f"orphan NIET gestopt (Runner 2-proces): pid {o['pid']} {o['cmd'][:60]}")
    cands = [o for o in state["orphans"] if o["pid"] not in keep and not INFRA.search(o.get("cmd") or "")]
    live = [o for o in cands if alive(o["pid"], o["start"])]
    deadline = time.time() + (0 if any(o["cmd"] == "claude" for o in live) else grace)  # een levende claude zonder taakproces heeft niemand meer die meeleest
    while live and time.time() < deadline:
        time.sleep(min(1.0, cfg["poll_interval_s"]))
        live = [o for o in live if alive(o["pid"], o["start"])]
    for o in cands:
        if o in live:
            log(f"orphan opgeruimd: pid {o['pid']} {o['cmd'][:60]}")
            stop_group(o["pgid"], start=o["start"] if o["pgid"] == o["pid"] else None)
            force_kill(o["pid"], o["start"])
            end = time.time() + 2   # stoppen is asynchroon: even laten afronden voordat we 'kon niet' melden
            while alive(o["pid"], o["start"]) and time.time() < end:
                time.sleep(0.1)
            if alive(o["pid"], o["start"]):
                log(f"orphan pid {o['pid']} kon niet worden gestopt (rechten?)")
                report.append(f"Proces '{o['cmd'][:80]}' liep nog en kon door Runner 2 niet worden gestopt.")
            else:
                report.append(f"Proces '{o['cmd'][:80]}' liep nog en is door Runner 2 gestopt.")
        else:
            report.append(f"Proces '{o['cmd'][:80]}' liep na de onderbreking nog en is daarna zelf afgelopen "
                          "(uitkomst niet bekend; controleer de uitvoer of draai opnieuw).")
    return report


WATCH_FRESH_S = 30   # gelijk aan controller._watch: een watch die langer niet scande, neemt geen startverzoek aan


def _watch_fresh() -> bool:
    try:
        return time.time() - json.loads((config.r2_home() / "watch.json").read_text())["epoch"] < WATCH_FRESH_S
    except (OSError, ValueError, KeyError, TypeError):
        return False


def spawn(tid: str, handoff: bool = True) -> None:
    """Start een los taakproces (overleeft watch en Terminal); op Linux als eigen systemd-eenheid.

    Mac vanuit ssh (o.a. de centrale wachtrij op GERARD): niet zelf starten maar een startverzoek voor de watch
    neerleggen; de watch draait in de inlogsessie en kan wél bij de Claude-login. Zonder levende watch: direct.
    De watch zelf roept met handoff=False aan (nooit een verzoek aan zichzelf)."""
    from pathlib import Path
    if handoff and osadapter.needs_session_handoff() and _watch_fresh():
        store.update(tid, spawn_request=store.now())
        log(f"{tid}: taakproces aangevraagd bij de watch (ssh-sessie kan de Claude-login niet lezen)")
        return
    unit = osadapter.spawn_taskproc(tid, store.task_dir(tid) / "taskproc.log",
                                    Path(__file__).resolve().parent.parent, config.r2_home())
    if unit:
        store.update(tid, unit=unit)
    log(f"{tid}: taakproces gestart" + (f" (eenheid {unit})" if unit else ""))


def _fail(tid: str, kind: str, message: str, **extra) -> int:
    store.update(tid, status="failed", failure={"kind": kind, "message": message, "at": store.now()}, **extra)
    return 1


def run_turn(tid: str) -> int:
    cfg = config.load_config()
    lock = store.MachineLock(tid)
    try:
        lock.acquire()
    except store.Busy as e:
        print(e, file=sys.stderr)
        return 3
    st = store.load(tid)
    if st["status"] == "stopped":  # gestopt tussen spawn en start: niets doen
        lock.release()
        return 0
    if st["status"] == "running" and st.get("pid") not in (None, os.getpid()) and alive(st["pid"], st.get("pid_start")):
        # Tweede vangnet naast de machinelock: er loopt al een taakproces voor deze taak; nooit een tweede beurt ernaast.
        log(f"{tid}: taakproces pid {st['pid']} loopt nog; dubbele start geweigerd")
        lock.release()
        return 3
    caf = _caffeinate(cfg, tid)
    try:
        return _turn(tid, cfg)
    finally:
        if caf:
            caf.terminate()
            caf.wait()
            log(f"{tid}: caffeinate gestopt")
        lock.release()


def _caffeinate(cfg: dict, tid: str):
    p = osadapter.keep_awake(cfg)
    if p:
        log(f"{tid}: caffeinate gestart (pid {p.pid})")
    return p


def _turn(tid: str, cfg: dict) -> int:
    t_start = time.time()
    st = store.load(tid)
    tdir = store.task_dir(tid)
    turn = st["turn"] + 1
    (tdir / "turns").mkdir(exist_ok=True)
    (tdir / "pause").unlink(missing_ok=True)

    env = session.build_env(cfg, st, turn)
    binary = session.claude_binary(cfg, env)
    expected = cfg["expected_claude_version"]
    found = session.executable_version(binary, env)
    if not expected or found != expected:
        return _fail(tid, "config_error", f"Claude Code versie {found!r}, verwacht {expected!r}")

    if st["status"] == "running":  # vorig taakproces verdween zonder netjes af te sluiten
        me = {"pid": st["claude_pid"], "pgid": st["claude_pid"], "start": st["claude_pid_start"], "cmd": "claude"}
        extra = [c for c in st["children"] + ([me] if st["claude_pid"] else []) if alive(c["pid"], c["start"])]
        st = store.update(tid, orphans=extra, resume_note={"kind": "interrupted", "tools": st["active_tools"]})
    t_orph = time.time()
    report = settle_orphans(st, cfg, cfg["orphan_grace_s"]) if st["orphans"] else []
    orphan_wait = round(time.time() - t_orph, 2)
    restored = session.restore_transcript(cfg, st)

    users_before = _user_records(cfg, st)
    resume = session.find_transcript(cfg, st["session_id"]) is not None
    prompt = session.build_prompt(dict(st, in_flight=st["inbox"]), report)
    st = store.update(tid, status="running", turn=turn, in_flight=st["inbox"], inbox=[], orphans=[],
                      pid=os.getpid(), pid_start=proc_start(os.getpid()), turn_started=store.now(),
                      failure=None, question=None, active_tools=[], children=[], claude_version=found, orphan_wait_s=orphan_wait,
                      warnings=st["warnings"] + ([f"transcript hersteld uit back-up bij beurt {turn}"] if restored else []))
    files = session.write_session_files(cfg, st)
    cmd = session.build_cmd(cfg, st, binary, files, resume)
    t_spawn = time.time()
    try:
        with open(tdir / "turns" / f"{turn}.stderr", "wb") as err:
            proc = subprocess.Popen(cmd, cwd=st["worktree"], env=env, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, stderr=err, **_detach_kwargs())
    except OSError as e:
        return _fail(tid, "config_error", f"Claude starten mislukt: {e}", inbox=st["in_flight"], in_flight=[])
    store.update(tid, claude_pid=proc.pid, claude_pid_start=proc_start(proc.pid))
    try:
        proc.stdin.write(prompt.encode())
        proc.stdin.close()
    except BrokenPipeError:
        pass

    ev = _read_stream(proc, cfg, tid, tdir, turn, expected)
    t_exit = time.time()
    return _finish_turn(tid, cfg, st, ev, users_before, t_start, t_spawn, t_exit, turn)


def _detach_kwargs() -> dict:
    """Claude krijgt een eigen sessie (POSIX) of procesgroep zonder console (Windows)."""
    if osadapter.WINDOWS:
        return {"creationflags": osadapter.child_process_flags()}
    return {"start_new_session": True}


class _Chunks:
    """Leest de stdout van Claude in stukken met een time-out. POSIX: selectors; Windows: leesdraad (selectors kan daar geen pipes)."""

    def __init__(self, proc, timeout_s: float):
        self.proc, self.timeout_s = proc, timeout_s
        if osadapter.WINDOWS:
            self.q = queue.Queue()
            threading.Thread(target=self._pump, daemon=True).start()
        else:
            self.sel = selectors.DefaultSelector()
            self.sel.register(proc.stdout, selectors.EVENT_READ)

    def _pump(self):
        fd = self.proc.stdout.fileno()
        while True:
            try:
                chunk = os.read(fd, 65536)
            except OSError:
                chunk = b""
            self.q.put(chunk)
            if not chunk:
                return

    def read(self) -> bytes | None:
        """Volgend stuk; None = niets binnen de time-out; b"" = einde van de stroom."""
        if osadapter.WINDOWS:
            try:
                return self.q.get(timeout=self.timeout_s)
            except queue.Empty:
                return None
        if self.sel.select(self.timeout_s):
            return os.read(self.proc.stdout.fileno(), 65536)
        return None


def _interrupt(proc, start: str | None = None) -> None:
    """Pauze: POSIX stuurt SIGINT (Claude rondt de beurt netjes af). Windows kent geen SIGINT voor een losgekoppeld
    proces zonder console; daar wordt de hele Claude-boom gestopt en hervat de volgende beurt met --resume."""
    if osadapter.WINDOWS:
        stop_group(proc.pid, start=start)
    else:
        proc.send_signal(signal.SIGINT)


def _read_stream(proc, cfg, tid, tdir, turn, expected) -> dict:
    """Leest stream-json tot het proces echt eindigt (W3), bewaakt kinderen, pauze en stilstand."""
    out = {"results": [], "active": {}, "children": {}, "seen": {}, "problem": None, "pause_sent": False, "ticks_s": 0.0,
           "events": 0, "retries": 0}
    stream = open(tdir / "turns" / f"{turn}.stream.jsonl", "w", encoding="utf-8")
    chunks = _Chunks(proc, cfg["poll_interval_s"])
    claude_start = proc_start(proc.pid)
    buf, last_tick, t0, pause_at = b"", 0.0, time.time(), None

    def handle(line: bytes):
        stream.write(line.decode(errors="replace") + "\n")
        stream.flush()
        try:
            e = json.loads(line)
        except ValueError:
            return
        out["events"] += 1
        typ = e.get("type")
        if typ == "system" and e.get("subtype") == "init":
            bad = session.check_init(e, expected)
            marker = tdir / f"session-start.{turn}"
            end = time.time() + 1  # hook draait normaal vóór init; korte marge
            while not marker.exists() and time.time() < end:
                time.sleep(0.1)
            if not marker.exists():
                bad.append("SessionStart-hookmarker ontbreekt")
            if bad:
                out["problem"] = "init-controle: " + "; ".join(bad)
                proc.terminate()
        elif typ == "system" and e.get("subtype") == "api_retry":
            out["retries"] += 1
        elif typ == "assistant":
            for b in (e.get("message") or {}).get("content") or []:
                if b.get("type") == "tool_use":
                    inp = b.get("input") or {}
                    out["active"][b["id"]] = f"{b['name']}: {str(inp.get('command') or inp.get('file_path') or '')[:100]}"
        elif typ == "user":
            c = (e.get("message") or {}).get("content")
            for b in c if isinstance(c, list) else []:
                if b.get("type") == "tool_result":
                    out["active"].pop(b.get("tool_use_id"), None)
        elif typ == "result":
            out["results"].append(e)

    def tick():
        t = time.time()
        rows = snapshot()
        keep = protected_pids(rows)
        kids = [k for k in descendants(rows, proc.pid, claude_start) if k["pid"] not in keep]
        out["children"] = {(k["pid"], k["start"]): k for k in kids}
        out["seen"].update(out["children"])
        store.update(tid, children=list(out["children"].values()), active_tools=list(out["active"].values()))
        out["ticks_s"] += time.time() - t

    while True:
        chunk = chunks.read()
        if chunk is not None:
            if not chunk:
                break
            buf += chunk
            *lines, buf = buf.split(b"\n")
            for line in lines:
                if line.strip():
                    handle(line)
        elif proc.poll() is not None:
            break
        now = time.time()
        if now - last_tick >= cfg["poll_interval_s"]:
            last_tick = now
            tick()
            if (tdir / "pause").exists() and not out["pause_sent"] and proc.poll() is None:
                out["pause_sent"], pause_at = True, now
                _interrupt(proc, claude_start)
            if pause_at and now - pause_at > PAUSE_KILL_AFTER_S and proc.poll() is None:
                proc.kill()
            if not out["events"] and now - t0 > cfg["first_event_timeout_s"] and proc.poll() is None:
                out["problem"] = f"geen events binnen {cfg['first_event_timeout_s']} s"
                proc.kill()
    if buf.strip():
        handle(buf)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    out["rc"] = proc.returncode
    stream.close()
    return out


def _finish_turn(tid, cfg, st, ev, users_before, t_start, t_spawn, t_exit, turn) -> int:
    tdir = store.task_dir(tid)
    sigs = store.read_signals(tdir, turn)
    ask = next((s for s in sigs if s["kind"] == "ask"), None)
    fin = next((s for s in sigs if s["kind"] == "finish"), None)
    last = ev["results"][-1] if ev["results"] else None  # laatste result-event geldt
    st = store.load(tid)
    delivered = _user_records(cfg, st) > users_before
    tools = list(ev["active"].values())
    orphans = [{"pid": k["pid"], "pgid": k["pgid"], "start": k["start"], "cmd": k["cmd"]}
               for k in ev["seen"].values() if "runner2.sessiontools" not in k["cmd"] and not INFRA.search(k["cmd"])
               and alive(k["pid"], k["start"])]
    paused = (tdir / "pause").exists()
    (tdir / "pause").unlink(missing_ok=True)
    fields: dict = {"orphans": orphans, "children": [], "active_tools": [], "claude_pid": None,
                    "claude_pid_start": None, "pid": None, "pid_start": None,
                    "in_flight": [], "resume_note": None}
    if not delivered:
        fields["inbox"] = st["in_flight"] + st["inbox"]
    if last:
        fields["last_result"] = {k: last.get(k) for k in ("subtype", "is_error", "result", "num_turns",
                                                           "duration_ms", "total_cost_usd")}
        fields["last_result"]["result"] = (last.get("result") or "")[:2000]
    new_note = None
    exit_ok = last is not None and not last.get("is_error") and ev["rc"] == 0
    if ev["problem"]:
        fields.update(status="failed", failure={"kind": "config_error" if "init" in ev["problem"] else "stalled",
                                                "message": ev["problem"], "at": store.now()})
    elif ask:
        fields.update(status="waiting_for_human", question=ask["payload"])
    elif fin:
        fields.update(status="completed", finish=fin["payload"])
    elif paused:
        fields.update(status="paused")
        new_note = {"kind": "paused"}
    elif exit_ok:
        fields.update(status="waiting_for_human", question={
            "kind": "no_signal", "question": "Claude eindigde zonder runner_finish of runner_ask.",
            "why_needed": "Is het werk af, of moet Claude verder? Antwoord met r2 send.",
            "last_message": (last.get("result") or "")[:1000]})
    else:
        kind = "api_error" if last else "interrupted"
        msg = (last.get("result") if last else f"proces eindigde zonder result (exitcode {ev['rc']})")
        auth = auth_problem(str(msg or "")) or auth_problem(_stderr_tail(tdir, turn))
        rec = st.get("recovery") or {}
        if auth:
            kind = "auth_error"
            if not msg or not auth_problem(str(msg)):
                msg = f"{msg}; stderr: {_stderr_tail(tdir, turn).strip()[-300:]}"
        failure = {"kind": kind, "message": str(msg)[:500], "at": store.now(), **({"auth": auth} if auth else {})}
        if auth and not (auth == "not_logged_in"
                         and int(rec.get("auth_attempts") or 0) < int(cfg.get("auth_retry_attempts", 0))):
            # Geen blinde herhaling: de gebruiker moet opnieuw aanmelden. De vraag komt in Monitor 2 / de wachtrij.
            q = dict(auth_question(auth, str(msg)), pending_inbox=len(fields.get("inbox", st["inbox"])))
            fields.update(status="waiting_for_human", failure=failure, question=q)
            log(f"{tid}: aanmeldfout ({auth}), wacht op nieuwe aanmelding door de gebruiker")
        else:
            fields.update(status="failed", failure=failure)
            if auth:   # één herhaling via de watch, niet meteen (zie watch.recoverable_failure)
                fields["recovery"] = dict(rec, next_retry_at=time.time() + float(cfg.get("auth_retry_delay_s", 30)))
        new_note = {"kind": kind, "tools": tools} if delivered else None
    fields["resume_note"] = new_note or (None if delivered else st["resume_note"])
    if fields["status"] != "failed":
        fields["recovery"] = {}
    st.update(fields)
    log(f"{tid}: beurt {turn} klaar, status {fields['status']}")
    backup = session.backup_transcript(cfg, st)
    if fin:
        try:
            st["integration"] = gitops.dry_run_integration(st)
        except Exception as e:  # rapportage, geen reden om de afronding te blokkeren
            st["integration"] = {"error": str(e)}
        st["orphans"] = []
        if orphans:
            settle_orphans(dict(st, orphans=orphans), cfg, 0)
    t_end = time.time()
    r = last or {}
    turns = st.get("turns", []) + [{
        "turn": turn, "wall_s": round(t_end - t_start, 2), "claude_s": round(t_exit - t_spawn, 2),
        "orphan_wait_s": st["orphan_wait_s"],
        "overhead_s": round((t_end - t_start) - (t_exit - t_spawn) - st["orphan_wait_s"], 2), "ticks_s": round(ev["ticks_s"], 2),
        "model_turns": r.get("num_turns"), "results": len(ev["results"]), "events": ev["events"],
        "cost_usd": r.get("total_cost_usd")}]
    store.update(tid, **{k: st[k] for k in fields}, transcript_backup=backup, transcript_path=st["transcript_path"],
                 integration=st["integration"], warnings=st["warnings"], turns=turns)
    if fin:
        actions.record(tid, fin["payload"])
    return 0


if __name__ == "__main__":
    sys.exit(run_turn(sys.argv[1]))
