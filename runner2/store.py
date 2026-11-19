"""Taakstatus (state.json, atomair), signalen van de sessietools en de machinebrede taaklock."""
import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from . import config
from .filelock import lock_exclusive, replace
from .procs import alive, proc_start

STATUSES = ("created", "running", "waiting_for_human", "paused", "completed", "failed", "stopped")
TERMINAL = ("completed", "failed", "stopped")
# Een gestopte taak is terminaal: geen enkel taakproces mag de status daarna nog wijzigen.
STOPPED_LOCKED = ("status", "failure", "question", "finish", "inbox", "turn")


def now() -> str:
    # strftime("%z") geeft op Windows de zonenaam i.p.v. "+0200"; datetime formatteert zelf en is overal gelijk.
    return datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def task_dir(task_id: str) -> Path:
    return config.tasks_dir() / task_id


def fsync_dir(path: Path) -> None:
    """Maak een rename/verwijdering in deze map duurzaam. Een I/O-fout wordt NIET ingeslikt (fail-closed): de aanroeper mag dan geen
    duurzame verwerking claimen en moet zijn wachtbestanden behouden. Alleen op platforms zonder map-fsync (Windows) is dit een no-op."""
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}.{threading.get_ident()}.{time.time_ns()}")
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    replace(tmp, path)
    fsync_dir(path.parent)


def new_task(repo: str, prompt: str, worktree: str, branch: str, base: dict, model: str, tid: str | None = None) -> dict:
    tid = tid or "R2-" + uuid.uuid4().hex[:12]
    state = {
        "task_id": tid, "repo": repo, "prompt": prompt, "worktree": worktree, "branch": branch,
        "base": base, "model": model, "session_id": str(uuid.uuid4()), "status": "created",
        "turn": 0, "inbox": [prompt], "in_flight": [], "pid": None, "pid_start": None,
        "claude_pid": None, "claude_pid_start": None, "claude_version": None, "children": [],
        "orphans": [], "unit": None, "active_tools": [], "resume_note": None, "question": None, "finish": None,
        "last_result": None, "failure": None, "transcript_path": None, "transcript_backup": None,
        "integration": None, "warnings": [], "created": now(), "updated": now(),
        "turn_started": None, "turns": [], "orphan_wait_s": 0, "recovery": {},
    }
    save(state)
    return state


def save(state: dict) -> None:
    state["updated"] = now()
    atomic_write(task_dir(state["task_id"]) / "state.json", json.dumps(state, indent=1))


def load(task_id: str) -> dict:
    return json.loads((task_dir(task_id) / "state.json").read_text())


@contextmanager
def _flock(task_id: str):
    d = task_dir(task_id)
    d.mkdir(parents=True, exist_ok=True)
    with open(d / ".state.lock", "w") as f:
        lock_exclusive(f)
        yield


def update(task_id: str, **fields) -> dict:
    """Lees-wijzig-schrijf onder een bestandslock, zodat CLI en taakproces elkaar niet overschrijven."""
    with _flock(task_id):
        state = load(task_id)
        if state["status"] == "stopped":
            fields = {k: v for k, v in fields.items() if k not in STOPPED_LOCKED}
        state.update(fields)
        save(state)
        return state


def all_tasks() -> list:
    out = []
    for p in sorted(config.tasks_dir().glob("R2-*/state.json")) if config.tasks_dir().exists() else []:
        try:
            out.append(json.loads(p.read_text()))
        except (OSError, ValueError):
            pass
    return sorted(out, key=lambda s: s["created"])


# --- signalen van de sessietools (runner_ask / runner_finish), per beurt ---

def signals_path(task_dir_: Path) -> Path:
    return Path(task_dir_) / "signals.jsonl"


def read_signals(task_dir_: Path, turn: int) -> list:
    p = signals_path(task_dir_)
    if not p.exists():
        return []
    out = []
    for line in p.read_text().splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("turn") == turn:
            out.append(rec)
    return out


def record_signal(task_dir_: Path, turn: int, kind: str, payload: dict) -> str | None:
    """Legt een signaal vast; weigert een tweede signaal in dezelfde beurt (geeft dan de reden terug)."""
    existing = read_signals(task_dir_, turn)
    if existing:
        return f"In deze beurt is al {existing[0]['kind']} aangeroepen; een tweede signaal wordt geweigerd."
    with open(signals_path(task_dir_), "a") as f:
        f.write(json.dumps({"turn": turn, "kind": kind, "at": now(), "payload": payload}) + "\n")
        f.flush()
        os.fsync(f.fileno())
    return None


# --- machinebrede lock: maximaal één actieve programmeertaak, identiteit = pid + starttijd ---

class Busy(Exception):
    pass


class MachineLock:
    def __init__(self, task_id: str):
        self.task_id = task_id
        self.path = config.r2_home() / "machine.lock"

    def _holder(self):
        """Houder van het slot als die nog leeft (pid + starttijd); None = vrij of stale.

        Een houder die bestaat maar niet te controleren is (Windows: proces met hogere rechten) geldt als levend."""
        try:
            h = json.loads(self.path.read_text())
        except (OSError, ValueError):
            return None
        return h if isinstance(h, dict) and alive(h.get("pid") or 0, h.get("start")) else None

    @contextmanager
    def _guard(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path.with_suffix(".guard"), "w") as g:
            lock_exclusive(g)  # tegen gelijktijdig overnemen of opruimen van een stale lock
            yield

    def clear_stale(self) -> bool:
        """Verwijdert het slot alleen als de houder aantoonbaar weg is, onder dezelfde guard als acquire (anders kan
        een opruimer het slot wissen dat een nieuw taakproces net heeft gezet)."""
        with self._guard():
            if not self.path.exists() or self._holder():
                return False
            self.path.unlink(missing_ok=True)
            return True

    def acquire(self) -> None:
        with self._guard():
            h = self._holder()
            if h:
                raise Busy(f"taak {h['task_id']} draait al (pid {h['pid']}); één actieve taak per machine")
            atomic_write(self.path, json.dumps(
                {"task_id": self.task_id, "pid": os.getpid(), "start": proc_start(os.getpid())}))

    def release(self) -> None:
        h = None
        try:
            h = json.loads(self.path.read_text())
        except (OSError, ValueError):
            pass
        if h and h.get("pid") == os.getpid():
            self.path.unlink(missing_ok=True)
