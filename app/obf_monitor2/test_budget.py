"""Adaptief testtijd-budget voor de gerichte tests van de integrator, in plaats van de starre 900 seconden.

Waarom. Eén vaste grens van 900 s past niet bij elke integratie: twee kleine beleidstests duren seconden, een aanvraag met
achttien eigen testbestanden op een drukke Mac kan langer nodig hebben (15-10: 427 tests in 387 s; onder de Background-rem
liep alles over de 900 s, zie ``docs/audits/mac_integrator_herstel_2026-10-15.md``). Dit bestand schat per integratie een
budget uit de eigen meetgeschiedenis van de integrator en bewaakt de testrun zelf.

Regels (deterministisch, zelfde invoer = zelfde budget)
-------------------------------------------------------
* Per testbestand de hoogste van de laatste ``RECENT`` gemeten duren op **deze** uitvoeromgeving (host, platform,
  ``taskpolicy``-voorvoegsel, testomhulling). Metingen van een andere omgeving tellen niet.
* Een bestand zonder bruikbare meting: ``UNKNOWN_FILE_S`` (conservatief). Opstart/verzamelen: hoogste recente overhead, anders
  ``DEFAULT_OVERHEAD_S``. Is precies deze set eerder gemeten, dan nooit minder dan de hoogste recente duur van die set.
* Belasting: is de machine nu drukker (``loadavg / cpu``) dan tijdens de meting, dan schaalt de schatting mee (hooguit
  ``MAX_LOAD_FACTOR``); is hij rustiger, dan wordt er nooit iets afgetrokken.
* Is een eerdere run met een deelverzameling van deze tests op zijn budget gestrand, dan minstens ``ESCALATE`` × dat budget.
* Budget = schatting × ``MARGIN_FACTOR`` + ``MARGIN_S``, begrensd op ``[MIN_BUDGET_S, MAX_BUDGET_S]``: nooit strenger dan de
  oude 900 s, nooit boven de harde grens van 3600 s (een uur).
* Elke fout in de schatting (onleesbare historie, onverwachte waarde) geeft het terugvalbudget ``FALLBACK_BUDGET_S`` (de oude
  900 s) met de reden erbij; ``OBF_INTEGRATION_TEST_BUDGET=fixed`` zet het adaptieve deel helemaal uit (zelfde terugvalpad).

Bewaking tijdens de run (``run_watched``)
-----------------------------------------
Uitvoer wordt live gelezen. Afbreken (het testproces en al zijn nakomelingen) bij overschrijding van het budget of als er
``STALL_S`` lang geen enkele uitvoer meer komt (stilgevallen test; ruim boven de 90 s per test uit ``pytest.ini``). Een
afgebroken run is altijd mislukt, ook als het proces daarna nog 0 teruggeeft. Tijd telt op de monotone klok (een slapende Mac
verbruikt geen budget). Bewust geen eigen sessie of procesgroep: dan ruimt launchd bij een herstart van de integrator de tests nog
steeds mee op, zoals voorheen.

Wat het budget nooit doet: tests weglaten, de testset kiezen of iets uit de doelcommit geloven. De historie staat in de map
met toestemmingen van de controller; een vervalste meting kan het budget alleen verlagen tot de ondergrens (de run faalt dan
dicht) of verhogen tot de harde grens.
"""

from __future__ import annotations

import calendar
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Callable

SCHEMA = 1
MODE_ENV = "OBF_INTEGRATION_TEST_BUDGET"
HISTORY_FILE = "test-durations.jsonl"
PROGRESS_FILE = "test-progress.json"

MIN_BUDGET_S = 900             # nooit strenger dan de oude vaste grens
MAX_BUDGET_S = 3600            # harde bovengrens per integratie, over alle testgroepen samen
FALLBACK_BUDGET_S = 900        # terugvalpad bij elke twijfel
UNKNOWN_FILE_S = 300.0         # een testbestand zonder meting
DEFAULT_OVERHEAD_S = 60.0      # opstart en verzamelen zonder meting
MARGIN_FACTOR = 1.5
MARGIN_S = 120.0
MAX_LOAD_FACTOR = 2.0
ESCALATE = 1.5
RECENT = 5
STALL_S = 300.0                # zo lang geen uitvoer = stilgevallen
KILL_GRACE_S = 10.0
PROGRESS_EVERY_S = 5.0
HISTORY_KEEP = 500
HISTORY_MAX_BYTES = 4 * 1024 * 1024
JUNIT_MAX_BYTES = 8 * 1024 * 1024
TAIL_BYTES = 64 * 1024

_PERCENT = re.compile(rb"\[\s*(\d{1,3})%\]")


# ----------------------------------------------------------------------
# Omgeving en historie
# ----------------------------------------------------------------------


def environment(prefix: list[str] | None = None, wrapper: list[str] | None = None) -> dict:
    """Vingerafdruk van de uitvoeromgeving: alleen metingen met dezelfde vingerafdruk tellen mee."""
    return {"host": socket.gethostname(), "platform": sys.platform, "prefix": " ".join(prefix or []),
            "wrapper": os.path.basename((wrapper or [""])[0])}


def load_ratio() -> float | None:
    try:
        return round(os.getloadavg()[0] / max(1, os.cpu_count() or 1), 3)
    except (OSError, AttributeError):
        return None


def _load_factor(ratio) -> float:
    if not isinstance(ratio, (int, float)) or isinstance(ratio, bool) or ratio != ratio or ratio < 0:
        return 1.0
    return min(MAX_LOAD_FACTOR, max(1.0, float(ratio)))


def _num(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value != value or not 0 <= value <= 7 * 24 * 3600:
        return None
    return float(value)


def read_history(directory: Path | str | None) -> tuple[list[dict], str]:
    """Geldige records (oud -> nieuw) en een eventueel probleem. Kapotte regels worden overgeslagen, nooit geraden."""
    if directory is None:
        return [], "geen historie ingesteld"
    path = Path(directory) / HISTORY_FILE
    try:
        if path.is_symlink() or (path.exists() and not path.is_file()):
            return [], "het historiebestand is geen gewoon bestand"
        if not path.exists():
            return [], "nog geen historie"
        if path.stat().st_size > HISTORY_MAX_BYTES:
            return [], "het historiebestand is te groot"
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return [], "het historiebestand is niet te lezen"
    out, bad = [], 0
    for line in lines[-HISTORY_KEEP:]:
        try:
            rec = json.loads(line)
        except ValueError:
            bad += 1
            continue
        if not isinstance(rec, dict) or rec.get("schema") != SCHEMA or not isinstance(rec.get("repo"), str) \
                or not isinstance(rec.get("tests"), list) or not isinstance(rec.get("env"), dict) \
                or rec.get("outcome") not in ("passed", "failed", "timeout", "stalled", "error"):
            bad += 1
            continue
        out.append(rec)
    return out, (f"{bad} onleesbare regel(s) overgeslagen" if bad else "")


def append_history(directory: Path | str, record: dict) -> None:
    """Voegt één regel toe (0600, fsync); houdt het bestand begrensd door bij overschrijding de oudste regels te laten vallen."""
    path = Path(directory) / HISTORY_FILE
    line = json.dumps({"schema": SCHEMA, **record}, ensure_ascii=False, sort_keys=True) + "\n"
    if path.is_symlink():
        raise OSError("historiebestand is een symlink")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as handle:
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())
    if path.stat().st_size > HISTORY_MAX_BYTES // 2:
        keep = path.read_text(encoding="utf-8", errors="replace").splitlines()[-HISTORY_KEEP:]
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write("\n".join(keep) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)


# ----------------------------------------------------------------------
# De schatting
# ----------------------------------------------------------------------


def estimate(tests: list[str], history: list[dict], *, repo: str, runner: str, env: dict, ratio: float | None,
             mode: str = "adaptive", history_problem: str = "") -> dict:
    """Het budget voor één testgroep. Raised niet: elke fout geeft het terugvalbudget met reden."""
    base = {"runner": runner, "tests": list(tests), "min_s": MIN_BUDGET_S, "max_s": MAX_BUDGET_S, "load_ratio": ratio,
            "history_problem": history_problem or None}
    if mode == "fixed":
        return {**base, "mode": "fixed", "budget_s": FALLBACK_BUDGET_S, "estimate_s": None,
                "reason": f"{MODE_ENV}=fixed: vaste {FALLBACK_BUDGET_S} s"}
    try:
        return {**base, **_estimate(tests, history, repo=repo, runner=runner, env=env, ratio=ratio)}
    except Exception as exc:  # noqa: BLE001 -- fail-closed naar het bekende, begrensde terugvalbudget
        return {**base, "mode": "fallback", "budget_s": FALLBACK_BUDGET_S, "estimate_s": None,
                "reason": f"schatting mislukt ({exc.__class__.__name__}); terugval op {FALLBACK_BUDGET_S} s"}


def _estimate(tests: list[str], history: list[dict], *, repo: str, runner: str, env: dict, ratio: float | None) -> dict:
    wanted = set(tests)
    same = [r for r in history if r.get("repo") == repo and r.get("runner") == runner and r.get("env") == env]
    now_factor = _load_factor(ratio)
    per_file: dict[str, list[float]] = {}
    overheads: list[float] = []
    set_runs: list[float] = []
    timeouts: list[float] = []
    for rec in reversed(same):   # nieuw -> oud
        factor = max(1.0, now_factor / _load_factor(rec.get("load_ratio")))
        rec_tests = rec.get("tests") or []
        if rec.get("outcome") in ("timeout",) and set(rec_tests) <= wanted:
            budget = _num(rec.get("budget_s"))
            if budget is not None:
                timeouts.append(budget)
            continue
        if rec.get("outcome") not in ("passed", "failed"):
            continue
        files = rec.get("files") if isinstance(rec.get("files"), dict) else {}
        for name, seconds in files.items():
            value = _num(seconds)
            if name in wanted and value is not None and len(per_file.setdefault(name, [])) < RECENT:
                per_file[name].append(value * factor)
        overhead = _num(rec.get("overhead_s"))
        if overhead is not None and len(overheads) < RECENT:
            overheads.append(overhead * factor)
        elapsed = _num(rec.get("elapsed_s"))
        if elapsed is not None and set(rec_tests) == wanted and len(set_runs) < RECENT:
            set_runs.append(elapsed * factor)
    known = {name: max(values) for name, values in per_file.items() if values}
    unknown = sorted(wanted - set(known))
    overhead = max(overheads) if overheads else DEFAULT_OVERHEAD_S
    raw = sum(known.values()) + UNKNOWN_FILE_S * len(unknown) + overhead
    if set_runs:
        raw = max(raw, max(set_runs))
    budget = raw * MARGIN_FACTOR + MARGIN_S
    reasons = [f"{len(known)} bestand(en) gemeten, {len(unknown)} zonder meting ({UNKNOWN_FILE_S:.0f} s elk)"]
    if timeouts:
        budget = max(budget, max(timeouts) * ESCALATE)
        reasons.append(f"eerder gestrand op {max(timeouts):.0f} s; opgehoogd")
    if now_factor > 1.0:
        reasons.append(f"belasting x{now_factor:.2f}")
    clamped = int(min(MAX_BUDGET_S, max(MIN_BUDGET_S, round(budget))))
    if clamped == MAX_BUDGET_S and budget > MAX_BUDGET_S:
        reasons.append(f"begrensd op de harde grens van {MAX_BUDGET_S} s")
    elif clamped == MIN_BUDGET_S:
        reasons.append(f"ondergrens {MIN_BUDGET_S} s")
    return {"mode": "adaptive", "estimate_s": round(raw, 1), "budget_s": clamped, "known_files": len(known),
            "unknown_files": unknown, "reason": "; ".join(reasons)}


def mode(env: dict | None = None) -> str:
    raw = ((env if env is not None else os.environ).get(MODE_ENV) or "adaptive").strip().lower()
    return "fixed" if raw == "fixed" else ("adaptive" if raw in ("", "adaptive") else "fixed")


# ----------------------------------------------------------------------
# Per-bestand-duren uit het JUnit-rapport van pytest
# ----------------------------------------------------------------------


def junit_file_durations(path: Path | str, tests: list[str]) -> tuple[dict[str, float], float] | None:
    """``({testbestand: seconden}, som)`` uit een JUnit-XML-rapport, of ``None`` als het er niet (bruikbaar) is."""
    import xml.etree.ElementTree as ET

    try:
        path = Path(path)
        if not path.is_file() or path.is_symlink() or path.stat().st_size > JUNIT_MAX_BYTES:
            return None
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError, ValueError):
        return None
    modules = {t[:-3].replace("/", "."): t for t in tests if t.endswith(".py")}
    out: dict[str, float] = {}
    total = 0.0
    for case in root.iter("testcase"):
        seconds = _num(_float(case.get("time")))
        if seconds is None:
            continue
        name = case.get("file")
        if name not in modules.values():
            name = None
            classname = case.get("classname") or ""
            for module, test in modules.items():
                if classname == module or classname.startswith(module + "."):
                    name = test
                    break
        if name is None:
            continue
        out[name] = out.get(name, 0.0) + seconds
        total += seconds
    return ({k: round(v, 3) for k, v in out.items()}, total) if out else None


def _float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ----------------------------------------------------------------------
# Bewaakte uitvoering
# ----------------------------------------------------------------------


def run_watched(cmd: list[str], *, cwd: str, env: dict, budget_s: float, stall_s: float | None = None,
                on_progress: Callable[[dict], None] | None = None, clock: Callable[[], float] = time.monotonic,
                poll_s: float = 0.2) -> dict:
    """Start ``cmd`` en breekt het en al zijn nakomelingen af bij budgetoverschrijding of stilval.

    Geeft ``{outcome: done|timeout|stalled|error, returncode, elapsed_s, tail, percent, error?}``. ``timeout`` en ``stalled``
    zijn altijd mislukt; de aanroeper mag ze nooit als geslaagd tellen."""
    stall_s = STALL_S if stall_s is None else stall_s
    start = clock()
    try:
        proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as exc:
        return {"outcome": "error", "returncode": None, "elapsed_s": 0.0, "tail": [], "percent": None,
                "error": f"de tests konden niet starten ({exc.__class__.__name__})"}
    buf: deque[bytes] = deque()
    state = {"size": 0, "last": clock(), "percent": None, "bytes": 0}
    lock = threading.Lock()

    def reader() -> None:
        while True:
            try:
                chunk = os.read(proc.stdout.fileno(), 8192)
            except OSError:
                return
            if not chunk:
                return
            with lock:
                buf.append(chunk)
                state["size"] += len(chunk)
                state["bytes"] += len(chunk)
                state["last"] = clock()
                hits = _PERCENT.findall(chunk)
                if hits:
                    state["percent"] = min(100, int(hits[-1]))
                while state["size"] > TAIL_BYTES and len(buf) > 1:
                    state["size"] -= len(buf.popleft())

    thread = threading.Thread(target=reader, name="testuitvoer", daemon=True)
    thread.start()
    outcome = "done"
    last_report = -PROGRESS_EVERY_S
    while True:
        try:
            proc.wait(timeout=poll_s)
            break
        except subprocess.TimeoutExpired:
            pass
        now = clock()
        with lock:
            quiet = now - state["last"]
            percent = state["percent"]
        if now - start >= budget_s:
            outcome = "timeout"
            break
        if quiet >= stall_s:
            outcome = "stalled"
            break
        if on_progress is not None and now - last_report >= PROGRESS_EVERY_S:
            last_report = now
            _safe(on_progress, {"elapsed_s": round(now - start, 1), "budget_s": budget_s, "quiet_s": round(quiet, 1),
                                "percent": percent, "state": "running"})
    if outcome != "done":
        _kill_tree(proc)
    thread.join(timeout=5)
    elapsed = clock() - start
    if outcome == "done" and elapsed > budget_s:
        outcome = "timeout"   # klaar, maar te laat: nooit stilzwijgend geslaagd
    with lock:
        text = b"".join(buf).decode("utf-8", "replace")
        percent = state["percent"]
    try:
        proc.stdout.close()
    except OSError:
        pass
    return {"outcome": outcome, "returncode": proc.returncode if outcome == "done" else None, "elapsed_s": round(elapsed, 2),
            "tail": text.strip().splitlines()[-8:], "percent": percent}


def descendants(pid: int) -> list[int]:
    """Alle nakomelingen van ``pid`` volgens ``ps`` (kinderen eerst gevonden, dan kleinkinderen, ...). Leeg bij twijfel."""
    try:
        out = subprocess.run(["ps", "-A", "-o", "pid=", "-o", "ppid="], capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    children: dict[int, list[int]] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            children.setdefault(int(parts[1]), []).append(int(parts[0]))
    found, todo = [], [pid]
    while todo:
        for child in children.get(todo.pop(), []):
            if child not in found and child != pid:
                found.append(child)
                todo.append(child)
    return found


def _kill_tree(proc: subprocess.Popen) -> None:
    """TERM, wachten, dan KILL; voor het proces én de nakomelingen die vóór het afbreken bestonden (daarna zijn ze wees en
    niet meer terug te vinden). Pids van ons eigen proces worden nooit geraakt."""
    family = [p for p in descendants(proc.pid) if p != os.getpid()]
    for sig, wait in ((signal.SIGTERM, KILL_GRACE_S), (signal.SIGKILL, 5.0)):
        for pid in family:
            try:
                os.kill(pid, sig)
            except OSError:
                pass
        try:
            proc.send_signal(sig)
        except OSError:
            pass
        try:
            proc.wait(timeout=wait)
            if sig == signal.SIGTERM:
                continue   # het hoofdproces is weg; nakomelingen die TERM negeerden krijgen alsnog KILL
            return
        except subprocess.TimeoutExpired:
            continue


def _safe(callback: Callable[[dict], None], data: dict) -> None:
    try:
        callback(data)
    except Exception:  # noqa: BLE001 -- voortgang tonen mag de testrun nooit breken
        pass


def write_progress(directory: Path | str | None, data: dict) -> None:
    """Momentopname van de lopende testrun (0644, atomair) voor het statusoverzicht. Fouten worden genegeerd."""
    if directory is None:
        return
    try:
        path = Path(directory) / PROGRESS_FILE
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"schema": SCHEMA, **data}, ensure_ascii=False, sort_keys=True))
        os.replace(tmp, path)
    except OSError:
        pass


def read_progress(directory: Path | str | None) -> dict | None:
    if directory is None:
        return None
    try:
        data = json.loads((Path(directory) / PROGRESS_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) and data.get("schema") == SCHEMA else None


# ----------------------------------------------------------------------
# Voor het statusoverzicht (alleen tonen)
# ----------------------------------------------------------------------

#: Velden van de lopende run en van een afgeronde testgroep die naar het statusoverzicht gaan; de rest valt weg.
CURRENT_FIELDS = ("state", "runner", "grant", "elapsed_s", "budget_s", "estimate_s", "percent", "quiet_s", "updated_at", "age_s")
LAST_FIELDS = ("runner", "grant", "outcome", "elapsed_s", "budget_s", "estimate_s", "mode", "at", "tests_count")
CURRENT_STATES = ("running", "interrupted", "passed", "failed", "timeout", "stalled", "error")
LAST_KEEP = 4
_TEXT = re.compile(r"^[A-Za-z0-9._:+-]{1,40}$")


def _iso_epoch(text) -> float | None:
    """``2026-10-15T16:36:00Z`` (zoals ``integration._iso``) naar epoch, of ``None``."""
    try:
        return float(calendar.timegm(time.strptime(str(text), "%Y-%m-%dT%H:%M:%SZ")))
    except (ValueError, OverflowError):
        return None


def _clean(source: dict, fields: tuple[str, ...]) -> dict:
    """Alleen bekende velden; getallen moeten getallen zijn, tekst kort en zonder vreemde tekens. Twijfel = ``None``."""
    out = {}
    for key in fields:
        value = source.get(key)
        if key in ("elapsed_s", "budget_s", "estimate_s", "quiet_s", "age_s"):
            value = _num(value)
        elif key in ("percent", "tests_count"):
            value = int(value) if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 10_000 else None
        else:
            value = value if isinstance(value, str) and _TEXT.match(value) else None
        out[key] = value
    return out


def shape_summary(data) -> dict | None:
    """De vaste vorm van ``summary`` (ook voor een statusbestand dat van een andere machine komt)."""
    if not isinstance(data, dict):
        return None
    current = data.get("current")
    current = _clean(current, CURRENT_FIELDS) if isinstance(current, dict) else None
    if current is not None and current["state"] not in CURRENT_STATES:
        current = None
    last = data.get("last") if isinstance(data.get("last"), list) else []
    last = [_clean(item, LAST_FIELDS) for item in last[:LAST_KEEP] if isinstance(item, dict)]
    return {"current": current, "last": last, "max_s": MAX_BUDGET_S, "stall_s": STALL_S,
            "history_problem": data["history_problem"][:200] if isinstance(data.get("history_problem"), str) else None}


def summary(directory: Path | str | None, *, active: bool, now: float | None = None) -> dict | None:
    """Wat het statusoverzicht over de gerichte tests toont; ``None`` zonder map. Raised niet.

    ``current``: de laatste voortgang (verstreken / budget / schatting / percentage / stil sinds, ``age_s`` = hoe oud die stand is).
    Staat daar nog *running* terwijl de controller geen integratie uitvoert (``active`` onwaar), dan is die run nooit afgemaakt
    (bv. een herstart) en heet hij ``interrupted``. ``last``: de testgroepen van de laatst afgeronde run uit de historie, met
    geschat en werkelijk. Niets hiervan beslist over een integratie; het is alleen om te tonen."""
    if directory is None:
        return None
    try:
        now = time.time() if now is None else now
        current = read_progress(directory)
        if current is not None:
            current = dict(current)
            stamp = _iso_epoch(current.get("updated_at"))
            current["age_s"] = round(max(0.0, now - stamp), 1) if stamp is not None else None
            if current.get("state") == "running" and not active:
                current["state"] = "interrupted"
        history, problem = read_history(directory)
        last: list[dict] = []
        if history:
            grant = history[-1].get("grant")
            for rec in reversed(history):
                if rec.get("grant") != grant or len(last) >= LAST_KEEP:
                    break
                last.append({**rec, "tests_count": len(rec.get("tests") or [])})
            last.reverse()
        problem = None if problem in ("", "nog geen historie") else problem
        return shape_summary({"current": current, "last": last, "history_problem": problem})
    except Exception:  # noqa: BLE001 -- tonen mag nooit iets breken
        return None
