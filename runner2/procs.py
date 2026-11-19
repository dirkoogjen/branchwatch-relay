"""Procesidentiteit (pid + starttijd) en bewaking van de processen onder een Claude-beurt (W9, W10).

Stoppen gebeurt alleen na controle van de identiteit: een hergebruikte pid of een procesboom die via een hergebruikte
ouder-pid 'aangroeit' mag nooit een vreemd proces raken. De watch en de taakprocessen van Runner 2 zelf
(`protected_pids`) worden nooit als kind of wees gestopt.
"""
import json
import os
import re
import signal
import subprocess
import sys
import time

from . import config, winproc

WINDOWS = sys.platform == "win32"
UNKNOWN = winproc.UNKNOWN
_ENV = {**os.environ, "LC_ALL": "C"}
# Runner 2-infrastructuur die nooit als kind/wees van een Claude-beurt telt (opdrachtregel `python -m runner2.<x>`).
INFRA = re.compile(r"-m\s+runner2\.(watch|taskproc)\b")


def proc_start(pid: int) -> str | None:
    """Starttijd van een proces (ps lstart / FILETIME), None als het niet bestaat, UNKNOWN als het niet vast te stellen is."""
    if not pid:
        return None
    if WINDOWS:
        return winproc.proc_start(pid)
    r = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, env=_ENV)
    out = r.stdout.strip()
    return " ".join(out.split()) or None


def snapshot() -> list:
    """Alle processen: pid, ppid, pgid, start, command."""
    if WINDOWS:
        return winproc.snapshot()
    r = subprocess.run(["ps", "-ww", "-axo", "pid=,ppid=,pgid=,lstart=,command="],
                       capture_output=True, text=True, env=_ENV)
    rows = []
    for line in r.stdout.splitlines():
        t = line.split(None, 8)
        if len(t) < 9:
            continue
        try:
            rows.append({"pid": int(t[0]), "ppid": int(t[1]), "pgid": int(t[2]),
                         "start": " ".join(t[3:8]), "cmd": t[8][:200]})
        except ValueError:
            continue
    return rows


def start_key(start) -> float | None:
    """Starttijd als vergelijkbaar getal (Windows: FILETIME-tekst, POSIX: lstart); None = onbekend."""
    if not start or start == UNKNOWN:
        return None
    s = str(start)
    if s.isdigit():
        return int(s)
    try:
        return time.mktime(time.strptime(s, "%a %b %d %H:%M:%S %Y"))
    except ValueError:
        return None


def descendants(rows: list, root: int, root_start: str | None = None) -> list:
    """Afstammelingen van `root` volgens ouder-pid, met bescherming tegen hergebruikte pid's.

    Windows controleert de ouder-pid niet: is de ouder afgelopen en zijn pid hergebruikt, dan lijkt een veel ouder proces
    een kind van de nieuwe houder. Daarom telt een proces alleen als kind als het niet vóór zijn ouder startte, en (is de
    pid van de ouder inmiddels van een ander proces) als het vóór die nieuwe houder startte. `root_start` is de bekende
    starttijd van de wortel; zonder die geldt de starttijd van de levende wortel in `rows`.
    """
    by_pid = {p["pid"]: p for p in rows}
    kids: dict = {}
    for p in rows:
        kids.setdefault(p["ppid"], []).append(p)
    if root_start is None:
        root_start = (by_pid.get(root) or {}).get("start")
    out, seen, todo = [], {root}, [(root, root_start)]
    while todo:
        pid, pstart = todo.pop()
        pk = start_key(pstart)
        holder = by_pid.get(pid)
        hk = start_key(holder["start"]) if holder and holder.get("start") != pstart else None
        for c in kids.get(pid, []):
            if c["pid"] in seen:
                continue
            ck = start_key(c.get("start"))
            if ck is not None and pk is not None and ck < pk:
                continue   # ouder dan zijn 'ouder': de ouder-pid is hergebruikt
            if ck is not None and hk is not None and ck >= hk:
                continue   # kind van de nieuwe houder van een hergebruikte pid, niet van ons proces
            seen.add(c["pid"])
            out.append(c)
            todo.append((c["pid"], c.get("start")))
    return out


def alive(pid: int, start: str | None) -> bool:
    """Zelfde instantie? Een bestaand maar ontoegankelijk proces (UNKNOWN) telt als levend, nooit als 'weg'."""
    if not pid or not start:
        return False
    now = proc_start(pid)
    return now == start or now == UNKNOWN


def _watch_identity() -> tuple:
    try:
        w = json.loads((config.r2_home() / "watch.json").read_text())
        return int(w["pid"]), w.get("start")
    except (OSError, ValueError, KeyError, TypeError):
        return None, None


def protected_pids(rows: list | None = None) -> set:
    """Pid's die Runner 2 nooit als kind of wees stopt: dit proces, de watch (watch.json) en elk proces met
    `-m runner2.watch` of `-m runner2.taskproc` in de opdrachtregel."""
    out = {os.getpid()}
    wpid, wstart = _watch_identity()
    if wpid:
        row = next((r for r in rows if r["pid"] == wpid), None) if rows else None
        now = row["start"] if row else proc_start(wpid)
        if not wstart or now in (wstart, UNKNOWN):
            out.add(wpid)   # zonder bekende starttijd: liever één proces te veel sparen dan de watch stoppen
    for r in rows or []:
        if INFRA.search(r.get("cmd") or ""):
            out.add(r["pid"])
    return out


def _win_stop_tree(pid: int, start: str | None) -> None:
    rows = winproc.snapshot()
    root = next((r for r in rows if r["pid"] == pid), None)
    if start is None and root is None:
        return   # identiteit niet vast te stellen: niets stoppen
    ref = start or root["start"]
    victims = ([root] if root and root["start"] == ref else []) + descendants(rows, pid, ref)
    keep = protected_pids(rows)
    for v in victims:
        if v["pid"] not in keep:
            winproc.kill_exact(v["pid"], v["start"])


def stop_group(pgid: int, grace: float = 3.0, start: str | None = None) -> None:
    """TERM naar de procesgroep, na een korte wachttijd KILL. Windows: de gecontroleerde procesboom (zie descendants),
    elk proces apart op pid + starttijd; `start` is de starttijd van de wortel."""
    if WINDOWS:
        _win_stop_tree(pgid, start)
        return
    if pgid <= 1 or pgid == os.getpgrp():
        return
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
        except (ProcessLookupError, PermissionError):
            return
        end = time.time() + grace
        while time.time() < end:
            time.sleep(0.1)
            try:
                os.killpg(pgid, 0)
            except (ProcessLookupError, PermissionError):  # weg, of alleen nog een zombie
                return


def terminate(pid: int, start: str | None = None) -> None:
    """Beëindigt één proces direct (POSIX: SIGTERM, Windows: TerminateProcess), alleen als het nog hetzelfde is."""
    if WINDOWS:
        winproc.terminate(pid, start)
        return
    if start and not alive(pid, start):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass


def force_kill(pid: int, start: str | None = None) -> None:
    """Dwingend stoppen van één proces (POSIX: SIGKILL, Windows: TerminateProcess), alleen als het nog hetzelfde is."""
    if WINDOWS:
        winproc.kill_exact(pid, start)
        return
    if start and not alive(pid, start):
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
