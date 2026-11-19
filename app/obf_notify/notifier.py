"""De notifier: observeert items, houdt het geheugen bij en verstuurt wat nog niet gemeld is.

Alle beslissingen over "is een mens nodig" komen uit :mod:`detect` (dus uit de
controller zelf). Deze module bepaalt alleen *wanneer* er verstuurd wordt: eenmalig per
menselijke grens (deduplicatie), niet bij UIT, met begrensde retry, en zonder ooit een
uitzondering door te laten naar de aanroeper. Ze schrijft nooit controller- of projectstatus.
"""

from __future__ import annotations

import fcntl
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from .detect import ACTION_REQUIRED, COMPLETED, FAILED, Item, parse_iso
from .ledger import ABANDONED, OBSOLETE, PENDING, SENT, SUPPRESSED, Ledger, fingerprint
from .message import build_message, redact
from .settings import NotificationSwitch, NotifyConfig, atomic_write_json, notify_home, now_iso, parse_quiet_hours, read_json
from .transport import Transport, TransportError

BACKOFF_SECONDS = (60, 120, 300, 900, 1800, 3600)
MAX_ATTEMPTS = 8
MAX_SENDS_PER_TICK = 5
#: Een verlaten melding wordt na een procesherstart eenmalig opnieuw geprobeerd, maar
#: niet vaker dan dit -- een crashende service mag geen verzendstorm worden.
REOPEN_AFTER_SECONDS = 3600
STATUS_FILE = "status.json"
HEARTBEAT_STALE_FACTOR = 4

#: Tweede veiligheidslaag onder de bron (``obf_mcp/human_boundary.py``): een menselijke
#: grens wordt pas gemeld als hij ononderbroken minstens zo lang én bij minstens zoveel
#: waarnemingen open stond. Een vloer onder ``debounce_seconds``: ook een configuratie
#: van 0 of een programmeerfout in een bron maakt van een flapper nooit een mail.
STABLE_MIN_SECONDS = 15.0
STABLE_MIN_OBSERVATIONS = 2
#: Zo lang moet een grens aantoonbaar weg zijn voor hij als afgehandeld telt. Komt
#: dezelfde grens daarbinnen terug, dan is het dezelfde open beslissing: geen tweede mail.
RESOLVE_AFTER_SECONDS = 120.0


def _log(message: str) -> None:
    sys.stderr.write(f"[notify] {time.strftime('%Y-%m-%d %H:%M:%S')} {message}\n")
    sys.stderr.flush()


def in_quiet_hours(config: NotifyConfig, now: float) -> bool:
    window = parse_quiet_hours(config.quiet_hours)
    if window is None:
        return False
    local = datetime.fromtimestamp(now)
    minute = local.hour * 60 + local.minute
    start, end = window
    return (start <= minute < end) if start < end else (minute >= start or minute < end)


class Notifier:
    def __init__(self, *, switch: NotificationSwitch, config_loader: Callable[[], NotifyConfig],
                 transport_factory: Callable[[NotifyConfig], Transport],
                 collectors: list[Callable[[], list[Item]]], home: Path | None = None,
                 clock: Callable[[], float] = time.time, log: Callable[[str], None] = _log):
        self.home = home or notify_home()
        self.switch = switch
        self.config_loader = config_loader
        self.transport_factory = transport_factory
        self.collectors = collectors
        self.clock = clock
        self.log = log
        self.ledger = Ledger(self.home / "ledger.json")
        self._last_error: dict | None = None
        self._stop = threading.Event()
        self._reopen_abandoned()

    # --------------------------------------------------------------- opstart

    def _reopen_abandoned(self) -> None:
        now = self.clock()
        for note in self.ledger.notifications.values():
            if note.get("state") == ABANDONED and now - float(note.get("last_attempt_at") or 0) >= REOPEN_AFTER_SECONDS:
                note.update(state=PENDING, attempts=0, next_attempt_at=0)
        self.ledger.save(now)

    # ------------------------------------------------------------------ tick

    def collect(self) -> dict[str, Item]:
        items: dict[str, Item] = {}
        for collector in self.collectors:
            try:
                for item in collector() or []:
                    items[item.key] = item
            except Exception as exc:  # noqa: BLE001 -- een bron die faalt mag de rest niet raken
                self.log(f"bron overgeslagen: {redact(f'{type(exc).__name__}: {exc}', 120)}")
        return items

    def tick(self) -> dict:
        """Eén observatie- en verzendronde. Gooit nooit."""
        try:
            return self._tick()
        except Exception as exc:  # noqa: BLE001
            self.log(f"tick mislukt: {redact(f'{type(exc).__name__}: {exc}', 160)}")
            return {"error": True}

    def _tick(self) -> dict:
        now = self.clock()
        config = self.config_loader()
        switch = self.switch.read()
        items = self.collect()
        self._observe(items, config, switch, now)
        sent = self._deliver(items, config, switch, now) if switch.enabled else 0
        self.ledger.save(now)
        self._write_status(config, switch, now)
        return {"items": len(items), "sent": sent, "pending": len(self.ledger.pending())}

    # ----------------------------------------------------------- observeren

    def _stale(self, item: Item, config: NotifyConfig, now: float) -> bool:
        moment = parse_iso(item.activity_at)
        return moment is not None and now - moment > config.stale_after_hours * 3600

    def _observe(self, items: dict[str, Item], config: NotifyConfig, switch, now: float) -> None:
        notes = self.ledger.notifications
        for key, item in items.items():
            entry = self.ledger.items.setdefault(key, {"episode": 0, "open": None, "first_seen": 0})
            entry["seen_at"] = now
            if item.human:
                boundary = item.boundary_id or "|".join((item.status, item.human_tool, item.step))
                if entry.get("open") != boundary:
                    # Een andere grens dan de open (of de eerste): een nieuwe beslissing.
                    entry.update(episode=int(entry.get("episode") or 0) + 1, open=boundary, first_seen=now)
                if entry.get("human_since") is None:
                    entry.update(human_since=now, human_seen=0)
                entry["human_seen"] = int(entry.get("human_seen") or 0) + 1
                entry["clear_since"] = None
                fp = fingerprint(key, ACTION_REQUIRED, boundary, entry["episode"])
                note = notes.get(fp)
                if note is None and not self._stale(item, config, now):
                    notes[fp] = {"state": PENDING, "kind": ACTION_REQUIRED, "key": key, "boundary": boundary,
                                 "episode": entry["episode"], "created_at": now, "attempts": 0,
                                 "next_attempt_at": 0, "eligible_at": now + self._stable_seconds(config)}
                elif note is not None and note.get("state") == OBSOLETE and not self._stale(item, config, now):
                    # Dezelfde grens kwam binnen RESOLVE_AFTER_SECONDS terug en is nog nooit
                    # gemeld: opnieuw in de wachtrij, weer pas na een stabiele periode.
                    note.update(state=PENDING, attempts=0, next_attempt_at=0,
                                eligible_at=now + self._stable_seconds(config))
            else:
                entry.update(human_since=None, human_seen=0)
                for fp, note in notes.items():
                    if note.get("key") == key and note.get("kind") == ACTION_REQUIRED and note.get("state") == PENDING:
                        note["state"] = OBSOLETE  # de grens is weg voor hij gemeld kon worden
                if entry.get("open") is not None:
                    since = entry.get("clear_since")
                    if since is None:
                        entry["clear_since"] = now
                    elif now - float(since) >= RESOLVE_AFTER_SECONDS:
                        entry.update(open=None, clear_since=None)  # afgehandeld: een volgende grens is nieuw
            if item.outcome:
                fp = fingerprint(key, item.outcome)
                if fp not in notes:
                    notes[fp] = self._terminal_note(item, key, config, switch, now)
        for fp, note in notes.items():
            if note.get("state") == PENDING and note.get("kind") == ACTION_REQUIRED \
                    and now - float(note.get("created_at") or now) > config.stale_after_hours * 3600:
                note["state"] = OBSOLETE

    @staticmethod
    def _stable_seconds(config: NotifyConfig) -> float:
        return max(float(config.debounce_seconds), STABLE_MIN_SECONDS)

    def _stable(self, entry: dict, config: NotifyConfig, now: float) -> bool:
        """Staat de grens ononderbroken en vaak genoeg open om te melden?"""
        since = entry.get("human_since")
        return since is not None and int(entry.get("human_seen") or 0) >= STABLE_MIN_OBSERVATIONS \
            and now - float(since) >= self._stable_seconds(config)

    def _terminal_note(self, item: Item, key: str, config: NotifyConfig, switch, now: float) -> dict:
        note = {"state": PENDING, "kind": item.outcome, "key": key, "created_at": now, "attempts": 0,
                "next_attempt_at": 0, "eligible_at": 0}
        moment = parse_iso(item.activity_at)
        enabled_at = parse_iso(switch.enabled_at)
        wanted = config.notify_completed if item.outcome == COMPLETED else config.notify_failed
        if not switch.enabled:
            note.update(state=SUPPRESSED, why="off")       # terwijl UIT: nooit alsnog melden
        elif not wanted:
            note.update(state=SUPPRESSED, why="disabled")
        elif moment is None or enabled_at is None or moment < enabled_at:
            note.update(state=SUPPRESSED, why="before_enabled")  # oud nieuws
        return note

    # -------------------------------------------------------------- versturen

    def _deliver(self, items: dict[str, Item], config: NotifyConfig, switch, now: float) -> int:
        quiet = in_quiet_hours(config, now)
        sent = 0
        transport = None
        for fp, note in list(self.ledger.pending()):
            if sent >= MAX_SENDS_PER_TICK:
                break
            item = items.get(note.get("key"))
            if item is None or now < float(note.get("next_attempt_at") or 0):
                continue
            kind = note["kind"]
            if kind == ACTION_REQUIRED:
                entry = self.ledger.items.get(note["key"]) or {}
                if not item.human or entry.get("episode") != note.get("episode") \
                        or (note.get("boundary") is not None and entry.get("open") != note.get("boundary")):
                    note["state"] = OBSOLETE
                    continue
                if not self._stable(entry, config, now):
                    continue
                if quiet and not config.quiet_hours_urgent:
                    continue
            elif quiet:
                continue  # COMPLETED/FAILED wachten tot de stille uren voorbij zijn
            if not self.switch.read().enabled:
                break  # AAN -> UIT werkt onmiddellijk, ook midden in een ronde
            if transport is None:
                transport = self.transport_factory(config)
            try:
                transport.send(build_message(kind, item))
            except TransportError as exc:
                self._failed(note, str(exc), now)
                continue
            except Exception as exc:  # noqa: BLE001
                self._failed(note, redact(f"{type(exc).__name__}: {exc}", 160), now)
                continue
            note.update(state=SENT, sent_at=now, last_attempt_at=now)
            note.pop("last_error", None)
            self._last_error = None
            sent += 1
            self.log(f"melding verstuurd: {kind} {item.key}")
        return sent

    def _failed(self, note: dict, error: str, now: float) -> None:
        attempts = int(note.get("attempts") or 0) + 1
        note.update(attempts=attempts, last_attempt_at=now, last_error=error)
        if attempts >= MAX_ATTEMPTS:
            note["state"] = ABANDONED
            self.log(f"melding opgegeven na {attempts} pogingen ({note['kind']}): {error}")
        else:
            note["next_attempt_at"] = now + BACKOFF_SECONDS[min(attempts - 1, len(BACKOFF_SECONDS) - 1)]
            self.log(f"verzenden mislukt (poging {attempts}/{MAX_ATTEMPTS}, {note['kind']}): {error}")
        self._last_error = {"at": now_iso(), "message": error}

    # ------------------------------------------------------------------ status

    def _write_status(self, config: NotifyConfig, switch, now: float) -> None:
        last = self.ledger.last_sent()
        atomic_write_json(self.home / STATUS_FILE, {
            "schema": "obf-notify-status-v1", "pid": os.getpid(), "heartbeat_at": now,
            "poll_seconds": config.poll_seconds, "enabled": switch.enabled,
            "configured": config.transport_ready,
            "pending": len(self.ledger.pending()),
            "last_sent": ({"at": last.get("sent_at"), "kind": last.get("kind")} if last else None),
            "last_error": self._last_error})

    # -------------------------------------------------------------- draaien

    def run_forever(self) -> None:
        while not self._stop.is_set():
            self.tick()
            self._stop.wait(self.config_loader().poll_seconds)

    def stop(self) -> None:
        self._stop.set()


def read_status(home: Path | None = None, *, now: float | None = None) -> dict:
    """Wat de monitor toont. Alleen lezen, nooit geheimen of adressen."""
    home = home or notify_home()
    data = read_json(home / STATUS_FILE)
    data = data if isinstance(data, dict) else {}
    now = time.time() if now is None else now
    beat = data.get("heartbeat_at")
    poll = float(data.get("poll_seconds") or 30)
    running = isinstance(beat, (int, float)) and now - beat <= poll * HEARTBEAT_STALE_FACTOR + 30
    return {"notifier_running": bool(running), "pending": data.get("pending", 0),
            "last_sent": data.get("last_sent"), "last_error": data.get("last_error")}


def notification_snapshot(home: Path | None = None) -> dict:
    """De ene read-only weergave voor monitor en /healthz: schakelaar + service + laatste melding."""
    from .settings import load_config  # noqa: PLC0415

    switch = NotificationSwitch(home).read()
    config = load_config(home)
    return {**read_status(home), "enabled": switch.enabled, "updated_at": switch.updated_at,
            "updated_by": switch.updated_by, "degraded": switch.degraded, "configured": config.transport_ready}


class InstanceLock:
    """Één notifier tegelijk: twee processen zouden dezelfde melding twee keer versturen."""

    def __init__(self, home: Path | None = None):
        home = home or notify_home()
        home.mkdir(parents=True, exist_ok=True)
        self._fh = open(home / "notifier.lock", "a+")

    def acquire(self) -> bool:
        try:
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False
