"""Welke Runner-2-gebeurtenissen Monitor 2 meldt (beeld, toon, spraak), en het meldingsjournaal daaronder.

Eén plek voor de regels, geen tweede meldsysteem naast Monitor 2:

* ``task_event`` / ``events_for`` zijn zuiver: uit de recente taken van één node (Runner-2-status plus keten- en
  supervisorblok, precies wat ``r2_adapter.describe_task`` al toont) volgt welke gebeurtenissen er zijn. Niet alleen
  de getoonde taak telt: een voltooiingscontrole of beoordeling is de nieuwste taak, maar het einde van de keten of
  run staat op de laatste schakel; daarom kijkt de adapter naar alle recente taken.
* ``NotifyJournal`` geeft elke *nieuwe* gebeurtenis (sleutel) één volgnummer en bewaart dat op schijf. Een pagina
  onthoudt het laatste volgnummer dat zij verwerkte; wat ze miste (tabblad op de achtergrond, herlaadmoment, node even
  weg, Monitor 2 zelf herstart) haalt ze bij de volgende poll in, en niets wordt tweemaal gemeld.

Geen netwerk en geen klok behalve die van de aanroeper; de browser beslist over stille uren en geluid.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any

from .chain_notifications import ChainPolicy

log = logging.getLogger("obf_monitor2")

#: Zo veel van de nieuwste taken per node leveren gebeurtenissen (een keten eindigt op de laatste schakel; de
#: voltooiingscontrole of beoordeling die erna komt staat dan ook bovenaan).
EVENT_TASKS = 6
#: WACHT OP ACTIE klinkt pas als dezelfde vraag zo lang en zo vaak achter elkaar in beeld bleef (een flits is geen vraag).
HUMAN_CONFIRM_S = 8.0
HUMAN_CONFIRM_READS = 2
#: Een taak die klaar is, is pas een runner-klaarmelding als dezelfde runner zo lang vrij bleef.
DONE_CONFIRM_S = 15.0
DONE_CONFIRM_READS = 2
JOURNAL_KEEP = 60
JOURNAL_ENV = "OBF_MONITOR2_NOTIFY_JOURNAL"

_USER_STOPS = ("stopped_by_user", "task_stopped")


def _event(node_id: str, kind: str, key: str, speech: str, *, at: str | None, task_id: str, title: str | None) -> dict:
    return {"key": f"{node_id}|{key}", "kind": kind, "speech": speech, "at": at, "task_id": task_id, "title": title}


def _supervisor_event(node_id: str, label: str, view: dict, sup: dict, at: str | None) -> dict | None:
    """Een taak van een supervisorrun: een voltooide schakel of beoordeling is een tussenstop zolang de supervisor
    actief is en zelf verdergaat. Klaar is de run pas bij ``project_complete``; een limiet, blokkade of fout vraagt
    aandacht; stoppen door een gebruiker niet."""
    ids = {"task_id": view.get("task_id") or "", "title": view.get("title")}
    if sup.get("state") == "active":
        if not sup.get("warning"):
            return None
        return _event(node_id, "error", f"sup-wait|{sup.get('id')}|{sup.get('chain') or 0}",
                      f"De autonome run op {label} wacht en gaat niet vanzelf verder. Kijk even op de monitor.", at=at, **ids)
    if sup.get("state") == "project_complete":
        return _event(node_id, "done", f"done|{sup.get('id')}", f"Claude is klaar op {label}.", at=sup.get("ended_at") or at, **ids)
    stop = sup.get("stopped_because") or {}
    if stop.get("kind") in _USER_STOPS:
        return None
    what = (" heeft zijn limiet bereikt." if sup.get("state") == "limit_reached"
            else " is geblokkeerd." if sup.get("state") == "blocked" else " is gestopt.")
    return _event(node_id, "error", f"sup-end|{sup.get('id')}", f"De autonome run op {label}{what} Kijk even op de monitor.",
                  at=sup.get("ended_at") or at, **ids)


def _done_event(node_id: str, label: str, state: dict, view: dict, at: str | None) -> dict | None:
    task_id = view.get("task_id") or ""
    ids = {"task_id": task_id, "title": view.get("title")}
    sup, chain = view.get("supervisor"), view.get("chain")
    if sup:
        return _supervisor_event(node_id, label, view, sup, at)
    if not chain:
        return _event(node_id, "done", f"done|{task_id}", f"Claude is klaar op {label}.", at=view.get("finished_at") or at, **ids)
    # Een voltooiingscontrole hoort bij de keten maar is zelf geen schakel: haar oordeel (doel bevestigd of geweigerd)
    # komt als einde van de keten op de laatste schakel binnen. Eigen melding krijgt ze niet.
    if state.get("gate_role") or state.get("supervisor_role"):
        return None
    if chain.get("current_task") and chain["current_task"] != task_id:
        return None                                              # een oudere schakel is geen einde van de keten
    if chain.get("state") == "active":
        if not chain.get("warning"):
            return None                                          # de keten gaat zelf verder (volgende schakel, controle)
        return _event(node_id, "error", f"chain-wait|{chain.get('id')}|{task_id}",
                      f"De keten op {label} wacht en gaat niet vanzelf verder. Kijk even op de monitor.", at=at, **ids)
    ended = chain.get("ended_at") or at
    if chain.get("state") == "completed":
        return _event(node_id, "done", f"done|{chain.get('id')}", f"Claude is klaar op {label}.", at=ended, **ids)
    if (chain.get("stopped_because") or {}).get("kind") == "stopped_by_user":
        return None                                              # door een gebruiker gestopt
    what = " heeft zijn limiet bereikt." if chain.get("state") == "limit_reached" else " is gestopt."
    return _event(node_id, "error", f"chain-end|{chain.get('id')}", f"De keten op {label}{what} Kijk even op de monitor.",
                  at=ended, **ids)


def task_event(node_id: str, label: str, state: dict, view: dict, updated: str | None) -> dict | None:
    """De meldingswaardige gebeurtenis van één taak (``view`` = ``describe_task``), of None.

    ``state`` is de ruwe Runner-2-state (voor ``gate_role``/``supervisor_role``), ``updated`` het UTC-tijdstip van de
    laatste wijziging. Alleen WACHT OP ACTIE, KLAAR (bij een keten of supervisorrun: het echte einde) en MISLUKT melden."""
    key = view.get("state")
    task_id = view.get("task_id") or ""
    ids = {"task_id": task_id, "title": view.get("title")}
    if key == "wacht_op_actie":
        asked = (view.get("question") or {}).get("asked_at") or ""
        return _event(node_id, "human", f"human|{task_id}|{asked}", f"Claude wacht op een menselijke actie op {label}.",
                      at=updated, **ids)
    if key == "klaar":
        return _done_event(node_id, label, state, view, updated)
    if key == "mislukt":
        sup = view.get("supervisor")
        if sup and sup.get("state") != "active":
            return _supervisor_event(node_id, label, view, sup, updated)
        # Een mislukte schakel terwijl de supervisor actief is, wacht op zijn beoordeling: dat is geen eindstand.
        if sup and sup.get("phase") in ("between_chains", "judging", "starting_chain"):
            return None
        return _event(node_id, "error", f"error|{task_id}", f"De taak op {label} is mislukt. Kijk even op de monitor.",
                      at=updated, **ids)
    return None


def events_for(node_id: str, label: str, rows: list[tuple[dict, dict, str | None]]) -> list[dict]:
    """Alle gebeurtenissen van een node uit zijn recente taken (nieuwste eerst: ``(state, view, updated_utc)``).

    Dezelfde sleutel komt één keer voor (bij een keten of supervisorrun beschrijven meerdere taken hetzelfde einde)."""
    out: list[dict] = []
    seen: set[str] = set()
    for state, view, updated in rows:
        event = task_event(node_id, label, state, view, updated)
        if event and event["key"] not in seen:
            seen.add(event["key"])
            out.append(event)
    return out


class NotifyJournal:
    """Volgnummers voor nieuwe gebeurtenissen, bewaard op schijf (``path`` None = alleen geheugen).

    * Een node die voor het eerst gelezen wordt, krijgt zijn bestaande gebeurtenissen als *baseline*: ze krijgen
      een volgnummer maar worden nooit gemeld (een nieuwe pagina of een nieuw bestand is geen overgang).
    * Daarna is elke nieuwe sleutel een echte gebeurtenis, ook als Monitor 2 zelf op dat moment uit stond.
    * ``human`` (WACHT OP ACTIE) komt pas in het journaal als dezelfde vraag ``HUMAN_CONFIRM_S`` seconden en
      ``HUMAN_CONFIRM_READS`` leesrondes achter elkaar in beeld bleef.
    * ``done`` komt pas in het journaal als de runner ``DONE_CONFIRM_S`` seconden vrij bleef. Start er intussen een
      opvolgtaak, dan wordt het oude einde wel onthouden maar nooit later alsnog gemeld.
    * De ``id`` verandert alleen als het journaal vervangen wordt; een pagina begint dan opnieuw zonder te melden."""

    def __init__(self, path: Path | str | None = None, *, clock=time.time, keep: int = JOURNAL_KEEP):
        self._path = Path(path) if path else None
        self._clock, self._keep = clock, keep
        self._lock = threading.Lock()
        self._pending: dict[str, dict] = {}
        self._data = self._load()

    @classmethod
    def from_env(cls, **kwargs: Any) -> "NotifyJournal":
        path = os.environ.get(JOURNAL_ENV)
        if not path:
            log.warning("%s staat niet gezet: het meldingsjournaal blijft alleen in het geheugen (een herstart van "
                        "Monitor 2 verliest dan wat een pagina nog moest inhalen)", JOURNAL_ENV)
        return cls(path or None, **kwargs)

    def _load(self) -> dict:
        fresh = {"id": secrets.token_hex(6), "head": 0, "seeded": [], "known": [], "entries": []}
        if self._path is None:
            return fresh
        try:
            data = json.loads(self._path.read_text("utf-8"))
            if (isinstance(data, dict) and isinstance(data.get("id"), str) and isinstance(data.get("head"), int)
                    and isinstance(data.get("entries"), list)):
                data.setdefault("seeded", [])
                data.setdefault("known", [])
                return data
        except (OSError, ValueError):
            pass
        return fresh

    def _save(self) -> None:
        if self._path is None:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_name(self._path.name + ".tmp")
            tmp.write_text(json.dumps(self._data, ensure_ascii=False), "utf-8")
            os.replace(tmp, self._path)
        except OSError as exc:
            log.warning("meldingsjournaal niet te bewaren: %s", exc)

    def ingest(self, node_id: str, label: str, events: list[dict] | None) -> None:
        """Verwerk de gebeurtenissen die een geldige lezing van ``node_id`` nu toont (idempotent)."""
        if events is None:
            with self._lock:
                if node_id in ("linux", "gerard"):
                    self._pending = {k: p for k, p in self._pending.items() if p["node"] != node_id}
            return                                   # geen geldige lezing: niets aannemen
        now = self._clock()
        with self._lock:
            data = self._data
            known = set(data["known"])
            seeding = node_id not in data["seeded"]
            changed = False
            if seeding:
                data["seeded"].append(node_id)
                changed = True
            live = {event["key"] for event in events}
            newest_done = next((event["key"] for event in events if event.get("kind") == "done"), None)
            for key in [k for k, p in self._pending.items() if p["node"] == node_id and k not in live]:
                del self._pending[key]               # de vraag was al weg voordat ze stabiel was
            for event in events:
                key = event["key"]
                if key in known:
                    continue
                if event["kind"] == "done" and not seeding:
                    if event.get("runner_idle") is not True or key != newest_done:
                        self._pending.pop(key, None)
                        # Een opvolgtaak of een nieuwer einde maakt deze melding definitief verouderd.
                        data["known"].append(key)
                        known.add(key)
                        changed = True
                        continue
                    pending = self._pending.get(key)
                    if pending is None:
                        self._pending[key] = {"node": node_id, "kind": "done", "since": now, "reads": 1, "observation": event.get("observation")}
                        continue
                    if event.get("observation") is None or event.get("observation") != pending.get("observation"):
                        pending["reads"] += 1
                    pending["observation"] = event.get("observation")
                    if pending["reads"] < DONE_CONFIRM_READS or now - pending["since"] < DONE_CONFIRM_S:
                        continue
                    del self._pending[key]
                if event.get("confirm_s") and not seeding:
                    pending = self._pending.get(key)
                    if pending is None:
                        self._pending[key] = {"node": node_id, "since": now, "reads": 1,
                                              "observation": event.get("observation")}
                        continue
                    if event.get("observation") != pending.get("observation"):
                        pending["reads"] += 1
                    pending["observation"] = event.get("observation")
                    if pending["reads"] < 2 or now - pending["since"] < event["confirm_s"]:
                        continue
                    del self._pending[key]
                if event["kind"] == "human" and not seeding:
                    pending = self._pending.get(key)
                    if pending is None:
                        self._pending[key] = {"node": node_id, "kind": "human", "since": now, "reads": 1}
                        continue
                    pending["reads"] += 1
                    if pending["reads"] < HUMAN_CONFIRM_READS or now - pending["since"] < HUMAN_CONFIRM_S:
                        continue
                    del self._pending[key]
                data["head"] += 1
                data["known"].append(key)
                known.add(key)
                stored = {name: value for name, value in event.items() if name not in ("runner_idle", "observation", "confirm_s")}
                data["entries"].append({**stored, "seq": data["head"], "node": node_id, "label": label,
                                        "baseline": seeding, "seen_at": round(now, 3)})
                changed = True
            if changed:
                data["entries"] = data["entries"][-self._keep:]
                data["known"] = data["known"][-(self._keep * 4):]
                self._save()

    def feed(self) -> dict:
        with self._lock:
            return {"id": self._data["id"], "head": self._data["head"],
                    "events": [dict(entry) for entry in self._data["entries"]]}
