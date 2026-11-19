"""Koppeling van de automatische integratie met de open actiepunten van Runner 2.

Probleem: wat de controller (Monitor 2) na de afronding van een taak vaststelt -- een integratie die niet door kan (geen
fast-forward, beschermde bestanden, falende tests, vuile hoofdcheckout, ...) -- stond tot nu toe alleen in het Monitor 2-paneel,
de audittrail en het ontvangstbewijs. De taak was dan al afgerond en Monitor 2 kan een ``runner_finish`` niet meer wijzigen. Het
open-actiesscherm van Runner 2 (``controller.status()['human_actions']``) liet die punten dus niet zien.

Oplossing: de controller legt zo'n punt vast als gewoon Runner 2-actiepunt op de taak waar het bij hoort, via de bestaande publieke
functie ``runner2.actions.record`` (dezelfde functie die ``runner_finish`` gebruikt). Er verandert niets aan Runner 2 en deze module
schrijft nooit zelf in ``state.json``. Daardoor gelden dezelfde regels als voor elk ander punt: stabiele id (hash van taak +
tekst), deduplicatie tegen open en gesloten punten van dezelfde run, een eenmaal gesloten punt komt niet terug, een gestopte taak
krijgt geen punten.

Eigenschappen
-------------
* **Idempotent**: per (aanvraag, reden) één punt. De administratie is een markering ``R-x.act-<reden>`` (``O_EXCL``) in de map met
  toestemmingen (alleen voor de controller). Valt het proces tussen het vastleggen en de markering uit, dan maakt de volgende ronde
  hetzelfde punt (zelfde id) niet nogmaals aan: Runner 2 slaat het over en de markering wordt alsnog gezet.
* **Traceerbaar**: ``topic`` is ``integratie-<reden>-<aanvraag>`` (stabiel; voor Runner 2 is de topic leidend bij deduplicatie) en
  ``details`` noemt bron, aanvraag, taak, toestemming en herstelbranch. Elke koppeling staat in de hashketen-audittrail
  (``action_linked``) en in ``status()`` van de controller.
* **Nooit sluiten**: de koppeling voegt alleen toe. Ze sluit, wijzigt of heropent geen enkel punt; een mens (of de judge van Runner 2)
  sluit ze later, samen met de rest.
* **Verwisselbaar blokkeert niets**: een ontbrekende of afwijkende Runner 2 maakt het punt ``pending`` (wordt elke ronde opnieuw
  geprobeerd, met melding in het paneel); de integratie zelf wordt er nooit door geweigerd of vertraagd.
* **Fail-closed op de verkeerde map**: ``runner2.config.tasks_dir()`` moet dezelfde map zijn als de ``R2Records`` van de controller;
  anders wordt er niets geschreven (``home_mismatch``).

Wat het niet doet: punten sluiten, een taak of keten starten/wijzigen, iets zonder taak ergens aan hangen (een aanvraag zonder
bekende taak blijft ``unlinked``: alleen in paneel, audittrail en ontvangstbewijs), pushen of iets naar buiten sturen.
"""

from __future__ import annotations

import importlib
import json
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Protocol

from . import integration as ig

ENABLE_ENV = "OBF_INTEGRATION_R2_ACTIONS"      # "0" zet de koppeling uit; standaard aan zodra automatische integratie aan staat
SRC_ENV = "OBF_RUNNER2_SRC"
TASK_ID = re.compile(r"^R2-[0-9a-f]{12}$")
CODE = re.compile(r"^[a-z][a-z0-9_]{1,40}$")
GRACE_S = 15 * 60          # een wachtreden die een mens kan oplossen (vuile hoofdcheckout) wordt pas na zoveel tijd een punt
MAX_BATCH = 20             # hoogstens zoveel nieuwe punten per ronde
MAX_ATTEMPTS = 5           # pogingen bij een niet te duiden uitkomst (geen fout) voordat het punt als unlinked wordt gemarkeerd
SOURCE = "monitor2-auto-integratie"
LEDGER_KEEP = 20

# Gewone Nederlandse zinnen voor een niet-programmerende gebruiker (geen hash, branchnaam of intern id). ``{w}``/``{W}`` is het werk (zin-eigen en met hoofdletter).
_TEXTS = {
    "tracked_dirty": "{W} wacht om in main te komen, maar de hoofdmap van het project heeft nog niet-opgeslagen "
                     "wijzigingen. Wil je die opslaan of terugzetten? Daarna gaat het vanzelf door.",
    "main_not_checked_out": "{W} wacht om in main te komen, maar de hoofdmap van het project staat niet op main. "
                            "Wil je die terugzetten op main? Daarna gaat het vanzelf door.",
    "operation_in_progress": "{W} wacht om in main te komen, maar in de hoofdmap van het project is nog een "
                             "andere git-handeling bezig. Wil je die afmaken of afbreken?",
    "untracked_collision": "{W} wacht om in main te komen, maar losse bestanden in de hoofdmap staan in de weg. "
                           "Wil je die verplaatsen? Daarna gaat het vanzelf door.",
    "not_fast_forward": "{W} past niet meer direct op main, omdat main intussen verder is gegaan. "
                        "Wil je dat het werk opnieuw op de nieuwste main wordt opgebouwd, of wil je het zelf samenvoegen?",
    "conflicts": "{W} botst met wijzigingen in main. Wil je dat laten oplossen of zelf beslissen wat blijft?",
    "protected_paths": "{W} raakt bestanden die niet automatisch mogen worden samengevoegd. Wil je de wijziging "
                       "bekijken en zelf goedkeuren of afwijzen?",
    "special_entries": "{W} bevat koppelingen of ingesloten repositories. Wil je dat bekijken voordat het in "
                       "main komt?",
    "too_large": "{W} is groter dan automatisch samenvoegen toestaat. Wil je het bekijken en zelf in main "
                 "zetten?",
    "tests_failed": "De tests van {w} faalden vlak voor het in main zou komen; main is niet gewijzigd. "
                    "Wil je het laten herstellen?",
    "merged_tests_failed": "{W} staat in main, maar de tests daarna faalden. Wil je het laten staan of terugzetten?",
    "postcheck_failed": "Het samenvoegen van {w} in main gaf een onverwachte uitkomst. Wil je main controleren en beslissen of het "
                        "terug moet?",
    "merge_failed": "Het samenvoegen van {w} in main is mislukt. Wil je het laten onderzoeken?",
    "main_moved": "Terwijl {w} in main kwam, is main door iets anders verplaatst. Wil je main controleren en beslissen of het "
                  "werk opnieuw moet?",
    "recovery_failed": "Voor {w} kon geen herstelpunt worden gemaakt, dus is er niets samengevoegd. Wil je dit laten onderzoeken?",
    "source_moved": "{W} is veranderd nadat het was aangemeld voor main. Wil je het opnieuw laten aanmelden of goedkeuren?",
    "no_finish": "{W} is niet netjes afgesloten, dus is het niet automatisch in main gezet. Wil je het bekijken en "
                 "zelf goedkeuren?",
    "request_not_in_finish": "{W} is niet automatisch in main gezet omdat de afronding de aanvraag niet noemt. Wil je het "
                             "bekijken en zelf goedkeuren?",
    "chain_stopped": "{W} is niet automatisch in main gezet omdat de keten is gestopt zonder het doel te halen. "
                     "Wil je beslissen wat ermee gebeurt?",
    "end_commit_mismatch": "{W} is niet automatisch in main gezet omdat de aangemelde stand niet de laatste stand is. "
                           "Wil je het opnieuw laten aanmelden?",
    "tip_after_finish": "{W} bevat wijzigingen van na de afronding en is daarom niet automatisch in main gezet. "
                        "Wil je het bekijken en zelf goedkeuren?",
    "no_tests": "{W} is niet automatisch in main gezet omdat er geen tests voor zijn aangewezen. Wil je tests laten "
                "kiezen of het zelf goedkeuren?",
    "mode_not_allowed": "{W} vroeg een samenvoegvorm die niet automatisch mag. Wil je het zelf goedkeuren?",
    "expired": "{W} kon binnen de gestelde tijd niet automatisch in main komen. Wil je het opnieuw laten aanmelden of "
               "zelf goedkeuren?",
    "task_not_completed": "{W} is niet afgerond en daarom niet in main gezet. Wil je beslissen wat ermee moet?",
    "task_failed": "{W} meldde een fout en is daarom niet in main gezet. Wil je beslissen wat ermee moet?",
    "link_not_clean": "{W} is door Runner 2 niet schoon afgerond en daarom niet in main gezet. Wil je het bekijken?",
    "worktree_dirty": "{W} is niet in main gezet omdat de werkmap nog niet-opgeslagen wijzigingen had. Wil je het "
                      "bekijken?",
    "worktree_mismatch": "{W} is niet in main gezet omdat de werkmap niet op de aangemelde stand staat. Wil je het "
                         "bekijken?",
}
_GENERIC = ("De automatische integratie van {w} naar main is niet doorgegaan en vraagt een besluit. "
            "Wil je beslissen of je het zelf goedkeurt of laat aanpassen?")


class ActionBackend(Protocol):
    def record(self, task_id: str, item: dict) -> dict: ...


class LinkError(Exception):
    """De koppeling kan nu niet schrijven (Runner 2 niet gevonden, verkeerde map, ...). Het punt blijft ``pending``."""


_PLAIN_NAME = re.compile(r"^[\w .,:'&()/+-]+$")
_JARGON = re.compile(r"(?i)(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])\b[0-9a-f]{7,}\b|\b(?:r2|runner2|origin|feature|fix|claude|codex)/|"
                     r"\b(?:R2|C|A|PRUN|CTRL|NOD)-[0-9a-f]{6,}|(?<![\w-])H\d{1,2}\b")


def action_text(code: str, title: str | None) -> str:
    """De korte tekst voor de mens. Een taaktitel gaat er alleen in als die gewone woorden is (anders: titelloze zin), zodat de
    tekst Runner 2's eigen leesbaarheidscontrole (geen hash, branchnaam of intern id) altijd doorstaat."""
    template = _TEXTS.get(code, _GENERIC)
    name = re.sub(r"\s+", " ", str(title or "")).strip()[:70]
    if not name or not _PLAIN_NAME.match(name) or _JARGON.search(name):
        w = "het werk van deze taak"
    else:
        w = f"het werk \u201c{name}\u201d"
    return template.format(w=w, W=w[0].upper() + w[1:])


def topic_for(request_id: str, code: str) -> str:
    return ("integratie-" + code.replace("_", "-") + "-" + request_id[2:])[:60]


# ----------------------------------------------------------------------
# Runner 2-kant: alleen via de publieke functie ``runner2.actions.record``
# ----------------------------------------------------------------------


def _load_actions_module(env: dict) -> object:
    try:
        return importlib.import_module("runner2.actions")
    except ImportError:
        pass
    raw = env.get(SRC_ENV)
    candidates = [Path(raw).expanduser()] if raw else [Path("~/StudioOBF-wt-runner2").expanduser(), Path("~/runner2-src").expanduser()]
    for candidate in candidates:
        if (candidate / "runner2" / "actions.py").is_file():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return importlib.import_module("runner2.actions")
    raise LinkError("runner2_not_found")


class Runner2Actions:
    """Legt een punt vast met ``runner2.actions.record`` en controleert het resultaat door de taakstatus terug te lezen."""

    def __init__(self, home: Path, *, module=None, env: dict | None = None):
        self.home = Path(home)
        self._module = module
        self._env = env if env is not None else dict(os.environ)

    def _actions(self):
        if self._module is None:
            self._module = _load_actions_module(self._env)
        if not callable(getattr(self._module, "record", None)) or not callable(getattr(self._module, "make_id", None)):
            raise LinkError("runner2_api_changed")
        return self._module

    def _check_home(self) -> None:
        # runner2.actions werkt op de map die runner2.config bepaalt (R2_HOME); dat moet dezelfde zijn als die van de controller.
        try:
            config = importlib.import_module("runner2.config")
            there = os.path.realpath(str(config.tasks_dir()))
        except Exception as exc:  # noqa: BLE001
            raise LinkError("runner2_config_unreadable") from exc
        if there != os.path.realpath(str(self.home / "tasks")):
            raise LinkError("home_mismatch")

    def _state(self, task_id: str) -> dict | None:
        try:
            data = json.loads((self.home / "tasks" / task_id / "state.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) and data.get("task_id") == task_id else None

    def record(self, task_id: str, item: dict) -> dict:
        if not TASK_ID.match(task_id or ""):
            return {"state": "unlinked", "reason": "no_task"}
        module = self._actions()
        aid = module.make_id(task_id, item["text"])
        state = self._state(task_id)
        if state is None:
            return {"state": "unlinked", "reason": "task_unknown", "action_id": aid}
        for existing in state.get("human_actions") or []:
            if isinstance(existing, dict) and existing.get("id") == aid:
                return {"state": "existing", "action_id": aid, "status": existing.get("status")}
        if state.get("status") == "stopped":
            return {"state": "unlinked", "reason": "task_stopped", "action_id": aid}
        self._check_home()
        try:
            module.record(task_id, {"open_actions": [item]})
        except Exception as exc:  # noqa: BLE001 -- Runner 2 gaf een fout: niets aannemen, later opnieuw
            raise LinkError("record_failed:" + exc.__class__.__name__) from exc
        state = self._state(task_id) or {}
        for found in state.get("human_actions") or []:
            if isinstance(found, dict) and found.get("id") == aid:
                return {"state": "recorded", "action_id": aid, "status": found.get("status")}
        for entry in reversed(state.get("suppressed_actions") or []):
            if isinstance(entry, dict) and entry.get("text") == item["text"]:
                return {"state": "suppressed", "action_id": aid, "why": entry.get("why"), "matched_id": entry.get("matched_id")}
        return {"state": "not_recorded", "action_id": aid}


# ----------------------------------------------------------------------
# De controller-kant
# ----------------------------------------------------------------------


class ActionLinker:
    """Zet kandidaten (zie ``AutoIntegrator.action_candidates``) om in Runner 2-actiepunten, idempotent en zonder iets te sluiten."""

    def __init__(self, store: ig.Store, backend: ActionBackend, r2_records, *, clock: Callable[[], float] = time.time):
        self.store, self.backend, self.r2 = store, backend, r2_records
        self.clock = clock
        self._lock = threading.Lock()
        self.problem: str | None = None
        self._attempts: dict[tuple, int] = {}
        self._reported: set = set()

    @staticmethod
    def _suffix(code: str) -> str:
        return ".act-" + (code if CODE.match(code or "") else "other")

    def sync(self, candidates: list[dict]) -> list[dict]:
        """Verwerkt de kandidaten; geeft de nieuw gelegde koppelingen. Raised nooit."""
        done: list[dict] = []
        with self._lock:
            problem = None
            for cand in candidates:
                if len(done) >= MAX_BATCH:
                    break
                rid, code = cand.get("request_id"), cand.get("code") or "other"
                if not ig.REQUEST_ID.match(rid or "") or self.store.marker(rid, self._suffix(code)) is not None:
                    continue
                try:
                    linked = self._link(cand)
                except LinkError as exc:
                    problem = str(exc)
                    break   # de oorzaak geldt voor alle kandidaten; volgende ronde opnieuw
                except Exception as exc:  # noqa: BLE001
                    problem = "unexpected:" + exc.__class__.__name__
                    break
                if linked is not None:
                    done.append(linked)
            self.problem = problem
            if problem and problem not in self._reported:
                self._reported.add(problem)
                try:
                    self.store.audit("action_link_problem", f"koppeling met Runner 2-actiepunten niet mogelijk: {problem}", problem=problem)
                except Exception:  # noqa: BLE001
                    pass
        return done

    def _link(self, cand: dict) -> dict | None:
        rid, code = cand["request_id"], cand.get("code") or "other"
        task_id = cand.get("task_id") or ""
        rec = self.r2.task(task_id) if TASK_ID.match(task_id) else None
        text = action_text(code, (rec or {}).get("title"))
        item = {"text": text, "reason": str(cand.get("text") or "")[:400], "topic": topic_for(rid, code),
                "details": self._details(cand, rid, code, task_id)}
        result = self.backend.record(task_id, item)
        state = result.get("state")
        key = (rid, code)
        if state == "not_recorded":
            self._attempts[key] = self._attempts.get(key, 0) + 1
            if self._attempts[key] < MAX_ATTEMPTS:
                return None
            result = {**result, "state": "unlinked", "reason": "not_recorded"}
            state = "unlinked"
        marker = {"state": state, "request": rid, "code": code, "task": task_id or None, "action_id": result.get("action_id"),
                  "topic": item["topic"], "at": ig._iso(self.clock()), "source": SOURCE, "grant": cand.get("grant") or None,
                  "reason": result.get("reason") or result.get("why"), "matched_id": result.get("matched_id")}
        marker = {k: v for k, v in marker.items() if v is not None}
        if not self.store.set_marker(rid, self._suffix(code), marker):
            return None
        self._attempts.pop(key, None)
        self.store.audit("action_linked", f"{rid}: {code}: Runner 2-actiepunt {state}"
                         + (f" ({result.get('action_id')})" if result.get("action_id") else ""),
                         request=rid, code=code, task=task_id or None, action_id=result.get("action_id"), link_state=state,
                         reason=marker.get("reason"))
        return marker

    @staticmethod
    def _details(cand: dict, rid: str, code: str, task_id: str) -> str:
        parts = ["Bron: Monitor 2 automatische integratie (niet door de taak zelf gemeld)", f"aanvraag {rid}", f"reden {code}"]
        if task_id:
            parts.append(f"taak {task_id}")
        if cand.get("grant"):
            parts.append(f"toestemming {cand['grant']}")
        if cand.get("recovery_branch"):
            parts.append(f"herstelbranch {cand['recovery_branch']}")
        parts.append("Handmatige route: Monitor 2, scherm Opdrachten, paneel Integratie naar main")
        return "; ".join(parts)

    def public(self) -> dict:
        """Voor het paneel: de laatste koppelingen (uit de markeringen) en een eventuele probleemmelding."""
        rows = []
        try:
            paths = sorted(self.store.dir.glob("R-*.act-*"), key=lambda p: p.stat().st_mtime, reverse=True)[:LEDGER_KEEP]
        except OSError:
            paths = []
        for path in paths:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if isinstance(data, dict):
                rows.append({k: data.get(k) for k in ("request", "code", "task", "action_id", "state", "at", "reason", "matched_id")})
        return {"enabled": True, "problem": self.problem, "linked": rows}


def enabled(env: dict | None = None) -> bool:
    env = env if env is not None else os.environ
    return str(env.get(ENABLE_ENV, "1")).strip().lower() not in ("0", "false", "no", "nee", "uit")
