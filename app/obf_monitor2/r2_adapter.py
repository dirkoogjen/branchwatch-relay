"""De dunne Runner-2-adapter van Monitor 2.

Monitor 2 is hier alleen een venster op Runner 2: de toestand komt uit de state en
controller van Runner 2 zelf (``runner2.controller.status`` / ``runner2.store`` /
``runner2.watch.needs_recovery``), en elke knop roept dezelfde controllerfunctie aan
als de CLI en de controller-MCP (``pause``, ``resume``, ``stop``, ``answer``). Er is
geen eigen taakstatus, herstel- of beslislogica: dit bestand vertaalt alleen naar de
Nederlandse woorden en velden die de pagina toont.

Per node één *backend* met dezelfde twee handelingen:

* ``snapshot(selected)`` -> een woordenboek met de nodetoestand en de gekozen taak;
* ``action(action, task_id, **fields)`` -> ``(http_status, antwoord)``.

``LocalRunner2Backend`` leest en bedient de Runner 2 op deze Mac. Voor een remote node
(LINUX of GERARD) is het dezelfde klasse met ``RemoteModules`` als bron: één ssh-aanroep per
leesronde (``runner2.rpc bundle``) en dezelfde controllerfuncties voor de knoppen, via
het node-transport van Runner 2 (``runner2.remote``). ``NotInstalledBackend`` is wat een
node zonder geconfigureerde route is (nu WINDOWS): eerlijk "nog niet geïnstalleerd",
zonder nepstatus en zonder terugval op Runner 1.
"""

from __future__ import annotations

import importlib
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

from . import action_board, notify_events

#: Op welke node dit proces draait (``mac``, standaard, of ``gerard``): die node wordt in-process gelezen en bediend.
LOCAL_NODE_ENV = "OBF_MONITOR2_LOCAL_NODE"
#: Waar de Runner-2-broncode staat als ``runner2`` niet al importeerbaar is.
SOURCE_ENV = "OBF_RUNNER2_SRC"
DEFAULT_SOURCE_CANDIDATES = (str(Path(__file__).resolve().parents[2]),)

#: Zo lang een leesronde hergebruikt wordt (Monitor 2 pollt elke 3 s, meerdere tabbladen).
CACHE_SECONDS = 1.5

#: Remote nodes kosten per leesronde een ssh-aanroep. Eén bundel wordt zo lang gedeeld door alle tabbladen en
#: taakkeuzes, dus hoogstens ~1 aanroep per 4 s per node (0,25/s), wat het aantal tabbladen ook is.
REMOTE_BUNDLE_SECONDS = 4.0
#: Na een mislukte ssh-ronde wacht de monitor zo lang met een nieuwe poging. Dat is langer dan de 8 s
#: terugval van ``runner2.remote``, zodat elke poging echt ssh doet en niet alleen die terugval herhaalt.
REMOTE_RETRY_AFTER_FAILURE_S = 10.0
#: Een statusaanroep die na zoveel seconden niet antwoordt is mislukt (Runner 2 zelf geeft 60 s). Een
#: gezonde node antwoordt in 0,1-1,5 s; wachten tot 60 s hield één haperende verbinding tot een minuut vast.
REMOTE_CALL_TIMEOUT_S = 20.0

#: Hoe lang een node zonder verse lezing mag zijn voordat de kaart dat eerlijk meldt. Een gewone
#: lezing duurt ~0,3-1,5 s en de pagina pollt elke 3 s; één gemiste ronde kost de ssh-terugval en de
#: eigen wachttijd (~10 s) en hoort daar nog niet in te vallen.
DELAYED_AFTER_S = 20.0        # vanaf hier: "GEGEVENS VERTRAAGD" met de laatste geldige stand
UNREACHABLE_AFTER_S = 45.0    # vanaf hier (én genoeg bewijs): NIET BEREIKBAAR
UNREACHABLE_FAILURES = 3      # echte, opeenvolgende mislukte leesrondes die als bewijs tellen
HUNG_READ_S = 90.0            # een lezing die ondanks de aanroeptimeout van 20 s zo lang vastzit (proces zit vast) telt ook als bewijs
MAX_KEPT_VIEWS = 8

log = logging.getLogger("obf_monitor2")

TASK_ID = re.compile(r"^R2-[0-9a-f]{12}$")
MAX_TEXT = 8000
MAX_REPORT_CHARS = 200_000    # een eindrapport is tekst van Claude; ruim genoeg, maar niet onbegrensd
MAX_REPORT_ITEMS = 100

ACTIONS = ("pause", "resume", "stop", "answer")

#: Opschoonvoorstel (DRY RUN van het actiebeheer van Runner 2): hoe lang een node-antwoord hergebruikt wordt, en de ondergrens
#: voor "Vernieuwen" zodat het venster geen ssh-aanroepen kan opjagen.
REVIEW_CACHE_SECONDS = 600.0
REVIEW_MIN_REFRESH_SECONDS = 30.0
REVIEW_MAX_ITEMS = 100        # per bak; meer wordt eerlijk als "N van M getoond" gemeld
REVIEW_UNSUPPORTED = "Deze Runner kent het opschoonvoorstel nog niet."


@dataclass(frozen=True)
class Node:
    id: str          # mac / linux / gerard / windows
    label: str       # MAC; ook de Runner-2-node-ID (ssh-route, API, wachtrij, historie): nooit voor de weergave hernoemen
    subtitle: str    # macOS
    platform: str    # icoon en illustratie
    name: str = ""   # alleen de weergavenaam (kaart, machineregel, taakblok, meldingen op de pagina); leeg = label

    @property
    def display(self) -> str:
        """Hoe de machine op de pagina heet (LINUX = "STUDIO"); nooit voor route, API, wachtrij of historie."""
        return self.name or self.label


NODES = (
    Node("mac", "MAC", "macOS", "mac"),
    Node("linux", "LINUX", "Ubuntu (NUC)", "linux", name="STUDIO"),   # sinds 2026-10-17 de fysieke Intel NUC (tailnetpeer studioobf), niet meer de VM
    Node("gerard", "GERARD", "Ubuntu (remote)", "gerard"),
    Node("windows", "WINDOWS", "Windows", "windows"),
)

#: Nederlandse toestanden van de pagina; naast kleur ook een woord.
STATES = {
    "bezig": ("BEZIG", "run"),
    "gepauzeerd": ("GEPAUZEERD", "off"),
    "wacht_op_actie": ("WACHT OP ACTIE", "human"),
    "herstellen": ("HERSTELLEN", "warn"),
    "klaar": ("KLAAR", "ok"),
    "mislukt": ("MISLUKT", "alert"),
    "gestopt": ("GESTOPT", "off"),
}

# ----------------------------------------------------------------------
# Vertaling van een Runner-2-taak naar wat de pagina toont (zuiver, geen I/O)
# ----------------------------------------------------------------------


def model_badge(model: Any) -> str | None:
    """``claude-sonnet-5-5`` -> ``Sonnet 5.5``; een onbekende naam blijft zoals hij is."""
    if not isinstance(model, str) or not model:
        return None
    match = re.match(r"^claude-([a-z]+)-(\d+)-(\d+)(?:-\d+)?$", model)
    if match:
        return f"{match.group(1).capitalize()} {match.group(2)}.{match.group(3)}"
    return model


def _clip(value: Any, limit: int) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    value = " ".join(value.split())
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def _iso_utc(value: Any) -> str | None:
    """Een Runner-2-tijdstempel (``2026-09-30T17:37:52+0200``) als UTC-ISO; de browser toont hem lokaal."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for pattern in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            moment = datetime.strptime(text, pattern)
        except ValueError:
            continue
        return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return None


def _first_prompt(state: dict) -> str | None:
    """De opdracht van de taak: ``prompt`` als Runner 2 die bewaart, anders de eerste wachtrijtekst."""
    for candidate in (state.get("prompt"), *(state.get("inbox") or []), *(state.get("in_flight") or [])):
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    return None


def state_key(state: dict, *, recovering: bool) -> str:
    """Runner-2-status -> Nederlandse toestand. ``recovering`` komt van ``runner2.watch.needs_recovery``."""
    status = state.get("status")
    if recovering:
        return "herstellen"
    if status == "waiting_for_human":
        return "wacht_op_actie"
    if status == "paused":
        return "gepauzeerd"
    if status == "completed":
        return "klaar"
    if status == "failed":
        return "mislukt"
    if status == "stopped":
        return "gestopt"
    return "bezig"  # created / running


def open_action_count(listing: dict | None) -> int:
    """Aantal openstaande menselijke actiepunten uit ``controller.status()`` (``human_actions.open``).

    Een oudere Runner 2 zonder dit blok, of een onleesbaar blok, telt als 0: er wordt nooit iets uit tekst afgeleid."""
    block = (listing or {}).get("human_actions")
    count = block.get("open") if isinstance(block, dict) else None
    return count if isinstance(count, int) and not isinstance(count, bool) and count > 0 else 0


#: Waarom een supervisieproject aandacht vraagt, per toestand van ``runner2.supervision`` (alleen de aandachtstoestanden).
SUPERVISION_REASONS = {
    "needed": "Er wacht een beslissing van de ChatGPT-supervisor: een open event is nog door niemand geclaimd en beoordeeld.",
    "blocked": "De supervisie staat vast: het dossier is niet betrouwbaar of een event is na te veel pogingen geëscaleerd.",
    "waiting_human": "Het project wacht op een antwoord of besluit van een mens voordat de supervisor verder kan.",
    "limit": "Het project heeft een limiet bereikt (opdrachten, tijd of runs zonder voortgang); er start niets nieuws.",
    "claimed": "Een supervisor heeft het event geclaimd en beslist nog.",
}
#: Leesbare namen voor de eventsoorten van ``runner2.supervision``; een onbekende soort blijft zoals hij is.
SUPERVISION_EVENT_TEXT = {"run_ended": "run afgelopen", "project_started": "project gestart", "goal_amended": "projectdoel gewijzigd",
                          "request_review": "herbeoordeling gevraagd", "blocker_resolved": "blokkade opgelost",
                          "manual_assign": "handmatige opdracht"}
MAX_SUPERVISION_PROJECTS = 12


def _int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _node_id(label: Any) -> str | None:
    """``MAC`` -> ``mac`` als het een bekende Monitor 2-node is (voor de rapportknop), anders None."""
    value = label.strip().lower() if isinstance(label, str) else ""
    return value if value in {node.id for node in NODES} else None


def _supervision_assignment(item: Any) -> dict | None:
    """Eén opdracht uit het dossier (``_assignment_brief`` van Runner 2): welke taak op welke Runner, met welke uitkomst."""
    if not isinstance(item, dict) or not isinstance(item.get("assignment_id"), str):
        return None
    task_id = item.get("task_id") if isinstance(item.get("task_id"), str) and TASK_ID.match(item["task_id"]) else None
    err = item.get("last_error")
    error = _clip(err.get("message") if isinstance(err, dict) else err, 240)
    observed = item.get("observed") if isinstance(item.get("observed"), dict) else {}
    return {"assignment_id": _clip(item["assignment_id"], 20), "task_id": task_id, "chain_id": _clip(item.get("chain_id"), 40),
            "node": _node_id(item.get("node")), "node_label": _clip(item.get("node"), 20), "goal": _clip(item.get("goal"), 240),
            "status": _clip(item.get("status"), 20), "outcome": _clip(item.get("outcome"), 40),
            "task_status": _clip(observed.get("task_status"), 30),
            "started_at": _iso_utc(item.get("started_at")), "ended_at": _iso_utc(item.get("ended_at")), "error": error}


def supervision_actions(project: dict) -> list[dict]:
    """Wat een mens of de ChatGPT-supervisor kan doen om dit project weer op gang te brengen.

    Alleen bestaande handelingen van Runner 2 (``runner2_supervision_claim``/``_decide``/``_admin`` en de taakknoppen);
    Monitor 2 voert hier zelf niets uit. Volgorde: de handeling die het punt oplost eerst."""
    pid, state = project.get("project_id") or "?", project.get("state")
    event = project.get("event") or {}
    task = project.get("assignment") or {}
    where = f"{task['task_id']} op {task.get('node_label') or '?'}" if task.get("task_id") else None
    out: list[dict] = []
    if state in ("needed", "claimed"):
        what = f"event {event['id']} ({SUPERVISION_EVENT_TEXT.get(event.get('kind'), event.get('kind'))})" if event.get("id") else "het open event"
        out.append({"title": "ChatGPT laten beslissen",
                    "how": f"Vraag de ChatGPT-supervisor om {what} van project {pid} te claimen (runner2_supervision_claim) en te beslissen "
                           f"(runner2_supervision_decide): doel bereikt, werk resteert of menselijke blokkade."})
    elif state == "blocked" and project.get("integrity"):
        out.append({"title": "Dossier herstellen",
                    "how": f"Controleer de Runner-opslag en herstel het dossier: runner2_supervision_admin action=repair project_id={pid} "
                           f"(mode=restore_prev, reseal met confirm=true, of accept)."})
    elif state == "blocked":
        target = f" event_id={event['id']}" if event.get("id") else ""
        out.append({"title": "Event vrijgeven",
                    "how": f"Geef het geëscaleerde event vrij: runner2_supervision_admin action=release_event project_id={pid}{target}; "
                           f"daarna kan de supervisor het opnieuw claimen."})
    elif state == "waiting_human":
        ids = [b["id"] for b in project.get("blockers") or [] if b.get("id")]
        if ids:
            out.append({"title": "Blokkade beantwoorden",
                        "how": f"Beantwoord de open vraag en sluit de blokkade: runner2_supervision_admin action=resolve_blocker "
                               f"project_id={pid} blocker_ids={','.join(ids)} met een toelichting (note)."})
        if where and task.get("task_status") == "waiting_for_human":
            out.append({"title": "Vraag van de taak beantwoorden",
                        "how": f"Taak {where} wacht op een antwoord: gebruik Beantwoorden op de kaart van die machine."})
    elif state == "limit":
        out.append({"title": "Limiet verruimen of afsluiten",
                    "how": f"Verruim de limieten (runner2_supervision_admin action=set_limits project_id={pid}) of sluit het project af (action=stop)."})
    if where:
        out.append({"title": "Taakrapport lezen", "how": f"Lees het volledige eindrapport van {where} (knop Lees taakrapport)."})
    out.append({"title": "Pauzeren of stoppen",
                "how": f"Is het project niet meer nodig: runner2_supervision_admin action=pause of action=stop project_id={pid}."})
    return out


def supervision_request(project: dict) -> str:
    """Een kant-en-klaar verzoek om aan de ChatGPT-supervisor te geven (kopieerknop); alleen feiten uit Runner 2."""
    pid = project.get("project_id") or "?"
    lines = [f"Runner 2-supervisie: project {pid} ({project.get('title') or pid}) staat op {project.get('label') or project.get('state')}."]
    if project.get("next_step"):
        lines.append(f"Volgende stap volgens Runner 2: {project['next_step']}")
    task = project.get("assignment") or {}
    if task.get("assignment_id"):
        lines.append(f"Laatste opdracht {task['assignment_id']}: taak {task.get('task_id') or '?'} op {task.get('node_label') or '?'}"
                     f" ({task.get('status') or '?'}{', uitkomst ' + task['outcome'] if task.get('outcome') else ''}).")
    lines.append(f"Lees de context met runner2_supervision_context (project_id={pid}), claim het open event met runner2_supervision_claim "
                 f"en beslis met runner2_supervision_decide.")
    return "\n".join(lines)


def describe_supervision(listing: dict | None, details: dict[str, dict] | None = None) -> dict | None:
    """Het supervisieblok uit ``controller.status()`` (``supervision``): per project label, volgende stap en laatste checkpoint.

    Per project ook wat het venster achter de badge SUPERVISIE NODIG toont: reden, open event, blokkades, status en de
    betreffende opdracht (Runner, taak-id). ``details`` is per project ``runner2.supervision.status(project_id)`` van dezelfde
    lezing (recente opdrachten); zonder (remote node, oudere Runner 2) blijft alleen wat het overzicht zelf geeft.
    ``None`` als Runner 2 geen supervisieprojecten kent (of een oudere versie draait); rommel breekt niets.
    """
    block = (listing or {}).get("supervision")
    if not isinstance(block, dict) or not isinstance(block.get("projects"), list):
        return None
    details = details if isinstance(details, dict) else {}
    projects = []
    for item in block["projects"][:MAX_SUPERVISION_PROJECTS]:
        if not isinstance(item, dict) or not item.get("project_id"):
            continue
        cp = item.get("checkpoint") if isinstance(item.get("checkpoint"), dict) else {}
        project = {
            "project_id": _clip(item.get("project_id"), 40), "title": _clip(item.get("title"), 80) or _clip(item.get("project_id"), 40),
            "label": _clip(item.get("label"), 40), "state": _clip(item.get("state"), 20), "attention": bool(item.get("attention")),
            "next_step": _clip(item.get("next_step"), 240), "checkpoint_at": _iso_utc(cp.get("at")),
            "checkpoint_revision": cp.get("revision") if isinstance(cp.get("revision"), int) else None,
            "blocked": item.get("state") == "blocked",
        }
        if project["attention"]:
            project.update(_supervision_attention(item, details.get(item.get("project_id"))))
            project["actions"] = supervision_actions(project)
            project["request"] = supervision_request(project)
        projects.append(project)
    attention = [p for p in projects if p["attention"]]
    return {"projects": projects, "attention": len(attention),
            "headline": ("SUPERVISIE GEBLOKKEERD" if any(p["blocked"] for p in attention) else "SUPERVISIE NODIG") if attention else None}


def _supervision_attention(item: dict, detail: Any) -> dict:
    """De velden voor het supervisievenster van één project met aandacht (zuiver; onleesbare delen vallen weg)."""
    ev = item.get("event") if isinstance(item.get("event"), dict) else {}
    event = ({"id": _clip(ev.get("id"), 20), "kind": _clip(ev.get("kind"), 30), "state": _clip(ev.get("state"), 20),
              "summary": _clip(ev.get("summary"), 240), "created_at": _iso_utc(ev.get("created_at"))} if ev.get("id") else None)
    integrity = item.get("integrity") if isinstance(item.get("integrity"), dict) else {}
    control = item.get("control") if isinstance(item.get("control"), dict) else {}
    backlog = item.get("backlog") if isinstance(item.get("backlog"), dict) else {}
    usage = item.get("usage") if isinstance(item.get("usage"), dict) else {}
    last = item.get("last_decision") if isinstance(item.get("last_decision"), dict) else None
    blockers = [{"id": _clip(b.get("id"), 20), "question": _clip(b.get("question"), 240)}
                for b in (item.get("blockers_open") if isinstance(item.get("blockers_open"), list) else [])
                if isinstance(b, dict) and b.get("id")][:5]
    # De betreffende opdracht: de lopende (overzicht), anders de laatste uit het dossier (details).
    recent = detail.get("recent_assignments") if isinstance(detail, dict) and isinstance(detail.get("recent_assignments"), list) else []
    history = [a for a in (_supervision_assignment(x) for x in reversed(recent)) if a]
    assignment = _supervision_assignment(item.get("assignment")) or (history[0] if history else None)
    return {
        "reason": SUPERVISION_REASONS.get(item.get("state")) or "Runner 2 vraagt aandacht voor dit supervisieproject.",
        "goal": _clip(item.get("goal"), 240), "event": event, "events_open": _int(item.get("events_open")),
        "blockers": blockers,
        "integrity": _clip(integrity.get("message"), 240) if integrity.get("status") == "blocked" else None,
        "control": _clip(control.get("state"), 20),
        "backlog": {k: _int(backlog.get(k)) for k in ("open", "done", "blocked")},
        "usage": {k: _clip(usage.get(k), 20) for k in ("assignments", "no_progress")},
        "last_decision": ({"kind": _clip(last.get("kind"), 30), "at": _iso_utc(last.get("at")), "by": _clip(last.get("by"), 40),
                           "reason": _clip(last.get("reason"), 200)} if last else None),
        "assignment": assignment,
        "assignments": history[:5],
    }


def open_action_items(listing: dict | None, states: dict[str, dict]) -> list[dict]:
    """De openstaande punten van deze node uit ``human_actions.items`` van dezelfde ``controller.status()``-lezing.

    Alleen lezen en node-lokaal: elke Runner 2 geeft alleen zijn eigen punten, en Monitor 2 heeft er geen tweede opslag
    voor. De brontaak krijgt zijn titel uit de gelezen states; onleesbare items vallen weg."""
    block = (listing or {}).get("human_actions")
    items = block.get("items") if isinstance(block, dict) else None
    out: list[dict] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict) or item.get("status") not in (None, "open"):
            continue
        text = item.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        source = item.get("source") if isinstance(item.get("source"), dict) else {}
        task_id = source.get("task_id") if isinstance(source.get("task_id"), str) else None
        title = (states.get(task_id) or {}).get("title") if task_id else None
        reason, details = item.get("reason"), item.get("details")
        out.append({"id": item.get("id"), "text": text.strip()[:MAX_TEXT],
                    "reason": reason.strip()[:MAX_TEXT] if isinstance(reason, str) and reason.strip() else None,
                    "details": details.strip()[:MAX_TEXT] if isinstance(details, str) and details.strip() else None,
                    "created_at": _iso_utc(item.get("created_at")),
                    "source": {"task_id": task_id, "title": _clip(title, 120), "role": source.get("role"),
                               "chain_id": source.get("chain_id"), "supervisor_id": source.get("supervisor_id")}})
    return out


def allowed_actions(key: str) -> dict:
    """Welke knoppen zinvol zijn; de controller weigert de rest zelf ook (``wrong_state``)."""
    return {
        "pause": key == "bezig",
        "resume": key in ("gepauzeerd", "mislukt"),
        "stop": key in ("bezig", "gepauzeerd", "wacht_op_actie", "herstellen", "mislukt"),
        "answer": key == "wacht_op_actie",
    }


def describe_chain(brief: dict | None) -> dict | None:
    """Het ketenblok uit ``controller.status(task_id)`` (``chain``); ``None`` voor een gewone taak."""
    chain = (brief or {}).get("chain")
    if not isinstance(chain, dict) or not chain.get("id"):
        return None
    stop = chain.get("stopped_because") if isinstance(chain.get("stopped_because"), dict) else None
    out = {"id": chain.get("id"), "state": chain.get("state"), "phase": chain.get("phase"),
           "link": chain.get("link"), "links_total": chain.get("links_total"),
           "current_task": chain.get("current_task"),
           "warning": _clip(chain.get("warning"), 200),
           "stopped_because": {"kind": stop.get("kind"), "message": _clip(stop.get("message"), 300)} if stop else None,
           "ended_at": _iso_utc(stop.get("at")) if stop else None}
    if gate := describe_completion_gate(chain.get("completion_gate")):
        out["completion_gate"] = gate
    return out


def describe_completion_gate(gate: object) -> dict | None:
    """Het blok ``completion_gate`` van een keten: waarom een 'doel bereikt' is geaccepteerd of geweigerd (alleen weergave)."""
    if not isinstance(gate, dict):
        return None
    last = gate.get("last") if isinstance(gate.get("last"), dict) else None
    now = gate.get("now") if isinstance(gate.get("now"), dict) else None
    return {"checks": gate.get("checks"), "refused": gate.get("refused"), "accepted": gate.get("accepted"),
            "last": {"decision": last.get("decision"), "link": last.get("link"), "summary": _clip(last.get("summary"), 400)}
            if last else None,
            "now": {"status": now.get("status"), "note": _clip(now.get("note"), 200), "blocked_on": _clip(now.get("blocked_on"), 200)}
            if now else None}


SUPERVISOR_ACTIVE = "active"
SUPERVISOR_COMPLETE = "project_complete"
SUPERVISOR_USER_STOPS = ("stopped_by_user", "task_stopped")


def describe_supervisor(brief: dict | None) -> dict | None:
    """Het supervisorblok uit ``controller.status(task_id)`` (``supervisor``); ``None`` voor een taak zonder supervisor."""
    sup = (brief or {}).get("supervisor")
    if not isinstance(sup, dict) or not sup.get("id"):
        return None
    stop = sup.get("stopped_because") if isinstance(sup.get("stopped_because"), dict) else None
    decision = sup.get("last_decision") if isinstance(sup.get("last_decision"), dict) else None
    return {"id": sup.get("id"), "state": sup.get("state"), "phase": sup.get("phase"),
            "chain": sup.get("chain"), "max_chains": sup.get("max_chains"), "chain_kind": sup.get("chain_kind"),
            "current_task": sup.get("current_task"), "commits_total": sup.get("commits_total"),
            "deadline": _iso_utc(sup.get("deadline")) if isinstance(sup.get("deadline"), str) else None,
            "last_decision": {"decision": decision.get("decision"), "reason": _clip(decision.get("reason"), 300)}
            if decision else None,
            "next_goal": _clip(sup.get("next_goal"), 300),
            "warning": _clip(sup.get("warning"), 200),
            "stopped_because": {"kind": stop.get("kind"), "message": _clip(stop.get("message"), 300)} if stop else None,
            "ended_at": _iso_utc(stop.get("at")) if stop else None}


def supervisor_state_key(key: str, sup: dict | None) -> str:
    """De getoonde toestand van een taak die bij een supervisorrun hoort.

    Een voltooide schakel of beoordeling is geen einde zolang de supervisor actief is: dan is de run BEZIG
    (de volgende keten start vanzelf). Echt KLAAR is de run pas bij ``project_complete``; een limiet, blokkade
    of fout van de supervisor is MISLUKT (met de reden), en een stop door een gebruiker is GESTOPT."""
    if not sup or key != "klaar":
        return key
    state = sup.get("state")
    if state == SUPERVISOR_ACTIVE:
        return "bezig"
    if state == SUPERVISOR_COMPLETE:
        return "klaar"
    stop = sup.get("stopped_because") or {}
    if state == "stopped" or stop.get("kind") in SUPERVISOR_USER_STOPS:
        return "gestopt"
    return "mislukt"


def describe_task(state: dict, brief: dict | None, *, recovering: bool, watch_healthy: bool | None,
                  max_attempts: int | None = None) -> dict:
    """Eén taak zoals de pagina hem toont. ``brief`` is wat ``controller.status(task_id)`` teruggeeft."""
    brief = brief or {}
    supervisor = describe_supervisor(brief)
    key = supervisor_state_key(state_key(state, recovering=recovering), supervisor)
    label, tone = STATES[key]
    view: dict[str, Any] = {
        "task_id": state.get("task_id"),
        "title": _clip(state.get("title") or brief.get("title"), 120),
        "prompt": _clip(_first_prompt(state), 600),
        "status": state.get("status"),
        "state": key, "label": label, "tone": tone,
        "model": state.get("model"), "model_badge": model_badge(state.get("model")),
        "role": state.get("role"),
        "elapsed_s": brief.get("elapsed_s"),
        "finished_at": (supervisor["ended_at"] if supervisor else _iso_utc(state.get("updated"))) if key == "klaar" else None,
        "turn": brief.get("turn", state.get("turn")),
        "turn_elapsed_s": brief.get("turn_elapsed_s"),
        "session_id": state.get("session_id"),
        "worktree": state.get("worktree"),
        "branch": state.get("branch"),
        "activity": [_clip(item, 140) for item in (brief.get("activity") or []) if isinstance(item, str)],
        "pausing": bool(brief.get("pausing")),
        "process_alive": bool(brief.get("active")),
        "actions": allowed_actions(key),
        "chain": describe_chain(brief),
        "supervisor": supervisor,
        "question": None, "recovery": None, "failure": None, "summary": None, "tests": None, "stop": None,
        "warnings": [_clip(w, 200) for w in (state.get("warnings") or [])[-3:] if isinstance(w, str)],
    }
    question = brief.get("question") if isinstance(brief.get("question"), dict) else None
    if key == "wacht_op_actie":
        # De vraag zelf: uit de controllerstatus, met de ruwe state als terugval.
        raw = state.get("question") if isinstance(state.get("question"), dict) else {}
        question = question or {"text": raw.get("question"), "reason": raw.get("why_needed"),
                                "kind": raw.get("kind"), "recommendation": raw.get("recommendation"),
                                "alternatives": raw.get("alternatives") or []}
        view["question"] = {
            "text": _clip(question.get("text"), MAX_TEXT) or "Runner 2 wacht op een antwoord, maar meldt geen vraagtekst.",
            "reason": _clip(question.get("reason"), 1200),
            "kind": question.get("kind"),
            "recommendation": _clip(question.get("recommendation"), 1200),
            "alternatives": [_clip(a, 300) for a in (question.get("alternatives") or []) if isinstance(a, (str, int, float))][:8],
            "asked_at": question.get("asked_at") or state.get("updated"),
        }
    recovery = state.get("recovery") if isinstance(state.get("recovery"), dict) else {}
    attempts = recovery.get("attempts") or 0
    if key == "herstellen" or attempts:
        view["recovery"] = {"attempts": attempts, "max_attempts": max_attempts, "active": key == "herstellen",
                            "watch_healthy": watch_healthy,
                            "same_session": bool(state.get("session_id")),
                            "cause": (state.get("failure") or {}).get("kind") if isinstance(state.get("failure"), dict)
                            else None}
    failure = state.get("failure") if isinstance(state.get("failure"), dict) else None
    if failure and key in ("mislukt", "herstellen"):
        view["failure"] = {"kind": failure.get("kind"), "message": _clip(failure.get("message"), 500)}
    if supervisor and key == "mislukt" and not view["failure"] and supervisor["stopped_because"]:
        view["failure"] = {"kind": supervisor["stopped_because"]["kind"], "message": supervisor["stopped_because"]["message"]}
    finish = state.get("finish") if isinstance(state.get("finish"), dict) else None
    if finish:
        view["summary"] = _clip(finish.get("summary"), 1200)
        view["tests"] = {"run": [_clip(t, 200) for t in (finish.get("tests_run") or []) if isinstance(t, str)][:10],
                         "result": _clip(finish.get("test_results"), 600)}
    if isinstance(state.get("stop"), dict):
        view["stop"] = {"reason": _clip(state["stop"].get("reason"), 300), "at": state["stop"].get("at"),
                        "from": state["stop"].get("from")}
    return view


def current_task_id(listing: dict, states: dict[str, dict], recovering: Callable[[dict], bool]) -> str | None:
    """Welke taak de kaart standaard toont: de actieve; anders de nieuwste die nog niet klaar is
    (gepauzeerd, wachtend, herstellend); anders de nieuwste. Een actieve supervisorrun gaat voor. ``listing`` = ``controller.status()``."""
    active = listing.get("active_task")
    if active:
        return active
    for sup in listing.get("supervisors") or []:  # een actieve supervisorrun toont zijn huidige taak, ook tussen ketens
        if isinstance(sup, dict) and sup.get("state") == SUPERVISOR_ACTIVE and sup.get("current_task") in states:
            return sup["current_task"]
    briefs = listing.get("tasks") or []  # nieuwste eerst
    for brief in briefs:
        st = states.get(brief.get("task_id"))
        if st and (st.get("status") in ("paused", "waiting_for_human", "running", "created") or recovering(st)):
            return brief["task_id"]
    return briefs[0]["task_id"] if briefs else None


# ----------------------------------------------------------------------
# Eindrapport van een afgeronde taak (zuiver, geen I/O)
# ----------------------------------------------------------------------


def has_report(state: dict | None) -> bool:
    """Een afgeronde taak heeft een leesbaar rapport als Claude bij ``runner_finish`` een samenvatting meegaf."""
    if not isinstance(state, dict) or state.get("status") != "completed":
        return False
    finish = state.get("finish")
    summary = finish.get("summary") if isinstance(finish, dict) else None
    return isinstance(summary, str) and bool(summary.strip())


def pick_report(shown_id: str | None, briefs: list[dict], states: dict[str, dict]) -> dict | None:
    """Welke afgeronde taak de kaart als rapport aanbiedt: de getoonde taak als die klaar is, anders het
    laatst afgeronde rapport op deze machine (``briefs`` is nieuwste eerst). Zo blijft een rapport
    bereikbaar nadat er een nieuwe taak is gestart."""
    ids = ([shown_id] if shown_id else []) + [b.get("task_id") for b in briefs]
    for task_id in ids:
        state = states.get(task_id)
        if has_report(state):
            return {"task_id": task_id, "title": _clip(state.get("title"), 120),
                    "finished_at": _iso_utc(state.get("updated")), "current": task_id == shown_id}
    return None


def _items(values: Any) -> list[str]:
    out: list[str] = []
    for value in values if isinstance(values, list) else []:
        if isinstance(value, str):
            text = value.strip()
        elif isinstance(value, dict):
            text = " — ".join(str(value[k]).strip() for k in ("text", "reason") if value.get(k))
        else:
            text = ""
        if text:
            out.append(text[:MAX_TEXT])
    return out[:MAX_REPORT_ITEMS]


def _text(value: Any) -> str | None:
    return value.strip()[:MAX_REPORT_CHARS] if isinstance(value, str) and value.strip() else None


def _yes_no(value: Any) -> str:
    return "ja" if value else "nee"


def describe_report(result: dict) -> dict:
    """Het eindresultaat van ``runner2_result`` als leesbare secties plus één platte tekst (voor Kopiëren).

    Alles komt uit het resultaat zelf; er wordt niets bijgemaakt. Lege onderdelen vallen weg."""
    sections: list[dict] = []

    def add(title: str, *, text: str | None = None, items: list[str] | None = None) -> None:
        if text or items:
            sections.append({"title": title, **({"text": text} if text else {}), **({"items": items} if items else {})})

    final = result.get("final_status")
    facts = [("Taak", result.get("task_id")), ("Titel", result.get("title")), ("Eindstatus", final),
             ("Model", model_badge(result.get("model"))), ("Looptijd", _duration_text(result.get("elapsed_s"))),
             ("Branch", result.get("branch")), ("Worktree", result.get("worktree")),
             ("Repository", result.get("repository")), ("Sessie", result.get("session_id"))]
    add("Taak", items=[f"{label}: {value}" for label, value in facts if isinstance(value, str) and value.strip()])
    add("Samenvatting", text=_text(result.get("summary")))
    add("Aandachtspunten en vervolg", items=_items(result.get("attention")))
    add("Wijzigingen (zoals Claude meldde)", items=_items(result.get("reported_changes")))
    commits = [c for c in (result.get("commits") or []) if isinstance(c, str)]
    add("Commits", items=commits[:MAX_REPORT_ITEMS])
    files = [f for f in (result.get("files_changed") or []) if isinstance(f, str)]
    add("Gewijzigde bestanden", items=files[:MAX_REPORT_ITEMS])
    if result.get("uncommitted"):
        add("Waarschuwing", text="Er staan nog niet-gecommitte wijzigingen in de worktree.")
    tests = result.get("tests") if isinstance(result.get("tests"), dict) else {}
    add("Tests", text=_text(tests.get("result")), items=_items(tests.get("run")))
    dry = result.get("integration_dry_run") if isinstance(result.get("integration_dry_run"), dict) else {}
    if dry.get("error"):
        add("Integratie (dry-run)", text=_text(str(dry["error"])))
    elif dry:
        add("Integratie (dry-run, er is niets geïntegreerd)", items=[
            f"Basis: {dry.get('base_ref')}", f"Commits voor op de basis: {dry.get('commits_ahead')}",
            f"Fast-forward: {_yes_no(dry.get('fast_forward'))}", f"Conflicten: {_yes_no(dry.get('conflicts'))}",
            f"Worktree schoon: {_yes_no(dry.get('worktree_clean'))}"])
    chain = result.get("chain") if isinstance(result.get("chain"), dict) else None
    if chain:
        links = [" · ".join(str(part) for part in (f"Schakel {l.get('link')}", l.get("task_id"),
                                                    f"{l['commits']} commits" if l.get("commits") is not None else None,
                                                    l.get("chain_status")) if part)
                 + (f" — {l['summary']}" if l.get("summary") else "")
                 for l in chain.get("tasks") or [] if isinstance(l, dict)]
        stop = chain.get("stopped_because") if isinstance(chain.get("stopped_because"), dict) else {}
        add("Keten", text=" · ".join(str(p) for p in (chain.get("state"), _text(chain.get("goal")),
                                                      stop.get("message")) if p) or None, items=links[:MAX_REPORT_ITEMS])
        gate = chain.get("completion_gate") if isinstance(chain.get("completion_gate"), dict) else None
        if gate:
            checks = [h["summary"] for h in gate.get("history") or []
                      if isinstance(h, dict) and h.get("summary")]
            add("Voltooiingscontrole", text="Meldt een schakel 'doel bereikt', dan laat Runner 2 dat eerst onafhankelijk "
                                            "controleren aan de hand van de roadmap/backlog van het project.",
                items=checks[:MAX_REPORT_ITEMS])
    sup = result.get("supervisor") if isinstance(result.get("supervisor"), dict) else None
    if sup:
        decision = sup.get("last_decision") if isinstance(sup.get("last_decision"), dict) else {}
        lines = [f"Staat: {sup.get('state')}", f"Ketens: {sup.get('chain')} van maximaal {sup.get('max_chains')}",
                 f"Commits totaal: {sup.get('commits_total')}" if sup.get("commits_total") is not None else None,
                 f"Laatste beslissing: {decision.get('decision')} — {decision.get('reason')}" if decision.get("decision") else None]
        add("Autonome run", text=_text(sup.get("goal")), items=[line for line in lines if line])
    stop = result.get("stop") if isinstance(result.get("stop"), dict) else {}
    add("Gestopt", text=_text(stop.get("reason")))
    error = result.get("error") if isinstance(result.get("error"), dict) else {}
    add("Fout", text=_text(f"{error.get('kind')}: {error.get('message')}") if error else None)
    add("Gitfout", text=_text(result.get("git_error")))
    actions = result.get("human_actions") if isinstance(result.get("human_actions"), dict) else {}
    if isinstance(actions.get("open"), int) and actions["open"] > 0:
        add("Openstaande actiepunten", text=f"Er staan nog {actions['open']} menselijke actiepunten open op deze run; "
                                            "de inhoud ervan staat niet in dit rapport.")
    return {"task_id": result.get("task_id"), "title": _clip(result.get("title"), 160), "final_status": final,
            "sections": sections, "text": report_text(sections)}


def report_text(sections: list[dict]) -> str:
    """De platte tekst van de secties: precies wat het venster toont, en wat Kopiëren op het klembord zet."""
    blocks = []
    for section in sections:
        lines = [section["title"].upper()]
        if section.get("text"):
            lines.append(section["text"])
        lines.extend("- " + item for item in section.get("items") or [])
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _duration_text(seconds: Any) -> str | None:
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or seconds < 0:
        return None
    minutes = int(seconds) // 60
    return f"{minutes // 60} u {minutes % 60:02d} m" if minutes >= 60 else f"{minutes} min" if minutes else f"{int(seconds)} s"


# ----------------------------------------------------------------------
# Backends
# ----------------------------------------------------------------------

class ReviewUnsupported(Exception):
    """De node kent het opschoonvoorstel niet (oude Runner 2) of gaf iets terug dat geen plan is: dan tonen we niets."""


_REVIEW_BUCKETS = ("would_close", "review", "needs_decision")


def _review_entry(entry: Any) -> dict | None:
    if not isinstance(entry, dict) or not isinstance(entry.get("id"), str) or not isinstance(entry.get("text"), str):
        return None
    out: dict[str, Any] = {"id": entry["id"], "text": _clip(entry["text"], 400) or "", "label": _clip(entry.get("label"), 200),
                           "task_id": entry.get("task_id") if isinstance(entry.get("task_id"), str) else None,
                           "created_at": entry.get("created_at") if isinstance(entry.get("created_at"), str) else None}
    if isinstance(entry.get("rule"), str):
        out["rule"] = entry["rule"]
    if isinstance(entry.get("repo"), str) and entry["repo"]:
        out["repo"] = entry["repo"]
    evidence = entry.get("evidence") if isinstance(entry.get("evidence"), dict) else None
    item = evidence.get("item") if evidence and isinstance(evidence.get("item"), dict) else None
    if item and isinstance(item.get("id"), str):
        out["evidence_id"] = item["id"]
        out["evidence_kind"] = evidence.get("kind") if isinstance(evidence.get("kind"), str) else None
        out["evidence_text"] = _clip(item.get("text"), 200)
        out["evidence_task_id"] = item.get("task_id") if isinstance(item.get("task_id"), str) else None
        out["evidence_note"] = _clip(item.get("resolved_note"), 200)
    elif isinstance(entry.get("evidence_id"), str):
        out["evidence_id"] = entry["evidence_id"]
    if isinstance(entry.get("protected"), list):
        out["protected"] = [str(x) for x in entry["protected"] if isinstance(x, str)][:10]
    if isinstance(entry.get("facts"), list):
        out["facts"] = [_clip(x, 240) for x in entry["facts"][:8] if isinstance(x, str)]
    if isinstance(entry.get("hint"), str) and entry["hint"]:
        out["hint"] = _clip(entry["hint"], 300)
    if isinstance(entry.get("possible_duplicate_of"), str):
        out["possible_duplicate_of"] = entry["possible_duplicate_of"]
    return out


def describe_action_review(node: Node, plan: Any) -> dict:
    """Het DRY RUN-plan van het actiebeheer (``runner2_actions review=true``) als blok voor het venster *Opschoonvoorstel*.

    Alleen lezen en zonder iets te verzinnen: geen plan, een plan van een andere node of zonder tellingen is
    ``ReviewUnsupported``. Dezelfde velden als ``runner2.actionreview.summary`` (ook een summary-blok is invoer), maar zonder
    de grens van 20 per bak: het venster toont ze allemaal (tot ``REVIEW_MAX_ITEMS``) en meldt eerlijk wat er niet bij staat.
    """
    if not isinstance(plan, dict) or not isinstance(plan.get("plan_id"), str) or not isinstance(plan.get("counts"), dict):
        raise ReviewUnsupported(REVIEW_UNSUPPORTED)
    if str(plan.get("node") or "").upper() != node.label.upper():
        raise ReviewUnsupported("Het antwoord hoort niet bij " + node.display + "; het wordt niet getoond.")
    counts = {}
    for key in ("open", *_REVIEW_BUCKETS):
        value = plan["counts"].get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ReviewUnsupported(REVIEW_UNSUPPORTED)
        counts[key] = value
    block: dict[str, Any] = {"node": node.id, "plan_id": plan["plan_id"], "mode": plan.get("mode"),
                             "generated_at": plan.get("generated_at") if isinstance(plan.get("generated_at"), str) else None,
                             "counts": counts, "closing_enabled": bool(plan.get("closing_enabled"))}
    breaker = plan.get("breaker")
    block["breaker"] = ({"message": _clip(breaker.get("message"), 400) or "De stroomonderbreker is afgegaan."}
                        if isinstance(breaker, dict) else None)
    shown = 0
    for bucket in _REVIEW_BUCKETS:
        raw = plan.get(bucket) if isinstance(plan.get(bucket), list) else []
        entries = [e for e in (_review_entry(x) for x in raw[:REVIEW_MAX_ITEMS]) if e]
        block[bucket] = entries
        shown += len(entries)
    block["possible_duplicates"] = sum(1 for bucket in ("review", "needs_decision") for e in block[bucket] if e.get("possible_duplicate_of"))
    hidden = counts["would_close"] + counts["review"] + counts["needs_decision"] - shown
    block["not_shown"] = max(0, hidden)
    decide = counts["review"] + counts["needs_decision"]
    head = f"{node.display}: {counts['open']} open \u00b7 zou sluiten {counts['would_close']} \u00b7 vereist beslissing {decide}"
    if counts["review"]:
        head += f" (waarvan {counts['review']} twijfelgeval{'len' if counts['review'] != 1 else ''})"
    block["headline"] = head
    return block


class Runner2Backend(Protocol):
    kind: str

    def snapshot(self, selected: str | None = None) -> dict: ...
    def action(self, action: str, task_id: str, **fields: str) -> tuple[int, dict]: ...
    def report(self, task_id: str) -> tuple[int, dict]: ...
    def action_review(self, refresh: bool = False) -> tuple[int, dict]: ...
    def action_history(self) -> tuple[int, dict]: ...
    def health(self) -> dict: ...
    def close(self) -> None: ...


class NotInstalledBackend:
    """Een node zonder Runner 2: eerlijk en zonder terugval op iets anders."""

    kind = "not_installed"

    def __init__(self, node: Node):
        self.node = node

    def snapshot(self, selected: str | None = None) -> dict:  # noqa: ARG002
        return {"installed": False, "available": False, "operational": False,
                "headline": "NIET GEÏNSTALLEERD", "tone": "off",
                "message": f"Runner 2 is op {self.node.display} nog niet geïnstalleerd.",
                "watch": None, "task": None, "tasks": [], "runner2": None}

    def action(self, action: str, task_id: str, **fields: str) -> tuple[int, dict]:  # noqa: ARG002
        return 409, {"ok": False, "reason": "not_installed",
                     "message": f"Runner 2 is op {self.node.display} nog niet geïnstalleerd."}

    def report(self, task_id: str) -> tuple[int, dict]:  # noqa: ARG002
        return 409, {"ok": False, "reason": "not_installed",
                     "message": f"Runner 2 is op {self.node.display} nog niet geïnstalleerd."}

    def action_review(self, refresh: bool = False) -> tuple[int, dict]:  # noqa: ARG002
        return 409, {"ok": False, "reason": "not_installed",
                     "message": f"Runner 2 is op {self.node.display} nog niet geïnstalleerd."}

    def action_history(self) -> tuple[int, dict]:
        return 409, {"ok": False, "reason": "not_installed",
                     "message": f"Runner 2 is op {self.node.display} nog niet geïnstalleerd."}

    def health(self) -> dict:
        return {"kind": self.kind, "installed": False}

    def close(self) -> None:
        return None


class NoRouteBackend:
    """Een node waar deze machine (nog) geen route naartoe heeft: eerlijk 'niet bereikbaar vanaf hier'.

    Geen 'niet geïnstalleerd' (dat weten we niet) en nooit een verzonnen RUNNER ACTIEF: er is geen lezing.
    ``route_missing`` laat de pagina dit onderscheiden van een node die wel een route heeft maar uitvalt.
    """

    kind = "no_route"

    def __init__(self, node: Node, local: Node):
        self.node, self.local = node, local

    def _message(self) -> str:
        return (f"{self.local.display} heeft nog geen werkende route naar {self.node.display} (geen ssh-route voor {self.node.label} "
                f"in nodes.toml of de machine is niet bereikbaar); de Runner-2-status van {self.node.display} kan hier niet worden gelezen.")

    def snapshot(self, selected: str | None = None) -> dict:  # noqa: ARG002
        return {"installed": True, "available": False, "operational": False, "route_missing": True,
                "headline": f"NIET BEREIKBAAR VANAF {self.local.display}", "tone": "warn", "message": self._message(),
                "watch": None, "task": None, "tasks": [], "runner2": None}

    def action(self, action: str, task_id: str, **fields: str) -> tuple[int, dict]:  # noqa: ARG002
        return 503, {"ok": False, "reason": "node_unreachable", "message": self._message()}

    def report(self, task_id: str) -> tuple[int, dict]:  # noqa: ARG002
        return 503, {"ok": False, "reason": "node_unreachable", "message": self._message()}

    def action_review(self, refresh: bool = False) -> tuple[int, dict]:  # noqa: ARG002
        return 503, {"ok": False, "reason": "node_unreachable", "message": self._message()}

    def action_history(self) -> tuple[int, dict]:
        return 503, {"ok": False, "reason": "node_unreachable", "message": self._message()}

    def health(self) -> dict:
        return {"kind": self.kind, "installed": True, "route": False}

    def close(self) -> None:
        return None


def local_node_id() -> str:
    value = (os.environ.get(LOCAL_NODE_ENV) or "mac").strip().lower()
    return value if value in {node.id for node in NODES} else "mac"


def _source_candidates() -> list[Path]:
    env = os.environ.get(SOURCE_ENV)
    raw = [env] if env else list(DEFAULT_SOURCE_CANDIDATES)
    return [Path(item).expanduser() for item in raw]


def load_runner2_modules() -> Any:
    """``runner2.controller`` / ``store`` / ``watch``, uit een al importeerbare ``runner2`` of uit de bronmap.

    Runner 2 zit niet in deze repository; de bronmap is ``$OBF_RUNNER2_SRC`` of de standaardplek.
    """
    try:
        return _import_modules()
    except ImportError:
        pass
    for candidate in _source_candidates():
        if (candidate / "runner2" / "controller.py").is_file():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return _import_modules()
    raise ImportError("Runner 2 is niet gevonden (zet OBF_RUNNER2_SRC op de map met het pakket runner2).")


def load_runner2_remote() -> Any:
    """Laadt alleen het lichte node-transport, ook als Runner 2 in een losse bronmap staat."""
    try:
        return importlib.import_module("runner2.remote")
    except ImportError:
        pass
    for candidate in _source_candidates():
        if (candidate / "runner2" / "remote.py").is_file():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return importlib.import_module("runner2.remote")
    raise ImportError("Runner-2-nodetransport is niet gevonden.")


def _import_modules() -> Any:
    class Modules:
        controller = importlib.import_module("runner2.controller")
        store = importlib.import_module("runner2.store")
        watch = importlib.import_module("runner2.watch")
        config = importlib.import_module("runner2.config")
        # Optioneel: een oudere Runner 2 zonder supervisie blijft gewoon werken (alleen geen supervisiedetails).
        supervision = _optional_module("runner2.supervision")

    return Modules


def _optional_module(name: str) -> Any:
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


class RemoteModules:
    """Dezelfde vorm als ``runner2.controller`` / ``store`` / ``watch`` / ``config``, maar voor een remote node.

    Een leesronde begint met ``controller.status()``: die haalt één bundel (lijst, states, details,
    herstelvlaggen, config) op via ``runner2.remote``; ``store``, ``watch`` en ``config`` lezen daaruit.
    """

    def __init__(self, node_name: str, remote: Any, controller_module: Any, *, bundle_seconds: float = 0.0,
                 clock: Callable[[], float] = time.monotonic):
        self._node, self._remote, self._bundle = node_name, remote, {}
        self._bundle_seconds, self._clock, self._bundle_at = bundle_seconds, clock, None
        self.controller = self._Controller(self, controller_module.ControllerError)
        self.store = self._Store(self)
        self.watch = self._Watch(self)
        self.config = self._Config(self)

    def _call(self, tool: str, args: dict | None = None) -> Any:
        return self._remote.call(self._node, tool, args or {})

    def _fetch_bundle(self) -> None:
        """Eén ssh-aanroep per ``bundle_seconds``, hoeveel tabbladen of taakkeuzes er ook lezen."""
        now = self._clock()
        if self._bundle_at is not None and self._bundle and 0 <= now - self._bundle_at < self._bundle_seconds:
            return
        self._bundle = self._call("bundle")
        self._bundle_at = now

    def invalidate(self) -> None:
        """Na een knopdruk: de volgende lezing haalt direct een verse bundel."""
        self._bundle_at = None

    class _Controller:
        def __init__(self, outer: "RemoteModules", error: type):
            self._o, self.ControllerError = outer, error

        def status(self, task_id: str | None = None) -> dict:
            if task_id is None:
                self._o._fetch_bundle()
                return self._o._bundle["listing"]
            return self._o._bundle["details"][task_id]

        def result(self, task_id: str) -> dict:
            return self._o._call("runner2_result", {"task_id": task_id})

        def pause(self, task_id: str) -> dict:
            return self._o._call("runner2_pause", {"task_id": task_id})

        def resume(self, task_id: str, note: str = "") -> dict:
            return self._o._call("runner2_resume", {"task_id": task_id, "note": note})

        def stop(self, task_id: str, reason: str = "") -> dict:
            return self._o._call("runner2_stop", {"task_id": task_id, "reason": reason})

        def answer(self, task_id: str, text: str) -> dict:
            return self._o._call("runner2_answer", {"task_id": task_id, "answer": text})

        def review_actions(self) -> dict:
            """Alleen het opschoonvoorstel (DRY RUN) ophalen; nooit samen met ``resolve``."""
            return self._o._call("runner2_actions", {"review": True})

        def list_actions(self, task_id: str | None = None, include_resolved: bool = False) -> dict:
            """Alleen lezen: open (en desgewenst afgehandelde) punten; geen ``resolve``, geen ``review``."""
            args: dict = {"include_resolved": bool(include_resolved)}
            if task_id:
                args["task_id"] = task_id
            return self._o._call("runner2_actions", args)

    class _Store:
        def __init__(self, outer: "RemoteModules"):
            self._o = outer

        def load(self, task_id: str) -> dict:
            return self._o._bundle["states"][task_id]

    class _Watch:
        def __init__(self, outer: "RemoteModules"):
            self._o = outer

        def needs_recovery(self, state: dict) -> bool:
            return bool(self._o._bundle["recovering"].get(state.get("task_id")))

    class _Config:
        def __init__(self, outer: "RemoteModules"):
            self._o = outer

        def load_config(self) -> dict:
            return self._o._bundle["config"]


def remote_modules(node: Node) -> RemoteModules | None:
    """``RemoteModules`` voor deze node als Runner 2 daar een route voor heeft (``nodes.toml``), anders None."""
    try:
        remote = load_runner2_remote()
        if node.label not in remote.nodes():
            return None
        base = load_runner2_modules()
        # Alleen dit proces: zonder dit blijft één hangende ssh-aanroep een minuut op de kaart staan.
        remote.CALL_TIMEOUT_S = min(float(getattr(remote, "CALL_TIMEOUT_S", REMOTE_CALL_TIMEOUT_S)), REMOTE_CALL_TIMEOUT_S)
        return RemoteModules(node.label, remote, base.controller, bundle_seconds=REMOTE_BUNDLE_SECONDS)
    except Exception:  # noqa: BLE001 -- geen route of oudere Runner 2: dan is de node "niet geïnstalleerd"
        return None


class LocalRunner2Backend:
    """De Runner 2 op deze machine, in dit proces aangeroepen via zijn eigen controllerfuncties."""

    kind = "local_runner2"

    def __init__(self, node: Node, *, modules: Any = None, clock: Callable[[], float] = time.time,
                 cache_seconds: float = CACHE_SECONDS, retry_after_failure: float = 0.0,
                 cold_start_grace: bool = False):
        self.node = node
        self._modules = modules
        self._load_error: str | None = None
        self._clock = clock
        self._cache_seconds = cache_seconds
        self._cache: tuple[float, str | None, dict] | None = None
        self._lock = threading.Lock()          # één lezing tegelijk per node (single-flight)
        self._action_lock = threading.Lock()   # één knopdruk tegelijk per node
        self._review_lock = threading.Lock()   # één opschoonvoorstel-lezing tegelijk per node
        self._review_cache: tuple[float, dict] | None = None
        self._last_good: dict[str | None, tuple[float, dict]] = {}   # keuze -> (moment, laatste geldige stand)
        self._failures = 0                     # opeenvolgende echte, mislukte leesrondes
        self._retry_after_failure = retry_after_failure
        self._cold_start_grace = cold_start_grace   # remote: zonder eerdere stand eerst 'verbinden', pas bij herhaling rood
        self._retry_at = 0.0                   # tot dan geen nieuwe poging na een mislukte ronde
        self._last_error: str | None = None
        self._read_started: float | None = None
        self._phase = "ok"                     # alleen om overgangen één keer te loggen
        self._repositories: list[str] | None = None   # repositories van de taken in de laatste geslaagde lezing

    # -- toegang tot Runner 2

    def _mods(self) -> Any:
        if self._modules is None and self._load_error is None:
            try:
                self._modules = load_runner2_modules()
            except ImportError as exc:
                self._load_error = str(exc)
        return self._modules

    def _unavailable(self, message: str) -> dict:
        return {"installed": True, "available": False, "operational": False, "headline": "RUNNER 2 NIET BEREIKBAAR",
                "tone": "alert", "message": message, "watch": None, "task": None, "tasks": [], "runner2": None}

    # -- lezen

    def snapshot(self, selected: str | None = None) -> dict:
        """De stand van deze node. Nooit een vals 'niet bereikbaar' door één trage of mislukte lezing:
        zolang er een eerdere geldige stand is, blijft die zichtbaar (met een vertraagd-markering) tot
        er overtuigend bewijs van uitval is. Een verse geldige lezing herstelt direct."""
        now = self._clock()
        cached = self._cache
        if cached and cached[1] == selected and now - cached[0] < self._cache_seconds:
            return cached[2]
        if not self._lock.acquire(blocking=False):
            # Er loopt al een lezing (mogelijk hangend): niet erachter aanschuiven en geen poolthread
            # vasthouden; toon de laatste geldige stand. Zonder zo'n stand is er niets te tonen: wachten.
            if selected in self._last_good:
                return self._present(selected, self._clock())
            self._lock.acquire()
        try:
            now = self._clock()
            cached = self._cache
            if cached and cached[1] == selected and now - cached[0] < self._cache_seconds:
                return cached[2]
            if now < self._retry_at:
                # Na een mislukte ronde niet meteen opnieuw ssh'en: toon de laatste geldige stand en tel
                # dit niet als extra bewijs van uitval.
                return self._present(selected, now)
            self._read_started = now
            try:
                result, error = self._read(selected)
            finally:
                self._read_started = None
            done = self._clock()
            if error is None:
                self._failures, self._last_error, self._retry_at = 0, None, 0.0
                self._last_good[selected] = (done, result)
                while len(self._last_good) > MAX_KEPT_VIEWS:
                    self._last_good.pop(next(iter(self._last_good)))
                self._transition("ok")
                view = result
            else:
                self._failures += 1
                self._last_error = error
                self._retry_at = done + self._retry_after_failure
                view = self._present(selected, done, failed=result)
            self._cache = (done, selected, view)
            return view
        finally:
            self._lock.release()

    def repositories(self) -> list[str] | None:
        """De repositories waarin Runner 2 op deze node taken heeft (laatste geslaagde lezing); ``None`` = nog niet gelezen.
        Voor de repositorykeuze van het opdrachtformulier (``jobs_repos``); start zelf geen lezing."""
        return list(self._repositories) if self._repositories is not None else None

    def fallback_view(self, selected: str | None = None) -> dict | None:
        """De laatste geldige stand (met vertraagd-markering), of ``None`` als die er nog niet is."""
        if selected not in self._last_good:
            return None
        return self._present(selected, self._clock())

    def _present(self, selected: str | None, now: float, *, failed: dict | None = None) -> dict:
        """Wat de kaart toont als er geen verse lezing is: de laatste geldige stand, vertraagd of uiteindelijk onbereikbaar."""
        last = self._last_good.get(selected)
        if last is None:
            if not self._cold_start_grace or self._failures >= UNREACHABLE_FAILURES or self._load_error is not None:
                self._transition("down")
                return failed or self._unavailable(self._last_error or "Er is nog geen gegevens van deze node ontvangen.")
            return self._connecting()
        seen_at, view = last
        age = max(0.0, now - seen_at)
        hung = self._read_started is not None and now - self._read_started >= HUNG_READ_S
        if age >= UNREACHABLE_AFTER_S and (self._failures >= UNREACHABLE_FAILURES or hung):
            self._transition("down", age)
            gone = failed or self._unavailable(self._last_error or "Geen antwoord van deze node.")
            return {**gone, "last_seen_s_ago": round(age), "failures": self._failures}
        if age >= DELAYED_AFTER_S:
            self._transition("delayed", age)
            return {**view, "data_delay": {"seconds": round(age), "failures": self._failures, "reason": self._last_error}}
        return view

    def _connecting(self) -> dict:
        """Nog nooit een geldige lezing en nog te weinig bewijs van uitval: eerlijk 'verbinden', niet rood."""
        return {"installed": True, "available": False, "operational": False, "headline": "VERBINDEN MET NODE",
                "tone": "warn", "message": "De eerste gegevens van deze node komen nog binnen.",
                "data_delay": {"seconds": None, "failures": self._failures, "reason": self._last_error},
                "watch": None, "task": None, "tasks": [], "runner2": None}

    def _transition(self, phase: str, age: float = 0.0) -> None:
        if phase == self._phase:
            return
        previous, self._phase = self._phase, phase
        text = {"ok": "hersteld: verse gegevens ontvangen", "delayed": "gegevens vertraagd",
                "down": "NIET BEREIKBAAR"}[phase]
        log.warning("%s: %s (was %s; %.0f s sinds laatste geldige lezing, %d mislukte rondes, laatste fout: %s)",
                    self.node.label, text, previous, age, self._failures, self._last_error)

    def _read(self, selected: str | None) -> tuple[dict, str | None]:
        """Eén leesronde: ``(stand, None)`` als hij slaagde, anders ``(onbereikbaar-kaart, foutmelding)``."""
        mods = self._mods()
        if mods is None:
            message = self._load_error or "Runner 2 kon niet worden geladen."
            return self._unavailable(message), message
        try:
            listing = mods.controller.status()
            tasks = list(listing.get("tasks") or [])
            self._repositories = [b["repository"] for b in tasks if isinstance(b, dict) and isinstance(b.get("repository"), str)]
            watch = dict(listing.get("watch") or {})
            states = {}
            for brief in tasks:
                try:
                    states[brief["task_id"]] = mods.store.load(brief["task_id"])
                except (OSError, ValueError, KeyError):
                    continue
            recovering = self._recovering(mods)
            task_id = selected if selected and selected in states else current_task_id(listing, states, recovering)
            task = None
            if task_id and task_id in states:
                state = states[task_id]
                detail = mods.controller.status(task_id)
                cfg = self._config(mods)
                task = describe_task(state, detail, recovering=recovering(state),
                                     watch_healthy=watch.get("healthy"),
                                     max_attempts=cfg.get("max_recovery_attempts"))
        except Exception as exc:  # noqa: BLE001 -- een onleesbare Runner-2-map mag het dashboard niet breken
            message = getattr(exc, "message", None) or f"Runner 2 kon niet worden gelezen ({exc.__class__.__name__})."
            return self._unavailable(message), message
        events = self._events(mods, tasks, states, recovering, watch, task_id, task)
        if events is not None:
            idle = listing.get("active_task") is None
            for event in events:
                event["runner_idle"] = idle
        # Separate from selected presentation: every unfinished task/chain,
        # supervisor or recovery can veto whole-node completion.
        chain_busy, chain_paused, chain_problem = False, False, None
        try:
            # The task list is a recent window; active chains/supervisors have
            # their own node-wide lists and can outlive every displayed leaf.
            for block in (listing.get("chains") or []) + (listing.get("supervisors") or []):
                if block.get("state") == "active":
                    chain_busy = True
                    if block.get("phase") == "paused":
                        chain_paused = True
                    if block.get("warning") or block.get("phase") in ("failed", "blocked"):
                        chain_problem = _clip(block.get("warning") or block.get("waiting_for") or "De autonome keten wacht na een fout.", 240)
            for brief in tasks:
                state = states[brief["task_id"]]
                detail = mods.controller.status(brief["task_id"])
                for key in ("chain", "supervisor"):
                    block = detail.get(key) or {}
                    if block.get("state") == "active":
                        chain_busy = True
                        if block.get("warning"):
                            chain_problem = _clip(block["warning"], 240)
                    # Only the newest leaf's terminal outcome is current;
                    # unrelated old failed runs remain history.
                    if brief is tasks[0] and block.get("state") in ("failed", "blocked", "limit_reached"):
                        chain_problem = _clip((block.get("stopped_because") or {}).get("message") or "De autonome keten is niet succesvol afgerond.", 240)
                if state.get("status") not in ("completed", "failed", "stopped") or recovering(state):
                    chain_busy = True
                if state.get("status") == "paused":
                    chain_paused = True
            if listing.get("active_task"):
                chain_busy = True
        except (KeyError, TypeError, OSError, ValueError):
            chain_busy = None  # incomplete observation must never prove idle
        # RemoteModules may serve the same bundle to different selected views.
        # Preserve that bundle's age so cache rereads cannot confirm completion.
        bundle_age = max(0, mods._clock() - mods._bundle_at) if isinstance(mods, RemoteModules) and mods._bundle_at is not None else 0
        observation_at = time.time() - bundle_age
        healthy = bool(watch.get("healthy"))
        action_open = open_action_count(listing)
        blocking = bool(task and task["state"] == "wacht_op_actie")
        return {
            "installed": True, "available": True, "operational": True,
            "headline": "RUNNER 2 ACTIEF" if healthy else "WATCH STAAT NIET",
            "tone": "ok" if healthy else "alert",
            "message": None if healthy else "De Runner-2-watch meldt zich niet; onderbroken taken worden nu niet hersteld.",
            "watch": {"healthy": healthy, "last_scan_s_ago": watch.get("last_scan_s_ago")},
            "observation_at": observation_at, "observation_id": mods._bundle_at if isinstance(mods, RemoteModules) else observation_at,
            "chain_busy": chain_busy, "chain_paused": chain_paused, "chain_problem": chain_problem,
            "active_task": listing.get("active_task"),
            "task": task,
            # Meldingswaardige gebeurtenissen uit de recente taken (niet alleen de getoonde): zie ``notify_events``.
            "events": events,
            # Het laatst afgeronde rapport van deze machine, los van de getoonde taak: het blijft bereikbaar
            # als er een nieuwe taak loopt. De inhoud komt pas bij het openen (``report``).
            "report": pick_report(task_id, tasks, states),
            # Alleen de teller: bij WACHT OP ACTIE (blokkerende vraag) valt ACTIE OPEN weg, de data blijft in runner2.action_open.
            "action_open": 0 if blocking else action_open,
            # De punten zelf voor het venster achter de badge; dezelfde lezing en hetzelfde verbergen als de teller.
            "action_items": [] if blocking else open_action_items(listing, states),
            # Dezelfde punten in drie categorieën (mens / Claude bezig / technische controle), alleen indelen en tonen.
            "action_board": None if blocking or not action_open else self._action_board(mods, listing, states, recovering, action_open),
            "supervision": describe_supervision(listing, self._supervision_details(mods, listing)),
            "tasks": [{"task_id": b.get("task_id"), "title": _clip(b.get("title"), 80),
                       "status": b.get("status"),
                       "state": STATES[state_key(states[b["task_id"]], recovering=recovering(states[b["task_id"]]))][0]
                       if b.get("task_id") in states else None}
                      for b in tasks[:8]],
            "runner2": {"tasks_total": listing.get("count"), "action_open": action_open},
        }, None

    @staticmethod
    def _action_board(mods: Any, listing: dict, states: dict, recovering: Callable[[dict], bool], action_open: int) -> dict | None:
        """De indeling van de open punten uit dezelfde lezing; een mislukte detaillezing maakt een punt onzeker, nooit onzichtbaar."""
        items = open_action_items(listing, states)
        details: dict[str, dict] = {}
        for task_id in {i["source"]["task_id"] for i in items if i["source"]["task_id"] in states}:
            try:
                detail = mods.controller.status(task_id)
            except Exception:  # noqa: BLE001
                continue
            if isinstance(detail, dict):
                details[task_id] = detail
        return action_board.build_board(items, states, details, recovering, open_total=action_open)

    def _events(self, mods: Any, tasks: list, states: dict, recovering: Callable[[dict], bool], watch: dict,
                shown_id: str | None, shown: dict | None) -> list[dict] | None:
        """De gebeurtenissen van deze node uit zijn nieuwste taken, of None als ze niet te bepalen zijn (dan nemen
        meldingen niets aan). Alleen taken in een eindtoestand of wachtend leveren iets; de rest wordt overgeslagen."""
        try:
            cfg = self._config(mods)
            rows = []
            for brief in tasks[:notify_events.EVENT_TASKS]:
                tid = brief.get("task_id")
                state = states.get(tid)
                if not state or (state.get("status") not in ("completed", "failed", "waiting_for_human")):
                    continue
                if tid == shown_id and shown is not None:
                    view = shown
                else:
                    view = describe_task(state, mods.controller.status(tid), recovering=recovering(state),
                                         watch_healthy=watch.get("healthy"), max_attempts=cfg.get("max_recovery_attempts"))
                rows.append((state, view, _iso_utc(state.get("updated"))))
            return notify_events.events_for(self.node.id, self.node.display, rows)
        except Exception:  # noqa: BLE001 -- meldingen mogen het dashboard nooit breken
            log.warning("%s: gebeurtenissen niet te bepalen", self.node.label, exc_info=True)
            return None

    @staticmethod
    def _supervision_details(mods: Any, listing: dict) -> dict[str, dict]:
        """Per supervisieproject met aandacht ``runner2.supervision.status(project_id)``: dezelfde dossierlezing als
        ``runner2_supervision_status``, alleen lezen. Alleen in-process (de lokale node); een remote node of een oudere
        Runner 2 zonder het module geeft niets, en een fout kost alleen de details, nooit de kaart."""
        module = getattr(mods, "supervision", None)
        block = listing.get("supervision")
        if module is None or not isinstance(block, dict) or not isinstance(block.get("projects"), list):
            return {}
        out: dict[str, dict] = {}
        for item in block["projects"][:MAX_SUPERVISION_PROJECTS]:
            pid = item.get("project_id") if isinstance(item, dict) and item.get("attention") else None
            if not isinstance(pid, str):
                continue
            try:
                detail = module.status(pid)
            except Exception:  # noqa: BLE001 -- een onleesbaar dossier toont dan alleen het overzicht
                log.warning("supervisie: details van %s niet te lezen", pid, exc_info=True)
                continue
            if isinstance(detail, dict):
                out[pid] = detail
        return out

    @staticmethod
    def _recovering(mods: Any) -> Callable[[dict], bool]:
        def check(state: dict) -> bool:
            try:
                return bool(mods.watch.needs_recovery(state))
            except (KeyError, TypeError):
                return False
        return check

    @staticmethod
    def _config(mods: Any) -> dict:
        try:
            return mods.config.load_config()
        except Exception:  # noqa: BLE001
            return {}

    # -- bedienen: precies de controllerfuncties van Runner 2

    def action(self, action: str, task_id: str, **fields: str) -> tuple[int, dict]:
        if action not in ACTIONS:
            return 400, {"ok": False, "reason": "bad_action", "message": "Onbekende actie."}
        if not TASK_ID.match(task_id or ""):
            return 400, {"ok": False, "reason": "invalid_input", "message": "Ongeldige taak."}
        mods = self._mods()
        if mods is None:
            return 503, {"ok": False, "reason": "runner2_unavailable",
                         "message": self._load_error or "Runner 2 kon niet worden geladen."}
        controller = mods.controller
        self._cache, self._retry_at = None, 0.0
        getattr(mods, "invalidate", lambda: None)()
        text = fields.get("text") or ""
        with self._action_lock:
            try:
                if action == "pause":
                    answer = controller.pause(task_id)
                elif action == "resume":
                    answer = controller.resume(task_id, text)
                elif action == "stop":
                    answer = controller.stop(task_id, fields.get("reason") or "via Monitor 2")
                else:
                    answer = controller.answer(task_id, text)
            except controller.ControllerError as exc:
                self._cache = None
                return _ERROR_STATUS.get(exc.code, 409), {"ok": False, "reason": exc.code, "message": exc.message}
            except Exception as exc:  # noqa: BLE001
                self._cache = None
                return 503, {"ok": False, "reason": "runner2_unavailable",
                             "message": f"Runner 2 gaf een fout ({exc.__class__.__name__})."}
        self._cache = None
        getattr(mods, "invalidate", lambda: None)()
        return 200, {"ok": True, "action": action, "task_id": task_id, "result": answer,
                     "message": _DONE_TEXT[action]}

    # -- eindrapport: alleen lezen, via dezelfde ``controller.result`` als ``runner2_result``

    def report(self, task_id: str) -> tuple[int, dict]:
        if not TASK_ID.match(task_id or ""):
            return 400, {"ok": False, "reason": "invalid_input", "message": "Ongeldige taak."}
        mods = self._mods()
        if mods is None:
            return 503, {"ok": False, "reason": "runner2_unavailable",
                         "message": self._load_error or "Runner 2 kon niet worden geladen."}
        try:
            result = mods.controller.result(task_id)
        except mods.controller.ControllerError as exc:
            return _ERROR_STATUS.get(exc.code, 409), {"ok": False, "reason": exc.code, "message": exc.message}
        except Exception as exc:  # noqa: BLE001
            return 503, {"ok": False, "reason": "runner2_unavailable",
                         "message": f"Runner 2 gaf een fout ({exc.__class__.__name__})."}
        report = describe_report(result if isinstance(result, dict) else {})
        titles = {section["title"] for section in report["sections"]}
        if not (titles - {"Taak"}) or (result.get("final_status") == "completed" and "Samenvatting" not in titles):
            return 404, {"ok": False, "reason": "no_report", "message": "Deze taak heeft geen eindrapport."}
        return 200, {"ok": True, "task_id": task_id, "report": report}

    # -- opschoonvoorstel: alleen lezen (DRY RUN van het actiebeheer); sluit, wijzigt en bevestigt nooit iets

    def action_review(self, refresh: bool = False) -> tuple[int, dict]:
        """Het voorstel van deze node, 10 minuten gecached; ``refresh`` slaat de cache over (hoogstens eens per 30 s).

        Alleen aangeroepen als iemand het venster opent of ververst, nooit door de poll. Een node zonder opschoonvoorstel
        (oude Runner 2) of een onbereikbare node geeft een nette melding, nooit verzonnen aantallen.
        """
        with self._review_lock:
            now = self._clock()
            cached = self._review_cache
            if cached:
                age = now - cached[0]
                if age < (REVIEW_MIN_REFRESH_SECONDS if refresh else REVIEW_CACHE_SECONDS):
                    return 200, {"ok": True, "review": cached[1], "cached": True, "age_s": round(max(0.0, age))}
            mods = self._mods()
            if mods is None:
                return 503, {"ok": False, "reason": "runner2_unavailable", "message": self._load_error or "Runner 2 kon niet worden geladen."}
            controller = mods.controller
            fetch = getattr(controller, "review_actions", None)
            if not callable(fetch):
                return 200, {"ok": False, "reason": "review_unsupported", "message": REVIEW_UNSUPPORTED}
            try:
                block = describe_action_review(self.node, fetch())
            except ReviewUnsupported as exc:
                return 200, {"ok": False, "reason": "review_unsupported", "message": str(exc)}
            except Exception as exc:  # noqa: BLE001
                code = getattr(exc, "code", None)
                message = str(getattr(exc, "message", "") or exc)
                if code == "invalid_input" or re.search(r"onbekend veld|oude runner|unknown (field|argument)", message, re.I):
                    return 200, {"ok": False, "reason": "review_unsupported", "message": REVIEW_UNSUPPORTED}
                if code in ("node_unreachable", "unsupported_node"):
                    return 503, {"ok": False, "reason": "node_unreachable",
                                 "message": _clip(message, 300) or f"{self.node.display} is niet bereikbaar."}
                return 503, {"ok": False, "reason": "runner2_unavailable",
                             "message": f"Runner 2 gaf een fout ({exc.__class__.__name__}): {_clip(message, 200) or 'geen melding'}"}
            self._review_cache = (self._clock(), block)
            return 200, {"ok": True, "review": block, "cached": False, "age_s": 0}

    def action_history(self) -> tuple[int, dict]:
        """De volledige actiehistorie van deze node (open én afgehandeld), alleen lezen. Alleen aangeroepen als iemand het venster
        opent, nooit door de poll; niets wordt gesloten, gewijzigd of onthouden. Onbekend of onbereikbaar geeft een melding, geen lege lijst."""
        mods = self._mods()
        if mods is None:
            return 503, {"ok": False, "reason": "runner2_unavailable", "message": self._load_error or "Runner 2 kon niet worden geladen."}
        fetch = getattr(mods.controller, "list_actions", None)
        if not callable(fetch):
            return 200, {"ok": False, "reason": "history_unsupported", "message": action_board.HISTORY_UNSUPPORTED}
        try:
            history = action_board.describe_history(self.node.id, fetch(None, True))
        except ValueError as exc:
            return 200, {"ok": False, "reason": "history_unsupported", "message": str(exc)}
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "code", None)
            message = str(getattr(exc, "message", "") or exc)
            if code == "invalid_input" or re.search(r"onbekend veld|oude runner|unknown (field|argument)", message, re.I):
                return 200, {"ok": False, "reason": "history_unsupported", "message": action_board.HISTORY_UNSUPPORTED}
            if code in ("node_unreachable", "unsupported_node"):
                return 503, {"ok": False, "reason": "node_unreachable",
                             "message": _clip(message, 300) or f"{self.node.display} is niet bereikbaar."}
            return 503, {"ok": False, "reason": "runner2_unavailable",
                         "message": f"Runner 2 gaf een fout ({exc.__class__.__name__}): {_clip(message, 200) or 'geen melding'}"}
        return 200, {"ok": True, "history": history}

    def health(self) -> dict:
        mods = self._mods()
        return {"kind": self.kind, "installed": True, "loaded": mods is not None, "error": self._load_error}

    def close(self) -> None:
        return None


_ERROR_STATUS = {"invalid_input": 400, "not_found": 404, "busy": 409, "wrong_state": 409, "not_finished": 409,
                 "unsupported_node": 409, "repo_invalid": 409, "runner2_unavailable": 503, "node_unreachable": 503}
_DONE_TEXT = {"pause": "Pauze aangevraagd; de beurt wordt bij de eerstvolgende controle onderbroken.",
              "resume": "Taak hervat.", "stop": "Taak gestopt.", "answer": "Antwoord doorgegeven."}


def build_backends(nodes: tuple[Node, ...] = NODES, *, local: Runner2Backend | None = None,
                   remote_source: Callable[[Node], Any] = remote_modules,
                   local_id: str | None = None) -> dict[str, Runner2Backend]:
    """Per node zijn backend: de lokale node (MAC, of GERARD als dit proces daar draait) in-process,
    iedere geconfigureerde remote node via het Runner-2-transport, en een node zonder route als
    ``NotInstalledBackend`` (op de Mac) of ``NoRouteBackend`` (op GERARD: eerlijk 'niet bereikbaar vanaf hier')."""
    local_id = local_id or local_node_id()
    here = next((node for node in nodes if node.id == local_id), None)
    backends: dict[str, Runner2Backend] = {}
    for node in nodes:
        if node.id == local_id:
            backends[node.id] = local or LocalRunner2Backend(node)
        elif (mods := remote_source(node)) is not None:
            backends[node.id] = LocalRunner2Backend(node, modules=mods,
                                                retry_after_failure=REMOTE_RETRY_AFTER_FAILURE_S, cold_start_grace=True)
        elif here is not None and local_id != "mac":
            backends[node.id] = NoRouteBackend(node, here)
        else:
            backends[node.id] = NotInstalledBackend(node)
    return backends
