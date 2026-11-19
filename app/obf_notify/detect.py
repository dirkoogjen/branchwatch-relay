"""Van een statusbeeld naar een genormaliseerd ``Item``. Puur: geen I/O, geen klok.

De vraag "is er een mens nodig?" wordt hier **niet** beantwoord maar overgenomen:
leidend is ``human_action`` / ``waiting_for_human`` / ``requires_human`` zoals de
controller en de projectsessie ze zelf uitspreken, plus hun terminale toestanden.
Er staat dus geen lijst "statussen die een melding geven" in dit bestand.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

ACTION_REQUIRED = "ACTION_REQUIRED"
COMPLETED = "COMPLETED"
FAILED = "FAILED"
KINDS = (ACTION_REQUIRED, COMPLETED, FAILED)

# De enige twee controllerstatussen die een definitieve uitkomst dragen. Namen komen
# uit obf_mcp.controller (dezelfde als monitor/relevance); hier alleen als tekst zodat
# deze module geen import van de controller nodig heeft.
_TASK_COMPLETE_VERIFIED = "complete_verified"
_TASK_STOP = "stop"


@dataclass(frozen=True)
class Item:
    """Eén taak of projectsessie, in de vorm die de notifier nodig heeft."""

    key: str                  # stabiele identiteit, bv. "task:mac:CTRL-1" of "project:ubuntu:PRUN-x"
    item_type: str            # "task" | "project" | "auth"
    item_id: str
    title: str
    node: str                 # "mac" | "ubuntu" | "gerard" | ...
    status: str
    human: bool = False       # staat er nu een menselijke grens open?
    human_tool: str = ""      # human_action.tool of .kind
    human_why: str = ""       # korte uitleg uit human_action
    human_choices: tuple[str, ...] = ()
    step: str = ""            # ronde/stap: maakt "dezelfde toestand" en "nieuwe grens" onderscheidbaar
    boundary_id: str = ""     # stabiele identiteit van de open grens (obf_mcp/human_boundary.py)
    outcome: str | None = None       # COMPLETED / FAILED bij een definitief einde
    outcome_reason: str = ""
    activity_at: str | None = None   # ISO-tijdstip van de laatste activiteit / het einde


def _text(value: Any, limit: int = 400) -> str:
    return " ".join(str(value).split())[:limit] if isinstance(value, (str, int, float)) else ""


def parse_iso(value: Any) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        moment = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def item_from_task(view: Any, *, node: str, task_id: str, title: str | None = None,
                   activity_at: str | None = None) -> Item | None:
    """Een controllertaak (Mac of node). ``None`` = onbekend/onvolledig: niets afleiden."""
    if not isinstance(view, dict):
        return None
    status = view.get("status")
    if not isinstance(status, str) or not status or status in ("loading", "onbekend", "creating"):
        return None
    if view.get("vanished") or status in ("not_on_node", "released_by_project"):
        return None
    terminal = bool(view.get("terminal"))
    human = view.get("human_action") if isinstance(view.get("human_action"), dict) else None
    autonomous = bool(((view.get("apply") or {}).get("autonomous"))) if isinstance(view.get("apply"), dict) else False
    # Een stop die het levende werkproces nog zelf afhandelt is nooit een grens. De bron
    # zet dan al waiting_for_human=False; dit is alleen de dubbele bodem.
    settling = bool((view.get("human_boundary") or {}).get("settling")) \
        if isinstance(view.get("human_boundary"), dict) else False
    is_human = bool(human and view.get("waiting_for_human") and not terminal and not autonomous and not settling)
    outcome, reason = None, ""
    if terminal:
        if status == _TASK_COMPLETE_VERIFIED:
            outcome = COMPLETED
        elif status == _TASK_STOP and not view.get("stop_requested"):
            outcome, reason = FAILED, _text(view.get("reason"))
    rnd = view.get("round") if isinstance(view.get("round"), dict) else {}
    step = f"r{rnd.get('number')}" if rnd.get("number") is not None else ""
    if is_human and human.get("proposal_sha256"):
        step += ":" + _text(human.get("proposal_sha256"), 16)
    tool = _text(human.get("tool") or human.get("kind"), 80) if is_human else ""
    return Item(
        key=f"task:{node}:{task_id}", item_type="task", item_id=task_id,
        title=_text(title or view.get("title") or task_id, 120), node=node, status=status,
        human=is_human, human_tool=tool,
        human_why=_text(human.get("why"), 300) if is_human else "",
        step=step, outcome=outcome, outcome_reason=reason,
        boundary_id=(_text(human.get("boundary_id"), 80) or _derived_id(status, tool, step)) if is_human else "",
        activity_at=activity_at or _iso(view.get("updated_at")))


def item_from_project(view: Any, *, node: str) -> Item | None:
    """Een projectsessie (``NodeProjects.status()``-beeld)."""
    if not isinstance(view, dict):
        return None
    session_id = view.get("session_id")
    state = view.get("state")
    if not isinstance(session_id, str) or not isinstance(state, str) or not state:
        return None
    terminal = bool(view.get("terminal"))
    human = view.get("human_action") if isinstance(view.get("human_action"), dict) else None
    is_human = bool(human and not terminal and (view.get("requires_human") or view.get("waiting_for_human")))
    boundary = view.get("boundary") if isinstance(view.get("boundary"), dict) else {}
    outcome, reason = None, ""
    if terminal:
        if state == "completed":
            outcome = COMPLETED
        elif state == "stopped" and not view.get("stop_requested"):
            outcome, reason = FAILED, _text(view.get("final_reason"))
    choices = tuple(_text(item.get("label"), 60) for item in (human or {}).get("choices") or []
                    if isinstance(item, dict) and item.get("label"))
    step = ":".join(_text(part, 40) for part in (boundary.get("kind"), boundary.get("task_id"),
                                                 view.get("tasks_completed")) if part not in (None, ""))
    timeline = [t for t in view.get("timeline") or [] if isinstance(t, dict)]
    activity = view.get("finished_at") if terminal else None
    activity = activity or (timeline[-1].get("at") if timeline else None) or view.get("created_at")
    tool = _text((human or {}).get("kind") or (human or {}).get("tool"), 80) if is_human else ""
    return Item(
        key=f"project:{node}:{session_id}", item_type="project", item_id=session_id,
        title=_text(view.get("title") or session_id, 120), node=node, status=state,
        human=is_human,
        human_tool=_text((human or {}).get("kind") or (human or {}).get("tool"), 80) if is_human else "",
        human_why=_text((human or {}).get("question") or (human or {}).get("why"), 300) if is_human else "",
        human_choices=choices if is_human else (), step=step,
        boundary_id=_derived_id(state, tool, step, boundary.get("reason")) if is_human else "",
        outcome=outcome, outcome_reason=reason, activity_at=_iso(activity))


def _derived_id(*parts: Any) -> str:
    """Identiteit voor een bron die (nog) geen ``boundary_id`` meestuurt."""
    return "d-" + "|".join(_text(part, 80) for part in parts if part not in (None, ""))


def _iso(value: Any) -> str | None:
    return value if isinstance(value, str) and parse_iso(value) is not None else None
