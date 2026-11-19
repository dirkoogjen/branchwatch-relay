"""Open Runner 2-acties in drie categorieën (beslissing 44 van 23-10-2026) en de volledige actiehistorie.

Alleen indelen en tonen. Deze module leest wat Runner 2 al levert (``controller.status()['human_actions']``, de state van de
brontaak en het keten-/supervisorblok van ``controller.status(task_id)``), schrijft nergens, sluit niets en markeert nooit een actie
als voltooid. Er zijn geen nieuwe rechten: de lijst is een alleen-lezen weergave van de eigen punten van elke machine; bedienen
(pauzeren, hervatten, stoppen, beantwoorden) blijft zoals het was op de machinekaart.

De drie categorieën, in volgorde van belang:

* ``mens``     -- een menselijke beslissing is nodig. Dit is ook de veilige standaard: is er iets onzeker of onleesbaar, dan komt
                  het punt hier (met ``zeker: false`` en de reden) en verdwijnt het nooit uit beeld.
* ``claude``   -- in uitvoering bij Claude: de keten, supervisor of taak waar het punt uit komt loopt aantoonbaar nog
                  (taak ``created``/``running`` of keten in fase starting/running/between_links, niet herstellend).
* ``controle`` -- wacht op technische controle: de keten laat een gemeld 'doel bereikt' eerst onafhankelijk controleren
                  (fase ``completion_check``, de voltooiingscontrole van Runner 2).

Een geblokkeerde, gepauzeerde of mislukte run is altijd ``mens``. De indeling is een momentopname van de feiten; zodra de status
verandert, verschuift het punt vanzelf bij de volgende lezing.
"""

from __future__ import annotations

from typing import Any

MENS, CLAUDE, CONTROLE = "mens", "claude", "controle"

CATEGORIES = (
    (MENS, "Menselijke beslissing nodig", "Hier wacht iets op jou. Ook alles waarvan de status onzeker is staat hier."),
    (CLAUDE, "In uitvoering bij Claude", "De taak of keten waar dit punt uit komt loopt nog."),
    (CONTROLE, "Wacht op technische controle", "Runner 2 laat een gemeld 'doel bereikt' eerst onafhankelijk controleren."),
)

#: Fasen van een actieve keten/supervisor waarin Claude aantoonbaar bezig is, en waarin een mens aan zet is.
_CLAUDE_PHASES = frozenset({"starting", "running", "between_links", "chain_running", "judging", "between_chains", "starting_chain"})
_HUMAN_PHASES = frozenset({"waiting_for_human", "paused", "failed", "stopped", "blocked"})
_RUN_STATUSES = ("created", "running")
_HUMAN_STATUSES = frozenset({"waiting_for_human", "paused", "failed", "stopped", "completed"})

HISTORY_MAX = 300          # hoeveel punten de historie in één antwoord toont (nieuwste eerst); de rest wordt eerlijk geteld
HISTORY_UNSUPPORTED = "Deze Runner kan de actiehistorie niet leveren; er wordt niets getoond."


def _clip(value: Any, limit: int) -> str | None:
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())
    if not text:
        return None
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _result(category: str, certain: bool, basis: str) -> dict:
    return {"category": category, "certain": certain, "basis": basis}


def classify_item(item: dict, states: dict[str, dict], details: dict[str, dict], recovering=lambda state: False) -> dict:
    """De categorie van één open punt, met of het zeker is en op welk feit het steunt (zuiver; gooit nooit).

    ``states``: taakstates van deze node; ``details``: ``controller.status(task_id)`` per brontaak (chain/supervisor); ``recovering``:
    ``runner2.watch.needs_recovery``. Alles wat ontbreekt of niet te duiden is wordt ``mens`` met ``certain=False``."""
    source = item.get("source") if isinstance(item.get("source"), dict) else {}
    task_id = source.get("task_id")
    state = states.get(task_id) if isinstance(task_id, str) else None
    if not isinstance(state, dict):
        return _result(MENS, False, "De brontaak is niet leesbaar; de status van dit punt is onbekend.")
    try:
        if recovering(state):
            return _result(MENS, False, "De brontaak wordt hersteld; de uitkomst is nog niet zeker.")
    except Exception:  # noqa: BLE001 -- onleesbaar herstelvlag: liever onzeker dan fout
        return _result(MENS, False, "Of de brontaak wordt hersteld is niet te lezen.")
    detail = details.get(task_id) if isinstance(details.get(task_id), dict) else None
    for key, name in (("chain", "keten"), ("supervisor", "supervisor")):
        block = detail.get(key) if detail else None
        if not isinstance(block, dict) or block.get("state") != "active":
            continue
        phase = block.get("phase")
        if key == "chain" and phase == "completion_check":
            return _result(CONTROLE, True, "De keten wacht op de voltooiingscontrole van Runner 2.")
        if phase in _CLAUDE_PHASES:
            return _result(CLAUDE, True, f"De {name} loopt nog (fase: {phase}).")
        if phase in _HUMAN_PHASES:
            return _result(MENS, True, f"De {name} staat stil en wacht op een mens (fase: {phase}).")
        return _result(MENS, False, f"De fase van de {name} is niet te duiden ({phase or 'onbekend'}).")
    in_run = any(isinstance(holder.get(key), str) and holder.get(key) for holder in (state, source) for key in ("chain_id", "supervisor_id"))
    if detail is None and in_run:
        # Een keten of supervisor kan nog lopen, maar zijn blok is niet gelezen: dan niet raden dat de run klaar is.
        return _result(MENS, False, "De keten of supervisor van de brontaak is niet te lezen; of die nog loopt is onbekend.")
    status = state.get("status")
    if status in _RUN_STATUSES:
        return _result(CLAUDE, True, "De brontaak loopt nog.")
    if status in _HUMAN_STATUSES:
        return _result(MENS, True, "De brontaak is afgerond of wacht; het punt blijft open voor een mens.")
    return _result(MENS, False, "De status van de brontaak is niet te duiden.")


def build_board(items: list[dict], states: dict[str, dict], details: dict[str, dict], recovering=lambda state: False, *,
                open_total: int | None = None) -> dict:
    """Het bord: de drie categorieën met hun punten, plus tellingen. ``items`` zijn de open punten van ``open_action_items``.

    Elk punt komt in precies één categorie; de som is dus ``len(items)``. ``not_shown`` is het aantal open punten dat Runner 2
    telt maar niet meegaf (``human_actions.items`` is begrensd): die worden niet geraden maar eerlijk gemeld."""
    columns = {key: [] for key, _t, _h in CATEGORIES}
    for item in items:
        verdict = classify_item(item, states, details, recovering)
        columns[verdict["category"]].append({**item, "category": verdict["category"], "certain": verdict["certain"],
                                             "basis": verdict["basis"]})
    shown = len(items)
    total = open_total if isinstance(open_total, int) and not isinstance(open_total, bool) and open_total >= shown else shown
    return {"categories": [{"key": key, "title": title, "hint": hint, "count": len(columns[key]), "items": columns[key]}
                           for key, title, hint in CATEGORIES],
            "total": total, "shown": shown, "not_shown": total - shown,
            "uncertain": sum(1 for rows in columns.values() for row in rows if not row["certain"]),
            "read_only": True}


def _history_entry(item: Any) -> dict | None:
    if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not isinstance(item.get("text"), str):
        return None
    status = item.get("status")
    source = item.get("source") if isinstance(item.get("source"), dict) else {}
    out = {"id": item["id"], "text": _clip(item["text"], 400) or "", "status": status if status in ("open", "resolved") else "onbekend",
           "created_at": item.get("created_at") if isinstance(item.get("created_at"), str) else None,
           "source": {"task_id": source.get("task_id") if isinstance(source.get("task_id"), str) else None,
                      "chain_id": source.get("chain_id") if isinstance(source.get("chain_id"), str) else None}}
    if out["status"] == "resolved":
        out.update(resolved_at=item.get("resolved_at") if isinstance(item.get("resolved_at"), str) else None,
                   resolved_by=_clip(item.get("resolved_by"), 80), resolved_note=_clip(item.get("resolved_note"), 300))
    return out


def describe_history(node_id: str, result: Any) -> dict:
    """De volledige actiehistorie van één machine uit ``runner2_actions include_resolved``: open én afgehandelde punten, nieuwste eerst.

    Zuiver en alleen lezen; een antwoord zonder lijst is ``ValueError`` (dan toont het venster een melding, geen verzonnen lege lijst).
    Wie of wat een punt sloot (``resolved_by``) en de notitie blijven zichtbaar, zodat opschoning controleerbaar blijft."""
    if not isinstance(result, dict) or not isinstance(result.get("items"), list):
        raise ValueError(HISTORY_UNSUPPORTED)
    entries = [e for e in (_history_entry(x) for x in result["items"]) if e]
    entries.sort(key=lambda e: (e.get("resolved_at") or e.get("created_at") or "", e["id"]), reverse=True)
    resolved = sum(1 for e in entries if e["status"] == "resolved")
    return {"node": node_id, "total": len(entries), "open": sum(1 for e in entries if e["status"] == "open"), "resolved": resolved,
            "items": entries[:HISTORY_MAX], "not_shown": max(0, len(entries) - HISTORY_MAX), "read_only": True}
