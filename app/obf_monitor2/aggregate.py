"""Wat Monitor 2 accountbreed toont, los van Runner 2: het Claude-usageblok.

Zuiver: geen netwerk, geen klok, geen DOM. De machines en de Runner-2-taken komen uit
``r2_adapter``; hier alleen de Claude-usage, met precies wat Claude zelf meldt.
"""

from __future__ import annotations



def usage_block(usage: dict | None) -> dict:
    """Eén accountbreed blok. Alleen wat Claude zelf meldt; niets wordt geschat."""
    if not isinstance(usage, dict):
        return {"status": "unavailable", "limits": [], "message": "Geen usagegegevens."}
    limits = []
    for limit in usage.get("limits") or []:
        if not isinstance(limit, dict) or not isinstance(limit.get("percent"), int):
            continue
        limits.append({"label": str(limit.get("label") or "—"), "percent": limit["percent"],
                       "resets_at": limit.get("resets_at"), "severity": limit.get("severity") or "normal",
                       "group": "model" if limit.get("kind") == "weekly_scoped" else "account"})
    return {"status": usage.get("status") or "unavailable", "limits": limits,
            "checked_at_local": usage.get("checked_at_local"), "age_seconds": usage.get("age_seconds"),
            "stale": bool(usage.get("stale")), "message": usage.get("message"),
            "problem": usage.get("problem"), "auth_note": usage.get("auth_note"),
            "login_expiry": usage.get("login_expiry") if isinstance(usage.get("login_expiry"), dict) else None,
            "source": usage.get("source")}
