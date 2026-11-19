"""De bronnen van de notifier: dezelfde read-only bronnen als de webmonitor, zonder de webmonitor.

De notifier is dus onafhankelijk van een browser en van de monitorprocessen. Hij leest:

* de Mac: ``MonitorSource`` (lock-vrije lezing van de duurzame controllerstate);
* elke node: ``NodeMonitorSource`` (het lokale noderegister + read-only statusaanroepen naar
  de node), waarvan de projectsessies en losse controllertaken worden genormaliseerd.

Kindtaken van een projectsessie worden overgeslagen: de sessie is daar de autoriteit en
beslist zelf of een uitkomst een menselijke grens is.
"""

from __future__ import annotations

from .detect import Item, item_from_project, item_from_task, parse_iso

#: Terminale Mac-taken van vóór dit venster hoeven niet meer opgevraagd te worden.
MAC_TERMINAL_LOOKBACK_SECONDS = 48 * 3600


def mac_collector(source, *, clock):
    """``source``: een ``obf_monitor_web.source.MonitorSource``."""

    def collect() -> list[Item]:
        items: list[Item] = []
        now = clock()
        for row in source.list_tasks():
            task_id = row.get("task_id")
            status = row.get("status")
            if not task_id or status in (None, "creating"):
                continue
            updated = parse_iso(row.get("updated_at"))
            if row.get("terminal") and (updated is None or now - updated > MAC_TERMINAL_LOOKBACK_SECONDS):
                continue
            try:
                view = source.task_status(task_id)
            except Exception:  # noqa: BLE001 -- één onleesbare taak mag de rest niet blokkeren
                continue
            item = item_from_task({**view, "updated_at": row.get("updated_at")}, node="mac", task_id=task_id,
                                  title=row.get("title"), activity_at=row.get("updated_at"))
            if item is not None:
                items.append(item)
        return items

    return collect


def node_collector(source, *, node: str):
    """``source``: een ``NodeMonitorSource`` voor één node."""

    def collect() -> list[Item]:
        snapshot = source.notification_snapshot()
        items: list[Item] = []
        for view in snapshot["projects"]:
            item = item_from_project(view, node=node)
            if item is not None:
                items.append(item)
        for entry in snapshot["tasks"]:
            item = item_from_task(entry["view"], node=node, task_id=entry["view"].get("node_task_id") or "",
                                  activity_at=entry.get("activity_at"))
            if item is not None and item.item_id:
                items.append(item)
        return items

    return collect


#: De Claude-login op de Mac (audit INF-02): één stabiel item, open zodra het einde binnen
#: de waarschuwingstermijn valt. De grens-id is de vervaldatum zelf, dus één melding per
#: login; na een nieuwe aanmelding verschuift de datum en sluit de oude grens vanzelf.
LOGIN_ITEM_KEY = "auth:mac:claude-login"


def mac_login_collector(*, clock, auth_status_path=None):
    """Leest alleen het statusbestand van de runner-preflight (zonder token)."""
    from obf_monitor_web import claude_usage  # noqa: PLC0415 -- dezelfde lezer als Monitor 2

    def collect() -> list[Item]:
        path = auth_status_path or claude_usage._default_auth_status_path()  # noqa: SLF001
        expiry = claude_usage.login_expiry(claude_usage.read_auth_status(path), clock())
        if expiry is None:
            return []
        return [Item(key=LOGIN_ITEM_KEY, item_type="auth", item_id="claude-login", title="Claude-login (Mac)",
                     node="mac", status="login_verlopen" if expiry["expired"] else
                     ("login_verloopt" if expiry["warning"] else "login_geldig"),
                     human=bool(expiry["warning"]), human_tool="claude_login_expiring" if expiry["warning"] else "",
                     human_why=expiry["message"] or "",
                     boundary_id=f"login-expiry|{expiry['expires_at']}" if expiry["warning"] else "")]

    return collect

