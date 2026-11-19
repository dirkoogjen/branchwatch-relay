"""Compacte, alleen-lezen NUC-integratorstand voor de STUDIO-kaart.

Hergebruikt de begrensde bestandslezer en diens upload-freshness. De bron heet historisch
``mac-integrator``; de hostbinding voorkomt dat een Mac-stand onder STUDIO belandt.
Geen toegang tot de controller, git, toestemmingen of integratiehandelingen.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

from . import integration as ig
from . import remote_status

RESULT_HOLD_S = 180
MAX_TICK_S = 3780  # maximaal testbudget (3600 s), plus de uploadmarge


def _text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:180]


def _epoch(value, now: float) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = ig._epoch(value)
    except (ValueError, OverflowError):
        return None
    return stamp if 0 < stamp <= now + 5 else None


def read(path: Path | None, *, now: float | None = None, expected_host: str = "studioobf", _remote=None) -> dict:
    now = time.time() if now is None else now
    unknown = {"state": "onbekend", "detail": "Geen actuele integratorstatus beschikbaar.",
               "at": None, "last_result": None, "valid_for_s": 0}
    remote = _remote if _remote is not None else remote_status.read(path, now=now)
    if not remote or remote["state"] != "ok":
        return unknown
    data = remote["status"]
    if data.get("host") != expected_host or not isinstance(data.get("integration_enabled"), bool):
        return unknown
    auto = data["auto"]
    if not isinstance(auto.get("enabled"), bool) or not isinstance(auto.get("audit_ok"), bool):
        return unknown
    running, paused, error = auto.get("running"), auto.get("paused"), data.get("last_error")
    if running is not None and (not isinstance(running, str) or not re.fullmatch(r"G-[0-9a-f]{12}", running)):
        return unknown
    if paused is not None and not isinstance(paused, dict):
        return unknown
    if error is not None and not isinstance(error, str):
        return unknown
    generated = _epoch(data.get("generated_at"), now)
    tick = _epoch(data.get("last_tick_at"), now)
    active = data.get("tick_started_at")
    started = _epoch(active, now) if active is not None else None
    limit = remote["stale_after_s"]
    if generated is None or (active is not None and (started is None or now - started > MAX_TICK_S)):
        return unknown
    if active is None and (tick is None or now - tick > limit):
        return unknown
    # Ook de browser bewaakt de resterende geldigheid als een volgende poll blijft hangen.
    valid = min(limit - max(0, now - generated),
                MAX_TICK_S - (now - started) if started is not None else limit - (now - tick))
    if valid <= 0:
        return unknown
    out = {**unknown, "state": "wachten", "detail": "", "valid_for_s": min(limit, valid)}
    # De producent wist last_error pas ná een afgeronde ronde: een lopende retry heeft voorrang.
    previous_error = (" Vorige ronde: " + _text(error)) if error else ""
    if running:
        return {**out, "state": "integreren", "detail": _text("Integratie en gerichte nacontroles lopen." + previous_error)}
    if started is not None and data["integration_enabled"] and auto["enabled"] and not paused:
        return {**out, "state": "running", "detail": _text("Controleert integratieaanvragen." + previous_error)}
    if error:
        return {**out, "state": "mislukt", "detail": _text(data["last_error"]), "at": data.get("last_tick_at")}
    if auto["audit_ok"] is False:
        return {**out, "state": "mislukt", "detail": "Auditcontrole mislukt; integratie geblokkeerd."}
    if auto.get("paused"):
        return {**out, "detail": "Gepauzeerd."}
    if not data["integration_enabled"] or not auto["enabled"]:
        return {**out, "detail": "Automatische integratie staat uit."}
    if auto.get("waiting"):
        return {**out, "detail": "Wacht op een vrijgegeven aanvraag of voorwaarde."}
    # Sorteer beide lijsten op tijd: een oude fout mag een latere geslaagde poging niet overschrijven.
    events = []
    for item in auto.get("attention", []) + auto.get("recent", []):
        if not isinstance(item, dict):
            continue
        if not isinstance(item.get("state"), str):
            return unknown
        stamp = _epoch(item.get("at"), now)
        if stamp is not None:
            events.append((stamp, item))
    if not events:
        return out
    stamp, item = max(events, key=lambda event: event[0])
    state = {"merged": "geslaagd", "failed": "mislukt"}.get(item.get("state"))
    if state is None:
        return {**out, "detail": _text(item.get("code") or "Wacht op actie.")}
    # Bind a reported result to its exact Runner task; a newer task must not
    # inherit a green integration dot from a preceding cycle.
    match = re.fullmatch(r"r2/(R2-[0-9a-f]{12}) -> main \([0-9a-f]{10,40}\)", _text(item.get("text")))
    binding = {"task_id": match.group(1)} if match else {}
    last = {"state": state, "at": item["at"], **binding}
    detail = _text((str(item.get("code") or "") + ": " + str(item.get("text") or "")).strip(": ")) if state == "mislukt" else ""
    if now - stamp > RESULT_HOLD_S:
        return {**out, "detail": detail, "last_result": last}
    return {**out, **binding, "state": state, "detail": detail, "at": item["at"],
            "valid_for_s": min(valid, RESULT_HOLD_S - max(0, now - stamp))}


def read_nodes(nuc_path, *, now=None, env=None):
    """Fixed per-host sources; missing/invalid sources stay unknown independently."""
    import os
    env = os.environ if env is None else env
    gerard = env.get('OBF_GERARD_INTEGRATOR_STATUS')
    return {
        'linux': read(nuc_path, now=now, expected_host=env.get('OBF_NUC_INTEGRATOR_HOST', 'studioobf')),
        'gerard': read(Path(gerard).expanduser() if gerard else None, now=now,
                       expected_host=env.get('OBF_GERARD_INTEGRATOR_HOST', 'gerard')),
    }


def notification_sources(nuc_path, *, now=None, env=None):
    """Same validated host/tick source as the cards, with task-bound diagnostics.

    Use one raw read per host for both validation and notification decisions.
    A heartbeat or historical green result is never completion evidence.
    """
    import os
    env = os.environ if env is None else env
    paths = {'linux': (nuc_path, env.get('OBF_NUC_INTEGRATOR_HOST', 'studioobf')),
             'gerard': (Path(env['OBF_GERARD_INTEGRATOR_STATUS']).expanduser() if env.get('OBF_GERARD_INTEGRATOR_STATUS') else None,
                        env.get('OBF_GERARD_INTEGRATOR_HOST', 'gerard'))}
    result = {}
    for node, (path, host) in paths.items():
        remote = remote_status.read(path, now=now)
        summary = read(path, now=now, expected_host=host, _remote=remote)
        if summary['state'] == 'onbekend':
            result[node] = summary
            continue
        data = remote['status']; auto = data['auto']
        linked = {item.get('request'): item.get('task') for item in (auto.get('r2_actions') or {}).get('linked', []) if isinstance(item, dict)}
        attention = [{**item, 'task_id': linked.get(item.get('id'))} for item in auto.get('attention', []) if isinstance(item, dict)]
        result[node] = {**summary, 'observation': data.get('generated_at'), 'attention': attention,
                        'waiting': auto.get('waiting') or [], 'running': bool(auto.get('running') or data.get('tick_started_at')),
                        'paused': bool(auto.get('paused')), 'enabled': data.get('integration_enabled') and auto.get('enabled'),
                        'error': data.get('last_error') or (auto.get('audit_problem') or 'Auditcontrole mislukt' if auto.get('audit_ok') is False else None)}
    return result
