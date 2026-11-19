"""Het statusbestand van de losse integrator op de Mac lezen (§13.2), alleen om te tonen.

Monitor 2 op GERARD integreert zelf niets; de Mac stuurt met ``scp`` een alleen-lezen statusbestand
(``mac_integrator.status``). Dit module leest dat bestand voorzichtig en geeft een vaste vorm terug voor
``GET /api/integration`` (veld ``remote_auto``):

* geen symlink, alleen een gewoon bestand, hoogstens ``MAX_BYTES``;
* onleesbare of half geschreven JSON (een upload is bezig) → ``updating`` ("wordt bijgewerkt"), nooit een fout;
* ``generated_at`` ouder dan ``STALE_FACTOR`` × het verzendinterval van de Mac → ``stale`` ("verouderd"), met de laatste stand;
* alleen bekende velden gaan door, en alles wat op een sleutel, code of hash lijkt wordt nogmaals weggehaald (``scrub``);
  ``auto.test_run`` (verstreken / budget / schatting van de gerichte tests) alleen in de vaste vorm van ``test_budget.shape_summary``.

Er is geen enkele handeling: wat hier staat kan niets uitvoeren, stilzetten of hervatten.
"""

from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path

from . import integration as ig
from . import mac_integrator as mi
from . import test_budget as tb

ENV = "OBF_INTEGRATION_REMOTE_STATUS"
MAX_BYTES = 256 * 1024
STALE_FACTOR = 3
#: Velden van het statusbestand die naar de pagina gaan; de rest valt weg.
TOP_FIELDS = ("schema", "source", "host", "generated_at", "started_at", "poll_s", "last_tick_at", "tick_started_at", "last_error",
              "integration_enabled", "read_only", "upload", "auto")
AUTO_FIELDS = ("configured", "enabled", "problem", "repos", "paused", "running", "poll_s", "waiting", "attention", "recent",
               "ignored_preexisting", "r2_actions", "audit_ok", "audit_problem", "test_run")
UPLOAD_FIELDS = ("enabled", "host", "interval_s", "last_attempt_at", "last_ok_at", "problem")
MAX_ITEMS = 50

MESSAGES = {
    "missing": "Er is nog geen statusbestand van de integrator ontvangen.",
    "refused": "Het statusbestand is geen gewoon, leesbaar bestand of is te groot; het wordt niet gelezen.",
    "updating": "Het statusbestand wordt bijgewerkt.",
    "invalid": "Het statusbestand heeft niet de verwachte vorm (geen status van de losse integrator).",
}


def path_from_env(env: dict | None = None) -> Path | None:
    value = ((env if env is not None else os.environ).get(ENV) or "").strip()
    return Path(value).expanduser() if value else None


def _read(path: Path) -> tuple[str, bytes | None]:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return "missing", None
    except OSError:
        # ELOOP (symlink), geen leesrecht, een fifo of map: niet lezen.
        return "refused", None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_BYTES:
            return "refused", None
        with os.fdopen(fd, "rb", closefd=False) as handle:
            data = handle.read(MAX_BYTES + 1)
        return ("refused", None) if len(data) > MAX_BYTES else ("ok", data)
    finally:
        os.close(fd)


def _pick(source, fields: tuple[str, ...]) -> dict:
    return {k: source[k] for k in fields if isinstance(source, dict) and k in source}


def _shape(data: dict) -> dict:
    out = _pick(data, TOP_FIELDS)
    out["upload"] = _pick(data.get("upload"), UPLOAD_FIELDS)
    auto = _pick(data.get("auto"), AUTO_FIELDS)
    for key in ("waiting", "attention", "recent", "ignored_preexisting"):
        if not isinstance(auto.get(key), list):
            auto[key] = []
        auto[key] = auto[key][:MAX_ITEMS]
    # Testtijd-budget en voortgang: alleen de vaste vorm van test_budget (bekende velden, getallen of korte tekst).
    auto["test_run"] = tb.shape_summary(auto.get("test_run"))
    out["auto"] = auto
    return mi.scrub(out)


def _interval(data: dict) -> int:
    upload = data.get("upload") if isinstance(data.get("upload"), dict) else {}
    value = upload.get("interval_s")
    if not isinstance(value, int) or isinstance(value, bool):
        value = mi.UPLOAD_INTERVAL_S
    return min(3600, max(15, value))


def read(path: Path | None, now: float | None = None) -> dict | None:
    """``None`` als er geen pad is ingesteld; anders ``{configured, state, message, age_s, stale_after_s, status}``."""
    if path is None:
        return None
    now = time.time() if now is None else now
    out = {"configured": True, "state": "ok", "message": None, "age_s": None, "stale_after_s": None, "status": None}
    state, raw = _read(path)
    if state != "ok":
        return {**out, "state": state, "message": MESSAGES[state]}
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return {**out, "state": "updating", "message": MESSAGES["updating"]}
    if not isinstance(data, dict) or data.get("schema") != mi.STATUS_SCHEMA or data.get("source") != mi.SOURCE \
            or not isinstance(data.get("auto"), dict):
        return {**out, "state": "invalid", "message": MESSAGES["invalid"]}
    try:
        age = int(now - ig._epoch(str(data.get("generated_at"))))
    except ValueError:
        return {**out, "state": "invalid", "message": MESSAGES["invalid"]}
    limit = STALE_FACTOR * _interval(data)
    out.update(age_s=age, stale_after_s=limit, status=_shape(data))
    if age > limit:
        out.update(state="stale", message=f"Verouderd: de laatste stand van de bronmachine is {age} s oud (grens {limit} s). "
                                          "De bronmachine stuurt niets meer door, of de integrator staat stil.")
    return out
