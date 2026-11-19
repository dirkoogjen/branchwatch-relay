"""Duurzame instellingen: de AAN/UIT-schakelaar, de (niet-geheime) configuratie en het secret.

Alles staat in dezelfde duurzame map als de runner-route (``~/obf-runner-bridge``),
buiten de repository en buiten elk proces. Zo is de stand dezelfde voor de notifier,
voor de drie monitors en voor de CLI, en overleeft hij herstarts van elk daarvan.

Fail-safe: ontbreekt de schakelaar, is hij onleesbaar of onbegrijpelijk, dan staat
het op UIT. Een verse installatie stuurt dus nooit iets naar buiten.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HOME_ENV = "OBF_NOTIFY_HOME"
DEFAULT_HOME = Path("~/obf-runner-bridge/notifications")

SWITCH_SCHEMA = "obf-notify-switch-v1"
KEYRING_SERVICE = "StudioOBF-Notify"
PASSWORD_ENV = "OBF_NOTIFY_SMTP_PASSWORD"
MAX_ACTOR_CHARS = 64


def notify_home() -> Path:
    return Path(os.environ.get(HOME_ENV) or DEFAULT_HOME).expanduser()


def now_iso(epoch: float | None = None) -> str:
    moment = datetime.now(timezone.utc) if epoch is None else datetime.fromtimestamp(epoch, tz=timezone.utc)
    return moment.isoformat(timespec="seconds")


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=1, sort_keys=True)
            handle.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


# ------------------------------------------------------------------ schakelaar

@dataclass(frozen=True)
class SwitchState:
    enabled: bool = False
    #: Wanneer de schakelaar voor het laatst van UIT naar AAN ging. Afgeronde/mislukte
    #: taken van vóór dat moment worden nooit alsnog gemeld.
    enabled_at: str | None = None
    updated_at: str | None = None
    updated_by: str | None = None
    degraded: bool = False

    def as_dict(self) -> dict:
        return {"enabled": self.enabled, "enabled_at": self.enabled_at, "updated_at": self.updated_at,
                "updated_by": self.updated_by, "degraded": self.degraded}


class NotificationSwitch:
    """De ene globale schakelaar Meldingen AAN/UIT."""

    def __init__(self, home: Path | None = None, *, clock=None):
        self.path = (home or notify_home()) / "switch.json"
        self._clock = clock

    def read(self) -> SwitchState:
        if not self.path.exists():
            return SwitchState()  # eerste installatie: UIT, geen probleem
        data = read_json(self.path)
        if not isinstance(data, dict) or data.get("schema") != SWITCH_SCHEMA \
                or not isinstance(data.get("enabled"), bool):
            return SwitchState(degraded=True)
        return SwitchState(enabled=data["enabled"], enabled_at=data.get("enabled_at"),
                           updated_at=data.get("updated_at"), updated_by=data.get("updated_by"))

    def set(self, enabled: bool, *, actor: str = "cli") -> SwitchState:
        if not isinstance(enabled, bool):
            raise ValueError("enabled moet True of False zijn")
        current = self.read()
        now = now_iso(self._clock() if self._clock else None)
        enabled_at = current.enabled_at
        if enabled and not current.enabled:
            enabled_at = now
        actor = re.sub(r"[^A-Za-z0-9._:-]", "", str(actor))[:MAX_ACTOR_CHARS] or "onbekend"
        atomic_write_json(self.path, {"schema": SWITCH_SCHEMA, "enabled": enabled, "enabled_at": enabled_at,
                                      "updated_at": now, "updated_by": actor})
        return self.read()


# ---------------------------------------------------------------- configuratie

SECURITY_MODES = ("starttls", "ssl", "none")
_TRUE = {"1", "true", "yes", "ja", "on", "aan"}
_FALSE = {"0", "false", "no", "nee", "off", "uit"}
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_CLOCK = re.compile(r"^(\d{1,2}):(\d{2})$")


def _bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in _TRUE:
            return True
        if low in _FALSE:
            return False
    return default


def parse_quiet_hours(value: Any) -> tuple[int, int] | None:
    """``"22:00-07:00"`` -> (1320, 420) minuten na middernacht; alles anders -> None."""
    if not isinstance(value, str) or "-" not in value:
        return None
    parts = [part.strip() for part in value.split("-", 1)]
    minutes = []
    for part in parts:
        match = _CLOCK.match(part)
        if not match or int(match.group(1)) > 23 or int(match.group(2)) > 59:
            return None
        minutes.append(int(match.group(1)) * 60 + int(match.group(2)))
    return (minutes[0], minutes[1]) if minutes[0] != minutes[1] else None


@dataclass(frozen=True)
class NotifyConfig:
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_security: str = "starttls"
    smtp_user: str = ""
    sender: str = ""
    recipient: str = ""
    notify_completed: bool = True
    notify_failed: bool = True
    poll_seconds: float = 30.0
    #: Een menselijke grens moet zo lang onafgebroken openstaan voor er gemeld wordt.
    debounce_seconds: float = 60.0
    #: Taken/sessies zonder activiteit sinds zoveel uur zijn oud nieuws.
    stale_after_hours: float = 72.0
    #: Optioneel, "22:00-07:00". ACTION_REQUIRED wordt standaard óók dan afgeleverd.
    quiet_hours: str | None = None
    quiet_hours_urgent: bool = True
    problems: tuple[str, ...] = field(default=())

    @property
    def transport_ready(self) -> bool:
        return bool(self.smtp_host and self.sender and self.recipient) and not self.problems

    def public(self) -> dict:
        """Wat naar de monitor mag: nooit een adres of wachtwoord, alleen of het compleet is."""
        return {"configured": self.transport_ready, "smtp_security": self.smtp_security,
                "notify_completed": self.notify_completed, "notify_failed": self.notify_failed,
                "quiet_hours": self.quiet_hours, "quiet_hours_urgent": self.quiet_hours_urgent}


def config_path(home: Path | None = None) -> Path:
    return (home or notify_home()) / "config.json"


def load_config(home: Path | None = None, environ: dict | None = None) -> NotifyConfig:
    """Bestand eerst, ``OBF_NOTIFY_*``-omgevingsvariabelen daarbovenop. Geen wachtwoorden hier."""
    env = os.environ if environ is None else environ
    raw = read_json(config_path(home))
    data: dict[str, Any] = dict(raw) if isinstance(raw, dict) else {}
    for key in ("smtp_host", "smtp_port", "smtp_security", "smtp_user", "sender", "recipient",
                "notify_completed", "notify_failed", "poll_seconds", "debounce_seconds",
                "stale_after_hours", "quiet_hours", "quiet_hours_urgent"):
        value = env.get("OBF_NOTIFY_" + key.upper())
        if value not in (None, ""):
            data[key] = value
    problems: list[str] = []

    def num(key: str, default: float, low: float, high: float) -> float:
        try:
            value = float(data.get(key, default))
        except (TypeError, ValueError):
            problems.append(f"{key} is geen getal")
            return default
        return min(max(value, low), high)

    try:
        port = int(data.get("smtp_port", 587))
        if not 1 <= port <= 65535:
            raise ValueError
    except (TypeError, ValueError):
        problems.append("smtp_port is ongeldig")
        port = 587
    security = str(data.get("smtp_security", "starttls")).strip().lower()
    host = str(data.get("smtp_host", "")).strip()
    if security not in SECURITY_MODES:
        problems.append("smtp_security moet starttls, ssl of none zijn")
        security = "starttls"
    if security == "none" and host and host.lower() not in _LOCAL_HOSTS:
        problems.append("smtp_security=none is alleen toegestaan voor localhost")
    quiet = data.get("quiet_hours")
    if quiet and parse_quiet_hours(quiet) is None:
        problems.append("quiet_hours moet de vorm 22:00-07:00 hebben")
        quiet = None
    return NotifyConfig(
        smtp_host=host, smtp_port=port, smtp_security=security,
        smtp_user=str(data.get("smtp_user", "")).strip(),
        sender=str(data.get("sender", "")).strip(), recipient=str(data.get("recipient", "")).strip(),
        notify_completed=_bool(data.get("notify_completed"), True),
        notify_failed=_bool(data.get("notify_failed"), True),
        poll_seconds=num("poll_seconds", 30.0, 5.0, 600.0),
        debounce_seconds=num("debounce_seconds", 60.0, 0.0, 3600.0),
        stale_after_hours=num("stale_after_hours", 72.0, 1.0, 24.0 * 30),
        quiet_hours=str(quiet) if quiet else None,
        quiet_hours_urgent=_bool(data.get("quiet_hours_urgent"), True),
        problems=tuple(problems))


def save_config(values: dict, home: Path | None = None) -> Path:
    """Schrijft alleen bekende, niet-geheime velden."""
    allowed = {"smtp_host", "smtp_port", "smtp_security", "smtp_user", "sender", "recipient",
               "notify_completed", "notify_failed", "poll_seconds", "debounce_seconds",
               "stale_after_hours", "quiet_hours", "quiet_hours_urgent"}
    path = config_path(home)
    current = read_json(path)
    merged = dict(current) if isinstance(current, dict) else {}
    merged.update({key: value for key, value in values.items() if key in allowed and value is not None})
    atomic_write_json(path, merged)
    return path


# --------------------------------------------------------------------- secret

def read_password(config: NotifyConfig) -> str | None:
    """Sleutelhanger eerst, dan de omgevingsvariabele. Nooit uit een bestand in de repo."""
    if not config.smtp_user:
        return None
    try:
        import keyring  # noqa: PLC0415

        value = keyring.get_password(KEYRING_SERVICE, config.smtp_user)
        if value:
            return value
    except Exception:  # noqa: BLE001 -- geen kluis beschikbaar is een toestand, geen crash
        pass
    return os.environ.get(PASSWORD_ENV) or None


def store_password(user: str, password: str) -> None:
    import keyring  # noqa: PLC0415

    keyring.set_password(KEYRING_SERVICE, user, password)
