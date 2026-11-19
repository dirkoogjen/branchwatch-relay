"""De Claude-aanmelding op de Mac, vóór elke taak die Claude op de Mac start.

Waarom dit bestaat
------------------
Het toegangstoken van Claude Code leeft ongeveer acht uur. Tussen twee taken
vernieuwt niets op de Mac het, dus zonder deze stap begon de eerste taak van de
dag gewoon, liep tegen een 401 aan en moest de runner halverwege vernieuwen. De
runner heeft daarvoor nu een eigen, geteste voorcontrole:
``python -m obf_claude_runner auth-preflight`` (runner-commit 72d1922). Die kijkt
naar de aanmelding, laat zo nodig **Claude Code zelf** vernieuwen en zegt daarna
in één JSON-object of er gestart mag worden.

Dit bestand is uitsluitend de dunne adapter naar die CLI. Er staat hier geen
credentiallogica: geen sleutelhanger, geen token, geen OAuth, geen refresh. Wat
de CLI antwoordt wordt teruggebracht tot een handvol codes en tijden; stderr en
alle overige tekst worden nooit doorgegeven.

Alleen ``valid`` laat een start door. Al het andere weigert fail-closed, met een
melding die zegt wat er aan de hand is -- en ``claude auth login`` alleen
wanneer de runner werkelijk heeft vastgesteld dat een interactieve aanmelding
nodig is.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

#: De vijf uitkomsten van de runner-preflight (``obf_claude_runner.auth_preflight.STATUSES``).
VALID = "valid"
REFRESHABLE = "refreshable"
LOGIN_REQUIRED = "login_required"
REFRESH_VERSION_UNVALIDATED = "refresh_version_unvalidated"
TEMPORARY_FAILURE = "temporary_failure"
STATUSES = (VALID, REFRESHABLE, LOGIN_REQUIRED, REFRESH_VERSION_UNVALIDATED, TEMPORARY_FAILURE)

#: Ruim boven wat de runner zelf maximaal nodig heeft: wachten op het refresh-slot
#: (300 s) plus Claude Codes eigen refreshverzoek (180 s) plus de canaries.
PREFLIGHT_TIMEOUT_SECONDS = 600.0

#: Wat van het runnerantwoord hier gebruikt mag worden. Alles wat niet in deze
#: lijst staat, bestaat voor Studio OBF niet.
_FIELDS = ("status", "reason", "start_allowed", "refreshed", "refresh_attempted", "claude_version",
           "refresh_version_validated", "access_token_expires_at", "refresh_token_expires_at",
           "last_refresh_at", "last_refresh_result", "security_finding", "api_error_status")
_CODE = re.compile(r"^[a-z0-9_]{1,64}$")
_VERSION = re.compile(r"^\d+\.\d+\.\d+$")
_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")

LOGIN_COMMAND = "claude auth login"


class MacAuthError(Exception):
    """De Mac-preflight weigert een start. Draagt een code, nooit een geheim."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class MacAuthOutcome:
    status: str
    reason: str | None = None
    start_allowed: bool = False
    refreshed: bool = False
    claude_version: str | None = None
    last_refresh_at: str | None = None

    def response_note(self) -> dict[str, Any] | None:
        """Wat een startantwoord hierover zegt: alleen iets als er werkelijk vernieuwd is."""
        if self.status != VALID or not self.refreshed:
            return None
        return {"status": VALID, "refreshed": True, "refreshed_at": self.last_refresh_at,
                "message": "Claude-auth automatisch vernieuwd vóór deze start."}


def _clean(payload: Any) -> dict[str, Any]:
    """Alleen bekende velden, en alleen in de vorm die ze horen te hebben."""
    if not isinstance(payload, dict):
        return {}
    clean: dict[str, Any] = {}
    for name in _FIELDS:
        value = payload.get(name)
        if isinstance(value, bool):
            clean[name] = value
        elif name in ("status", "reason", "last_refresh_result") and isinstance(value, str) and _CODE.match(value):
            clean[name] = value
        elif name == "claude_version" and isinstance(value, str) and _VERSION.match(value):
            clean[name] = value
        elif name.endswith("_at") and isinstance(value, str) and _ISO.match(value):
            clean[name] = value
        elif name == "api_error_status" and isinstance(value, int):
            clean[name] = value
    return clean


def refusal_message(outcome: MacAuthOutcome) -> tuple[str, str]:
    """(code, melding) voor een geweigerde start. Er is dan niets gestart."""
    reason = f" ({outcome.reason})" if outcome.reason else ""
    if outcome.status == LOGIN_REQUIRED:
        return ("claude_login_required",
                f"Claude Code opnieuw aanmelden vereist{reason}. Open Terminal en voer `{LOGIN_COMMAND}` uit. "
                "Probeer daarna de taak opnieuw; de controle loopt dan vanzelf opnieuw. Er is niets gestart.")
    if outcome.status == REFRESH_VERSION_UNVALIDATED:
        version = outcome.claude_version or "onbekend"
        return ("claude_refresh_version_unvalidated",
                f"De geïnstalleerde Claude Code-versie ({version}) is nog niet gevalideerd voor automatische "
                f"vernieuwing van de aanmelding{reason}. Er is niets gestart; er wordt niet automatisch "
                "ingelogd en niet uitgeweken naar een andere route. Eerst moet de runner deze versie valideren.")
    if outcome.status == TEMPORARY_FAILURE:
        return ("claude_auth_temporary_failure",
                f"De Claude-aanmeldcontrole op de Mac is tijdelijk mislukt{reason}. Dat betekent niet dat de "
                "aanmelding verlopen is: opnieuw aanmelden is hiervoor niet nodig. Probeer het later opnieuw. "
                "Er is niets gestart.")
    if outcome.status == REFRESHABLE:
        return ("claude_auth_refresh_not_performed",
                "De Claude-aanmeldcontrole gaf 'refreshable' terug terwijl vernieuwen was toegestaan; dat "
                "hoort niet de eindstand te zijn. Fail-closed geweigerd; er is niets gestart.")
    return ("claude_auth_unusable",
            "De Claude-aanmeldcontrole gaf geen bruikbare uitkomst; fail-closed geweigerd. Er is niets gestart.")


class MacAuthPreflight:
    """Roept de runner-CLI aan met precies de Python, runner en Claude die de run gebruikt."""

    def __init__(self, *, runner_python: Path, runner_root: Path, claude_executable: Path | None,
                 timeout_seconds: float = PREFLIGHT_TIMEOUT_SECONDS,
                 run: Callable[..., subprocess.CompletedProcess] = subprocess.run):
        self.runner_python = Path(runner_python)
        self.runner_root = Path(runner_root)
        self.claude_executable = Path(claude_executable) if claude_executable else None
        self.timeout_seconds = timeout_seconds
        self._run = run

    @classmethod
    def from_environment(cls) -> "MacAuthPreflight":
        # Dezelfde configuratie die de brug gebruikt om de runner zelf te starten.
        from .claude_bridge import BridgeConfig  # noqa: PLC0415

        config = BridgeConfig.from_environment()
        return cls(runner_python=config.runner_python, runner_root=config.runner_root,
                   claude_executable=config.claude_executable)

    def argv(self) -> list[str]:
        argv = [str(self.runner_python), "-m", "obf_claude_runner", "auth-preflight"]
        if self.claude_executable is not None:
            argv += ["--claude-executable", str(self.claude_executable)]
        return argv

    def check(self) -> MacAuthOutcome:
        env = dict(os.environ)
        src = str(self.runner_root / "src")
        env["PYTHONPATH"] = src if not env.get("PYTHONPATH") else f"{src}{os.pathsep}{env['PYTHONPATH']}"
        try:
            completed = self._run(self.argv(), cwd=str(self.runner_root), env=env, capture_output=True,
                                  text=True, timeout=self.timeout_seconds, check=False,
                                  stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            return MacAuthOutcome(TEMPORARY_FAILURE, "auth_preflight_timeout")
        except OSError:
            return MacAuthOutcome(TEMPORARY_FAILURE, "auth_preflight_unavailable")
        lines = [line for line in (completed.stdout or "").splitlines() if line.strip()]
        try:
            payload = _clean(json.loads(lines[-1])) if lines else {}
        except ValueError:
            payload = {}
        status = payload.get("status")
        if status not in STATUSES:
            return MacAuthOutcome(TEMPORARY_FAILURE, "auth_preflight_unusable")
        return MacAuthOutcome(
            status=status, reason=payload.get("reason"),
            start_allowed=payload.get("start_allowed") is True and status == VALID,
            refreshed=payload.get("refreshed") is True, claude_version=payload.get("claude_version"),
            last_refresh_at=payload.get("last_refresh_at"))

    def require(self) -> MacAuthOutcome:
        """``valid`` en ``start_allowed``, of een :class:`MacAuthError`. Nooit iets daartussen."""
        outcome = self.check()
        if outcome.status == VALID and outcome.start_allowed:
            return outcome
        code, message = refusal_message(outcome)
        raise MacAuthError(code, message)
