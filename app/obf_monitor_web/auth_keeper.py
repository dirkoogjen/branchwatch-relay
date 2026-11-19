"""De Claude-aanmelding op de Mac vers houden, zonder dat er een Mac-taak voor nodig is.

Waarom
------
Het toegangstoken van Claude Code leeft ongeveer acht uur. Tot nu toe werd het
alleen vernieuwd door de runner-preflight vóór een Mac-taak. Wie urenlang alleen
via Gerard werkt, zag op de monitor "Usage verouderd" en had op de Mac een
verlopen token.

Wat dit doet -- en wat niet
---------------------------
Bij elke gewone usage-update kijkt :meth:`MacAuthKeeper.ensure` alleen naar de
*vervaltijd* van het token (dezelfde sleutelhangerlezing die de usagekaart al
doet; de tokenwaarde wordt niet bewaard). Is het token nog ruim geldig, dan
gebeurt er niets. Zit het in Claude Codes eigen vernieuwingsvenster of is het
verlopen, dan roept hij **de bestaande runner-preflight** aan
(``obf_mcp.mac_auth.MacAuthPreflight`` -> ``obf_claude_runner auth-preflight``):
dezelfde route, dezelfde sandbox, hetzelfde refresh-slot
(``~/obf-runner-bridge/auth-refresh.lock``) en dezelfde sleutelhangeropslag als
vóór een Mac-taak. Er is hier geen OAuth, geen sleutelhangerschrijven en geen
tweede refreshroute.

Die runnerroute laat Claude Code het token vernieuwen voorafgaand aan één
minimale, tool-loze ``claude -p``-aanvraag in plan-mode ("OK"): het enige
bewezen veilige vernieuwingspad (zie ``auth_refresh.py`` in de runner). Er wordt
geen programmeertaak, controllertaak, bridge-job of projectsessie gestart.

GERARD (Linux)
--------------
Dezelfde keeper draait ook op GERARD (``LinuxClaudeRefresh``). Daar bestaat de runner-
preflight van de Mac niet; Claude Code vernieuwt zijn token echter zelf bij het starten
van een sessie. De keeper start daarom -- alleen binnen het vernieuwingsvenster of na
verloop -- één minimale, tool-loze ``claude -p``-sessie met het abonnement van deze
machine (zonder API-sleutel in de omgeving) en bewijst het resultaat door de vervaltijd
opnieuw te lezen. Is het refresh-token zelf verlopen, dan wordt niets gestart en meldt de
kaart "interactieve aanmelding vereist". Vroeger gaf ``keeper_for_this_machine`` buiten
macOS ``None``: op GERARD vernieuwde dan niets het token terwijl de kaart "wordt
automatisch vernieuwd" beloofde, zodat een rustige GERARD dagen op "verlopen" bleef staan.

Waarom het venster 280 s is en niet de 600 s van de preflight
------------------------------------------------------------
De preflight weigert een *taakstart* met minder dan 10 minuten geldigheid. Maar
Claude Code vernieuwt zelf pas als het token binnen vijf minuten verloopt; een
refresh die eerder wordt aangevraagd doet de "OK"-aanvraag zonder te
vernieuwen en eindigt als ``refresh_not_proven``. Daarom pas binnen 280 s (met
marge op die vijf minuten) of na verloop.

Nooit dubbel
------------
* binnen één monitorproces: één ``ensure`` tegelijk;
* tussen de drie monitors: een niet-blokkerend ``flock`` op
  ``~/obf-runner-bridge/auth-keeper.lock`` -- wie het niet krijgt, doet niets;
* tegen een gelijktijdige Mac-taak: het runner-refresh-slot; wie als tweede komt
  ziet het vernieuwde token en vernieuwt niet opnieuw (``refreshed_by_other``).

Na een mislukking wordt pas na een wachttijd opnieuw geprobeerd; die staat in
een geheimloos gedeeld bestand, zodat de drie monitors dezelfde pauze houden.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

#: Claude Code vernieuwt zelf binnen vijf minuten vóór verloop; dit is die grens met marge.
REFRESH_WINDOW_SECONDS = 280
#: Na een tijdelijke fout: niet eerder opnieuw.
RETRY_AFTER_FAILURE_SECONDS = 600
#: Na "opnieuw aanmelden vereist" helpt vernieuwen niet; zelden opnieuw kijken.
RETRY_AFTER_LOGIN_REQUIRED_SECONDS = 3600

#: Langer dan dit "bezig" is een achtergebleven toestand, geen lopende vernieuwing.
STALE_REFRESHING_SECONDS = 900
#: Alleen korte codes verlaten deze module, nooit vrije tekst.
_CODE = re.compile(r"^[a-z0-9_]{1,64}$")

DEFAULT_STATE_FILE = Path("~/obf-runner-bridge/claude-auth-keeper.json")
DEFAULT_LOCK_FILE = Path("~/obf-runner-bridge/auth-keeper.lock")
STATE_SCHEMA = "obf-claude-auth-keeper-v1"

IDLE = "idle"
REFRESHING = "refreshing"
REFRESHED = "refreshed"
VALID_ALREADY = "valid"
FAILED = "failed"
LOGIN_REQUIRED = "login_required"
SKIPPED = "skipped"


def read_access_expiry(reader: Callable[[], dict] | None = None) -> float | None:
    """De vervaltijd (epoch s) van het toegangstoken, of ``None``. Nooit de tokenwaarde."""
    if reader is None:
        from .claude_usage import _read_credentials  # noqa: PLC0415 -- dezelfde leesroute

        reader = _read_credentials
    payload = reader()
    oauth = payload.get("claudeAiOauth") if isinstance(payload, dict) else None
    value = oauth.get("expiresAt") if isinstance(oauth, dict) else None
    del payload, oauth
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        return None
    return float(value) / 1000.0


def _iso(epoch: float | None) -> str | None:
    return None if epoch is None else datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")


def _epoch(iso) -> float | None:
    if not isinstance(iso, str):
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


class MacAuthKeeper:
    """Houdt het Mac-token vers via de bestaande runner-preflight. Thread-safe."""

    def __init__(self, *, preflight_factory: Callable[[], object] | None = None,
                 expiry_reader: Callable[[], float | None] = read_access_expiry,
                 clock: Callable[[], float] = time.time,
                 state_path: Path | None = None, lock_path: Path | None = None,
                 window_seconds: float = REFRESH_WINDOW_SECONDS):
        self._preflight_factory = preflight_factory or _default_preflight
        self._expiry = expiry_reader
        self._clock = clock
        self._state_path = Path(state_path or DEFAULT_STATE_FILE).expanduser()
        self._lock_path = Path(lock_path or DEFAULT_LOCK_FILE).expanduser()
        self._window = window_seconds
        self._busy = threading.Lock()
        self._active = False

    # -- lezen --------------------------------------------------------------

    def state(self) -> dict:
        """Wat de kaart over de autonome vernieuwing mag zeggen: codes en tijden."""
        shared = self._read_state()
        if self._active:
            return {**shared, "state": REFRESHING}
        if shared.get("state") == REFRESHING:
            # Een "bezig" van een monitor die halverwege stierf blijft niet eeuwig staan.
            started = _epoch(shared.get("attempted_at"))
            if started is None or self._clock() - started > STALE_REFRESHING_SECONDS:
                return {**shared, "state": IDLE}
        return shared

    def _read_state(self) -> dict:
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"state": IDLE}
        if not isinstance(payload, dict) or payload.get("schema") != STATE_SCHEMA:
            return {"state": IDLE}
        clean = {"state": payload.get("state") if isinstance(payload.get("state"), str) else IDLE}
        if isinstance(payload.get("reason"), str) and _CODE.match(payload["reason"]):
            clean["reason"] = payload["reason"]
        for key in ("attempted_at", "next_attempt_at", "finished_at"):
            if _epoch(payload.get(key)) is not None and len(payload[key]) <= 40:
                clean[key] = payload[key]
        return clean

    def _write_state(self, **fields) -> None:
        payload = {"schema": STATE_SCHEMA, **{k: v for k, v in fields.items() if v is not None}}
        try:
            self._state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(self._state_path.parent),
                                                 prefix=self._state_path.name + ".", suffix=".tmp", delete=False)
            try:
                with handle:
                    os.fchmod(handle.fileno(), 0o600)
                    json.dump(payload, handle, sort_keys=True)
                os.replace(handle.name, self._state_path)
            except BaseException:
                Path(handle.name).unlink(missing_ok=True)
                raise
        except OSError:
            pass  # niet kunnen delen is geen storing

    # -- beslissen ----------------------------------------------------------

    def needs_refresh(self, now: float) -> bool:
        try:
            expires = self._expiry()
        except Exception:  # noqa: BLE001 -- onleesbaar: de usagekaart meldt dat zelf
            return False
        return expires is not None and expires - now < self._window

    def ensure(self) -> str:
        """Eén controle; vernieuwt alleen als het nodig en toegestaan is. Geeft de uitkomst."""
        if not self._busy.acquire(blocking=False):
            return SKIPPED
        try:
            now = self._clock()
            if not self.needs_refresh(now):
                return IDLE
            shared = self._read_state()
            next_at = _epoch(shared.get("next_attempt_at"))
            if next_at is not None and now < next_at:
                return SKIPPED
            fd = self._try_lock()
            if fd is None:
                return SKIPPED  # een andere monitor is er al mee bezig
            try:
                self._active = True
                self._write_state(state=REFRESHING, attempted_at=_iso(now))
                outcome = self._run_preflight()
                return self._record(outcome, now)
            finally:
                self._active = False
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
        finally:
            self._busy.release()

    def _try_lock(self) -> int | None:
        try:
            self._lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        except OSError:
            return None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                return None
            return None
        return fd

    def _run_preflight(self):
        try:
            return self._preflight_factory().check()
        except Exception:  # noqa: BLE001 -- een fout hier mag de monitor niet raken
            return None

    def _record(self, outcome, started: float) -> str:
        finished = self._clock()
        status = getattr(outcome, "status", None)
        reason = getattr(outcome, "reason", None)
        reason = reason if isinstance(reason, str) and _CODE.match(reason) else None
        if status == "valid":
            result = REFRESHED if getattr(outcome, "refreshed", False) else VALID_ALREADY
            self._write_state(state=result, reason=reason, attempted_at=_iso(started), finished_at=_iso(finished))
            return result
        if status == "login_required":
            self._write_state(state=LOGIN_REQUIRED, reason=reason, attempted_at=_iso(started),
                              finished_at=_iso(finished),
                              next_attempt_at=_iso(finished + RETRY_AFTER_LOGIN_REQUIRED_SECONDS))
            return LOGIN_REQUIRED
        self._write_state(state=FAILED, reason=reason or (status if isinstance(status, str) else "preflight_unusable"),
                          attempted_at=_iso(started), finished_at=_iso(finished),
                          next_attempt_at=_iso(finished + RETRY_AFTER_FAILURE_SECONDS))
        return FAILED


def _default_preflight():
    # Dezelfde adapter én dezelfde configuratie als vóór een Mac-taak; die kiest
    # zelf de ``claude`` (ook op het kale PATH van een LaunchAgent).
    from obf_mcp.mac_auth import MacAuthPreflight  # noqa: PLC0415

    return MacAuthPreflight.from_environment()


# ---------------------------------------------------------------- GERARD (Linux)

#: Een Claude-sessie die niets doet behalve starten: Claude Code vernieuwt het token bij het opstarten
#: van een sessie zelf (``auth: init`` -> ``checkAndRefreshOAuthTokenIfNeeded``), onder zijn eigen slot.
#: Tool-loos, plan-mode, goedkoopste model, niets op schijf: geen taak, geen projectsessie.
LINUX_REFRESH_TIMEOUT_SECONDS = 120
_LINUX_ENV_KEEP = ("HOME", "PATH", "LANG", "LC_ALL", "USER", "LOGNAME", "TMPDIR")
#: Alleen als Claude zelf om opnieuw aanmelden vraagt; de tekst wordt gelezen en weggegooid.
_LOGIN_HINT = re.compile(r"/login|auth login|authentication_error|invalid authentication|oauth.{0,40}(expired|revoked)",
                         re.IGNORECASE)


@dataclass(frozen=True)
class RefreshOutcome:
    """Zelfde vorm als ``MacAuthOutcome`` voor de keeper: alleen korte codes, nooit tekst of tokens."""

    status: str  # valid | login_required | failed
    reason: str
    refreshed: bool = False


class LinuxClaudeRefresh:
    """Preflight-tegenhanger voor GERARD: laat Claude Code zijn eigen token vernieuwen.

    Er is hier geen OAuth-code, geen schrijven naar het referentiebestand en geen tweede
    vernieuwroute: de enige handeling is één minimale ``claude -p``-sessie, door het
    abonnement van deze machine, zonder API-sleutel of tokens in de omgeving. Daarna wordt
    alleen de *vervaltijd* opnieuw gelezen om te bewijzen dat het gelukt is.
    """

    def __init__(self, *, claude_path: str | None = None, credentials_reader: Callable[[], dict] | None = None,
                 runner: Callable = subprocess.run, clock: Callable[[], float] = time.time,
                 which: Callable[[str], str | None] = shutil.which, home: Path | None = None,
                 window_seconds: float = REFRESH_WINDOW_SECONDS,
                 timeout: float = LINUX_REFRESH_TIMEOUT_SECONDS):
        self._claude = claude_path
        self._reader = credentials_reader
        self._run = runner
        self._clock = clock
        self._which = which
        self._home = Path(home).expanduser() if home is not None else Path.home()
        self._window = window_seconds
        self._timeout = timeout

    def _times(self) -> tuple[float | None, float | None]:
        """(vervaltijd toegangstoken, vervaltijd refresh-token) in epoch s; geen tokenwaarden."""
        reader = self._reader
        if reader is None:
            from .claude_usage import _read_credentials  # noqa: PLC0415

            reader = _read_credentials
        try:
            payload = reader()
        except Exception:  # noqa: BLE001
            return None, None
        oauth = payload.get("claudeAiOauth") if isinstance(payload, dict) else None
        if not isinstance(oauth, dict):
            return None, None

        def seconds(value):
            return None if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0 else value / 1000.0

        return seconds(oauth.get("expiresAt")), seconds(oauth.get("refreshTokenExpiresAt"))

    def _find_claude(self) -> str | None:
        if self._claude:
            return self._claude
        found = self._which("claude")
        if found:
            return found
        local = self._home / ".local" / "bin" / "claude"
        return str(local) if local.exists() else None

    def _environment(self) -> dict:
        env = {name: os.environ[name] for name in _LINUX_ENV_KEEP if name in os.environ}
        env.setdefault("HOME", str(self._home))
        env.setdefault("PATH", f"{self._home}/.local/bin:/usr/local/bin:/usr/bin:/bin")
        return env  # bewust geen ANTHROPIC_*, CLAUDE_* of OBF_*: alleen het abonnement, nooit een API-sleutel

    def check(self) -> RefreshOutcome:
        expires, refresh_end = self._times()
        now = self._clock()
        if refresh_end is not None and refresh_end <= now:
            return RefreshOutcome("login_required", "refresh_token_expired")
        if expires is None:
            return RefreshOutcome("failed", "geen_token")
        if expires - now > self._window:
            return RefreshOutcome("valid", "still_valid")
        claude = self._find_claude()
        if claude is None:
            return RefreshOutcome("failed", "claude_not_found")
        command = [claude, "-p", "OK", "--permission-mode", "plan", "--tools", "", "--no-session-persistence",
                   "--disable-slash-commands", "--strict-mcp-config", "--model", "haiku"]
        try:
            with tempfile.TemporaryDirectory(prefix="monitor2-auth-") as workdir:
                done = self._run(command, cwd=workdir, env=self._environment(), stdin=subprocess.DEVNULL,
                                 capture_output=True, text=True, timeout=self._timeout)
        except subprocess.TimeoutExpired:
            return RefreshOutcome("failed", "timeout")
        except (OSError, subprocess.SubprocessError):
            return RefreshOutcome("failed", "claude_start_failed")
        after, _ = self._times()
        if after is not None and after > expires + 60 and after - self._clock() > self._window:
            return RefreshOutcome("valid", "refreshed", refreshed=True)
        text = f"{getattr(done, 'stdout', '') or ''}\n{getattr(done, 'stderr', '') or ''}"
        if _LOGIN_HINT.search(text):
            return RefreshOutcome("login_required", "login_required")
        if getattr(done, "returncode", 0) != 0:
            return RefreshOutcome("failed", "claude_exit")
        return RefreshOutcome("failed", "refresh_not_proven")


def _linux_state_dir() -> Path:
    raw = os.environ.get("OBF_CLAUDE_AUTH_STATUS_FILE")
    return Path(raw).expanduser().parent if raw else Path("~/.local/state/monitor2").expanduser()


def keeper_for_this_machine() -> MacAuthKeeper | None:
    """Mac: de runner-preflight. GERARD (Linux): een minimale Claude-sessie. Elders niets.

    ``OBF_MONITOR_AUTH_KEEPER=0`` zet de autonome vernieuwing uit.
    """
    import sys  # noqa: PLC0415

    if os.environ.get("OBF_MONITOR_AUTH_KEEPER") == "0":
        return None
    if sys.platform == "darwin":
        return MacAuthKeeper()
    if sys.platform.startswith("linux"):
        state_dir = _linux_state_dir()
        return MacAuthKeeper(preflight_factory=LinuxClaudeRefresh,
                             state_path=state_dir / "claude-auth-keeper.json",
                             lock_path=state_dir / "claude-auth-keeper.lock")
    return None
