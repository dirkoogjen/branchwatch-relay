"""Wat er van het Claude Max-abonnement nog over is, zoals Claude het zelf zegt.

Eén bron, één component, twee monitors
--------------------------------------
Beheerder gebruikt op de Mac en op de Ubuntu-node hetzelfde Claude Max-account. De
planlimieten zijn dus *accountbreed*: ze horen niet bij een machine en niet bij
een taak. Daarom is dit één module, die door beide webmonitors (8770 Mac, 8771
node) op precies dezelfde manier wordt gebruikt; er is geen Mac-variant en geen
Linux-variant van dezelfde feature.

De bron
-------
``GET https://api.anthropic.com/api/oauth/usage`` -- hetzelfde adres dat Claude
Code zelf gebruikt voor ``/usage``, met hetzelfde OAuth-token uit de
sleutelhanger. Het antwoord bevat een veld ``limits``: een lijst met per limiet
een ``kind`` (``session``, ``weekly_all``, ``weekly_scoped``), een ``percent`` en
een ``resets_at``. Dat is de enige plek waar de getallen vandaan komen.

Er wordt hier dus **niets uitgerekend**. Geen percentage uit tokens, geen
schatting uit kosten, geen afgelezen scherm. Wat Claude niet meldt, meldt de
monitor niet.

Wat er bewust *niet* uit wordt overgenomen
------------------------------------------
``spend`` (de Cost/Usage Credits), ``extra_usage`` en ``seven_day_breakdown``
("What's using your limits?"). Die zijn niet gevraagd en horen niet op een
werkmonitor; ze worden niet doorgegeven en dus ook niet per ongeluk getoond.

Geheimen blijven hier
---------------------
Het token wordt per ophaalpoging vers gelezen, meegegeven in één header en
daarna losgelaten. Het staat niet in de snapshot, niet in het cachebestand, niet
in een foutmelding en niet in een logregel -- een fout wordt vertaald naar een
korte code (``netwerk``, ``geweigerd``, ``geen_token``). Wat de browser krijgt is
uitsluitend: een label, een percentage, een resetmoment en een leeftijd.

De aanmelding: alleen wat de runner-preflight vaststelde
------------------------------------------------------
Een verlopen *toegangstoken* is geen verlopen aanmelding: zolang het
refresh-token geldig is, kan Claude Code het vernieuwen. Dat gebeurt uitsluitend
in ``obf_claude_runner auth-preflight``: vóór elke Mac-taak, en -- sinds de
:mod:`.auth_keeper` -- ook vanuit deze usage-update zodra het token in Claude
Codes vernieuwingsvenster zit, zonder dat er een Mac-taak nodig is. De preflight
schrijft daarna een geheimloos statusbestand (``~/obf-runner-bridge/claude-auth-status.json``).
Dat bestand is de enige bron voor wat de kaart over de aanmelding zegt:
"interactieve aanmelding vereist" verschijnt alleen als de preflight dat werkelijk
heeft vastgesteld. Na een geslaagde vernieuwing vervalt de wachttijd na een
eerdere fout, zodat de usage meteen opnieuw wordt opgehaald.

Eerlijk over ouderdom
---------------------
Een snapshot draagt altijd zijn leeftijd. Binnen ``FRESH_SECONDS`` is hij vers;
daarna wordt er op de achtergrond opnieuw opgehaald terwijl de oude waarde nog
even mag blijven staan. Lukt dat niet, dan wordt de waarde na
``STALE_AFTER_SECONDS`` uitdrukkelijk als verouderd gemarkeerd en na
``FORGET_AFTER_SECONDS`` helemaal losgelaten: dan staat er "Niet beschikbaar".
Een oud getal wordt nooit als actueel gepresenteerd.

Zuinig met verzoeken
--------------------
De browser vraagt elke twee seconden om de monitorstatus; Anthropic wordt
hoogstens eens per ``FRESH_SECONDS`` bevraagd, en na een fout hoogstens eens per
``RETRY_AFTER_ERROR_SECONDS``. Bovendien delen 8770 en 8771 één cachebestand in
``~/obf-runner-bridge/``, dezelfde duurzame map als de runnerkeuze: wie het eerst
ophaalt, haalt op voor allebei. Het ophalen gebeurt altijd op een achtergrond-
draad, zodat een trage of hangende verbinding de monitorpagina nooit ophoudt.
"""

from __future__ import annotations

import json
import os
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
USAGE_HOST = "api.anthropic.com"

#: Zoals Claude Code zelf: een OAuth-token van het abonnement, geen API-sleutel.
OAUTH_BETA = "oauth-2025-04-20"

#: De sleutelhanger op macOS; op andere platformen het bestand dat Claude Code daar gebruikt.
KEYCHAIN_SERVICE = "Claude Code-credentials"
CREDENTIALS_FILE = Path("~/.claude/.credentials.json")

#: Zo vers dat er niets hoeft te gebeuren.
FRESH_SECONDS = 120
#: Daarna is een waarde nog bruikbaar, maar hij wordt als verouderd getoond.
STALE_AFTER_SECONDS = 600
#: En daarna helemaal niet meer: liever "Niet beschikbaar" dan een oud getal.
FORGET_AFTER_SECONDS = 3600
#: Na een mislukte poging niet meteen opnieuw aankloppen.
RETRY_AFTER_ERROR_SECONDS = 60

#: Het geheimloze statusbestand van de runner-preflight (runner-commit 72d1922).
DEFAULT_AUTH_STATUS_FILE = Path("~/obf-runner-bridge/claude-auth-status.json")
AUTH_STATUS_FILE_ENV = "OBF_CLAUDE_AUTH_STATUS_FILE"
AUTH_STATUS_SCHEMA = "obf-claude-auth-status-v1"
#: Zolang meldt de kaart dat de aanmelding automatisch vernieuwd is.
AUTH_REFRESH_NOTE_SECONDS = 8 * 3600

#: Harde grenzen op het verzoek zelf.
TIMEOUT_SECONDS = 12.0
MAX_RESPONSE_BYTES = 256 * 1024

#: Eén cachebestand voor beide monitors, in dezelfde duurzame map als de runnerkeuze.
DEFAULT_CACHE_FILE = Path("~/obf-runner-bridge/claude-usage.json")
CACHE_FILE_ENV = "OBF_CLAUDE_USAGE_CACHE"
CACHE_SCHEMA = "obf-claude-usage-v1"

#: De drie soorten die getoond worden, in deze volgorde. Alles wat Claude verder
#: meldt (bestedingen, tegoeden, verdeling per product) blijft hier buiten.
_KIND_ORDER = {"session": 0, "weekly_all": 1, "weekly_scoped": 2}

#: Korte, geheimloze redenen. De browser krijgt alleen deze woorden te zien.
_PROBLEM_TEXT = {
    "geen_token": "Geen Claude Code-aanmelding gevonden op deze machine.",
    "token_verlopen": "Toegangstoken verlopen — wordt automatisch vernieuwd.",
    "geweigerd": "Claude weigert de usage-aanvraag (401/403).",
    "netwerk": "Usage tijdelijk niet beschikbaar (Claude onbereikbaar); Claude-auth is geldig.",
    "tijdelijk": "Usage tijdelijk niet beschikbaar (429/5xx); Claude-auth is geldig.",
    "onleesbaar": "Claude gaf een antwoord dat de monitor niet begrijpt.",
    "geen_limieten": "Claude meldt op dit moment geen planlimieten.",
}

#: Problemen die over de aanmelding gaan; alleen daar mag de preflightstatus de tekst bepalen.
_AUTH_PROBLEMS = frozenset({"geen_token", "token_verlopen", "geweigerd"})

#: Machineleesbare toestand van de kaart, naast de tekst.
STATES = ("usage_actueel", "usage_verouderd", "auth_vernieuwd", "toegang_verlopen_vernieuwbaar",
          "login_vereist", "versie_niet_gevalideerd", "usage_tijdelijk_onbeschikbaar",
          "niet_beschikbaar", "wordt_opgehaald", "auth_wordt_vernieuwd", "auth_vernieuwing_mislukt")

#: Hoe vaak de monitor zelf (zonder open browsertabblad) de usage bijwerkt.
TICK_SECONDS = 60


class UsageUnavailable(Exception):
    """Ophalen lukte niet. Draagt een korte code, nooit een token of een ruwe body."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


# --------------------------------------------------------------- aanmelding


def _read_token() -> str:
    """Leest het OAuth-token vers uit de sleutelhanger (of het bestand ernaast).

    Vers, elke keer: Claude Code vernieuwt het token zelf, en een monitor die een
    oude kopie vasthoudt zou na zo'n vernieuwing blijven hangen op "geweigerd".
    De waarde wordt teruggegeven en verder nergens bewaard.
    """
    payload = _read_credentials()
    oauth = payload.get("claudeAiOauth") if isinstance(payload, dict) else None
    token = oauth.get("accessToken") if isinstance(oauth, dict) else None
    if not isinstance(token, str) or not token:
        raise UsageUnavailable("geen_token")
    expires_at = oauth.get("expiresAt")
    if isinstance(expires_at, (int, float)) and expires_at > 0:
        if expires_at / 1000.0 <= time.time():
            # Zonder deze controle zou elke poging in een 401 eindigen. Claude Code
            # vernieuwt het token bij zijn eerstvolgende gebruik; dan herstelt dit vanzelf.
            raise UsageUnavailable("token_verlopen")
    return token


def _read_credentials() -> dict:
    """De sleutelhanger eerst, het bestand als tweede. Fouten worden niet doorverteld."""
    try:
        done = subprocess.run(
            ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"],
            capture_output=True, text=True, timeout=10.0)
        if done.returncode == 0 and done.stdout.strip():
            return json.loads(done.stdout)
    except (OSError, subprocess.SubprocessError, ValueError):
        pass
    try:
        return json.loads(CREDENTIALS_FILE.expanduser().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _ssl_context() -> ssl.SSLContext:
    """Altijd certificaatcontrole. ``certifi`` waar de Python-installatie zelf geen wortels heeft."""
    try:
        import certifi  # noqa: PLC0415 -- optioneel, alleen voor de wortelcertificaten
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def fetch_usage(*, token_reader=_read_token, opener=None) -> dict:
    """Haalt het ruwe usage-antwoord op. Geeft nooit een token door aan de aanroeper."""
    token = token_reader()
    request = urllib.request.Request(USAGE_URL, headers={
        "Authorization": f"Bearer {token}",
        "anthropic-beta": OAUTH_BETA,
        "Accept": "application/json",
        "User-Agent": "obf-monitor-web/1.0",
    })
    open_url = opener or (lambda req, timeout: urllib.request.urlopen(
        req, timeout=timeout, context=_ssl_context()))
    try:
        with open_url(request, TIMEOUT_SECONDS) as response:
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as error:
        if error.code in (401, 403):
            raise UsageUnavailable("geweigerd") from None
        raise UsageUnavailable("tijdelijk" if error.code == 429 or error.code >= 500
                               else "netwerk") from None
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        raise UsageUnavailable("netwerk") from None
    if len(raw) > MAX_RESPONSE_BYTES:
        raise UsageUnavailable("onleesbaar")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise UsageUnavailable("onleesbaar") from None
    if not isinstance(payload, dict):
        raise UsageUnavailable("onleesbaar")
    return payload


# ------------------------------------------------------------ normaliseren


def normalize(payload: dict) -> list[dict]:
    """Zet het antwoord om in precies de drie soorten die de monitor toont.

    ``limits`` is de vorm waarin Claude zijn limieten zelf al genormaliseerd
    aanbiedt, inclusief toekomstige modelspecifieke weeklimieten -- daar staat nu
    bijvoorbeeld Fable in, en morgen iets anders, zonder dat hier iets hoeft te
    veranderen. Ontbreekt dat veld (een oudere server), dan worden de losse
    velden ``five_hour`` / ``seven_day`` / ``seven_day_<model>`` gelezen.
    """
    rows = payload.get("limits")
    limits = _from_limits(rows) if isinstance(rows, list) else []
    if not limits:
        limits = _from_legacy_fields(payload)
    limits.sort(key=lambda item: (_KIND_ORDER.get(item["kind"], 9), item["label"]))
    return limits


def _from_limits(rows: list) -> list[dict]:
    limits: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        kind = row.get("kind")
        if kind not in _KIND_ORDER:
            continue  # een soort die deze monitor niet toont
        percent = _percent(row.get("percent"))
        if percent is None:
            continue
        if kind == "weekly_scoped":
            model = _scope_model(row.get("scope"))
            if model is None:
                continue  # zonder modelnaam valt er niets eerlijks te zeggen
            key, label = f"weekly:{model.lower()}", f"Week · {model}"
        elif kind == "weekly_all":
            key, label = "weekly_all", "Week · alle modellen"
        else:
            key, label = "session", "5-uurslimiet"
        limits.append({"key": key, "kind": kind, "label": label, "percent": percent,
                       "resets_at": _iso(row.get("resets_at")),
                       "severity": _severity(row.get("severity"), percent)})
    return limits


def _from_legacy_fields(payload: dict) -> list[dict]:
    """Terugval voor een antwoord zonder ``limits``: dezelfde getallen, losse velden."""
    limits: list[dict] = []
    mapping = [("five_hour", "session", "session", "5-uurslimiet"),
               ("seven_day", "weekly_all", "weekly_all", "Week · alle modellen")]
    for field, kind, key, label in mapping:
        block = payload.get(field)
        percent = _percent((block or {}).get("utilization")) if isinstance(block, dict) else None
        if percent is None:
            continue
        limits.append({"key": key, "kind": kind, "label": label, "percent": percent,
                       "resets_at": _iso(block.get("resets_at")),
                       "severity": _severity(None, percent)})
    for field, value in payload.items():
        if not field.startswith("seven_day_") or not isinstance(value, dict):
            continue
        percent = _percent(value.get("utilization"))
        if percent is None:
            continue
        model = field[len("seven_day_"):].replace("_", " ").strip().title()
        limits.append({"key": f"weekly:{model.lower()}", "kind": "weekly_scoped",
                       "label": f"Week · {model}", "percent": percent,
                       "resets_at": _iso(value.get("resets_at")),
                       "severity": _severity(None, percent)})
    return limits


def _scope_model(scope) -> str | None:
    if not isinstance(scope, dict):
        return None
    model = scope.get("model")
    if not isinstance(model, dict):
        return None
    name = model.get("display_name") or model.get("id")
    name = name.strip() if isinstance(name, str) else ""
    return name or None


def _percent(value) -> int | None:
    """Alleen een getal dat Claude zelf gaf; nooit iets afgeleid uit tokens of kosten."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        percent = int(round(float(value)))
    except (OverflowError, ValueError):
        return None
    return max(0, min(999, percent))


def _severity(value, percent: int) -> str:
    """Wat Claude zegt gaat voor; zegt hij niets bijzonders, dan telt het percentage."""
    if isinstance(value, str) and value and value != "normal":
        return value
    if percent >= 90:
        return "critical"
    if percent >= 75:
        return "warning"
    return "normal"


def _iso(value) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return value


# ------------------------------------------------------- preflightstatus

_AUTH_CODE_FIELDS = ("status", "reason", "last_refresh_result")
_AUTH_TIME_FIELDS = ("checked_at", "access_token_expires_at", "refresh_token_expires_at", "last_refresh_at")


def _default_auth_status_path() -> Path:
    raw = os.environ.get(AUTH_STATUS_FILE_ENV)
    return Path(raw).expanduser() if raw else DEFAULT_AUTH_STATUS_FILE.expanduser()


def read_auth_status(path: Path) -> dict | None:
    """Het statusbestand van de runner-preflight, teruggebracht tot codes, tijden en een versie.

    Er staat geen token in dat bestand; toch wordt hier alleen overgenomen wat de
    kaart nodig heeft, in de vorm die het hoort te hebben. Onleesbaar is 'niets'.
    """
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("schema") != AUTH_STATUS_SCHEMA:
        return None
    clean: dict = {}
    for name in _AUTH_CODE_FIELDS:
        value = payload.get(name)
        if isinstance(value, str) and value.replace("_", "").isalnum() and len(value) <= 64:
            clean[name] = value
    for name in _AUTH_TIME_FIELDS:
        value = _iso(payload.get(name))
        if value is not None:
            clean[name] = value
    version = payload.get("claude_version")
    if isinstance(version, str) and len(version) <= 16 and all(p.isdigit() for p in version.split(".")):
        clean["claude_version"] = version
    if isinstance(payload.get("refresh_version_validated"), bool):
        clean["refresh_version_validated"] = payload["refresh_version_validated"]
    return clean if "status" in clean else None


def _epoch(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _local_hhmm(iso: str | None) -> str | None:
    moment = _epoch(iso)
    return None if moment is None else datetime.fromtimestamp(moment).strftime("%H:%M")


def describe_problem(problem: str | None, auth: dict | None, now: float) -> tuple[str, str]:
    """(toestand, tekst) voor een probleem, aangevuld met wat de preflight vaststelde."""
    auth = auth or {}
    if problem in _AUTH_PROBLEMS:
        if auth.get("status") == "login_required":
            return "login_vereist", "Interactieve aanmelding vereist — voer `claude auth login` uit."
        if auth.get("status") == "refresh_version_unvalidated":
            version = auth.get("claude_version") or "onbekend"
            return ("versie_niet_gevalideerd",
                    f"Automatische vernieuwing niet gevalideerd voor Claude Code {version}.")
    if problem == "token_verlopen":
        refresh_end = _epoch(auth.get("refresh_token_expires_at"))
        if refresh_end is not None and refresh_end <= now:
            # Nog niet door de preflight vastgesteld, dus nog geen aanmeldopdracht.
            return ("niet_beschikbaar",
                    "Toegangstoken verlopen; de automatische controle stelt vast of opnieuw aanmelden nodig is.")
        return "toegang_verlopen_vernieuwbaar", _PROBLEM_TEXT["token_verlopen"]
    if problem in ("netwerk", "tijdelijk"):
        return "usage_tijdelijk_onbeschikbaar", _PROBLEM_TEXT[problem]
    return "niet_beschikbaar", _PROBLEM_TEXT.get(problem or "", "Niet beschikbaar.")


#: Zo lang vóór het einde van de Claude-login (het refresh-token) waarschuwen Monitor 2
#: en de notifier: daarna weigert de preflight elke start tot iemand opnieuw aanmeldt.
LOGIN_EXPIRY_WARNING_DAYS = 7


def login_expiry(auth: dict | None, now: float) -> dict | None:
    """Wanneer de Claude-login op de Mac afloopt, en of dat nu een waarschuwing is (audit INF-02).

    Alleen uit wat de preflight zelf vastlegde (``refresh_token_expires_at``); zonder
    die tijd staat er niets -- er wordt niets geschat.
    """
    stamp = (auth or {}).get("refresh_token_expires_at")
    end = _epoch(stamp)
    if end is None:
        return None
    left = end - now
    days = max(0, int(left // 86400))
    local = datetime.fromtimestamp(end).strftime("%d-%m-%Y %H:%M")
    warning = left <= LOGIN_EXPIRY_WARNING_DAYS * 86400
    if left <= 0:
        message = f"Claude-login op de Mac is verlopen ({local}) — voer `claude auth login` uit."
    elif warning:
        message = (f"Claude-login op de Mac verloopt {local} (nog {days} {'dag' if days == 1 else 'dagen'}) — "
                   "meld vóór die tijd opnieuw aan met `claude auth login`.")
    else:
        message = None
    return {"expires_at": stamp, "expires_local": local, "days_left": days, "warning": warning,
            "expired": left <= 0, "message": message}


def refresh_note(auth: dict | None, now: float) -> str | None:
    """'Claude-auth automatisch vernieuwd om HH:MM', zolang dat recent en bewezen is."""
    if not auth or auth.get("last_refresh_result") != "refreshed":
        return None
    at = _epoch(auth.get("last_refresh_at"))
    if at is None or not 0 <= now - at <= AUTH_REFRESH_NOTE_SECONDS:
        return None
    return f"Claude-auth automatisch vernieuwd om {_local_hhmm(auth.get('last_refresh_at'))}."


# ------------------------------------------------------------------ bron


class ClaudeUsageProvider:
    """De accountbrede usage, klaar om in elke monitorreactie te worden meegegeven.

    ``snapshot()`` wacht nooit op het netwerk: hij geeft terug wat er is en zet
    zo nodig een achtergronddraad aan het werk. De monitorpagina blijft daardoor
    even snel als zonder deze kaart.
    """

    def __init__(self, *, cache_path: Path | None = None, fetcher=fetch_usage,
                 clock=time.time, fresh_seconds: float = FRESH_SECONDS,
                 stale_after: float = STALE_AFTER_SECONDS,
                 forget_after: float = FORGET_AFTER_SECONDS,
                 retry_after_error: float = RETRY_AFTER_ERROR_SECONDS,
                 background: bool = True, auth_status_path: Path | None = None,
                 auth_keeper=None):
        self._cache_path = Path(cache_path) if cache_path is not None else _default_cache_path()
        self._fetch = fetcher
        self._clock = clock
        self._fresh = fresh_seconds
        self._stale_after = stale_after
        self._forget_after = forget_after
        self._retry_after_error = retry_after_error
        self._background = background
        self._lock = threading.Lock()
        self._limits: list[dict] | None = None
        self._fetched_at: float | None = None
        self._problem: str | None = None
        self._last_attempt: float = 0.0
        self._refreshing = False
        self._loaded_shared = False
        self._auth_path = (Path(auth_status_path) if auth_status_path is not None
                           else _default_auth_status_path())
        self._auth: dict | None = None
        self._auth_stamp: tuple | None = None
        self._seen_refresh: str | None = None
        # De autonome aanmeldvernieuwing (alleen op de Mac, zie auth_keeper).
        # ``None`` = deze bron vernieuwt niets, precies zoals vroeger.
        self._keeper = auth_keeper
        self._ticker: threading.Thread | None = None

    # -- wat de monitor gebruikt ------------------------------------------

    def snapshot(self) -> dict:
        """Het beeld van nu, met zijn eigen leeftijd erbij. Blokkeert niet.

        Het verversen gebeurt uitdrukkelijk *buiten* het slot: anders zou de
        rechtstreekse variant (``background=False``, de tests) zichzelf
        vastzetten, en zou een trage verbinding in de achtergronddraad de
        monitorpagina alsnog ophouden.
        """
        with self._lock:
            if not self._loaded_shared:
                self._loaded_shared = True
                self._adopt_shared()
            self._follow_auth_status()
            now = self._clock()
            verversen = self._needs_refresh(now, self._age(now))
            if verversen:
                self._refreshing = True
                self._last_attempt = now
        if verversen:
            if self._background:
                threading.Thread(target=self._refresh_and_release, name="claude-usage",
                                 daemon=True).start()
            else:
                self._refresh_and_release()
        with self._lock:
            now = self._clock()
            return self._render(now, self._age(now))

    def start_ticker(self, interval: float = TICK_SECONDS) -> None:
        """Werk de usage (en zo nodig de aanmelding) bij, ook zonder browser. Idempotent."""
        if self._ticker is not None:
            return

        def loop() -> None:
            while True:
                try:
                    self.snapshot()
                except Exception:  # noqa: BLE001 -- een tik mag de draad nooit stoppen
                    pass
                time.sleep(interval)

        self._ticker = threading.Thread(target=loop, name="claude-usage-tick", daemon=True)
        self._ticker.start()

    def _follow_auth_status(self) -> None:
        """Het preflightbestand opnieuw lezen als het veranderd is (onder het slot).

        Een nieuwe, geslaagde vernieuwing heft de wachttijd na een eerdere fout
        op: de volgende ophaalpoging mag dan meteen. De monitor vernieuwt zelf
        nooit iets; hij kijkt alleen wat de preflight vóór een taak deed.
        """
        try:
            info = self._auth_path.stat()
            stamp = (info.st_mtime_ns, info.st_size)
        except OSError:
            stamp = None
        if stamp == self._auth_stamp:
            return
        self._auth_stamp = stamp
        self._auth = read_auth_status(self._auth_path) if stamp is not None else None
        refreshed_at = (self._auth or {}).get("last_refresh_at")
        if ((self._auth or {}).get("last_refresh_result") == "refreshed"
                and refreshed_at and refreshed_at != self._seen_refresh):
            self._seen_refresh = refreshed_at
            if self._problem is not None:
                self._last_attempt = 0.0

    def _age(self, now: float) -> float | None:
        """De leeftijd van wat er ligt -- en te oud is hetzelfde als niets."""
        if self._fetched_at is None:
            return None
        age = max(0.0, now - self._fetched_at)
        if age > self._forget_after:
            # Liever niets dan een uur oud getal dat op actueel lijkt.
            self._limits, self._fetched_at = None, None
            return None
        return age

    # -- verversen ---------------------------------------------------------

    def _needs_refresh(self, now: float, age: float | None) -> bool:
        if self._refreshing:
            return False
        # Na een mislukte poging een pauze -- ook wanneer er nog een oude waarde
        # ligt. Zonder dat zou een storing betekenen dat er bij elke pollronde
        # van de browser opnieuw wordt aangeklopt: twee keer per seconde, per
        # monitor, urenlang.
        if self._problem and now - self._last_attempt < self._retry_after_error:
            return False
        return age is None or age >= self._fresh

    def _refresh_and_release(self) -> None:
        try:
            self._refresh()
        finally:
            with self._lock:
                self._refreshing = False

    def _refresh(self) -> None:
        if self._keeper is not None:
            # Eerst de aanmelding: binnen Claude Codes vernieuwingsvenster of
            # verlopen laat de keeper de bestaande runner-preflight vernieuwen.
            # Anders doet hij niets. Een fout hier raakt de usage nooit.
            try:
                outcome = self._keeper.ensure()
            except Exception:  # noqa: BLE001
                outcome = None
            if outcome == "refreshed":
                with self._lock:
                    self._follow_auth_status()
                    self._problem = None
                # Na een vernieuwing meteen vers ophalen, niet uit de gedeelde cache.
                self._fetch_and_store()
                return
        # Een andere monitor kan net hebben opgehaald: dan is één gedeeld bestand
        # lezen genoeg en blijft Anthropic ongemoeid.
        shared = _read_shared(self._cache_path)
        if shared is not None and self._clock() - shared[1] < self._fresh:
            with self._lock:
                self._adopt(shared[0], shared[1])
            return
        self._fetch_and_store()

    def _fetch_and_store(self) -> None:
        try:
            payload = self._fetch()
            limits = normalize(payload)
        except UsageUnavailable as exc:
            with self._lock:
                self._problem = exc.code
            return
        except Exception:
            # Een onverwachte fout mag de monitor niet omver halen en mag ook
            # nooit een (mogelijk gevoelige) tekst doorgeven.
            with self._lock:
                self._problem = "onleesbaar"
            return
        if not limits:
            with self._lock:
                self._problem = "geen_limieten"
            return
        now = self._clock()
        with self._lock:
            self._adopt(limits, now)
        _write_shared(self._cache_path, limits, now)

    def _adopt(self, limits: list[dict], fetched_at: float) -> None:
        self._limits = limits
        self._fetched_at = fetched_at
        self._problem = None

    def _adopt_shared(self) -> None:
        shared = _read_shared(self._cache_path)
        if shared is not None and self._clock() - shared[1] <= self._forget_after:
            self._adopt(shared[0], shared[1])

    # -- tonen -------------------------------------------------------------

    def _keeper_view(self) -> dict | None:
        if self._keeper is None:
            return None
        try:
            state = self._keeper.state()
        except Exception:  # noqa: BLE001
            return None
        view = {"state": state.get("state"), "reason": state.get("reason")}
        retry = _local_hhmm(state.get("next_attempt_at"))
        if retry and state.get("state") in ("failed", "login_required"):
            view["next_attempt_local"] = retry
        return view

    def _keeper_message(self, keeper: dict | None) -> tuple[str, str] | None:
        """(toestand, tekst) wanneer de autonome vernieuwing iets te melden heeft."""
        if not keeper:
            return None
        if keeper.get("state") == "refreshing":
            return "auth_wordt_vernieuwd", "Claude-auth wordt automatisch vernieuwd…"
        if keeper.get("state") == "login_required" and self._problem in _AUTH_PROBLEMS:
            return ("login_vereist",
                    "Interactieve aanmelding vereist — voer `claude auth login` uit op deze machine.")
        if keeper.get("state") == "failed" and self._problem in _AUTH_PROBLEMS:
            reason = f" ({keeper['reason']})" if keeper.get("reason") else ""
            retry = f"; nieuwe poging om {keeper['next_attempt_local']}" if keeper.get("next_attempt_local") else ""
            return ("auth_vernieuwing_mislukt",
                    f"Toegangstoken verlopen — automatische vernieuwing tijdelijk mislukt{reason}{retry}. "
                    "De bestaande aanmelding is niet gewijzigd.")
        return None

    def _render(self, now: float, age: float | None) -> dict:
        view = self._render_base(now, age)
        view["login_expiry"] = login_expiry(self._auth, time.time())
        keeper = self._keeper_view()
        view["auth_keeper"] = keeper
        override = self._keeper_message(keeper)
        if override is not None and view["status"] in ("unavailable", "stale", "pending"):
            state, text = override
            view["state"] = state
            if view["status"] == "stale":
                base = (view.get("message") or "Usage verouderd").split(" — ")[0]
                view["message"] = f"{base} — {text[:1].lower()}{text[1:]}"
            else:
                view["message"] = text
        return view

    def _render_base(self, now: float, age: float | None) -> dict:
        source = f"{USAGE_HOST}/api/oauth/usage"
        wall = time.time()
        auth_note = refresh_note(self._auth, wall)
        if self._limits is None or age is None:
            if self._problem is None:
                return {"status": "pending", "state": "wordt_opgehaald", "limits": [], "checked_at": None,
                        "checked_at_local": None, "age_seconds": None, "stale": False,
                        "problem": None, "message": "Gebruik wordt opgehaald…", "auth_note": auth_note,
                        "source": source}
            state, message = describe_problem(self._problem, self._auth, wall)
            return {"status": "unavailable", "state": state, "limits": [], "checked_at": None,
                    "checked_at_local": None, "age_seconds": None, "stale": True,
                    "problem": self._problem, "message": message, "auth_note": auth_note,
                    "source": source}
        stale = age >= self._stale_after
        checked = datetime.fromtimestamp(self._fetched_at, tz=timezone.utc)
        message = None
        state = "auth_vernieuwd" if auth_note else "usage_actueel"
        if stale:
            state = "usage_verouderd"
            message = "Usage verouderd sinds " + checked.astimezone().strftime("%H:%M")
            if self._problem:
                reason = describe_problem(self._problem, self._auth, wall)[1].rstrip(".")
                message += f" — {reason[:1].lower()}{reason[1:]}"
        return {"status": "stale" if stale else "ok", "state": state,
                "limits": [dict(item) for item in self._limits],
                "checked_at": checked.isoformat(),
                "checked_at_local": checked.astimezone().strftime("%H:%M:%S"),
                "age_seconds": int(age), "stale": stale,
                "problem": self._problem if stale else None,
                "message": message, "auth_note": auth_note, "source": source}


# ------------------------------------------------------ gedeeld cachebestand


def _default_cache_path() -> Path:
    raw = os.environ.get(CACHE_FILE_ENV)
    return Path(raw).expanduser() if raw else DEFAULT_CACHE_FILE.expanduser()


def _read_shared(path: Path) -> tuple[list[dict], float] | None:
    """Wat de andere monitor als laatste ophaalde. Onleesbaar is simpelweg 'niets'."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("schema") != CACHE_SCHEMA:
        return None
    fetched_at = payload.get("fetched_at")
    limits = payload.get("limits")
    if not isinstance(fetched_at, (int, float)) or not isinstance(limits, list):
        return None
    clean = [item for item in limits
             if isinstance(item, dict) and isinstance(item.get("percent"), int)
             and isinstance(item.get("label"), str)]
    if not clean:
        return None
    return clean, float(fetched_at)


def _write_shared(path: Path, limits: list[dict], fetched_at: float) -> None:
    """Eén atomaire rename, en alleen de getoonde velden -- er staat geen token in."""
    payload = {"schema": CACHE_SCHEMA, "fetched_at": fetched_at, "limits": limits}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent),
                                             prefix=path.name + ".", suffix=".tmp",
                                             delete=False)
        try:
            with handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(handle.name, path)
        except BaseException:
            Path(handle.name).unlink(missing_ok=True)
            raise
    except OSError:
        # Niet kunnen delen is geen storing: elke monitor haalt dan gewoon zelf op.
        pass
