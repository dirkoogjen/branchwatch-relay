"""Losse integrator op de Mac: de automatische integratie naar lokale ``main`` zonder de Monitor 2-webserver (§13).

Waarom. Monitor 2 draait op GERARD (besluit 3), maar integreren moet gebeuren op de machine met de repository en de
Runner 2-administratie van de taken; GERARD kan de Mac niet bereiken. Dit proces draait daarom op de Mac zelf (eigen
LaunchAgent, voorbeeld in ``reference/``) en gebruikt **dezelfde controllerlogica** als het Monitor 2-proces:
``auto_integration.AutoIntegrator.tick`` (beleid met vastgezette hash, alle controles, gerichte tests op een export, alleen
``ff-only`` met herstelbranch, ontvangstbewijzen, hashketen-audittrail) en ``action_link`` (Runner 2-actiepunten via
``runner2.actions.record``, alleen toevoegen). Er komen geen beveiligingsregels bij en er gaan er geen af; het enige verschil is
een extra drempel: aanvragen van vóór de eerste start van deze integrator voor een repository worden niet automatisch
geïntegreerd (geen automatische integratie van bestaande, openstaande takken; de handmatige route blijft daarvoor).

Wat het is
----------
* ``run``: de lus, elke 10 s één ronde. De ondertekeningssleutel bestaat alleen in het geheugen van dit proces
  (``Store(key=secrets.token_bytes(32))``), nooit op schijf. Eén integrator tegelijk (``flock`` op ``integrator.lock`` in de
  map met toestemmingen). Zonder ``OBF_INTEGRATION=1`` en een geldig, vastgezet beleid doet de lus niets behalve de status
  bijhouden; het beleid wordt elke ronde opnieuw tegen de vastgezette hash gecontroleerd (gewijzigd bestand = uit).
* Geen webinterface en geen luisterende poort. Bediening via deze opdrachtregel: ``status``, ``pause`` (zonder code, de veilige
  richting) en ``resume`` (met de goedkeuringscode, alleen op een terminal; foute codes tellen over aanroepen heen).
* Een **alleen-lezen statusbestand** (vorm: het ``auto``-deel van ``GET /api/integration`` plus ``audit_ok``) dat periodiek met
  ``scp`` naar GERARD gaat. Er staat nooit een sleutel, goedkeuringscode of hash in (ook niet de hash van het beleid). Lukt het
  versturen niet, dan gaat de integratie gewoon door en staat de melding in ``upload.problem``.
* Een **hartslag**: duurt een ronde langer dan ``HEARTBEAT_S`` (de gerichte tests van een integratie, tot de harde grens van
  ``test_budget.MAX_BUDGET_S``), dan schrijft en verstuurt een eigen draad de status tussendoor, met ``auto.test_run`` (verstreken /
  budget / schatting / percentage). Zo toont GERARD de voortgang in plaats van *verouderd*. De hartslag leest alleen; de audittrail
  wordt dan niet opnieuw gecontroleerd (de uitkomst van de laatste volledige ronde blijft staan).

Wat het nooit doet: pushen, deployen, publiceren, een remote benaderen (alleen ``scp`` van het statusbestand naar het ingestelde
doel), menselijke actiepunten sluiten, iets anders dan lokale ``main`` en één herstelbranch verplaatsen.

Configuratie (omgeving, zoals voor Monitor 2; in de LaunchAgent onder ``EnvironmentVariables``)
---------------------------------------------------------------------------------------------
``OBF_INTEGRATION=1``, ``OBF_INTEGRATION_REPOS``, ``OBF_INTEGRATION_AUTO_POLICY`` + ``OBF_INTEGRATION_AUTO_POLICY_SHA256``,
``OBF_INTEGRATION_SECRET_HASH`` (alleen voor ``resume``), ``OBF_INTEGRATION_DIR``/``OBF_INTEGRATION_INBOX``,
``OBF_INTEGRATION_R2_HOME``, ``OBF_INTEGRATION_R2_ACTIONS``, ``OBF_INTEGRATION_TEST_WRAPPER``, ``OBF_INTEGRATION_APPROVERS``
(optioneel: wie mag stilzetten/hervatten), en voor de status:

``OBF_INTEGRATION_STATUS_FILE``          lokaal statusbestand (standaard ``<configuratiemap>/mac-integrator-status.json``)
``OBF_INTEGRATION_STATUS_SCP_TARGET``    ``gebruiker@host:/pad/bestand.json`` op GERARD; leeg = niet versturen
``OBF_INTEGRATION_STATUS_SCP_PORT``      ssh-poort (standaard die uit de ssh-configuratie)
``OBF_INTEGRATION_STATUS_SCP_IDENTITY``  sleutelbestand; dan zonder ``~/.ssh/config`` (``-F /dev/null``), zoals de back-uproute
``OBF_INTEGRATION_STATUS_INTERVAL_S``    hoe vaak versturen (standaard 60 s, 15..3600)

De opdrachtregel leest dezelfde variabelen uit de omgeving en, als die bestaat, uit de ``EnvironmentVariables`` van de
LaunchAgent (``--plist``; standaard ``~/Library/LaunchAgents/com.studioobf.obf-mac-integrator.plist``), zodat ``status``,
``pause`` en ``resume`` dezelfde mappen gebruiken als het draaiende proces.
"""

from __future__ import annotations

import getpass
import json
import os
import plistlib
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable

try:
    import fcntl
except ImportError:  # pragma: no cover -- alleen Mac/Linux
    fcntl = None  # type: ignore[assignment]

from . import action_link as al
from . import auto_integration as au
from . import integration as ig
from . import test_budget as tb

LABEL = "com.studioobf.obf-mac-integrator"
SOURCE = "mac-integrator"
STATUS_SCHEMA = 1
STATUS_FILE_ENV = "OBF_INTEGRATION_STATUS_FILE"
SCP_TARGET_ENV = "OBF_INTEGRATION_STATUS_SCP_TARGET"
SCP_PORT_ENV = "OBF_INTEGRATION_STATUS_SCP_PORT"
SCP_IDENTITY_ENV = "OBF_INTEGRATION_STATUS_SCP_IDENTITY"
UPLOAD_INTERVAL_ENV = "OBF_INTEGRATION_STATUS_INTERVAL_S"
UPLOAD_INTERVAL_S = 60
UPLOAD_TIMEOUT_S = 45
HEARTBEAT_S = 20.0
LOCK_FILE = "integrator.lock"
SINCE_FILE = "integrator.since.json"
GATE_FILE = "integrator.secret-gate.json"
DEFAULT_PLIST = Path("~/Library/LaunchAgents") / f"{LABEL}.plist"

#: ``gebruiker@host:/pad`` zonder spaties of shell-tekens; het pad eindigt op .json.
SCP_TARGET = re.compile(r"^(?:[A-Za-z0-9._-]{1,64}@)?[A-Za-z0-9._-]{1,253}:[A-Za-z0-9._/~-]{1,300}\.json$")
#: Sleutels die nooit in het statusbestand mogen (sleutel, code, hashes).
_HIDDEN_KEY = re.compile(r"secret|sha256|hash|token|passw|credential|cookie|^key|_key$|key_id", re.I)
#: Waarden die op een code-hash of sha256 lijken.
_HIDDEN_VALUE = re.compile(r"scrypt\$|\b[0-9a-f]{64}\b", re.I)


def _iso(ts: float) -> str:
    return ig._iso(ts)


def status_path(env: dict) -> Path:
    if env.get(STATUS_FILE_ENV):
        return Path(env[STATUS_FILE_ENV]).expanduser()
    conf = env.get("OBF_MONITOR2_CONF_DIR") or str(Path.home() / ".config" / "monitor2")
    return Path(conf).expanduser() / "mac-integrator-status.json"


def scrub(value):
    """Haalt alles weg wat op een sleutel, code of hash lijkt (sleutels én waarden), recursief. Commit-sha's (40 hex) blijven."""
    if isinstance(value, dict):
        return {k: scrub(v) for k, v in value.items() if not _HIDDEN_KEY.search(str(k))}
    if isinstance(value, (list, tuple)):
        return [scrub(v) for v in value]
    if isinstance(value, str) and _HIDDEN_VALUE.search(value):
        return _HIDDEN_VALUE.sub("[verborgen]", value)
    return value


# ----------------------------------------------------------------------
# Versturen van het statusbestand naar GERARD (scp; nooit iets anders)
# ----------------------------------------------------------------------


def _scp_problem(returncode: int | None, stderr: str) -> str:
    """Een vaste, korte melding; nooit de ruwe uitvoer (die kan een vingerafdruk van een hostsleutel bevatten)."""
    low = (stderr or "").lower()
    if returncode is None:
        return f"versturen naar GERARD duurde langer dan {UPLOAD_TIMEOUT_S} s"
    if "host key verification failed" in low or "remote host identification" in low:
        return "de hostsleutel van GERARD is onbekend of gewijzigd (known_hosts)"
    if "permission denied (" in low:   # sshd wijst de sleutel zelf af, bv. "Permission denied (publickey)."
        return "GERARD weigert de toegang (sleutel)"
    if "permission denied" in low:   # authenticatie lukte; het doelbestand zelf is niet overschrijfbaar (bv. mode 444)
        return "GERARD kan het statusbestand niet overschrijven (bestandsrechten)"
    if "no such file" in low or "not a directory" in low:
        return "de doelmap op GERARD bestaat niet"
    if any(x in low for x in ("could not resolve", "connection refused", "timed out", "no route", "network is unreachable",
                              "connection closed", "connection reset")):
        return "GERARD is niet bereikbaar"
    return f"versturen naar GERARD mislukt (scp gaf {returncode})"


class StatusUploader:
    """Stuurt het statusbestand met ``scp`` naar GERARD, in een eigen draad met time-out: de integratie wacht er nooit op."""

    def __init__(self, env: dict, *, runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
                 clock: Callable[[], float] = time.time):
        self.target = (env.get(SCP_TARGET_ENV) or "").strip()
        self.port = (env.get(SCP_PORT_ENV) or "").strip()
        self.identity = (env.get(SCP_IDENTITY_ENV) or "").strip()
        try:
            self.interval = min(3600, max(15, int(env.get(UPLOAD_INTERVAL_ENV) or UPLOAD_INTERVAL_S)))
        except ValueError:
            self.interval = UPLOAD_INTERVAL_S
        self.runner, self.clock = runner, clock
        self.problem: str | None = None
        self.config_problem: str | None = None
        self.last_attempt: float | None = None
        self.last_ok: float | None = None
        self._last_activity: str | None = None
        self._thread: threading.Thread | None = None
        if self.target and not SCP_TARGET.match(self.target):
            self.config_problem = f"{SCP_TARGET_ENV} heeft niet de vorm gebruiker@host:/pad/bestand.json; er wordt niets verstuurd"
        elif self.port and not (self.port.isdigit() and 0 < int(self.port) < 65536):
            self.config_problem = f"{SCP_PORT_ENV} is geen poortnummer; er wordt niets verstuurd"
        elif self.identity and not Path(self.identity).expanduser().is_file():
            self.config_problem = f"het sleutelbestand uit {SCP_IDENTITY_ENV} bestaat niet; er wordt niets verstuurd"

    @property
    def enabled(self) -> bool:
        return bool(self.target) and self.config_problem is None

    def command(self, local: Path) -> list[str]:
        cmd = ["scp", "-O", "-q", "-B", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"]
        if self.identity:
            cmd += ["-F", "/dev/null", "-o", "IdentitiesOnly=yes", "-i", str(Path(self.identity).expanduser())]
        if self.port:
            cmd += ["-P", self.port]
        return [*cmd, str(local), self.target]

    def public(self) -> dict:
        host = self.target.split(":", 1)[0].split("@")[-1] if self.target else None
        return {"enabled": self.enabled, "host": host, "interval_s": self.interval,
                "last_attempt_at": _iso(self.last_attempt) if self.last_attempt else None,
                "last_ok_at": _iso(self.last_ok) if self.last_ok else None,
                "problem": self.config_problem or self.problem}

    def due(self) -> bool:
        return self.enabled and (self.last_attempt is None or self.clock() - self.last_attempt >= self.interval) \
            and not (self._thread is not None and self._thread.is_alive())

    def maybe_upload(self, local: Path, *, wait: bool = False) -> bool:
        """Start een upload als het tijd is; ``wait`` (tests) wacht op de uitkomst. Raised nooit."""
        # A completed grant must not remain displayed as running until the next
        # minute upload. Only meaningful activity changes bypass that interval.
        try:
            data = json.loads(local.read_text(encoding="utf-8"))
            auto = data.get("auto") or {}
            activity = json.dumps({"running": auto.get("running"), "paused": auto.get("paused"),
                                   "enabled": auto.get("enabled"), "error": data.get("last_error"),
                                   "recent": auto.get("recent"), "attention": auto.get("attention")}, sort_keys=True)
        except (OSError, ValueError, AttributeError, TypeError):
            activity = self._last_activity
        changed = activity != self._last_activity
        busy = self._thread is not None and self._thread.is_alive()
        if busy or not self.enabled or not (changed or self.due()):
            return False
        self._last_activity = activity
        self.last_attempt = self.clock()
        self._thread = threading.Thread(target=self.upload, args=(local,), name="integrator-status-upload", daemon=True)
        self._thread.start()
        if wait:
            self._thread.join()
        return True

    def upload(self, local: Path) -> bool:
        """Verstuurt een schrijfbare kopie van ``local``, nooit het alleen-lezen statusbestand zelf.

        ``scp`` draagt de modus van de bronfile over wanneer het doelbestand nog niet bestaat; het lokale statusbestand is
        met opzet 0444 (§13.2), en zou dat dan ook op GERARD worden. Een volgende upload kan zo'n bestand niet meer
        overschrijven (geen schrijfbit, ook niet voor de eigenaar), en blijft daarna permanent mislukken. Een kortstondige
        kopie met mode 0644, in een eigen tijdelijke map, voorkomt dat; het lokale bestand blijft altijd alleen-lezen."""
        tmp_dir = None
        try:
            try:
                data = local.read_bytes()
            except OSError as exc:
                self.problem = f"statusbestand niet te lezen ({exc.__class__.__name__})"
                return False
            tmp_dir = tempfile.mkdtemp(prefix="obf-mac-integrator-upload-")
            source = Path(tmp_dir) / local.name
            source.write_bytes(data)
            source.chmod(0o644)
            try:
                proc = self.runner(self.command(source), capture_output=True, text=True, timeout=UPLOAD_TIMEOUT_S,
                                   env={**os.environ, "LC_ALL": "C"}, stdin=subprocess.DEVNULL)
            except subprocess.TimeoutExpired:
                self.problem = _scp_problem(None, "")
                return False
            except OSError:
                self.problem = "scp is op deze machine niet te starten"
                return False
            except Exception as exc:  # noqa: BLE001 -- versturen mag de integratie nooit hinderen
                self.problem = f"versturen mislukt ({exc.__class__.__name__})"
                return False
            if proc.returncode == 0:
                self.problem, self.last_ok = None, self.clock()
                return True
            self.problem = _scp_problem(proc.returncode, proc.stderr or "")
            return False
        finally:
            if tmp_dir is not None:
                shutil.rmtree(tmp_dir, ignore_errors=True)


# ----------------------------------------------------------------------
# De controller: AutoIntegrator met één extra drempel (geen bestaande takken)
# ----------------------------------------------------------------------


class MacAutoIntegrator(au.AutoIntegrator):
    """``AutoIntegrator`` met dezelfde controles, plus: een aanvraag van vóór de eerste start van deze integrator voor die
    repository (of een taak die toen al was afgerond) wordt niet automatisch geïntegreerd. Hij blijft in de inbox voor de
    handmatige route en staat als ``ignored_preexisting`` in de status (geen marker, geen audit, geen actiepunt)."""

    def __init__(self, *args, since: dict[str, float] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.since: dict[str, float] = dict(since or {})
        self.preexisting: dict[str, dict] = {}

    def process(self, request_id: str) -> dict | None:
        try:
            request, _sha = self.inbox.read(request_id)
            repo = os.path.realpath(os.path.expanduser(request["repo"]))
        except (ig.IntegrationError, OSError, ValueError):
            return None
        start = self.since.get(repo)
        if start is not None and self.policy.for_repo(repo) is not None:
            rec = self.r2.task(request.get("task_id") or "") or {}
            finished = au._epoch(rec.get("updated"))
            if request["created_epoch"] < start or (rec.get("status") == "completed" and finished is not None and finished < start):
                self.preexisting[request_id] = {"id": request_id, "state": "ignored", "code": "preexisting",
                                                "text": "aangevraagd vóór de start van de integrator op deze Mac; niet automatisch, "
                                                        "een mens kan dit handmatig integreren", "at": request.get("created_at")}
                return None
        self.preexisting.pop(request_id, None)
        return super().process(request_id)

    def status(self) -> dict:
        out = super().status()
        out["ignored_preexisting"] = sorted(self.preexisting.values(), key=lambda x: str(x.get("at")), reverse=True)[:au.ATTENTION_KEEP]
        return out


def load_since(store: ig.Store, repos: list[str], now: float) -> dict[str, float]:
    """Startmoment per repository: wordt één keer vastgelegd (eerste keer dat het beleid de repository noemt) en daarna nooit
    verschoven, ook niet na een herstart. Zo blijft een aanvraag die tijdens een herstart binnenkwam gewoon geldig."""
    store._ensure()
    path = store.dir / SINCE_FILE
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        data = data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        data = {}
    changed = False
    for repo in repos:
        if not isinstance(data.get(repo), str):
            data[repo] = _iso(now)
            changed = True
    if changed:
        tmp = path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(data, indent=1, sort_keys=True))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    out = {}
    for repo in repos:
        try:
            out[repo] = ig._epoch(data[repo])
        except (KeyError, ValueError, TypeError):
            out[repo] = now
    return out


class MacIntegrator:
    """Het proces: één integrator per map met toestemmingen, sleutel alleen in het geheugen, status naar een bestand en GERARD."""

    def __init__(self, env: dict, *, key: bytes | None = None, clock: Callable[[], float] = time.time,
                 run_test_suite: Callable[[str, list[str]], dict] | None = None, action_backend=None,
                 uploader: StatusUploader | None = None, poll_s: float = au.POLL_S, action_grace_s: float = au.ACTION_GRACE_S,
                 heartbeat_s: float = HEARTBEAT_S):
        self.env = dict(env)
        self.clock = clock
        self.poll_s = poll_s
        self.heartbeat_s = heartbeat_s
        self.on = ig.enabled(self.env)
        self.repos = ig.allowed_repos(self.env)
        self.store = ig.Store(ig.grants_dir(self.env), key=key if key is not None else secrets.token_bytes(32))
        self.inbox = ig.Inbox(ig.inbox_dir(self.env))
        self.records = au.R2Records(au.r2_home(self.env))
        self.status_file = status_path(self.env)
        self.uploader = uploader if uploader is not None else StatusUploader(self.env, clock=clock)
        self.policy = self._load_policy()
        #: Zoals in Monitor 2 geldt het beleid van de start; elke ronde wordt het opnieuw tegen de hash gecontroleerd (afwijking =
        #: uit), maar een bij de start ongeldig beleid wordt pas na een herstart (door een mens) actief.
        self.started_enabled = self.on and self.policy.enabled
        self.started_at = clock()
        self._stop = threading.Event()
        self._lock_fd: int | None = None
        self.last_tick_at: float | None = None
        self.last_error: str | None = None
        #: Begin van de lopende ronde (``None`` tussen rondes); de hartslag publiceert alleen tijdens een lange ronde.
        self.tick_started_at: float | None = None
        self.last_publish_at: float | None = None
        self._publish_lock = threading.Lock()
        #: Uitkomst van de laatste controle van de audittrail; ``None`` = nog nooit gecontroleerd (dan controleert ook de hartslag).
        self._audit: tuple[bool, str] | None = None
        linker = None
        if self.on and self.policy.enabled and al.enabled(self.env):
            backend = action_backend if action_backend is not None else al.Runner2Actions(au.r2_home(self.env), env=self.env)
            linker = al.ActionLinker(self.store, backend, self.records, clock=clock)
        self.auto = MacAutoIntegrator(self.store, self.inbox, self.policy, self.records, run_test_suite=run_test_suite, clock=clock,
                                      linker=linker, action_grace_s=action_grace_s)
        self._announced = False

    def _load_policy(self) -> au.AutoPolicy:
        if not self.on:
            return au.AutoPolicy(configured=bool(self.env.get(au.POLICY_ENV) or self.env.get(au.POLICY_SHA_ENV)),
                                 problem=f"{ig.ENABLE_ENV} staat niet op 1; de integrator doet niets")
        return au.load_policy(self.env, self.repos)

    # -- één integrator tegelijk

    def acquire(self) -> None:
        """Neemt ``integrator.lock`` (``flock``); raised ``IntegrationError('busy')`` als er al een integrator draait."""
        self.store._ensure()
        fd = os.open(self.store.dir / LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o600)
        if fcntl is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(fd)
                raise ig.IntegrationError("busy", "er draait al een integrator voor deze map met toestemmingen") from None
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        self._lock_fd = fd

    def release(self) -> None:
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = None

    def stop(self, *_args) -> None:
        self._stop.set()
        self.auto.close()

    # -- een ronde

    def step(self) -> list[dict]:
        """Eén ronde: beleid opnieuw tegen de vastgezette hash, ``tick`` (dezelfde controller), status schrijven en zo nodig
        versturen. Raised nooit."""
        out: list[dict] = []
        self.tick_started_at = self.clock()
        try:
            if self.started_enabled:
                self.policy = self._load_policy()
                self.auto.policy = self.policy
            if self.policy.enabled:
                if not self._announced:
                    self.auto.since = load_since(self.store, list(self.policy.repos), self.clock())
                    self.store.audit("auto_policy", f"{SOURCE}: automatische integratie actief voor " + ", ".join(sorted(self.policy.repos)),
                                     policy_sha256=self.policy.sha256, repos=[p.public() for p in self.policy.repos.values()],
                                     source=SOURCE, host=socket.gethostname())
                    self._announced = True
                out = self.auto.tick()
            self.last_error = None
        except Exception as exc:  # noqa: BLE001 -- de lus mag nooit stoppen op één fout; de volgende ronde probeert opnieuw
            self.last_error = exc.__class__.__name__
        self.last_tick_at = self.clock()
        self.tick_started_at = None
        self.publish()
        return out

    def heartbeat(self) -> bool:
        """Publiceert tussendoor als de lopende ronde al ``heartbeat_s`` niets publiceerde (bv. tijdens de tests). Raised nooit."""
        started = self.tick_started_at
        if started is None or self.clock() - max(started, self.last_publish_at or started) < self.heartbeat_s:
            return False
        return self.publish(verify_audit=False)

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(min(5.0, max(0.05, self.heartbeat_s / 4))):
            self.heartbeat()

    def run(self) -> int:
        self.acquire()
        beat = threading.Thread(target=self._heartbeat_loop, name="integrator-heartbeat", daemon=True)
        beat.start()
        try:
            while not self._stop.is_set():
                self.step()
                self._stop.wait(self.poll_s)
        finally:
            self._stop.set()
            beat.join(timeout=10)
            self.publish()
            self.release()
        return 0

    # -- status

    def status(self, *, verify_audit: bool = True) -> dict:
        if self.started_enabled and self.policy.enabled:
            auto = self.auto.status()
        else:
            auto = {**self.policy.public(), "paused": self.auto.paused(), "running": None, "actor": au.AUTO_ACTOR, "poll_s": self.poll_s,
                    "waiting": [], "attention": [], "recent": [], "ignored_preexisting": [],
                    "r2_actions": {"enabled": False, "problem": None, "linked": []},
                    "test_run": tb.summary(self.store.dir, active=False, now=self.clock())}
        if verify_audit or self._audit is None:
            try:
                self._audit = self.store.verify_audit()
            except OSError:
                self._audit = (False, "de audittrail is niet te lezen")
        ok, why = self._audit
        auto.update(audit_ok=ok, audit_problem=why)
        out = {"schema": STATUS_SCHEMA, "source": SOURCE, "host": socket.gethostname(), "generated_at": _iso(self.clock()),
               "started_at": _iso(self.started_at), "pid": os.getpid(), "poll_s": self.poll_s,
               "last_tick_at": _iso(self.last_tick_at) if self.last_tick_at else None, "last_error": self.last_error,
               "tick_started_at": _iso(self.tick_started_at) if self.tick_started_at else None,
               "integration_enabled": self.on, "read_only": True, "upload": self.uploader.public(), "auto": auto}
        return scrub(out)

    def publish(self, *, verify_audit: bool = True) -> bool:
        """Schrijft het statusbestand en verstuurt het als het tijd is. Eén tegelijk (ronde en hartslag delen het bestand).
        Raised nooit; ``False`` als er niets geschreven is."""
        with self._publish_lock:
            try:
                write_status(self.status_file, self.status(verify_audit=verify_audit))
            except Exception:  # noqa: BLE001
                return False
            self.last_publish_at = self.clock()
            self.uploader.maybe_upload(self.status_file)
        return True


def write_status(path: Path, data: dict) -> None:
    """Atomair (tijdelijk bestand + rename), leesbaar voor iedereen, niet schrijfbaar (0444)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(data, indent=1, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(tmp, 0o444)
    os.replace(tmp, path)


# ----------------------------------------------------------------------
# Opdrachtregel: run, status, pause, resume
# ----------------------------------------------------------------------


def plist_env(path: Path) -> dict:
    """De ``EnvironmentVariables`` van een LaunchAgent (alleen tekstwaarden). Een onleesbaar bestand geeft ``{}``."""
    try:
        with open(path, "rb") as handle:
            data = plistlib.load(handle)
    except (OSError, ValueError, plistlib.InvalidFileException):
        return {}
    env = data.get("EnvironmentVariables") if isinstance(data, dict) else None
    return {str(k): str(v) for k, v in env.items() if isinstance(v, str)} if isinstance(env, dict) else {}


def cli_env(plist: str | None, base: dict | None = None) -> dict:
    env = dict(base if base is not None else os.environ)
    path = Path(plist).expanduser() if plist else DEFAULT_PLIST.expanduser()
    if path.is_file():
        env.update(plist_env(path))
    return env


class PersistentGate:
    """``SecretGate`` waarvan de foutteller en blokkade over aanroepen van de opdrachtregel heen bewaard blijven (in de map met
    toestemmingen, 0600). Zonder dat zou elke nieuwe aanroep weer vijf pogingen geven."""

    def __init__(self, store: ig.Store, stored_hash: str, clock: Callable[[], float] = time.time):
        self.path = store.dir / GATE_FILE
        self.store = store
        self.gate = ig.SecretGate(stored_hash, clock=clock)
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.gate.failures = int(data.get("failures", 0))
            self.gate.locked_until = float(data.get("locked_until", 0.0))
        except (OSError, ValueError, TypeError, AttributeError):
            pass

    def _save(self) -> None:
        self.store._ensure()
        tmp = self.path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"failures": self.gate.failures, "locked_until": self.gate.locked_until}))
        os.replace(tmp, self.path)

    def check(self, secret) -> None:
        try:
            self.gate.check(secret)
        finally:
            if self.gate.configured:
                self._save()


def _canonical(by: str, env: dict) -> str:
    names = ig.approvers([], env)
    by = (by or "").strip()
    if not by or len(by) > 40:
        raise ig.IntegrationError("invalid_input", "geef een naam op (--by)")
    if not names:
        return by
    found = next((n for n in names if n.lower() == by.lower()), None)
    if found is None:
        raise ig.IntegrationError("forbidden", "alleen " + ", ".join(names) + " kunnen dit doen")
    return found


def _daemon_pid(store: ig.Store) -> int | None:
    try:
        text = (store.dir / LOCK_FILE).read_text().strip()
    except OSError:
        return None
    if not text.isdigit():
        return None
    pid = int(text)
    if fcntl is not None:
        try:   # de lock vrij krijgen = er draait geen integrator (meteen weer loslaten)
            fd = os.open(store.dir / LOCK_FILE, os.O_RDONLY)
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                fcntl.flock(fd, fcntl.LOCK_UN)
                return None
            except OSError:
                return pid
            finally:
                os.close(fd)
        except OSError:
            return None
    return pid if au._alive(pid) else None


def cli_main(argv: list[str] | None = None, env: dict | None = None, *, ask_code: Callable[[str], str] | None = None,
             is_terminal: Callable[[], bool] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="obf_mac_integrator", description=__doc__.split("\n\n")[0])
    parser.add_argument("--plist", help=f"LaunchAgent waarvan de EnvironmentVariables gelden (standaard {DEFAULT_PLIST})")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="de integrator zelf (voorgrond; voor de LaunchAgent)")
    sub.add_parser("status", help="toon de status (uit het statusbestand) en of de integrator draait")
    pause = sub.add_parser("pause", help="zet de automatische integratie stil (geen code nodig)")
    pause.add_argument("--by", default=None, help="naam (standaard de gebruikersnaam)")
    resume = sub.add_parser("resume", help="hervat de automatische integratie (vraagt de goedkeuringscode)")
    resume.add_argument("--by", default=None, help="naam (standaard de gebruikersnaam)")
    args = parser.parse_args(argv)
    env = cli_env(args.plist, env)

    def emit(data: dict) -> None:
        sys.stdout.write(json.dumps(data, indent=1, ensure_ascii=False) + "\n")

    if args.cmd == "run":
        integrator = MacIntegrator(env)
        signal.signal(signal.SIGTERM, integrator.stop)
        signal.signal(signal.SIGINT, integrator.stop)
        try:
            return integrator.run()
        except ig.IntegrationError as exc:
            emit({"ok": False, "code": exc.code, "message": exc.message})
            return 2
    store = ig.Store(ig.grants_dir(env))
    switch = au.AutoIntegrator(store, ig.Inbox(ig.inbox_dir(env)), au.AutoPolicy(), au.R2Records(au.r2_home(env)))
    try:
        if args.cmd == "status":
            path = status_path(env)
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                data = None
            except (OSError, ValueError):
                data = {"problem": "het statusbestand is onleesbaar"}
            pid = _daemon_pid(store)
            age = None
            if isinstance(data, dict) and data.get("generated_at"):
                try:
                    age = int(time.time() - ig._epoch(data["generated_at"]))
                except ValueError:
                    pass
            emit({"ok": True, "running": pid is not None, "pid": pid, "status_file": str(path), "age_s": age,
                  "paused": switch.paused(), "status": data})
            return 0
        by = _canonical(args.by or getpass.getuser(), env)
        if args.cmd == "pause":
            paused = switch.pause(f"{by} ({SOURCE})")
            emit({"ok": True, "paused": paused, "message": "Automatische integratie staat stil; handmatig goedkeuren blijft kunnen. "
                                                          "Een lopende integratie maakt haar stap nog af."})
            return 0
        # resume: alleen een mens op een terminal, met de goedkeuringscode
        if switch.paused() is None:
            emit({"ok": True, "paused": None, "message": "De automatische integratie stond niet stil."})
            return 0
        terminal = is_terminal() if is_terminal is not None else (sys.stdin.isatty() and sys.stderr.isatty())
        if not terminal:
            raise ig.IntegrationError("needs_terminal", "hervatten vraagt de goedkeuringscode op een terminal")
        gate = PersistentGate(store, ig.secret_hash(env))
        code = (ask_code or getpass.getpass)("Goedkeuringscode: ")
        try:
            gate.check(code)
        except ig.IntegrationError as exc:
            if exc.code in ("secret_wrong", "secret_locked"):
                store.audit("secret_failed", f"auto_resume: {exc.code} (naam {by}, {SOURCE})", action="auto_resume", by=by, code=exc.code)
            raise
        switch.resume(f"{by} ({SOURCE})")
        emit({"ok": True, "paused": None, "message": "Automatische integratie is hervat."})
        return 0
    except ig.IntegrationError as exc:
        emit({"ok": False, "code": exc.code, "message": exc.message})
        return 2
