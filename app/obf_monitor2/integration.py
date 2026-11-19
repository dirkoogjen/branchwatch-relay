"""Aanvraag -> voorbeeld -> menselijke goedkeuring -> uitvoering door de controller: één eenmalige, gebonden lokale git-integratie naar ``main``.

Waarom dit bestaat. Het Runner-2-beleid zegt: een taak pusht niet en merget niets naar main; integreren doet een mens.
``approval='integratie'`` op een opdracht (``runner2_jobs``) is alleen een goedkeuring *vóór de start*: er bestaat dan
nog geen commit, dus die goedkeuring kan nergens aan gebonden zijn en geeft geen recht om iets te mergen. Deze module
voegt de ontbrekende stappen toe, in vier fasen met een strikte rolverdeling:

1. **Aanvraag** (``request``, door Claude in de Runner-sandbox): een niet-ondertekend bestand ``R-<12 hex>.json`` in de
   *inbox*, de enige map die de sandbox mag schrijven. Een aanvraag is een bewering zonder enige bevoegdheid: de controller
   vertrouwt er niets van en leidt alle feiten zelf af.
2. **Voorbeeld** (``preview``, controller): Monitor 2 toont de mens verse feiten: main-HEAD, doelcommit, commits, bestanden,
   werkboom, untracked, en of de aanvraag nog klopt.
3. **Goedkeuring** (``grant``, mens): naam uit de goedkeurders *plus* een goedkeuringscode die alleen de mens kent
   (``OBF_INTEGRATION_SECRET_HASH``). Dit levert een toestemming (``G-<12 hex>``) op die aan repository, bronbranch,
   main-HEAD, doelcommit, modus en gerichte tests gebonden is en aan de aanvraag die hem veroorzaakte.
4. **Uitvoering** (``execute``, uitsluitend de controller = het Monitor 2-proces, buiten de sandbox): opnieuw alle
   controles, herstelbranch, ``git merge`` van exact de goedgekeurde sha, nacontrole en de gerichte tests. De opdrachtregel
   kan dit niet; er is geen ``run``.

Eigenschappen

* De HMAC-sleutel bestaat alleen in het geheugen van het Monitor 2-proces (``Store(key=...)``) en wordt nooit naar schijf
  geschreven: de sandbox kan hem niet lezen en na een herstart zijn openstaande toestemmingen ongeldig (opnieuw goedkeuren).
* Eenmalig: aanvraag, toestemming en uitvoering worden verbruikt met een ``O_CREAT|O_EXCL``-markering; een tweede poging, ook
  gelijktijdig, vindt die al bestaan.
* Verloopt (standaard na 24 uur) en kan worden ingetrokken zolang hij niet is gebruikt.
* Audittrail: ``audit.jsonl`` is append-only met fsync en een hashketen (``prev``/``hash``).

Er wordt nooit gepusht, gedeployd of gepubliceerd; de enige refs die veranderen zijn lokale ``main`` en één herstelbranch.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Callable

from . import test_budget as tb

try:  # alleen Mac/Linux; de uitvoerder is voor die machines
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

SCHEMA = 1
ENABLE_ENV = "OBF_INTEGRATION"
DIR_ENV = "OBF_INTEGRATION_DIR"
REPOS_ENV = "OBF_INTEGRATION_REPOS"
APPROVERS_ENV = "OBF_INTEGRATION_APPROVERS"
INBOX_ENV = "OBF_INTEGRATION_INBOX"
SECRET_HASH_ENV = "OBF_INTEGRATION_SECRET_HASH"
TEST_WRAPPER_ENV = "OBF_INTEGRATION_TEST_WRAPPER"
MAIN_BRANCH = "main"
MODES = ("ff-only", "no-ff")
DEFAULT_TTL_S = 24 * 3600
MAX_TTL_S = 7 * 24 * 3600
TEST_TIMEOUT_S = tb.FALLBACK_BUDGET_S   # alleen nog het terugvalbudget; het echte budget is adaptief (test_budget.py)
TASKPOLICY = "/usr/sbin/taskpolicy"
MAX_TESTS = 20
MAX_NOTE = 500
MAX_REQUEST_BYTES = 8 * 1024
MAX_INBOX_FILES = 200
LIST_REQUESTS = 40
REQUEST_TTL_S = 7 * 24 * 3600
MIN_SECRET_LEN = 10
SECRET_MAX_FAILURES = 5
SECRET_LOCK_S = 15 * 60

GRANT_ID = re.compile(r"^G-[0-9a-f]{12}$")
REQUEST_ID = re.compile(r"^R-[0-9a-f]{12}$")
TASK_ID = re.compile(r"^[A-Za-z0-9._-]{0,40}$")
#: Wijzigingen aan deze paden veranderen de controller zelf; de mens moet dat uitdrukkelijk bevestigen.
PROTECTED_PATHS = ("app/obf_monitor2/integration.py", "app/obf_monitor2/auto_integration.py", "app/obf_monitor2/server.py",
                   "app/obf_monitor2/static/integration.js", "scripts/obf_monitor2_integrate.py", "runner2/session.py",
                   "app/obf_monitor2/mac_integrator.py", "scripts/obf_mac_integrator.py",
                   "app/obf_monitor2/remote_status.py", "app/obf_monitor2/test_budget.py")
#: Omgevingsvariabelen die niet naar de gerichte tests gaan.
_SECRET_ENV = re.compile(r"KEY|TOKEN|SECRET|PASSW|CREDENTIAL|COOKIE|AUTH|^OBF_INTEGRATION", re.I)
SHA = re.compile(r"^[0-9a-f]{40}$")
BRANCH = re.compile(r"^[A-Za-z0-9._][A-Za-z0-9._/-]{0,99}$")
TEST_PATH = re.compile(r"^(?=.{1,180}$)(?:[A-Za-z0-9_.-]+/)*tests/[A-Za-z0-9_./-]{1,150}\.(py|mjs)$")

#: Bestanden in de git-map die zeggen dat er een andere handeling halverwege is.
IN_PROGRESS = ("MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "rebase-merge", "rebase-apply", "BISECT_LOG", "sequencer")


class IntegrationError(Exception):
    """Een geweigerde of mislukte handeling met een stabiele ``code`` en een Nederlandse tekst."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code, self.message = code, message


# ----------------------------------------------------------------------
# Configuratie
# ----------------------------------------------------------------------


def enabled(env: dict | None = None) -> bool:
    return ((env if env is not None else os.environ).get(ENABLE_ENV) or "").strip() == "1"


def grants_dir(env: dict | None = None) -> Path:
    env = env if env is not None else os.environ
    if env.get(DIR_ENV):
        return Path(env[DIR_ENV]).expanduser()
    conf = env.get("OBF_MONITOR2_CONF_DIR") or str(Path.home() / ".config" / "monitor2")
    return Path(conf).expanduser() / "integration"


def inbox_dir(env: dict | None = None) -> Path:
    """De map waarin de sandbox aanvragen mag neerzetten. Nooit dezelfde als (of binnen) de map met toestemmingen."""
    env = env if env is not None else os.environ
    if env.get(INBOX_ENV):
        return Path(env[INBOX_ENV]).expanduser()
    conf = env.get("OBF_MONITOR2_CONF_DIR") or str(Path.home() / ".config" / "monitor2")
    return Path(conf).expanduser() / "integration-inbox"


def secret_hash(env: dict | None = None) -> str:
    return ((env if env is not None else os.environ).get(SECRET_HASH_ENV) or "").strip()


def allowed_repos(env: dict | None = None) -> list[str]:
    """De repositories waarvoor een toestemming kan worden gegeven (echte paden). Leeg = niets toegestaan."""
    raw = (env if env is not None else os.environ).get(REPOS_ENV) or ""
    out = []
    for item in raw.split(","):
        item = item.strip()
        if item:
            real = os.path.realpath(os.path.expanduser(item))
            if real not in out:
                out.append(real)
    return out


def approvers(default: list[str], env: dict | None = None) -> list[str]:
    raw = (env if env is not None else os.environ).get(APPROVERS_ENV)
    if raw is None or not raw.strip():
        return list(default)
    return [x.strip() for x in raw.split(",") if x.strip()]


def _now() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch(text: str) -> float:
    return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()


# ----------------------------------------------------------------------
# Opslag: toestemmingen, markeringen, sleutel en audittrail
# ----------------------------------------------------------------------


class Store:
    """De map met toestemmingen (alleen voor de controller). ``G-x.json`` is onveranderlijk; ``G-x.used``/``.revoked`` en
    ``R-x.granted``/``.rejected`` zijn markeringen (O_EXCL).

    ``key`` is de HMAC-sleutel in het geheugen (zo gebruikt Monitor 2 hem: nooit op schijf). Zonder ``key`` wordt een
    sleutelbestand in dezelfde map gebruikt; dat is alleen voor tests en voor wie dat uitdrukkelijk wil.
    """

    def __init__(self, directory: Path, key: bytes | None = None):
        self.dir = Path(directory)
        self._memory_key = key

    def _ensure(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.dir, 0o700)
        except OSError:
            pass

    # -- sleutel

    def _key_path(self) -> Path:
        return self.dir / "hmac.key"

    def key(self, create: bool = False) -> bytes:
        if self._memory_key is not None:
            return self._memory_key
        path = self._key_path()
        try:
            data = path.read_bytes()
            if len(data) >= 32:
                return data
        except OSError:
            pass
        if not create:
            raise IntegrationError("no_key", "de ondertekeningssleutel ontbreekt; er is nog nooit een toestemming gegeven op deze machine")
        self._ensure()
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(secrets.token_bytes(32))
        return path.read_bytes()

    def key_id(self, create: bool = False) -> str:
        """Korte vingerafdruk van de sleutel: zo is een toestemming van vóór een herstart herkenbaar."""
        return hashlib.sha256(self.key(create)).hexdigest()[:12]

    @staticmethod
    def _sign(key: bytes, grant: dict) -> str:
        payload = json.dumps(grant, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return hmac.new(key, payload, hashlib.sha256).hexdigest()

    # -- toestemmingen

    def _path(self, item_id: str, suffix: str = ".json") -> Path:
        if not (GRANT_ID.match(item_id or "") or REQUEST_ID.match(item_id or "")):
            raise IntegrationError("invalid_input", "ongeldig toestemmings- of aanvraag-id")
        return self.dir / (item_id + suffix)

    def write_grant(self, grant: dict) -> None:
        key = self.key(create=True)
        self._ensure()
        body = json.dumps({"grant": grant, "sig": self._sign(key, grant)}, indent=1, ensure_ascii=False)
        path = self._path(grant["id"])
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())

    def read_grant(self, grant_id: str, verify: bool = True) -> dict:
        """De toestemming, met gecontroleerde ondertekening. Alles wat niet klopt is ``IntegrationError``.

        ``verify=False`` is voor weergave zonder sleutel (de opdrachtregel): de inhoud is dan niet gecontroleerd."""
        if not GRANT_ID.match(grant_id or ""):
            raise IntegrationError("invalid_input", "ongeldig toestemmings-id")
        try:
            data = json.loads(self._path(grant_id).read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise IntegrationError("not_found", f"toestemming {grant_id} bestaat niet") from None
        except (OSError, ValueError):
            raise IntegrationError("tampered", f"toestemming {grant_id} is onleesbaar") from None
        grant, sig = (data.get("grant"), data.get("sig")) if isinstance(data, dict) else (None, None)
        if not isinstance(grant, dict) or not isinstance(sig, str) or grant.get("id") != grant_id or grant.get("schema") != SCHEMA:
            raise IntegrationError("tampered", f"toestemming {grant_id} heeft een onverwachte vorm")
        if not verify:
            return grant
        if grant.get("key_id") != self.key_id():
            raise IntegrationError("key_changed", f"toestemming {grant_id} is gegeven vóór de laatste herstart van Monitor 2; "
                                   "geef hem opnieuw")
        if not hmac.compare_digest(self._sign(self.key(), grant), sig):
            raise IntegrationError("tampered", f"toestemming {grant_id}: de ondertekening klopt niet (bestand gewijzigd?)")
        return grant

    def marker(self, grant_id: str, suffix: str) -> dict | None:
        try:
            return json.loads(self._path(grant_id, suffix).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            return {"unreadable": True}

    def set_marker(self, grant_id: str, suffix: str, data: dict) -> bool:
        """Atomair en eenmalig: ``False`` als de markering al bestond."""
        self._ensure()
        try:
            fd = os.open(self._path(grant_id, suffix), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(data, ensure_ascii=False))
            handle.flush()
            os.fsync(handle.fileno())
        return True

    def state(self, grant: dict, now: float | None = None) -> str:
        gid = grant["id"]
        if self.marker(gid, ".used") is not None:
            return "used"
        if self.marker(gid, ".revoked") is not None:
            return "revoked"
        if (now if now is not None else _now()) >= _epoch(grant["expires_at"]):
            return "expired"
        return "open"

    def list_grants(self, limit: int = 30, verify: bool = True) -> list[dict]:
        out = []
        try:
            names = sorted((p for p in self.dir.glob("G-*.json") if not p.name.endswith(".result.json")),
                           key=lambda p: p.stat().st_mtime, reverse=True)
        except OSError:
            return []
        for path in names[:limit]:
            try:
                grant = self.read_grant(path.stem, verify=verify)
            except IntegrationError as exc:
                out.append({"id": path.stem, "state": "invalid", "problem": exc.code})
                continue
            used = self.marker(grant["id"], ".used")
            result = self.marker(grant["id"], ".result")
            out.append({**public(grant), "state": self.state(grant), "used": used, "result": result,
                        "verified": verify})
        return out

    # -- audittrail

    def audit(self, kind: str, text: str, **data) -> dict:
        self._ensure()
        path = self.dir / "audit.jsonl"
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "r+", encoding="utf-8") as handle:
            if fcntl is not None:
                fcntl.flock(handle, fcntl.LOCK_EX)
            handle.seek(0)
            last = ""
            for line in handle:
                if line.strip():
                    last = line.strip()
            prev = hashlib.sha256(last.encode("utf-8")).hexdigest() if last else "0" * 64
            record = {"at": _iso(_now()), "kind": kind, "text": text, **data, "prev": prev}
            record["hash"] = hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
            handle.seek(0, os.SEEK_END)
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        return record

    def verify_audit(self) -> tuple[bool, str]:
        """Controleert de hashketen. ``(True, "")`` of ``(False, reden)``."""
        path = self.dir / "audit.jsonl"
        if not path.exists():
            return True, ""
        prev_line = ""
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except ValueError:
                return False, f"regel {number} is onleesbaar"
            expect_prev = hashlib.sha256(prev_line.encode("utf-8")).hexdigest() if prev_line else "0" * 64
            claimed = record.pop("hash", None)
            if record.get("prev") != expect_prev:
                return False, f"regel {number}: de keten is verbroken"
            if hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest() != claimed:
                return False, f"regel {number}: de inhoud is gewijzigd"
            prev_line = line.strip()
        return True, ""


def public(grant: dict) -> dict:
    return {k: grant.get(k) for k in ("id", "repo", "main_branch", "source_branch", "expected_main_head", "target_commit", "mode",
                                      "tests", "job_id", "note", "approved_by", "approved_at", "expires_at", "request_id",
                                      "allow_protected", "auto")}


# ----------------------------------------------------------------------
# Aanvragen (de inbox is het enige wat de sandbox schrijft; alles erin is onbetrouwbaar)
# ----------------------------------------------------------------------

_REQUEST_KEYS = ("schema", "id", "created_at", "repo", "source_branch", "target_commit", "mode", "tests", "note", "task_id")
_REQUEST_NAME = re.compile(r"^R-[0-9a-f]{12}\.json$")


def _validate_request(data, request_id: str) -> dict:
    """Strikte vorm van een aanvraag; onbekende velden en onverwachte typen zijn ongeldig. Geeft een nieuw dict."""
    bad = IntegrationError("invalid_request", f"aanvraag {request_id} heeft een ongeldige vorm")
    if not isinstance(data, dict) or set(data) != set(_REQUEST_KEYS) or data["schema"] != SCHEMA or data["id"] != request_id:
        raise bad
    for key in ("created_at", "repo", "source_branch", "target_commit", "mode", "note", "task_id"):
        if not isinstance(data[key], str):
            raise bad
    try:
        created = _epoch(data["created_at"])
    except ValueError:
        raise bad from None
    if not data["repo"].strip() or len(data["repo"]) > 300 or not BRANCH.match(data["source_branch"]) \
            or data["source_branch"] == MAIN_BRANCH or not SHA.match(data["target_commit"]) or data["mode"] not in MODES \
            or len(data["note"]) > MAX_NOTE or not TASK_ID.match(data["task_id"]):
        raise bad
    tests = data["tests"]
    if not isinstance(tests, list) or not 1 <= len(tests) <= MAX_TESTS \
            or any(not isinstance(t, str) or not TEST_PATH.match(t) or ".." in t.split("/") for t in tests):
        raise bad
    return {"schema": SCHEMA, "id": request_id, "created_at": data["created_at"], "created_epoch": created, "repo": data["repo"],
            "source_branch": data["source_branch"], "target_commit": data["target_commit"], "mode": data["mode"],
            "tests": list(dict.fromkeys(tests)), "note": data["note"], "task_id": data["task_id"]}


class Inbox:
    """De inbox met aanvragen. De sandbox schrijft hier (``submit``); de controller leest (``read``/``list``) en schrijft
    alleen *ontvangstbewijzen* (``R-x.status.json``, informatief voor Claude). Geen enkele beslissing hangt van deze map af
    die niet daarna opnieuw uit git wordt afgeleid."""

    def __init__(self, directory: Path):
        self.dir = Path(directory)

    def _ensure(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)

    def _names(self) -> list[str]:
        try:
            return [e.name for e in os.scandir(self.dir) if _REQUEST_NAME.match(e.name)]
        except OSError:
            return []

    def submit(self, *, repo: str, source_branch: str, target_commit: str, tests, mode: str = "ff-only", note: str = "",
               task_id: str = "", now: float | None = None) -> dict:
        """Zet een aanvraag neer. Controleert alleen de vorm; heeft geen sleutel nodig en geeft geen enkel recht."""
        record = {"schema": SCHEMA, "id": "", "created_at": _iso(now if now is not None else _now()), "repo": repo,
                  "source_branch": source_branch, "target_commit": target_commit, "mode": mode, "tests": tests, "note": note,
                  "task_id": task_id or ""}
        self._ensure()
        if len(self._names()) >= MAX_INBOX_FILES:
            raise IntegrationError("inbox_full", "de inbox zit vol; wacht tot Monitor 2 aanvragen heeft verwerkt")
        for _ in range(8):
            record["id"] = "R-" + secrets.token_hex(6)
            clean = _validate_request(record, record["id"])   # raised bij ongeldige invoer
            body = json.dumps({k: record[k] for k in _REQUEST_KEYS}, indent=1, ensure_ascii=False)
            try:
                fd = os.open(self.dir / (record["id"] + ".json"), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            except FileExistsError:
                continue
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            return {k: v for k, v in clean.items() if k != "created_epoch"}
        raise IntegrationError("git_failed", "kon geen aanvraag-id kiezen")

    def read(self, request_id: str) -> tuple[dict, str]:
        """``(aanvraag, sha256 van het bestand)``. Volgt geen symlinks, leest hooguit ``MAX_REQUEST_BYTES``."""
        if not REQUEST_ID.match(request_id or ""):
            raise IntegrationError("invalid_input", "ongeldig aanvraag-id")
        path = self.dir / (request_id + ".json")
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            raise IntegrationError("not_found", f"aanvraag {request_id} bestaat niet") from None
        except OSError:
            raise IntegrationError("invalid_request", f"aanvraag {request_id} is geen gewoon bestand") from None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_REQUEST_BYTES:
                raise IntegrationError("invalid_request", f"aanvraag {request_id} is geen gewoon, klein bestand")
            raw = os.read(fd, MAX_REQUEST_BYTES + 1)
        finally:
            os.close(fd)
        try:
            data = json.loads(raw.decode("utf-8"))
        except ValueError:
            raise IntegrationError("invalid_request", f"aanvraag {request_id} is onleesbaar") from None
        return _validate_request(data, request_id), hashlib.sha256(raw).hexdigest()

    def listing(self, store: "Store", now: float | None = None, limit: int = LIST_REQUESTS) -> list[dict]:
        """Recente aanvragen met hun toestand (``open``/``granted``/``rejected``/``expired``/``invalid``), nieuwste eerst."""
        entries = []
        for name in self._names():
            try:
                entries.append((os.lstat(self.dir / name).st_mtime, name[:-5]))
            except OSError:
                continue
        out = []
        for _mtime, request_id in sorted(entries, reverse=True)[:limit]:
            try:
                request, sha = self.read(request_id)
            except IntegrationError as exc:
                out.append({"id": request_id, "state": "invalid", "problem": exc.code})
                continue
            public_request = {k: v for k, v in request.items() if k != "created_epoch"}
            out.append({**public_request, "sha256": sha, "state": request_state(store, request, now),
                        "handled": store.marker(request_id, ".granted") or store.marker(request_id, ".rejected")})
        return out

    def write_receipt(self, request_id: str, data: dict) -> None:
        """Informatief ontvangstbewijs voor de aanvrager; mislukken is nooit erg. De controller leest dit nooit terug."""
        if not REQUEST_ID.match(request_id or ""):
            return
        try:
            self._ensure()
            tmp = self.dir / f".{request_id}.status.tmp"
            tmp.write_text(json.dumps({**data, "request": request_id, "updated_at": _iso(_now()),
                                       "informational": "alleen Monitor 2 beslist; dit bestand geeft geen enkel recht"},
                                      indent=1, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self.dir / f"{request_id}.status.json")
        except OSError:
            pass


def request_state(store: "Store", request: dict, now: float | None = None) -> str:
    rid = request["id"]
    if store.marker(rid, ".granted") is not None:
        return "granted"
    if store.marker(rid, ".rejected") is not None:
        return "rejected"
    if (now if now is not None else _now()) >= request["created_epoch"] + REQUEST_TTL_S:
        return "expired"
    return "open"


def reject_request(store: "Store", inbox: Inbox, request_id: str, by: str, reason: str = "", *, approvers_list: list[str],
                   now: float | None = None) -> dict:
    canonical = next((n for n in approvers_list if isinstance(by, str) and n.lower() == by.strip().lower()), None)
    if canonical is None:
        raise IntegrationError("forbidden", "alleen " + ", ".join(approvers_list) + " kunnen een aanvraag afwijzen")
    if not isinstance(reason, str) or len(reason) > MAX_NOTE:
        raise IntegrationError("invalid_input", "de reden is te lang")
    request, _sha = inbox.read(request_id)
    state = request_state(store, request, now)
    if state != "open":
        raise IntegrationError("wrong_state", f"deze aanvraag is al {state}")
    if not store.set_marker(request_id, ".rejected", {"by": canonical, "at": _iso(now if now is not None else _now()),
                                                       "reason": reason.strip()}):
        raise IntegrationError("wrong_state", "deze aanvraag is al afgehandeld")
    store.audit("reject", f"{request_id}: aanvraag afgewezen door {canonical}", request=request_id, by=canonical,
                reason=reason.strip()[:200])
    inbox.write_receipt(request_id, {"state": "rejected", "by": canonical, "reason": reason.strip()})
    return {"id": request_id, "state": "rejected"}


# ----------------------------------------------------------------------
# De goedkeuringscode (iets wat alleen de mens weet; de sandbox heeft hem nooit)
# ----------------------------------------------------------------------


def hash_secret(secret: str, *, n: int = 2 ** 14, r: int = 8, p: int = 1) -> str:
    """``scrypt$n$r$p$zout$hash`` voor ``OBF_INTEGRATION_SECRET_HASH``. Alleen de hash komt in de configuratie."""
    if not isinstance(secret, str) or len(secret) < MIN_SECRET_LEN:
        raise IntegrationError("invalid_input", f"de goedkeuringscode moet minstens {MIN_SECRET_LEN} tekens hebben")
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(secret.encode("utf-8"), salt=salt, n=n, r=r, p=p, dklen=32, maxmem=128 * 1024 * 1024)
    return f"scrypt${n}${r}${p}${salt.hex()}${digest.hex()}"


def verify_secret(stored: str, secret) -> bool:
    try:
        scheme, n, r, p, salt, digest = (stored or "").split("$")
        if scheme != "scrypt" or not isinstance(secret, str) or not secret or len(secret) > 200:
            return False
        got = hashlib.scrypt(secret.encode("utf-8"), salt=bytes.fromhex(salt), n=int(n), r=int(r), p=int(p), dklen=32,
                             maxmem=128 * 1024 * 1024)
        return hmac.compare_digest(got.hex(), digest)
    except (ValueError, TypeError, OverflowError, MemoryError):
        return False


class SecretGate:
    """Controleert de goedkeuringscode met een globale blokkade na te veel fouten (de naam is een vrije keuze, dus per
    persoon tellen heeft geen zin). Alles in het geheugen; een herstart wist de teller, de hash blijft nodig."""

    def __init__(self, stored_hash: str, *, max_failures: int = SECRET_MAX_FAILURES, lock_s: int = SECRET_LOCK_S, clock=None):
        self.stored = stored_hash or ""
        self.max_failures, self.lock_s = max_failures, lock_s
        self.clock = clock or _now
        self.failures = 0
        self.locked_until = 0.0

    @property
    def configured(self) -> bool:
        return self.stored.startswith("scrypt$")

    def check(self, secret) -> None:
        """Raised ``IntegrationError`` met ``no_secret_configured``, ``secret_locked`` of ``secret_wrong``; anders niets."""
        if not self.configured:
            raise IntegrationError("no_secret_configured", "er is op deze machine geen goedkeuringscode ingesteld "
                                   f"({SECRET_HASH_ENV}); goedkeuren en uitvoeren zijn uit")
        now = self.clock()
        if now < self.locked_until:
            raise IntegrationError("secret_locked", f"te veel foute codes; probeer het over {int(self.locked_until - now) // 60 + 1} "
                                   "minuten opnieuw")
        if verify_secret(self.stored, secret):
            self.failures = 0
            return
        self.failures += 1
        if self.failures >= self.max_failures:
            self.locked_until, self.failures = now + self.lock_s, 0
            raise IntegrationError("secret_locked", "te veel foute codes; goedkeuren is 15 minuten geblokkeerd")
        raise IntegrationError("secret_wrong", "de goedkeuringscode klopt niet")


# ----------------------------------------------------------------------
# Git
# ----------------------------------------------------------------------


def _git(repo, *args: str, check: bool = True, hooks_off: bool = False, input_: str | None = None) -> subprocess.CompletedProcess:
    cmd = ["git", "-C", str(repo)]
    if hooks_off:
        cmd += ["-c", "core.hooksPath=/dev/null"]
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0", "LC_ALL": "C"}
    proc = subprocess.run([*cmd, *args], capture_output=True, text=True, encoding="utf-8", errors="replace", env=env, input=input_,
                          timeout=120)
    if check and proc.returncode:
        raise IntegrationError("git_failed", f"git {' '.join(args[:3])}: {(proc.stderr or proc.stdout).strip()[:300]}")
    return proc


def _rev(repo, ref: str) -> str | None:
    proc = _git(repo, "rev-parse", "--verify", "-q", ref + "^{commit}", check=False)
    out = proc.stdout.strip()
    return out if proc.returncode == 0 and SHA.match(out) else None


def resolve_repo(path: str, allowed: list[str]) -> str:
    if not isinstance(path, str) or not path.strip() or len(path) > 300:
        raise IntegrationError("invalid_input", "geef de repository op")
    real = os.path.realpath(os.path.expanduser(path.strip()))
    if real not in allowed:
        raise IntegrationError("repo_not_allowed", "deze repository staat niet in de lijst met toegestane repositories "
                               f"({REPOS_ENV}) van deze machine")
    if not os.path.isdir(real):
        raise IntegrationError("not_found", "de repository bestaat niet op deze machine")
    top = _git(real, "rev-parse", "--show-toplevel", check=False).stdout.strip()
    if not top or os.path.realpath(top) != real:
        raise IntegrationError("invalid_input", "dit pad is niet de hoofdmap van een git-repository")
    return real


def _git_path(repo, name: str) -> str:
    out = _git(repo, "rev-parse", "--git-path", name).stdout.strip()
    return out if os.path.isabs(out) else os.path.join(repo, out)


def in_progress(repo) -> list[str]:
    return [name for name in IN_PROGRESS if os.path.lexists(_git_path(repo, name))]


def tracked_changes(repo) -> list[str]:
    """Wijzigingen aan getrackte bestanden (ook gestaged); untracked bestanden tellen niet mee."""
    out = _git(repo, "status", "--porcelain", "--untracked-files=no").stdout
    return [line for line in out.splitlines() if line.strip()]


def untracked_snapshot(repo) -> dict[str, list]:
    """Alle niet-genegeerde untracked bestanden met grootte en mtime; om na afloop te bewijzen dat ze ongemoeid zijn."""
    out = _git(repo, "ls-files", "--others", "--exclude-standard", "-z").stdout
    snap = {}
    for rel in filter(None, out.split("\0")):
        try:
            st = os.lstat(os.path.join(repo, rel))
        except OSError:
            continue
        snap[rel] = [st.st_size, st.st_mtime_ns]
    return snap


def _added_paths(repo, base: str, target: str) -> list[str]:
    out = _git(repo, "diff", "--name-status", "-z", "--no-renames", f"{base}", f"{target}").stdout.split("\0")
    paths = []
    for i in range(0, len(out) - 1, 2):
        if out[i] == "A":
            paths.append(out[i + 1])
    return paths


def untracked_collisions(repo, base: str, target: str) -> list[str]:
    """Paden die de integratie nieuw toevoegt maar waar nu al iets (untracked of genegeerd) staat. Git weigert dan zelf ook."""
    hits = []
    for rel in _added_paths(repo, base, target):
        probe = Path(repo)
        blocked = os.path.lexists(os.path.join(repo, rel))
        for part in Path(rel).parent.parts:
            probe = probe / part
            if os.path.lexists(probe) and not probe.is_dir():
                blocked = True
        if blocked:
            hits.append(rel)
    return hits


def inspect(repo: str, source_branch: str) -> dict:
    """De feiten voor de mens (en voor de uitvoerder): alleen lezen."""
    if not BRANCH.match(source_branch or "") or source_branch == MAIN_BRANCH \
            or _git(repo, "check-ref-format", "--branch", source_branch, check=False).returncode:
        raise IntegrationError("invalid_input", "ongeldige bronbranch")
    main = _rev(repo, "refs/heads/" + MAIN_BRANCH)
    tip = _rev(repo, "refs/heads/" + source_branch)
    if main is None:
        raise IntegrationError("not_found", f"de branch {MAIN_BRANCH} bestaat niet in deze repository")
    if tip is None:
        raise IntegrationError("not_found", f"de branch {source_branch} bestaat niet in deze repository")
    ff = _git(repo, "merge-base", "--is-ancestor", main, tip, check=False).returncode == 0
    merged = _git(repo, "merge-base", "--is-ancestor", tip, main, check=False).returncode == 0
    conflicts = None
    if not ff and not merged:
        merge = _git(repo, "merge-tree", "--write-tree", main, tip, check=False)
        conflicts = merge.returncode == 1
    head_ref = _git(repo, "symbolic-ref", "-q", "HEAD", check=False).stdout.strip()
    log = _git(repo, "log", "--format=%H %s", "-n", "30", f"{main}..{tip}").stdout.strip().splitlines()
    files = _git(repo, "diff", "--name-only", f"{main}", f"{tip}").stdout.split()
    count = int(_git(repo, "rev-list", "--count", f"{main}..{tip}").stdout.strip() or 0)
    shortstat = _git(repo, "diff", "--shortstat", f"{main}", f"{tip}").stdout.strip()
    return {"repo": repo, "shortstat": shortstat, "protected_touched": sorted(set(files) & set(PROTECTED_PATHS)), "main_branch": MAIN_BRANCH, "main_head": main, "source_branch": source_branch, "source_tip": tip,
            "fast_forward": ff, "already_merged": merged, "conflicts": conflicts, "commits_ahead": count,
            "commits": [{"sha": line[:40], "subject": line[41:]} for line in log], "files": files[:200], "files_total": len(files),
            "checked_out": head_ref.removeprefix("refs/heads/") if head_ref else None,
            "tracked_changes": tracked_changes(repo)[:50], "untracked": sorted(untracked_snapshot(repo))[:50],
            "in_progress": in_progress(repo)}


# ----------------------------------------------------------------------
# Toestemming geven en intrekken (de mens, via Monitor 2)
# ----------------------------------------------------------------------


def _clean_tests(repo: str, target: str, tests) -> list[str]:
    if not isinstance(tests, list) or not tests or len(tests) > MAX_TESTS:
        raise IntegrationError("invalid_input", f"geef 1 tot {MAX_TESTS} gerichte testbestanden op (tests/...py of tests/...mjs)")
    out = []
    for item in tests:
        if not isinstance(item, str) or not TEST_PATH.match(item) or ".." in item.split("/"):
            raise IntegrationError("invalid_input", f"ongeldig testbestand: {str(item)[:60]}")
        if _git(repo, "cat-file", "-e", f"{target}:{item}", check=False).returncode:
            raise IntegrationError("invalid_input", f"testbestand {item} staat niet in de doelcommit")
        if item not in out:
            out.append(item)
    return out


def create_grant(store: Store, *, repo: str, source_branch: str, expected_main_head: str, target_commit: str, tests,
                 by: str, mode: str = "ff-only", note: str = "", job_id: str | None = None, ttl_s: int = DEFAULT_TTL_S,
                 allowed: list[str], approvers_list: list[str], job_check: Callable[[str, str], dict] | None = None,
                 inbox: Inbox | None = None, request_id: str | None = None, request_sha256: str | None = None,
                 allow_protected: bool = False, auto: dict | None = None, now: float | None = None) -> dict:
    """Legt de toestemming vast na controle dat wat de mens zag nog steeds waar is. Verandert niets in de repository.

    Met ``request_id`` is de toestemming de afhandeling van die aanvraag: het bestand moet nog precies zijn zoals de mens het
    zag (``request_sha256``), nog open zijn, en repository, bronbranch en doelcommit moeten gelijk zijn aan wat Claude vroeg.
    De aanvraag wordt daarbij eenmalig verbruikt.

    ``auto`` (alleen de controller, via ``auto_integration``) markeert een toestemming die voortkomt uit de eenmalige
    repository-instelling in plaats van uit een mens; zo'n toestemming wordt vóór de merge getest en is ``ff-only``."""
    canonical = next((n for n in approvers_list if isinstance(by, str) and n.lower() == by.strip().lower()), None)
    if canonical is None:
        raise IntegrationError("forbidden", "alleen " + ", ".join(approvers_list) + " kunnen een integratie goedkeuren")
    if mode not in MODES:
        raise IntegrationError("invalid_input", "modus: ff-only (standaard) of no-ff")
    if not isinstance(expected_main_head, str) or not SHA.match(expected_main_head) \
            or not isinstance(target_commit, str) or not SHA.match(target_commit):
        raise IntegrationError("invalid_input", "geef de verwachte main-commit en de doelcommit volledig op (40 tekens)")
    if not isinstance(note, str) or len(note) > MAX_NOTE:
        raise IntegrationError("invalid_input", "de opmerking is te lang")
    if not isinstance(ttl_s, int) or isinstance(ttl_s, bool) or not 60 <= ttl_s <= MAX_TTL_S:
        raise IntegrationError("invalid_input", "de geldigheid moet tussen 1 minuut en 7 dagen liggen")
    real = resolve_repo(repo, allowed)
    facts = inspect(real, source_branch)
    if facts["main_head"] != expected_main_head:
        raise IntegrationError("stale", "main is intussen verplaatst; bekijk de integratie opnieuw voordat je goedkeurt")
    if facts["source_tip"] != target_commit:
        raise IntegrationError("stale", "de bronbranch is intussen verplaatst; bekijk de integratie opnieuw voordat je goedkeurt")
    if facts["already_merged"] or facts["commits_ahead"] == 0:
        raise IntegrationError("nothing_to_do", "er is niets te integreren: main bevat deze commit al")
    if mode == "ff-only" and not facts["fast_forward"]:
        raise IntegrationError("not_fast_forward", "deze branch is geen fast-forward van main; eerst bijwerken, of kies no-ff uitdrukkelijk")
    if mode == "no-ff" and facts["conflicts"]:
        raise IntegrationError("conflicts", "samenvoegen geeft conflicten; los die eerst op")
    if facts["protected_touched"] and not allow_protected:
        raise IntegrationError("protected_paths", "deze wijziging raakt de integratie-controller of het Runner-beleid zelf ("
                               + ", ".join(facts["protected_touched"]) + "); bevestig dat uitdrukkelijk of integreer dit handmatig")
    clean_tests = _clean_tests(real, target_commit, tests)
    request = None
    if request_id:
        if inbox is None:
            raise IntegrationError("invalid_input", "aanvragen zijn op deze machine niet beschikbaar")
        request, sha = inbox.read(request_id)
        if not isinstance(request_sha256, str) or not hmac.compare_digest(request_sha256, sha):
            raise IntegrationError("request_changed", "de aanvraag is gewijzigd nadat je hem zag; bekijk hem opnieuw")
        state = request_state(store, request, now)
        if state != "open":
            raise IntegrationError("wrong_state", f"deze aanvraag is al {state}")
        if os.path.realpath(os.path.expanduser(request["repo"])) != real or request["source_branch"] != source_branch \
                or request["target_commit"] != target_commit:
            raise IntegrationError("request_mismatch", "repository, bronbranch of commit komen niet overeen met de aanvraag")
    job_info = None
    if job_id:
        if job_check is None:
            raise IntegrationError("invalid_input", "de opdracht kan hier niet worden gecontroleerd")
        job_info = job_check(job_id, real)
    start = now if now is not None else _now()
    grant = {"schema": SCHEMA, "id": "G-" + secrets.token_hex(6), "key_id": store.key_id(create=True), "repo": real, "main_branch": MAIN_BRANCH,
             "source_branch": source_branch, "expected_main_head": expected_main_head, "target_commit": target_commit,
             "mode": mode, "tests": clean_tests, "job_id": job_id or None, "job_approval": job_info, "note": note.strip(),
             "approved_by": canonical, "approved_at": _iso(start), "expires_at": _iso(start + ttl_s),
             "request_id": request_id or None, "request_sha256": request_sha256 if request else None,
             "allow_protected": bool(allow_protected and facts["protected_touched"]), "auto": auto or None}
    if request is not None:   # de aanvraag eenmalig verbruiken vóór de toestemming bestaat; bij een fout weer vrijgeven
        if not store.set_marker(request_id, ".granted", {"grant": grant["id"], "by": canonical, "at": _iso(start)}):
            raise IntegrationError("wrong_state", "deze aanvraag is intussen afgehandeld")
    try:
        store.write_grant(grant)
    except Exception:
        if request is not None:
            try:
                store._path(request_id, ".granted").unlink()
            except OSError:
                pass
        raise
    store.audit("grant", f"{grant['id']}: toestemming door {canonical} voor {source_branch} -> {MAIN_BRANCH} ({mode})",
                grant=grant["id"], by=canonical, repo=real, source_branch=source_branch, expected_main_head=expected_main_head,
                target_commit=target_commit, mode=mode, tests=clean_tests, job_id=job_id or None, expires_at=grant["expires_at"],
                request=request_id or None, request_sha256=grant["request_sha256"], allow_protected=grant["allow_protected"],
                requested_tests=request["tests"] if request else None, requested_mode=request["mode"] if request else None,
                requested_by_task=request["task_id"] if request else None, auto=auto or None)
    if request is not None and inbox is not None:
        inbox.write_receipt(request_id, {"state": "granted", "grant": grant["id"], "target_commit": target_commit,
                                         "expires_at": grant["expires_at"]})
    return public(grant)


def revoke_grant(store: Store, grant_id: str, by: str, *, approvers_list: list[str], now: float | None = None) -> dict:
    canonical = next((n for n in approvers_list if isinstance(by, str) and n.lower() == by.strip().lower()), None)
    if canonical is None:
        raise IntegrationError("forbidden", "alleen " + ", ".join(approvers_list) + " kunnen een toestemming intrekken")
    grant = store.read_grant(grant_id)
    state = store.state(grant, now)
    if state != "open":
        raise IntegrationError("wrong_state", f"deze toestemming is al {state}")
    if not store.set_marker(grant_id, ".revoked", {"by": canonical, "at": _iso(now if now is not None else _now())}):
        raise IntegrationError("wrong_state", "deze toestemming is al ingetrokken of gebruikt")
    store.audit("revoke", f"{grant_id}: ingetrokken door {canonical}", grant=grant_id, by=canonical)
    return {**public(grant), "state": "revoked"}


# ----------------------------------------------------------------------
# De uitvoerder (op de machine met de repository, door Claude of een mens aangeroepen)
# ----------------------------------------------------------------------


def _refuse(store: Store, grant_id: str, code: str, message: str, **data) -> IntegrationError:
    store.audit("refused", f"{grant_id}: geweigerd ({code}): {message}", grant=grant_id, code=code, **data)
    return IntegrationError(code, message)


def preflight(store: Store, grant_id: str, *, now: float | None = None) -> tuple[dict, dict]:
    """Alle controles vóór er iets verandert. Geeft ``(grant, feiten)`` of raised ``IntegrationError``; wijzigt niets
    behalve de audittrail. De toestemming blijft na een weigering open (er is niets veranderd) tot hij verloopt."""
    grant = store.read_grant(grant_id)
    state = store.state(grant, now)
    if state != "open":
        raise _refuse(store, grant_id, "not_open", f"deze toestemming is {state}")
    return grant, git_preconditions(store, grant)


def git_preconditions(store: Store, grant: dict) -> dict:
    """De git-controles op de hoofdcheckout (alleen lezen). Ook gebruikt vlak vóór de merge van een automatische
    integratie, nadat de tests zijn gedraaid: wat tijdens de tests is veranderd wordt zo alsnog gezien."""
    grant_id = grant["id"]
    if (grant.get("auto") or {}).get("human_approval") is not None:
        from . import human_approval as ha
        try:
            ha.validate_grant(store.dir, grant)
        except (OSError, ValueError, KeyError, TypeError):
            raise _refuse(store, grant_id, "human_approval_invalid", "de menselijke toestemming is gewijzigd of ingetrokken") from None
    repo = grant["repo"]
    if not os.path.isdir(repo):
        raise _refuse(store, grant_id, "not_found", "de repository bestaat niet meer")
    top = _git(repo, "rev-parse", "--show-toplevel", check=False).stdout.strip()
    if not top or os.path.realpath(top) != os.path.realpath(repo):
        raise _refuse(store, grant_id, "wrong_repo", "het pad is niet de hoofdmap van een git-repository")
    if "/.runner2/worktrees/" in repo.replace("\\", "/"):
        raise _refuse(store, grant_id, "wrong_repo", "integreren gebeurt in de hoofdcheckout, niet in een taakworktree")
    head_ref = _git(repo, "symbolic-ref", "-q", "HEAD", check=False).stdout.strip()
    if head_ref != "refs/heads/" + grant["main_branch"]:
        raise _refuse(store, grant_id, "main_not_checked_out", f"de hoofdcheckout staat niet op {grant['main_branch']} (nu: {head_ref or 'losse HEAD'})")
    busy = in_progress(repo)
    if busy:
        raise _refuse(store, grant_id, "operation_in_progress", "er is een andere git-handeling bezig: " + ", ".join(busy))
    dirty = tracked_changes(repo)
    if dirty:
        raise _refuse(store, grant_id, "tracked_dirty", f"de werkboom heeft {len(dirty)} niet-gecommitte wijziging(en) aan getrackte bestanden",
                      examples=dirty[:5])
    head = _rev(repo, "HEAD")
    if head != grant["expected_main_head"]:
        raise _refuse(store, grant_id, "main_moved", "main is niet meer de stand waarvoor toestemming is gegeven",
                      expected=grant["expected_main_head"], actual=head)
    tip = _rev(repo, "refs/heads/" + grant["source_branch"])
    if tip != grant["target_commit"]:
        raise _refuse(store, grant_id, "source_moved", "de bronbranch wijst niet meer naar de goedgekeurde commit",
                      expected=grant["target_commit"], actual=tip)
    ff = _git(repo, "merge-base", "--is-ancestor", head, grant["target_commit"], check=False).returncode == 0
    if grant["mode"] == "ff-only" and not ff:
        raise _refuse(store, grant_id, "not_fast_forward", "de doelcommit is geen fast-forward van main")
    if grant["mode"] == "no-ff" and not ff:
        if _git(repo, "merge-tree", "--write-tree", head, grant["target_commit"], check=False).returncode:
            raise _refuse(store, grant_id, "conflicts", "samenvoegen geeft conflicten")
    hits = untracked_collisions(repo, head, grant["target_commit"])
    if hits:
        raise _refuse(store, grant_id, "untracked_collision", "de integratie zou untracked bestanden overschrijven", paths=hits[:10])
    return {"head": head, "fast_forward": ff, "untracked": untracked_snapshot(repo),
            "files": _git(repo, "diff", "--name-only", head, grant["target_commit"]).stdout.split()[:200],
            "commits": int(_git(repo, "rev-list", "--count", f"{head}..{grant['target_commit']}").stdout.strip() or 0)}


def export_commit(repo: str, sha: str, dest: str) -> None:
    """Pakt exact de bomen van ``sha`` uit in ``dest`` (``git archive``): de repository zelf en de hoofdcheckout blijven
    onaangeroerd. Symlinks, apparaten en paden buiten ``dest`` worden niet uitgepakt."""
    if not SHA.match(sha or ""):
        raise IntegrationError("invalid_input", "ongeldige commit")
    with tempfile.TemporaryFile() as archive:
        proc = subprocess.run(["git", "-C", str(repo), "archive", "--format=tar", sha], stdout=archive, stderr=subprocess.PIPE,
                              timeout=120, env={**os.environ, "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C"})
        if proc.returncode:
            raise IntegrationError("git_failed", "git archive: " + proc.stderr.decode("utf-8", "replace").strip()[:300])
        archive.seek(0)
        root = os.path.realpath(dest)
        with tarfile.open(fileobj=archive, mode="r:") as tar:
            for member in tar:
                name = member.name
                parts = [p for p in name.split("/") if p not in ("", ".")]
                if not parts or ".." in parts or name.startswith("/") or not (member.isreg() or member.isdir()):
                    continue
                target = os.path.realpath(os.path.join(root, *parts))
                if target != root and not target.startswith(root + os.sep):
                    continue
                member.mode = (member.mode & 0o755) | 0o600
                member.uid = member.gid = 0
                member.uname = member.gname = ""
                if hasattr(tarfile, "data_filter"):
                    tar.extract(member, root, filter="data")
                else:  # pragma: no cover
                    tar.extract(member, root)


def pretest(repo: str, grant: dict, runner: Callable[[str, list[str]], dict] | None = None, store_dir: Path | None = None) -> dict:
    """Draait de gerichte tests van een automatische integratie op een export van exact de doelcommit, vóór main verandert."""
    with tempfile.TemporaryDirectory(prefix="obf-integratie-") as tmp:
        export_commit(repo, grant["target_commit"], tmp)
        return (runner or default_test_runner(store_dir, grant))(tmp, grant["tests"])


def test_wrapper(env: dict | None = None) -> list[str]:
    """Optioneel voorvoegsel (``OBF_INTEGRATION_TEST_WRAPPER``, bv. ``sandbox-exec -f profiel.sb``) waarmee de gerichte tests
    worden gestart; zo kunnen ze in een door de mens ingesteld OS-sandboxprofiel draaien. Komt uit de configuratie van Monitor 2,
    nooit uit de doelcommit."""
    import shlex

    raw = ((env if env is not None else os.environ).get(TEST_WRAPPER_ENV) or "").strip()
    try:
        return shlex.split(raw) if raw else []
    except ValueError:
        return ["\0ongeldig"]


def test_priority_prefix(platform: str | None = None, taskpolicy: str | None = None) -> list[str]:
    """macOS: start de gerichte tests met de bronnenregels van een gewone app (``taskpolicy -a``), begrensd op QoS ``utility``.

    De integrator draait als LaunchAgent, eerder met ``ProcessType`` ``Background``. macOS remt zo'n proces én al zijn
    kindprocessen sterk af (CPU en schijf): de twee vaste beleidstests kostten dan ~850 s van de 900 s, en elke StudioOBF-aanvraag
    van 15 oktober eindigde in "time-out na 900 s" (docs/audits/mac_integrator_herstel_2026-10-15.md). ``-a`` haalt de tests uit
    die rem, ``-c utility`` houdt ze onder de apps van de gebruiker. Werkt ongeacht ``ProcessType``; elders (of zonder
    ``taskpolicy``) geen voorvoegsel."""
    taskpolicy = taskpolicy or TASKPOLICY
    if (platform or sys.platform) != "darwin" or not os.access(taskpolicy, os.X_OK):
        return []
    return [taskpolicy, "-a", "-c", "utility"]


def default_test_runner(store_dir: Path | None, grant: dict) -> Callable[[str, list[str]], dict]:
    """``run_tests`` met de historie en voortgang in de map met toestemmingen van de controller (nooit in de doelcommit)."""
    def runner(repo: str, tests: list[str]) -> dict:
        return run_tests(repo, tests, history_dir=store_dir, history_repo=grant.get("repo"), grant_id=grant.get("id"))
    return runner


def run_tests(repo: str, tests: list[str], timeout: int | None = None, *, history_dir: Path | str | None = None,
              history_repo: str | None = None, grant_id: str | None = None) -> dict:
    """Alleen de in de toestemming genoemde testbestanden, zonder shell. Pytest voor ``.py``, ``node --test`` voor ``.mjs``.

    Het tijdsbudget is adaptief (``test_budget.estimate``: eigen meetgeschiedenis, belasting, marge; harde grens
    ``test_budget.MAX_BUDGET_S`` over alle groepen samen), tenzij ``timeout`` uitdrukkelijk is meegegeven. Een run die het budget
    overschrijdt of stilvalt wordt als geheel afgebroken en is altijd mislukt. Met ``history_dir`` wordt elke meting bewaard."""
    # Geen sleutels, tokens, cookies of integratieconfiguratie van Monitor 2 in de omgeving van code uit de doelcommit.
    env = {**{k: v for k, v in os.environ.items() if not _SECRET_ENV.search(k)}, "PYTHONDONTWRITEBYTECODE": "1",
           "PYTHONUNBUFFERED": "1",
           "PYTHONPATH": os.pathsep.join(filter(None, [str(Path(repo) / "app"), str(Path(repo) / "platform"), str(Path(repo) / "tests"), repo,
                                                       os.environ.get("PYTHONPATH", "")]))}
    wrapper = test_wrapper()
    prefix = test_priority_prefix()
    fingerprint = tb.environment(prefix, wrapper)
    history, history_problem = tb.read_history(history_dir) if history_dir is not None else ([], "")
    ratio = tb.load_ratio()
    budget_mode = tb.mode()
    report_dir = tempfile.mkdtemp(prefix="obf-integratie-rapport-")
    junit = os.path.join(report_dir, "junit.xml")
    groups = []
    py = [t for t in tests if t.endswith(".py")]
    mjs = [t for t in tests if t.endswith(".mjs")]
    if py:
        # Het JUnit-rapport (per-bestand-duren voor de historie) staat buiten de doelcommit; onder een testomhulling (eigen
        # sandboxprofiel) niet, omdat dat profiel die map misschien niet laat schrijven: dan alleen de duur van de groep.
        report = [] if wrapper else [f"--junitxml={junit}"]
        groups.append(("pytest", py, [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *report, *py]))
    if mjs:
        node = shutil.which("node")
        groups.append(("node", mjs, [node, "--test", *mjs] if node else None))
    results, ok = [], True
    total_elapsed = 0.0
    try:
        for name, files, cmd in groups:
            if cmd is None:
                results.append({"runner": name, "ok": False, "error": "node ontbreekt op deze machine"})
                ok = False
                continue
            if wrapper:
                if not shutil.which(wrapper[0]):
                    results.append({"runner": name, "ok": False, "error": f"de testomhulling {wrapper[0]} ontbreekt op deze machine"})
                    ok = False
                    continue
                cmd = [*wrapper, *cmd]
            cmd = [*prefix, *cmd]
            if timeout is not None:
                budget = {"runner": name, "tests": files, "mode": "explicit", "budget_s": int(timeout), "estimate_s": None,
                          "reason": "uitdrukkelijk opgegeven"}
            else:
                budget = tb.estimate(files, history, repo=history_repo or repo, runner=name, env=fingerprint, ratio=ratio,
                                     mode=budget_mode, history_problem=history_problem)
            remaining = int(tb.MAX_BUDGET_S - total_elapsed)
            if remaining <= 0:
                results.append({"runner": name, "ok": False, "budget": budget,
                                "error": f"de harde grens van {tb.MAX_BUDGET_S} s voor alle tests samen is al bereikt"})
                ok = False
                continue
            allowed = min(budget["budget_s"], remaining)
            budget["allowed_s"] = allowed

            def progress(data: dict, _name=name, _allowed=allowed, _estimate=budget.get("estimate_s")) -> None:
                tb.write_progress(history_dir, {**data, "grant": grant_id, "runner": _name, "budget_s": _allowed,
                                                "estimate_s": _estimate, "updated_at": _iso(_now())})

            if history_dir is not None:
                progress({"elapsed_s": 0.0, "quiet_s": 0.0, "percent": None, "state": "running"})
            run = tb.run_watched(cmd, cwd=repo, env=env, budget_s=allowed, on_progress=progress if history_dir is not None else None)
            total_elapsed += run["elapsed_s"]
            item = {"runner": name, "ok": run["outcome"] == "done" and run["returncode"] == 0, "returncode": run["returncode"],
                    "tail": run["tail"], "outcome": run["outcome"], "elapsed_s": run["elapsed_s"], "budget_s": allowed,
                    "estimate_s": budget.get("estimate_s"), "budget": budget}
            if run["outcome"] == "timeout":
                # De staart (tot het afbreken) laat zien of een test hing of dat het geheel gewoon te traag was.
                item["error"] = f"time-out na {allowed} s"
            elif run["outcome"] == "stalled":
                item["error"] = f"stilgevallen: {tb.STALL_S:.0f} s geen uitvoer meer (na {run['elapsed_s']:.0f} s); afgebroken"
            elif run["outcome"] == "error":
                item["error"] = run.get("error") or "de tests konden niet starten"
            results.append(item)
            ok = ok and item["ok"]
            if history_dir is not None:
                durations = tb.junit_file_durations(junit, files) if name == "pytest" and run["outcome"] == "done" else None
                outcome = {"done": "passed" if item["ok"] else "failed"}.get(run["outcome"], run["outcome"])
                record = {"at": _iso(_now()), "repo": history_repo or repo, "runner": name, "tests": sorted(files),
                          "env": fingerprint, "load_ratio": ratio, "load_ratio_end": tb.load_ratio(), "outcome": outcome,
                          "returncode": run["returncode"], "elapsed_s": run["elapsed_s"], "budget_s": allowed,
                          "estimate_s": budget.get("estimate_s"), "mode": budget.get("mode"), "grant": grant_id}
                if durations:
                    record["files"], summed = durations
                    record["overhead_s"] = round(max(0.0, run["elapsed_s"] - summed), 2)
                try:
                    tb.append_history(history_dir, record)
                except OSError as exc:
                    item["history_error"] = exc.__class__.__name__
                progress({"elapsed_s": run["elapsed_s"], "quiet_s": 0.0, "percent": run.get("percent"), "state": outcome})
    finally:
        shutil.rmtree(report_dir, ignore_errors=True)
    return {"ok": ok, "results": results, "elapsed_s": round(total_elapsed, 2), "max_s": tb.MAX_BUDGET_S}


def execute(store: Store, grant_id: str, *, check_only: bool = False, run_test_suite: Callable[[str, list[str]], dict] | None = None,
            now: float | None = None) -> dict:
    """Voert de goedgekeurde integratie uit, of (``check_only``) controleert alleen of hij nu zou mogen.

    Volgorde: voorcontroles (geen wijziging) -> toestemming verbruiken (eenmalig) -> herstelbranch op de oude main-stand ->
    ``git merge`` van exact de goedgekeurde commit -> nacontrole (commit, werkboom, untracked, bronbranch) -> gerichte tests.
    """
    grant, facts = preflight(store, grant_id, now=now)
    repo = grant["repo"]
    if check_only:
        return {"ok": True, "grant": grant["id"], "would_integrate": True, "mode": grant["mode"], "commits": facts["commits"],
                "files": facts["files"], "head": facts["head"], "target_commit": grant["target_commit"]}
    stamp = dt.datetime.fromtimestamp(now if now is not None else _now(), dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    recovery = f"recovery/integratie-{grant['id']}-{stamp}"
    if _git(repo, "check-ref-format", "--branch", recovery, check=False).returncode or _rev(repo, "refs/heads/" + recovery):
        raise _refuse(store, grant_id, "recovery_exists", "de herstelbranch bestaat al of is ongeldig")
    # Vanaf hier is de toestemming op: elke poging na dit punt is een poging, ook als ze mislukt.
    if not store.set_marker(grant_id, ".used", {"at": _iso(now if now is not None else _now()), "host": os.uname().nodename,
                                                "pid": os.getpid(), "recovery_branch": recovery}):
        raise _refuse(store, grant_id, "not_open", "deze toestemming is al gebruikt (gelijktijdige poging)")
    store.audit("start", f"{grant_id}: integratie gestart ({grant['mode']}) {grant['source_branch']} -> {grant['main_branch']}",
                grant=grant_id, repo=repo, head_before=facts["head"], target_commit=grant["target_commit"], recovery_branch=recovery,
                commits=facts["commits"], files=facts["files"][:50])
    out = {"ok": False, "grant": grant_id, "repo": repo, "mode": grant["mode"], "head_before": facts["head"],
           "target_commit": grant["target_commit"], "recovery_branch": recovery}

    def finish(result: str, ok: bool, **extra) -> dict:
        out.update(result=result, ok=ok, **extra)
        store.set_marker(grant_id, ".result", out)
        store.audit("result", f"{grant_id}: {result}", grant=grant_id, ok=ok, result=result,
                    head_after=out.get("head_after"), **{k: v for k, v in extra.items() if k in ("problems", "tests")})
        return out

    tested_before = None
    if grant.get("auto"):
        # Automatische integratie: eerst de gerichte tests op exact de doelcommit; main verandert pas als ze slagen. Bij ff-only
        # is de boom na de merge gelijk aan de geteste boom, dus er hoeft daarna niets opnieuw.
        try:
            tested_before = pretest(repo, grant, run_test_suite, store_dir=store.dir)
        except IntegrationError as exc:
            return finish("tests_failed", False, recovery_branch=None, problems=[exc.message])
        except Exception as exc:  # noqa: BLE001
            return finish("tests_failed", False, recovery_branch=None,
                          problems=[f"de tests konden niet draaien ({exc.__class__.__name__}); main is niet aangeraakt"])
        out["tests"] = tested_before
        if not tested_before.get("ok"):
            return finish("tests_failed", False, tests=tested_before, recovery_branch=None,
                          problems=["gerichte tests mislukt; main is niet aangeraakt, er is niets samengevoegd"])
        try:   # wat tijdens de tests is veranderd (main, bronbranch, werkboom, untracked) wordt hier alsnog gezien
            facts = git_preconditions(store, grant)
        except IntegrationError as exc:
            return finish(exc.code, False, recovery_branch=None, problems=[exc.message + "; niets samengevoegd"])
    try:
        _git(repo, "branch", recovery, facts["head"])
        if _rev(repo, "refs/heads/" + recovery) != facts["head"]:
            return finish("recovery_failed", False, problems=["de herstelbranch staat niet op de oude main-stand; niets samengevoegd"])
        if _rev(repo, "HEAD") != facts["head"]:   # laatste controle vlak voor de ref-wijziging
            return finish("main_moved", False, problems=["main verplaatste tijdens de voorbereiding; niets samengevoegd"])
        if grant["mode"] == "ff-only":
            merge = _git(repo, "merge", "--ff-only", "--no-edit", grant["target_commit"], check=False, hooks_off=True)
        else:
            merge = _git(repo, "merge", "--no-ff", "--no-edit", "-m", f"Integratie {grant['source_branch']} ({grant['id']})",
                         grant["target_commit"], check=False, hooks_off=True)
        if merge.returncode:
            if os.path.lexists(_git_path(repo, "MERGE_HEAD")):
                _git(repo, "merge", "--abort", check=False)
            return finish("merge_failed", False, head_after=_rev(repo, "HEAD"),
                          problems=[(merge.stderr or merge.stdout).strip()[:300]])
    except IntegrationError as exc:
        return finish("git_failed", False, head_after=_rev(repo, "HEAD"), problems=[exc.message])

    problems = []
    head_after = _rev(repo, "HEAD")
    out["head_after"] = head_after
    if _git(repo, "symbolic-ref", "-q", "HEAD", check=False).stdout.strip() != "refs/heads/" + grant["main_branch"]:
        problems.append("HEAD staat niet meer op main")
    if grant["mode"] == "ff-only":
        if head_after != grant["target_commit"]:
            problems.append("main wijst niet naar de goedgekeurde commit")
    else:
        parents = _git(repo, "rev-list", "--parents", "-n", "1", "HEAD").stdout.split()[1:]
        if parents != [facts["head"], grant["target_commit"]]:
            problems.append("de samenvoegcommit heeft niet de verwachte ouders")
    if _git(repo, "merge-base", "--is-ancestor", facts["head"], "HEAD", check=False).returncode:
        problems.append("de oude main-stand is geen voorouder van main meer")
    if _rev(repo, "refs/heads/" + recovery) != facts["head"]:
        problems.append("de herstelbranch is veranderd")
    if _rev(repo, "refs/heads/" + grant["source_branch"]) != grant["target_commit"]:
        problems.append("de bronbranch is veranderd")
    if tracked_changes(repo):
        problems.append("de werkboom heeft na afloop wijzigingen aan getrackte bestanden")
    if untracked_snapshot(repo) != facts["untracked"]:
        problems.append("de untracked bestanden zijn veranderd")
    if problems:
        return finish("postcheck_failed", False, problems=problems)

    if tested_before is not None:
        out["tested_before_merge"] = True
        return finish("merged", True, tests=tested_before, problems=[])
    tests = (run_test_suite or default_test_runner(store.dir, grant))(repo, grant["tests"])
    out["tests"] = tests
    if tracked_changes(repo):
        problems.append("de tests lieten wijzigingen aan getrackte bestanden achter")
    if not tests.get("ok"):
        return finish("merged_tests_failed", False, tests=tests, problems=problems or ["gerichte tests mislukt; main staat wel op de nieuwe stand, "
                      f"herstel kan met branch {recovery}"])
    return finish("merged", not problems, tests=tests, problems=problems)


def publish_receipt(store: Store, inbox: Inbox | None, grant_id: str) -> None:
    """Zet de stand van een toestemming als informatief ontvangstbewijs bij de aanvraag waaruit zij voortkwam."""
    if inbox is None:
        return
    try:
        grant = store.read_grant(grant_id)
        if not grant.get("request_id"):
            return
        state = store.state(grant)
        result = store.marker(grant_id, ".result")
        used = store.marker(grant_id, ".used")
        data = {"state": state, "grant": grant_id, "target_commit": grant["target_commit"], "expires_at": grant["expires_at"]}
        if used is not None:
            data["state"] = "executing" if result is None else ("merged" if result.get("ok") else "failed")
            data["recovery_branch"] = used.get("recovery_branch")
        if result is not None:
            data.update(result=result.get("result"), ok=result.get("ok"), head_after=result.get("head_after"),
                        problems=result.get("problems"), recovery_branch=result.get("recovery_branch"),
                        auto=bool(grant.get("auto")) or None)
        inbox.write_receipt(grant["request_id"], data)
    except IntegrationError:
        pass


# ----------------------------------------------------------------------
# Opdrachtregel (voor Claude in de sandbox: alleen aanvragen en kijken; voor een mens: kijken en een code hashen)
# ----------------------------------------------------------------------


def _main_checkout(cwd: str) -> str:
    """De hoofdcheckout van de repository waarin ``cwd`` staat (ook vanuit een taakworktree)."""
    common = _git(cwd, "rev-parse", "--path-format=absolute", "--git-common-dir").stdout.strip()
    if not common.endswith("/.git"):
        raise IntegrationError("invalid_input", "kon de hoofdcheckout niet bepalen; geef --repo op")
    return common[: -len("/.git")]


def cli_main(argv: list[str] | None = None, env: dict | None = None) -> int:
    """``obf_monitor2_integrate.py request|status|requests|list|show|audit|hash-secret|auto-policy|auto-verify``.

    Er is bewust **geen** ``run`` of ``check``: uitvoeren kan alleen het Monitor 2-proces na goedkeuring door een mens, en
    dit script heeft geen sleutel. ``request`` schrijft één bestand in de inbox en geeft daarmee geen enkel recht.
    """
    import argparse

    env = env if env is not None else dict(os.environ)
    parser = argparse.ArgumentParser(prog="obf_monitor2_integrate", description=__doc__.split("\n\n")[0])
    parser.add_argument("--dir", help=f"map met toestemmingen (alleen voor een mens; standaard ${DIR_ENV})")
    parser.add_argument("--inbox", help=f"map met aanvragen (standaard ${INBOX_ENV} of de Monitor 2-configuratiemap)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    req = sub.add_parser("request", help="vraag een integratie naar main aan (een mens moet die in Monitor 2 goedkeuren)")
    req.add_argument("--repo", help="hoofdcheckout van de repository (standaard: afgeleid uit de huidige map)")
    req.add_argument("--branch", help="bronbranch (standaard: de huidige branch)")
    req.add_argument("--commit", help="volledige sha van de branch-tip (standaard: de huidige tip; moet de tip zijn)")
    req.add_argument("--test", action="append", default=[], metavar="PAD", help="gericht testbestand in de commit (herhaalbaar, 1-20; bij automatische "
                     "integratie tellen de vaste tests uit het beleid mee voor die 20)")
    req.add_argument("--mode", choices=MODES, default="ff-only")
    req.add_argument("--note", default="", help="korte toelichting voor de mens")
    status = sub.add_parser("status", help="toon het (informatieve) ontvangstbewijs van een aanvraag")
    status.add_argument("request_id")
    sub.add_parser("requests", help="toon de aanvragen in de inbox")
    sub.add_parser("list", help="toon de recente toestemmingen (zonder sleutel: niet gecontroleerd)")
    show = sub.add_parser("show", help="toon één toestemming (niet gecontroleerd)")
    show.add_argument("grant_id")
    sub.add_parser("audit", help="controleer de hashketen van de audittrail")
    sub.add_parser("hash-secret", help="maak de hash van een nieuwe goedkeuringscode (alleen op een terminal)")
    pol = sub.add_parser("auto-policy", help="schrijf het beleidsbestand voor automatische integratie en toon de twee configuratieregels "
                                              "(een mens; activeert niets zolang de hash niet in de configuratie van Monitor 2 staat)")
    pol.add_argument("--repo", required=True, help="hoofdcheckout van de repository")
    pol.add_argument("--out", required=True, help="pad van het beleidsbestand (buiten de repository en buiten de inbox)")
    pol.add_argument("--scope", choices=("chain", "task"), default="chain")
    pol.add_argument("--test", action="append", default=[], metavar="PAD", help="vast gericht testbestand dat altijd draait (herhaalbaar)")
    pol.add_argument("--protect", action="append", default=[], metavar="PATROON", help="extra beschermd pad (herhaalbaar; voegt toe)")
    pol.add_argument("--max-commits", type=int, default=50)
    pol.add_argument("--max-files", type=int, default=300)
    pol.add_argument("--settle", type=int, default=30, help="rustperiode in seconden na afronding van de taak")
    pol.add_argument("--ttl-hours", type=int, default=6, help="hoe lang een aanvraag mag wachten op een voorwaarde")
    pol.add_argument("--require-wrapper", action="store_true",
                     help=f"alleen integreren als {TEST_WRAPPER_ENV} (OS-sandbox voor de tests) is ingesteld")
    sub.add_parser("auto-verify", help="controleer het beleidsbestand tegen de vastgezette hash in de omgeving")
    args = parser.parse_args(argv)
    inbox = Inbox(Path(args.inbox).expanduser() if args.inbox else inbox_dir(env))
    store = Store(Path(args.dir).expanduser() if args.dir else grants_dir(env))

    def emit(data: dict) -> None:
        sys.stdout.write(json.dumps(data, indent=1, ensure_ascii=False) + "\n")

    try:
        if args.cmd == "request":
            cwd = os.getcwd()
            repo = os.path.realpath(os.path.expanduser(args.repo)) if args.repo else _main_checkout(cwd)
            branch = args.branch or _git(cwd, "symbolic-ref", "--short", "-q", "HEAD", check=False).stdout.strip()
            if not BRANCH.match(branch or "") or branch == MAIN_BRANCH:
                raise IntegrationError("invalid_input", "geef een bronbranch op (niet main)")
            tip = _rev(repo, "refs/heads/" + branch)
            if tip is None:
                raise IntegrationError("not_found", f"de branch {branch} bestaat niet in {repo}")
            commit_sha = (args.commit or tip).strip().lower()
            if commit_sha != tip:
                raise IntegrationError("stale", "de opgegeven commit is niet de tip van de branch; een aanvraag gaat over de tip")
            tests = _clean_tests(repo, commit_sha, args.test)
            saved = inbox.submit(repo=repo, source_branch=branch, target_commit=commit_sha, tests=tests, mode=args.mode,
                                 note=args.note, task_id=env.get("R2_TASK_ID", "")[:40])
            emit({"ok": True, "request": saved, "inbox": str(inbox.dir),
                  "next": "Noem dit aanvraag-id in de summary van runner_finish. Is voor deze repository automatische integratie "
                          "ingesteld, dan controleert en integreert Monitor 2 zelf zodra je afrondt; anders keurt een mens goed in "
                          "Monitor 2 (scherm Opdrachten, paneel Integratie naar main). Uitvoeren doet alleen Monitor 2. "
                          f"Stand later: obf_monitor2_integrate.py status {saved['id']}"})
            return 0
        if args.cmd == "status":
            if not REQUEST_ID.match(args.request_id):
                raise IntegrationError("invalid_input", "ongeldig aanvraag-id")
            receipt = inbox.dir / f"{args.request_id}.status.json"
            try:
                info = json.loads(receipt.read_text(encoding="utf-8"))
            except FileNotFoundError:
                request, _sha = inbox.read(args.request_id)
                info = {"request": args.request_id, "state": "open", "target_commit": request["target_commit"],
                        "informational": "nog niet verwerkt door Monitor 2"}
            emit(info)
            return 0
        if args.cmd == "requests":
            names = sorted(inbox._names())
            out = []
            for name in names[-LIST_REQUESTS:]:
                try:
                    request, _sha = inbox.read(name[:-5])
                    out.append({k: request[k] for k in ("id", "created_at", "repo", "source_branch", "target_commit", "mode", "tests")})
                except IntegrationError as exc:
                    out.append({"id": name[:-5], "problem": exc.code})
            emit({"requests": out})
            return 0
        if args.cmd == "list":
            emit({"grants": store.list_grants(verify=False)})
            return 0
        if args.cmd == "audit":
            ok, why = store.verify_audit()
            emit({"ok": ok, "problem": why})
            return 0 if ok else 1
        if args.cmd == "show":
            grant = store.read_grant(args.grant_id, verify=False)
            emit({**public(grant), "state": store.state(grant), "used": store.marker(grant["id"], ".used"),
                  "result": store.marker(grant["id"], ".result"), "verified": False})
            return 0
        if args.cmd == "hash-secret":
            import getpass

            if not (sys.stdin.isatty() and sys.stderr.isatty()):
                raise IntegrationError("needs_terminal", "de goedkeuringscode wordt alleen op een terminal ingevoerd")
            first = getpass.getpass(f"Nieuwe goedkeuringscode (min. {MIN_SECRET_LEN} tekens): ")
            if getpass.getpass("Nogmaals: ") != first:
                raise IntegrationError("invalid_input", "de twee invoeren verschillen")
            sys.stdout.write(f"{SECRET_HASH_ENV}={hash_secret(first)}\n")
            return 0
        if args.cmd in ("auto-policy", "auto-verify"):
            from . import auto_integration

            if args.cmd == "auto-verify":
                return auto_integration.cli_verify(env, emit)
            try:
                return auto_integration.cli_policy(args, emit)
            except ValueError as exc:
                raise IntegrationError("invalid_input", str(exc)) from None
        return 2
    except IntegrationError as exc:
        emit({"ok": False, "code": exc.code, "message": exc.message})
        return 2
