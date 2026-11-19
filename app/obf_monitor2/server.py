"""HTTP-laag van Monitor 2: de Studio-OBF-monitor, een dunne UI op Claude Runner 2.

Alleen stdlib, alleen localhost, dezelfde beveiliging als altijd (audit SEC-06):

* een gesloten lijst routes; statische bestanden komen uit een vooraf ingelezen
  tabel, nooit uit een pad uit het verzoek;
* ``Host`` moet localhost zijn, de socket bindt op 127.0.0.1; de enige uitzondering is
  de Tailscale Serve-route vanaf een uitdrukkelijk toegestaan ander tailnetapparaat
  (``tailscale_route``: eigen ts.net-naam, door Serve gezette afzender en login);
* elke POST eist het actietoken als ``HttpOnly; SameSite=Strict``-cookie (dat
  alleen de pagina ``/`` zelf zet, nooit een GET met gegevens), een ``Origin``
  die precies de eigen herkomst is (ontbreken = weigeren), ``Sec-Fetch-Site:
  same-origin`` als de browser dat kopje stuurt, JSON van beperkte grootte en
  een eenmalig kenmerk.

Wat Monitor 2 zelf doet, en wat niet
------------------------------------
* **Machines en taken**: vier nodes (MAC, LINUX, GERARD, WINDOWS), elk met een backend
  uit ``r2_adapter``. Alleen MAC heeft Runner 2; die wordt in dit proces gelezen en
  bediend via de controllerfuncties van Runner 2 zelf. Geen eigen taakstatus, herstel-
  of beslislogica, en geen Runner 1.
* **Accountbrede dingen**: de meldingenschakelaar (``obf_notify``) en de Claude-usage
  met het gedeelde cachebestand (``obf_monitor_web.claude_usage``), inclusief de
  aanmeldvernieuwing op de Mac (``auth_keeper``).

Poort 8772, zodat de beheerder zijn bestaande SSH-doorsturing voor gebruik buitenshuis werkt.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import secrets
import signal
import socket
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from http.cookies import CookieError, SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from . import aggregate
from . import backup_status
from . import action_link as action_link_module
from . import auto_integration as auto_module
from . import integration as integration_module
from . import jobs_backend
from . import discussions_backend
from . import jobs_repos as jobs_repos_module
from . import lan_route as lan_module
from . import notify_events
from . import nuc_health as nuc_health_module
from .storingen import StudioStoringen
from . import r2_adapter
from . import remote_status as remote_status_module
from . import integrator_status
from . import tailscale_route as tailscale_module

DEFAULT_HOST = "127.0.0.1"
#: De definitieve poort (2026-09-26): die van de vroegere Gerard-monitor. de beheerder zijn
#: SSH-doorsturing voor externe toegang wijst hierheen en hoeft niet te veranderen.
DEFAULT_PORT = 8772
LAUNCH_AGENT_LABEL = "com.studioobf.obf-monitor2"

STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/monitor2.css": ("monitor2.css", "text/css; charset=utf-8"),
    "/monitor2.js": ("monitor2.js", "text/javascript; charset=utf-8"),
    "/discussions.js": ("discussions.js", "text/javascript; charset=utf-8"),
    "/integration.js": ("integration.js", "text/javascript; charset=utf-8"),
}

REPORT_ROUTE = "/api/report"
ACTION_REVIEW_ROUTE = "/api/action-review"
ACTION_HISTORY_ROUTE = "/api/action-history"
ACTION_ROUTE = "/api/action"
NOTIFY_ROUTE = "/api/notifications"
NOTIFY_TEST_ROUTE = "/api/notifications/test"
JOBS_ROUTE = "/api/jobs"
DISCUSSIONS_ROUTE = "/api/discussions"
INTEGRATION_ROUTE = "/api/integration"
POST_ROUTES = (ACTION_ROUTE, NOTIFY_ROUTE, NOTIFY_TEST_ROUTE, JOBS_ROUTE, INTEGRATION_ROUTE, DISCUSSIONS_ROUTE)

MAX_BODY_BYTES = 64 * 1024
#: Een opdracht kan een lange prompt dragen (Runner 2 staat 60.000 tekens toe).
MAX_JOBS_BODY_BYTES = 128 * 1024
MAX_SMALL_BODY_BYTES = 8 * 1024

#: De velden die een knopdruk mag meenemen; al het andere valt eraf. ``text`` is het
#: antwoord (Beantwoorden) of de optionele opmerking (Hervatten).
ACTION_FIELDS = ("node", "task", "action", "nonce", "text", "reason")
MAX_TEXT_CHARS = 8000

# ---- Opdrachten (de opdrachtwachtrij van Runner 2, ``jobs_backend``) ----

JOBS_USERS_ENV = "OBF_JOBS_USERS"
JOBS_REPOS_ENV = "OBF_JOBS_REPOS"
DEFAULT_JOBS_USERS = ""
DEFAULT_JOBS_REPOS = ""

#: Handelingen die de pagina mag doen. ``list`` is de GET; ``repair`` hoort niet op een knop.
JOBS_ACTIONS = ("add", "plan", "move", "set_node", "set_deps", "hold", "release", "approve", "reject", "cancel", "check",
                "retry", "resume", "archive", "pause_queue", "resume_queue", "set_prompt")
#: Toegestane velden met hun soort; al het andere is een fout (geen stille doorgifte).
JOBS_STR_FIELDS = {"job_id": 20, "prompt": 60_000, "repository": 300, "title": 120, "node": 10, "model": 20,
                   "approval": 20, "position": 10, "request_id": 64, "to": 10, "verdict": 20, "source": 10,
                   "note": 2000, "reason": 300, "old_sha256": 64}
JOBS_BOOL_FIELDS = ("supervisor", "require_check", "held", "split")
#: ``repository`` met dit woord laat Runner 2 de repository uit de tekst bepalen (zie ``runner2.repos``).
AUTO_REPOSITORY = "auto"
_PROJECT_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,40}$")
JOBS_ENUMS = {"node": ("MAC", "GERARD", "LINUX", "WINDOWS", "ALL"), "model": ("standaard", "opus", "sonnet"),
              "approval": ("", "geen", "start", "integratie", "publicatie", "risico"), "position": ("top",),
              "verdict": ("ok", "afgekeurd"), "source": ("chatgpt", "mens")}
MAX_JOB_DEPS = 10
_JOB_ID = re.compile(r"^J-[0-9a-f]{12}$")
_REQUEST_ID = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_JOB_TO = re.compile(r"^(top|bottom|up|down|\d{1,4})$")


def jobs_users(env: dict | None = None) -> list[str]:
    """De namen die een opdracht mogen aanmaken of beheren (``OBF_JOBS_USERS``, standaard Beheerder en Gerard)."""
    raw = (env if env is not None else os.environ).get(JOBS_USERS_ENV) or DEFAULT_JOBS_USERS
    names: list[str] = []
    for item in raw.split(","):
        item = item.strip()
        if item and item.lower() not in [n.lower() for n in names]:
            names.append(item)
    return names


def jobs_repos(env: dict | None = None) -> list[str]:
    """De repository-voorinstellingen van het invoerformulier (``OBF_JOBS_REPOS``, komma-gescheiden; ``RUNNER=pad`` mag)."""
    raw = (env if env is not None else os.environ).get(JOBS_REPOS_ENV) or DEFAULT_JOBS_REPOS
    return [item.strip() for item in raw.split(",") if item.strip()]


def clean_job_request(payload: dict, users: list[str]) -> tuple[dict, str] | tuple[None, tuple[str, str]]:
    """Het POST-lichaam naar ``({actie, velden, by}, "")``, of ``(None, (reden, tekst))``. Alleen controleren, niet beslissen."""
    action = payload.get("action")
    if action not in JOBS_ACTIONS:
        return None, ("bad_action", "Onbekende handeling.")
    allowed = set(JOBS_STR_FIELDS) | set(JOBS_BOOL_FIELDS) | {"action", "by", "nonce", "depends_on", "project_repos"}
    unknown = sorted(key for key in payload if key not in allowed)
    if unknown:
        return None, ("bad_field", f"Onbekend veld: {unknown[0]}.")
    by = payload.get("by")
    canonical = next((n for n in users if isinstance(by, str) and n.lower() == by.strip().lower()), None)
    if canonical is None:
        return None, ("bad_user", "Kies eerst wie je bent (" + ", ".join(users) + ").")
    fields: dict = {}
    for key, value in payload.items():
        if key in ("action", "by", "nonce"):
            continue
        if key in JOBS_BOOL_FIELDS:
            if not isinstance(value, bool):
                return None, ("bad_field", f"{key} moet aan of uit zijn.")
        elif key == "project_repos":
            if not isinstance(value, dict) or len(value) > 10 or any(
                    not isinstance(k, str) or not _PROJECT_ID.match(k) or not isinstance(v, str) or not v.strip() or len(v) > 300
                    for k, v in value.items()):
                return None, ("bad_field", "Een repositorykeuze is {project: repository}.")
        elif key == "depends_on":
            if not isinstance(value, list) or len(value) > MAX_JOB_DEPS \
                    or any(not isinstance(item, str) or not _JOB_ID.match(item) for item in value):
                return None, ("bad_field", "Voorgangers zijn een lijst opdracht-id's.")
        else:
            if not isinstance(value, str):
                return None, ("bad_field", f"{key} moet tekst zijn.")
            if len(value) > JOBS_STR_FIELDS[key]:
                return None, ("too_long", f"{key} is te lang.")
            if key in JOBS_ENUMS and value not in JOBS_ENUMS[key]:
                return None, ("bad_field", f"Ongeldige waarde voor {key}.")
            if key == "job_id" and not _JOB_ID.match(value):
                return None, ("bad_field", "Ongeldig opdracht-id.")
            if key == "old_sha256" and not _SHA256.match(value):
                return None, ("bad_field", "Ongeldige controlecode van de oude tekst.")
            if key == "request_id" and not _REQUEST_ID.match(value):
                return None, ("bad_field", "Ongeldig aanvraagkenmerk.")
            if key == "to" and not _JOB_TO.match(value):
                return None, ("bad_field", "Ongeldige positie.")
        fields[key] = value
    return {"action": action, "fields": fields, "by": canonical}, ""


_NONCE = re.compile(r"^[0-9a-f]{8,64}$")
_ALLOWED_HOSTS = {"127.0.0.1", "localhost", "[::1]", "::1"}
_HOST_PORT = re.compile(r"^(\[[0-9a-fA-F:]+\]|[^:]+)(?::(\d{1,5}))?$")

#: Het cookie dat een knopdruk autoriseert. Alleen de pagina zelf zet het, als
#: ``HttpOnly`` (geen script leest het) en ``SameSite=Strict``; het staat in geen
#: enkel GET-antwoord met gegevens. Vroeger stond het token in ``/api/dashboard``,
#: en daarmee kon elk lokaal proces -- ook een in de modelsandbox -- de menselijke
#: knoppen bedienen (audit SEC-06).
ACTION_COOKIE = "obf_monitor2_action"

_SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
        "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "DENY",
    "Cross-Origin-Resource-Policy": "same-origin",
}


def build_info() -> dict:
    """Build-id, commit en datum uit ``RELEASE.json`` van deze release (geschreven door ``release_guard``); anders 'onbekend'."""
    try:
        data = json.loads((Path(__file__).resolve().parents[2] / "RELEASE.json").read_text("utf-8"))
        return {key: data.get(key) for key in ("build_id", "commit", "built_at", "sequence", "replaces", "manifest_sha256")}
    except (OSError, ValueError):
        return {"build_id": "onbekend", "commit": None, "built_at": None, "sequence": None, "replaces": None, "manifest_sha256": None}


def _load_static() -> dict[str, tuple[bytes, str]]:
    """Eén keer inlezen. De plaatjes zijn gewone bestanden: een betere versie onder
    dezelfde naam vervangt ze zonder codewijziging (na een herstart van Monitor 2)."""
    return {route: ((STATIC_DIR / name).read_bytes(), kind) for route, (name, kind) in STATIC_FILES.items()}


def _with_build_info(static: dict[str, tuple[bytes, str]]) -> dict[str, tuple[bytes, str]]:
    """Vult in de pagina de build-plaatshouders (build-id, commit, datum) uit ``RELEASE.json``."""
    info = build_info()
    page, kind = static["/"]
    for key, value in (("BUILD_ID", info["build_id"]), ("BUILD_COMMIT", (info["commit"] or "onbekend")[:8]),
                       ("BUILD_DATUM", (info["built_at"] or "onbekend")[:10])):
        page = page.replace(("{{" + key + "}}").encode(), html.escape(str(value)).encode())
    return {**static, "/": (page, kind)}


# ----------------------------------------------------------------------
# Integratietoestemming (``integration``): de menselijke kant
# ----------------------------------------------------------------------

#: Foutcode van ``integration`` -> HTTP-status. Alles wat hier niet staat is 503, zonder details.
INTEGRATION_STATUS = {"invalid_input": 400, "invalid_request": 400, "forbidden": 403, "repo_not_allowed": 403, "route_not_allowed": 403,
                      "secret_wrong": 403, "secret_locked": 429, "not_found": 404, "wrong_state": 409, "stale": 409,
                      "nothing_to_do": 409, "not_fast_forward": 409, "conflicts": 409, "job_not_approved": 409, "disabled": 409,
                      "no_secret_configured": 409, "request_changed": 409, "request_mismatch": 409, "protected_paths": 409,
                      "key_changed": 409, "busy": 409, "confirm_mismatch": 409, "not_open": 409, "main_moved": 409,
                      "source_moved": 409, "tracked_dirty": 409, "main_not_checked_out": 409, "operation_in_progress": 409,
                      "untracked_collision": 409, "recovery_exists": 409, "wrong_repo": 409}
INTEGRATION_ACTIONS = ("preview", "grant", "revoke", "reject", "check", "execute", "auto_pause", "auto_resume")
#: Routes waarover de goedkeuringscode mag reizen: loopback en de Tailscale-route (versleuteld); niet het LAN (http).
INTEGRATION_ROUTES = ("local", "tailscale")
INTEGRATION_STR_FIELDS = {"repository": 300, "source_branch": 100, "expected_main_head": 40, "target_commit": 40, "mode": 10,
                          "note": integration_module.MAX_NOTE, "job_id": 20, "grant_id": 20, "by": 40, "request_id": 20,
                          "request_sha256": 64, "secret": 200, "confirm": 12, "reason": integration_module.MAX_NOTE}
INTEGRATION_MODES = integration_module.MODES


class IntegrationService:
    """Dunne laag tussen ``/api/integration`` en ``integration``: de controller. Hier (en alleen hier) wordt uitgevoerd.

    De sleutel leeft alleen in het geheugen van dit proces; de sandbox bereikt dit niet (geen sleutel, geen schrijfrecht in de
    map met toestemmingen) en heeft de goedkeuringscode niet. Eén uitvoering tegelijk."""

    def __init__(self, store: integration_module.Store | None, *, enabled: bool, repos: list[str], approvers: list[str],
                 job_check=None, inbox: integration_module.Inbox | None = None, gate: integration_module.SecretGate | None = None,
                 run_test_suite=None, background: bool = True, auto_policy: auto_module.AutoPolicy | None = None,
                 r2_records: auto_module.R2Records | None = None, auto_clock=time.time, action_backend=None,
                 action_grace_s: float = auto_module.ACTION_GRACE_S, remote_status: Path | None = None):
        self.store, self.enabled, self.repos, self.approvers = store, enabled, list(repos), list(approvers)
        self.job_check = job_check
        self.inbox = inbox
        self.gate = gate if gate is not None else integration_module.SecretGate("")
        self.run_test_suite = run_test_suite
        self.background = background
        self.running: str | None = None
        self.thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._exec_lock = threading.Lock()
        # Statusbestand van de losse integrator op de Mac (§13.2): alleen tonen, ook als integreren hier uit staat.
        self.remote_status = remote_status
        # Automatische integratie (eenmalige repository-instelling, ``auto_integration``): alleen met een geldig, vastgezet beleid.
        self.auto_policy = auto_policy if auto_policy is not None else auto_module.AutoPolicy()
        self.auto: auto_module.AutoIntegrator | None = None
        if enabled and store is not None and inbox is not None and self.auto_policy.enabled:
            records = r2_records or auto_module.R2Records(auto_module.r2_home())
            # Wat een mens moet beslissen komt ook als open actiepunt in Runner 2 (alleen toevoegen, nooit sluiten); zie ``action_link``.
            linker = (action_link_module.ActionLinker(store, action_backend, records, clock=auto_clock)
                      if action_backend is not None else None)
            self.auto = auto_module.AutoIntegrator(store, inbox, self.auto_policy, records, exec_lock=self._exec_lock,
                                                   run_test_suite=run_test_suite, clock=auto_clock, linker=linker,
                                                   action_grace_s=action_grace_s)

    def start_auto(self) -> None:
        if self.auto is not None:
            self.auto.start()

    def close(self) -> None:
        if self.auto is not None:
            self.auto.close()

    @classmethod
    def from_env(cls, server: "Monitor2Server", env: dict | None = None) -> "IntegrationService":
        on = integration_module.enabled(env)
        # De sleutel is alleen-geheugen: nooit op schijf, dus niet leesbaar voor een taak in de sandbox.
        store = integration_module.Store(integration_module.grants_dir(env), key=secrets.token_bytes(32)) if on else None
        inbox = integration_module.Inbox(integration_module.inbox_dir(env)) if on else None
        approvers = integration_module.approvers(server.jobs_users, env)
        repos = integration_module.allowed_repos(env)
        policy = auto_module.load_policy(env, repos) if on else auto_module.AutoPolicy()
        service = cls(store, enabled=on, repos=repos, approvers=approvers, inbox=inbox,
                      gate=integration_module.SecretGate(integration_module.secret_hash(env)),
                      job_check=lambda job_id, repo: _job_approval(server.jobs, job_id, repo), auto_policy=policy,
                      r2_records=auto_module.R2Records(auto_module.r2_home(env)),
                      action_backend=(action_link_module.Runner2Actions(auto_module.r2_home(env), env=env)
                                      if policy.enabled and action_link_module.enabled(env) else None),
                      remote_status=remote_status_module.path_from_env(env))
        service.start_auto()
        return service

    def meta(self) -> dict:
        out = {"enabled": self.enabled, "repos": self.repos, "approvers": self.approvers, "modes": list(INTEGRATION_MODES),
               "main_branch": integration_module.MAIN_BRANCH, "secret_configured": self.gate.configured}
        if self.enabled and self.store is not None:
            out["grants"] = self.store.list_grants()
            out["requests"] = self.inbox.listing(self.store) if self.inbox is not None else []
            out["running"] = self.running or (self.auto.running if self.auto is not None else None)
            out["auto"] = self.auto.status() if self.auto is not None else self.auto_policy.public()
            ok, why = self.store.verify_audit()
            out["audit_ok"], out["audit_problem"] = ok, why
        remote = remote_status_module.read(self.remote_status)
        if remote is not None:
            out["remote_auto"] = remote
        return out

    # -- hulpfuncties

    def _check_fields(self, payload: dict) -> tuple[int, dict] | None:
        allowed = set(INTEGRATION_STR_FIELDS) | {"action", "nonce", "tests", "ttl_minutes", "allow_protected"}
        unknown = sorted(key for key in payload if key not in allowed)
        if unknown:
            return 400, {"ok": False, "reason": "bad_field", "message": f"Onbekend veld: {unknown[0]}."}
        for key, limit in INTEGRATION_STR_FIELDS.items():
            if key in payload and (not isinstance(payload[key], str) or len(payload[key]) > limit):
                return 400, {"ok": False, "reason": "bad_field", "message": f"{key} moet korte tekst zijn."}
        tests = payload.get("tests")
        if tests is not None and (not isinstance(tests, list) or any(not isinstance(t, str) or len(t) > 200 for t in tests)):
            return 400, {"ok": False, "reason": "bad_field", "message": "tests moet een lijst bestandsnamen zijn."}
        ttl = payload.get("ttl_minutes")
        if ttl is not None and (not isinstance(ttl, int) or isinstance(ttl, bool)):
            return 400, {"ok": False, "reason": "bad_field", "message": "ttl_minutes moet een getal zijn."}
        if "allow_protected" in payload and not isinstance(payload["allow_protected"], bool):
            return 400, {"ok": False, "reason": "bad_field", "message": "allow_protected moet ja of nee zijn."}
        return None

    def _canonical(self, by) -> str:
        name = next((n for n in self.approvers if isinstance(by, str) and n.lower() == by.strip().lower()), None)
        if name is None:
            raise integration_module.IntegrationError("forbidden", "alleen " + ", ".join(self.approvers) + " kunnen dit doen")
        return name

    def _require_secret(self, payload: dict, what: str, by: str) -> None:
        try:
            self.gate.check(payload.get("secret"))
        except integration_module.IntegrationError as exc:
            if exc.code in ("secret_wrong", "secret_locked"):
                self.store.audit("secret_failed", f"{what}: {exc.code} (naam {by})", action=what, by=by, code=exc.code)
            raise

    def act(self, payload: dict, route: str = "local") -> tuple[int, dict]:
        action = payload.get("action")
        if not self.enabled or self.store is None:
            return 409, {"ok": False, "reason": "disabled", "message": "Integratietoestemming staat op deze machine uit (OBF_INTEGRATION=1)."}
        if action not in INTEGRATION_ACTIONS:
            return 400, {"ok": False, "reason": "bad_action", "message": "Onbekende handeling."}
        problem = self._check_fields(payload)
        if problem is not None:
            return problem
        if route not in INTEGRATION_ROUTES:
            return 403, {"ok": False, "reason": "route_not_allowed",
                         "message": "Integreren kan alleen op deze machine zelf of via de Tailscale-route, niet via het LAN."}
        try:
            return self._act(action, payload)
        except integration_module.IntegrationError as exc:
            return INTEGRATION_STATUS.get(exc.code, 503), {"ok": False, "reason": exc.code if exc.code in INTEGRATION_STATUS else "unavailable",
                                                          "message": exc.message}
        except Exception as exc:  # noqa: BLE001 -- nooit een stacktrace naar de pagina
            return 503, {"ok": False, "reason": "unavailable", "message": f"De integratiecontrole gaf een fout ({exc.__class__.__name__}); er is niets gewijzigd."}

    def _act(self, action: str, payload: dict) -> tuple[int, dict]:
        im = integration_module
        if action == "execute":
            return self._execute(payload)
        if action in ("auto_pause", "auto_resume"):
            return self._auto_switch(action, payload)
        with self._lock:
            if action == "preview":
                request, sha = None, None
                repository, branch = payload.get("repository", ""), payload.get("source_branch", "")
                if payload.get("request_id"):
                    if self.inbox is None:
                        raise im.IntegrationError("invalid_input", "aanvragen zijn op deze machine niet beschikbaar")
                    request, sha = self.inbox.read(payload["request_id"])
                    repository, branch = request["repo"], request["source_branch"]
                repo = im.resolve_repo(repository, self.repos)
                facts = im.inspect(repo, branch)
                answer = {"ok": True, "action": action, "facts": facts}
                if request is not None:
                    answer["request"] = {k: v for k, v in request.items() if k != "created_epoch"}
                    answer["request"]["state"] = im.request_state(self.store, request)
                    answer["request"]["sha256"] = sha
                    answer["request_current"] = facts["source_tip"] == request["target_commit"]
                return 200, answer
            if action == "grant":
                by = self._canonical(payload.get("by"))
                self._require_secret(payload, "grant", by)
                ttl = payload.get("ttl_minutes")
                grant = im.create_grant(
                    self.store, repo=payload.get("repository", ""), source_branch=payload.get("source_branch", ""),
                    expected_main_head=payload.get("expected_main_head", ""), target_commit=payload.get("target_commit", ""),
                    tests=payload.get("tests"), by=by, mode=payload.get("mode") or "ff-only", note=payload.get("note", ""),
                    job_id=payload.get("job_id") or None, ttl_s=(ttl * 60 if ttl is not None else im.DEFAULT_TTL_S),
                    allowed=self.repos, approvers_list=self.approvers, job_check=self.job_check, inbox=self.inbox,
                    request_id=payload.get("request_id") or None, request_sha256=payload.get("request_sha256") or None,
                    allow_protected=bool(payload.get("allow_protected")))
                return 200, {"ok": True, "action": action, "grant": grant,
                             "message": "Toestemming gegeven; één keer te gebruiken, gebonden aan deze commit. "
                                        "Uitvoeren is een aparte handeling."}
            if action == "reject":
                if self.inbox is None:
                    raise im.IntegrationError("invalid_input", "aanvragen zijn op deze machine niet beschikbaar")
                done = im.reject_request(self.store, self.inbox, payload.get("request_id", ""), payload.get("by", ""),
                                         payload.get("reason", ""), approvers_list=self.approvers)
                return 200, {"ok": True, "action": action, "request": done, "message": "Aanvraag afgewezen."}
            if action == "revoke":
                grant = im.revoke_grant(self.store, payload.get("grant_id", ""), payload.get("by", ""), approvers_list=self.approvers)
                im.publish_receipt(self.store, self.inbox, grant["id"])
                return 200, {"ok": True, "action": action, "grant": grant, "message": "Toestemming ingetrokken."}
            # check: droge controle van een bestaande toestemming; wijzigt niets, verbruikt niets
            self._canonical(payload.get("by"))
            result = im.execute(self.store, payload.get("grant_id", ""), check_only=True)
            return 200, {"ok": True, "action": action, "check": result}

    def _execute(self, payload: dict) -> tuple[int, dict]:
        im = integration_module
        by = self._canonical(payload.get("by"))
        grant_id = payload.get("grant_id", "")
        grant = self.store.read_grant(grant_id)
        if payload.get("confirm") != grant["target_commit"][:12]:
            raise im.IntegrationError("confirm_mismatch", "typ de eerste 12 tekens van de doelcommit over om uit te voeren")
        self._require_secret(payload, "execute", by)
        if not self._exec_lock.acquire(blocking=False):
            raise im.IntegrationError("busy", "er loopt al een integratie; wacht tot die klaar is")
        started = False
        try:
            state = self.store.state(grant)
            if state != "open":
                raise im.IntegrationError("not_open", f"deze toestemming is {state}")
            im.preflight(self.store, grant_id)   # direct antwoord bij een weigering; wijzigt niets
            self.store.audit("execute_requested", f"{grant_id}: uitvoering gevraagd door {by}", grant=grant_id, by=by)
            self.running = grant_id
            if self.background:
                self.thread = threading.Thread(target=self._run, args=(grant_id,), name="integration-execute", daemon=False)
                self.thread.start()
                started = True
                return 202, {"ok": True, "action": "execute", "started": True, "grant": grant_id,
                             "message": "De integratie is gestart; de uitkomst verschijnt in de lijst."}
            result = self._run(grant_id, release=False)
            return 200, {"ok": bool(result.get("ok")), "action": "execute", "started": True, "grant": grant_id, "result": result}
        finally:
            if not started:
                self.running = None
                self._exec_lock.release()

    def _auto_switch(self, action: str, payload: dict) -> tuple[int, dict]:
        """Stilzetten mag elke goedkeurder (veilige richting); hervatten vraagt ook de goedkeuringscode."""
        im = integration_module
        if self.auto is None:
            raise im.IntegrationError("not_found", "automatische integratie is op deze machine niet actief"
                                      + (f" ({self.auto_policy.problem})" if self.auto_policy.problem else ""))
        by = self._canonical(payload.get("by"))
        if action == "auto_pause":
            return 200, {"ok": True, "action": action, "paused": self.auto.pause(by),
                         "message": "Automatische integratie staat stil; handmatig goedkeuren blijft kunnen."}
        self._require_secret(payload, "auto_resume", by)
        self.auto.resume(by)
        return 200, {"ok": True, "action": action, "paused": None, "message": "Automatische integratie is hervat."}

    def _run(self, grant_id: str, release: bool = True) -> dict:
        im = integration_module
        try:
            im.publish_receipt(self.store, self.inbox, grant_id)
            try:
                result = im.execute(self.store, grant_id, run_test_suite=self.run_test_suite)
            except im.IntegrationError as exc:
                result = {"ok": False, "result": exc.code, "problems": [exc.message]}
            except Exception as exc:  # noqa: BLE001
                result = {"ok": False, "result": "controller_error", "problems": [exc.__class__.__name__]}
                if self.store.marker(grant_id, ".used") is not None and self.store.marker(grant_id, ".result") is None:
                    self.store.set_marker(grant_id, ".result", {"ok": False, "result": "controller_error", "grant": grant_id})
                    self.store.audit("result", f"{grant_id}: controller_error", grant=grant_id, ok=False, result="controller_error")
            im.publish_receipt(self.store, self.inbox, grant_id)
            return result
        finally:
            self.running = None
            if release:
                self._exec_lock.release()


def _job_approval(jobs, job_id: str, repo: str) -> dict:
    """Hergebruik van ``approval='integratie'``: de opdracht moet bestaan, voor integratie goedgekeurd zijn door een mens
    en op dezelfde repository werken. Dit is een extra voorwaarde; de toestemming zelf blijft aan de commit gebonden."""
    if not _JOB_ID.match(job_id):
        raise integration_module.IntegrationError("invalid_input", "ongeldig opdracht-id")
    status, answer = jobs.job(job_id)
    job = answer.get("job") if status == 200 and isinstance(answer, dict) else None
    if not isinstance(job, dict):
        raise integration_module.IntegrationError("not_found", "de opdracht is niet te lezen in de wachtrij van Runner 2")
    approval = job.get("approval") or {}
    if approval.get("kind") != "integratie" or approval.get("state") != "approved":
        raise integration_module.IntegrationError("job_not_approved", "deze opdracht heeft geen goedgekeurde integratie-goedkeuring")
    if os.path.realpath(os.path.expanduser(str(job.get("repo") or ""))) != repo:
        raise integration_module.IntegrationError("job_not_approved", "de opdracht hoort bij een andere repository")
    return {"by": approval.get("by"), "at": approval.get("at"), "job_status": job.get("status")}


# ----------------------------------------------------------------------
# Accountbrede componenten (dezelfde als op 8770/8771/8772)
# ----------------------------------------------------------------------

class Notifications:
    """De ene globale schakelaar Remote meldingen, via ``obf_notify`` zelf."""

    def __init__(self):
        try:
            from obf_notify.notifier import notification_snapshot  # noqa: PLC0415
            from obf_notify.settings import NotificationSwitch  # noqa: PLC0415
        except ImportError:
            self._snapshot = None
            self._switch = None
        else:
            self._snapshot = notification_snapshot
            self._switch = NotificationSwitch()

    def snapshot(self) -> dict:
        if self._snapshot is None:
            return {"available": False}
        try:
            return {"available": True, **self._snapshot()}
        except Exception:  # noqa: BLE001 -- een meldingenblok mag het dashboard nooit breken
            return {"available": False}

    def set(self, on: bool, *, actor: str) -> None:
        if self._switch is None:
            raise OSError("obf_notify ontbreekt")
        self._switch.set(on, actor=actor)

    def send_test(self, *, actor: str) -> None:
        """Stuur de bestaande veilige testmail; verander geen schakelaar of runnerstatus."""
        if self._switch is None:
            raise OSError("obf_notify ontbreekt")
        try:
            from obf_notify.message import build_test_message  # noqa: PLC0415
            from obf_notify.settings import load_config  # noqa: PLC0415
            from obf_notify.transport import EmailTransport, TransportError  # noqa: PLC0415
        except ImportError:
            raise OSError("e-mailtransport ontbreekt") from None
        try:
            EmailTransport(load_config()).send(build_test_message(switch_on=self._switch.read().enabled))
        except TransportError as exc:
            raise OSError(str(exc)) from None


class DisabledUsage:
    def snapshot(self):
        return {"status": "disabled"}


def _usage_provider():
    if os.environ.get("OBF_MONITOR_USAGE_TICK") != "1":
        return DisabledUsage()
    from obf_monitor_web.claude_usage import ClaudeUsageProvider

    keeper = None
    if sys.platform == "darwin":
        from obf_monitor_web.auth_keeper import keeper_for_this_machine
        keeper = keeper_for_this_machine()
    provider = ClaudeUsageProvider(auth_keeper=keeper)
    provider.start_ticker()
    return provider


# ----------------------------------------------------------------------
# Server
# ----------------------------------------------------------------------

class Monitor2Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], *, nodes=r2_adapter.NODES, backends=None, usage=None,
                 notifications=None, quiet: bool = False, read_only: bool = False, clock=time.time,
                 tailscale=None, notify_journal=None, jobs=None, jobs_users_list=None, jobs_repos_list=None,
                 repo_catalog=None, repo_discovery=None, integration=None, nuc_health=None, discussions=None,
                 storingen=None):
        self.nodes = list(nodes)
        # De Tailscale Serve-route (``tailscale_route``); ``None`` = alleen localhost.
        self.tailscale = tailscale_module.load() if tailscale is None else (tailscale or None)
        self.generation = uuid.uuid4().hex[:12]
        self.action_token = secrets.token_hex(24)
        self.read_only = read_only
        self.quiet = quiet
        self.clock = clock
        self.static = _with_build_info(_load_static())
        # Eén eigen pool per node: een trage of hangende node kan geen werkers van de andere nodes innemen.
        self.pools = {node.id: ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"monitor2-{node.id}")
                      for node in self.nodes}
        self.started_at = clock()
        # Eerst de poort: een tweede instantie (poort bezet) faalt hier, vóór er ook maar
        # één usage-tik is gestart. Zo kan er nooit dubbel gepold worden.
        super().__init__(address, Monitor2Handler)
        self.backends = backends if backends is not None else r2_adapter.build_backends(tuple(self.nodes))
        self.usage = usage if usage is not None else _usage_provider()
        self.notifications = notifications or Notifications()
        # Het meldingsjournaal onder geluid en spraak: elke nieuwe gebeurtenis een volgnummer (zie ``notify_events``).
        self.chain_notifications = notify_events.ChainPolicy()
        self._notification_lock = threading.Lock()
        self.journal = notify_journal if notify_journal is not None else notify_events.NotifyJournal.from_env(clock=clock)
        # De opdrachtwachtrij leeft in Runner 2; ``jobs`` is een dunne laag (injecteerbaar voor tests).
        self.jobs = jobs if jobs is not None else jobs_backend.JobsBackend()
        self.discussions = discussions if discussions is not None else discussions_backend.DiscussionsBackend()
        self.jobs_users = list(jobs_users_list) if jobs_users_list is not None else jobs_users()
        self.jobs_repos = list(jobs_repos_list) if jobs_repos_list is not None else jobs_repos()
        # Eenmalige integratietoestemming (``integration``); standaard uit, per machine aan te zetten met OBF_INTEGRATION=1.
        self.integration = integration if integration is not None else IntegrationService.from_env(self)
        self.incident_ingang = None
        # De NUC-gezondheid (``nuc_health``): standaard alleen op GERARD; ``False`` = uit. De achtergrondronde start pas in main().
        self.nuc_health = nuc_health_module.NucHealthFeed.from_env() if nuc_health is None else (nuc_health or None)
        self.storingen = StudioStoringen.from_env() if storingen is None else storingen
        # Welke repository per Runner gekozen mag worden (``jobs_repos``): toegestaan én gecontroleerd.
        local = r2_adapter.local_node_id().upper()
        if repo_catalog is not None:
            self.repo_catalog = repo_catalog
        elif jobs_repos_list is not None:
            self.repo_catalog = jobs_repos_module.RepoCatalog(jobs_repos_module.parse_entries(",".join(self.jobs_repos)),
                                                              local_node=local)
        else:
            self.repo_catalog = jobs_repos_module.RepoCatalog.from_env(local_node=local)
        # De door Runner 2 ontdekte repositories (op de achtergrond ververst); zonder ``repos_catalog`` geen "Automatisch bepalen".
        fetch = getattr(self.jobs, "repos_catalog", None)
        self.repo_discovery = repo_discovery if repo_discovery is not None else (
            jobs_repos_module.DiscoveryFeed(fetch) if callable(fetch) else None)

    def handle_error(self, request, client_address) -> None:
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, TimeoutError)):
            return
        super().handle_error(request, client_address)

    # ------------------------------------------------------------ gegevens

    def dashboard(self, selections: dict[str, str | None]) -> dict:
        now = self.clock()
        futures = {node.id: self.pools[node.id].submit(self.backends[node.id].snapshot, selections.get(node.id))
                   for node in self.nodes}
        nodes = [{"id": node.id, "label": node.label, "name": node.display, "subtitle": node.subtitle,
                  "platform": node.platform, **self._snapshot_within(node.id, futures[node.id], selections.get(node.id))} for node in self.nodes]
        self._record_notifications(nodes)
        backups = backup_status.read_nodes(now=now)
        for node in nodes:
            node["backup"] = backups.get(node["id"])
        return {
            "generation": self.generation,
            "actions_enabled": not self.read_only,
            "now": now,
            "nodes": nodes,
            "notify": self.notification_feed(),
            "usage": aggregate.usage_block(self.usage.snapshot()),
            "notifications": self.notifications.snapshot(),
            "incidenten": self.incident_punten(),
            "storingen": self.storingen.current(),
            # Alleen de laatst opgehaalde stand: een dashboardverzoek doet nooit zelf ssh naar de NUC.
            "nuc": self.nuc_health.current() if self.nuc_health is not None else None,
            # De ontvangen NUC-integratorstand lezen geeft geen integratierechten.
            "integrator": integrator_status.read(getattr(self.integration, "remote_status", None), now=self.clock(),
                                                 expected_host=os.environ.get("OBF_NUC_INTEGRATOR_HOST", "studioobf")),
            "integrators": integrator_status.read_nodes(getattr(self.integration, "remote_status", None), now=self.clock()),
        }

    def incident_punten(self):
        if self.incident_ingang is None:
            return {"ingesteld": False, "beschikbaar": False, "punten": []}
        try:
            return {"ingesteld": True, "beschikbaar": True, "punten": self.incident_ingang.punten()}
        except Exception:
            return {"ingesteld": True, "beschikbaar": False, "punten": []}

    def _record_notifications(self, nodes):
        with self._notification_lock:
            # One primary queue observation for both nodes; bypass the presentation
            # cache/window. Runner snapshots precede it so a newly admitted job vetoes
            # an earlier idle observation. Repeated fresh reads provide the debounce.
            fetch = getattr(self.jobs, "notification_snapshot", None)
            chain_queue = fetch() if callable(fetch) else None
            sources = integrator_status.notification_sources(getattr(self.integration, "remote_status", None), now=self.clock())
            for view in nodes:
                node = next(n for n in self.nodes if n.id == view["id"])
                events = view.get("events")
                if node.id in ("linux", "gerard"):
                    events = self.chain_notifications.events(node.id, node.display, view, chain_queue,
                                                             sources[node.id], now=self.clock())
                    self.journal.ingest(node.id, node.display, events)
                elif view.get("available"):
                    self.journal.ingest(node.id, node.display, events)
                view.pop("events", None)   # label in de melding = weergavenaam; sleutel blijft node.id

    def notification_feed(self):
        feed = self.journal.feed()
        # Preserve the durable journal, but never replay legacy Ubuntu leaf
        # completion messages after switching to whole-chain semantics.
        feed["events"] = [e for e in feed["events"] if not (
            e.get("node") in ("linux", "gerard") and e.get("kind") == "done"
            and not e.get("key", "").startswith(e["node"] + "|chain-done|"))]
        return feed

    def poll_chain_notifications(self):
        nodes = [n for n in self.nodes if n.id in ("linux", "gerard")]
        futures = {n.id: self.pools[n.id].submit(self.backends[n.id].snapshot, None) for n in nodes}
        views = [{"id": n.id, **self._snapshot_within(n.id, futures[n.id])} for n in nodes]
        self._record_notifications(views)

    #: MON-11: zo lang wacht één dashboardverzoek hoogstens op één nodebron.
    SOURCE_WAIT_SECONDS = 10.0

    def _snapshot_within(self, node_id: str, future, selected: str | None = None) -> dict:
        """De verse lezing, of -- blijft een bron hangen -- de laatste geldige stand met een vertraagd-markering,
        of (nog nooit een geldige stand) een eerlijke 'traag'-melding, telkens voor alleen die node.

        Een hangende bron (bestandslock, traag bestandssysteem, ssh) mocht alle tabbladen en de
        andere nodes laten vastlopen; nu krijgt alleen die kaart de melding, en 'niet bereikbaar'
        komt pas van de backend zelf na overtuigend bewijs.
        """
        from concurrent.futures import TimeoutError as FutureTimeout  # noqa: PLC0415

        try:
            return future.result(timeout=self.SOURCE_WAIT_SECONDS)
        except FutureTimeout:
            fallback = getattr(self.backends[node_id], "fallback_view", None)
            view = fallback(selected) if fallback else None
            if view is not None:
                delay = view.get("data_delay") or {"seconds": round(self.SOURCE_WAIT_SECONDS), "failures": 0, "reason": None}
                return {**view, "data_delay": delay}
            return {"installed": True, "available": False, "operational": False, "headline": "ANTWOORDT TRAAG",
                    "tone": "warn", "message": "De gegevens van deze node komen nu traag binnen.",
                    "data_delay": {"seconds": None, "failures": 0, "reason": None},
                    "watch": None, "task": None, "tasks": [], "runner2": None}

    def jobs_meta(self, overview: dict | None = None) -> dict:
        label = getattr(self.jobs, "control_label", lambda: None)()
        return {"control_node": label, "users": self.jobs_users, "nodes": list(jobs_backend.JOB_NODES),
                "models": ["standaard", "opus", "sonnet"], "approval_kinds": ["start", "integratie", "publicatie", "risico"],
                "repos": self.jobs_repos, "repo_choices": self.repo_choices(overview),
                "projects": self.repo_catalog.projects()}

    def repo_choices(self, overview: dict | None = None) -> dict:
        """Per Runner de repositories voor het formulier. Leest alleen wat de machinekaarten al lazen (geen extra ssh)."""
        catalog = self.repo_catalog
        for node in self.nodes:
            seen = getattr(self.backends.get(node.id), "repositories", lambda: None)()
            if seen:
                catalog.observe(node.id.upper(), seen)
        catalog.note_failures(overview)
        if self.repo_discovery is not None:
            catalog.set_discovered(self.repo_discovery.current())
        return catalog.all_choices()

    def health(self) -> dict:
        """Gezondheid per node en van de accountbrede delen; geen taakgegevens."""
        nodes = {}
        for node in self.nodes:
            backend = self.backends[node.id]
            info = dict(backend.health())
            if info.get("loaded"):
                snap = backend.snapshot(None)
                info["watch_healthy"] = (snap.get("watch") or {}).get("healthy")
                info["available"] = snap.get("available")
            nodes[node.id] = info
        usage = self.usage.snapshot() if self.usage is not None else {}
        return {"ok": True, "generation": self.generation, "build": build_info(),
                "uptime_seconds": round(self.clock() - self.started_at),
                "monitor": {"id": "monitor2", "label": "MONITOR 2"},
                "port": self.server_address[1], "service": LAUNCH_AGENT_LABEL,
                "nodes": nodes,
                "actions_enabled": not self.read_only,
                "claude_usage": {key: usage.get(key) for key in ("status", "age_seconds", "stale", "problem")},
                "notifications": {key: self.notifications.snapshot().get(key)
                                  for key in ("available", "enabled", "notifier_running")},
                "nuc": ({key: self.nuc_health.current().get(key) for key in ("state", "status_age_s", "checked_age_s")}
                        if self.nuc_health is not None else None)}

    def close_sources(self) -> None:
        self.storingen.close()
        if self.incident_ingang is not None:
            self.incident_ingang.close()
            self.incident_ingang = None
        self.integration.close()
        if self.nuc_health is not None:
            self.nuc_health.close()
        for backend in self.backends.values():
            backend.close()


class Monitor2LanServer(ThreadingHTTPServer):
    """Het tweede luisteradres (``lan_route``): dezelfde handler en dezelfde gegevens als de hoofdserver, andere toegangsregels."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, primary: Monitor2Server, lan: lan_module.LanRoute):
        self._primary = primary
        self.lan_route = lan
        # Alleen de tailnet-route met ``identity``: een eigen, uitdrukkelijk genoemd apparaat heeft geen sleutelcookie nodig.
        self.identity_check = lan_module.IdentityCheck(lan.identity, own_addresses={lan.bind}) if lan.identity is not None else None
        if not lan.bind:
            raise OSError("bindadres nog niet bekend")
        super().__init__((lan.bind, lan.port), Monitor2Handler)

    def __getattr__(self, name: str):
        # Alleen voor wat deze server niet zelf heeft (backends, usage, generation, ...): de hoofdserver.
        if name == "_primary":
            raise AttributeError(name)
        return getattr(self._primary, name)

    def handle_error(self, request, client_address) -> None:
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, TimeoutError)):
            return
        super().handle_error(request, client_address)


class Monitor2Handler(BaseHTTPRequestHandler):
    server_version = "ObfMonitor2/0.1"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    # ------------------------------------------------------------ helpers

    def _route(self) -> str | None:
        """``"local"``, ``"tailscale"`` of ``None`` (weigeren). Bepaald per aanvraag, fail-closed.

        ``local`` is ongewijzigd de localhost-grens van SEC-06. ``tailscale`` alleen als
        het ``Host``-kopje precies de ingestelde ts.net-naam is én Serve zelf bevestigt dat
        de aanvraag van een toegestaan ander tailnetapparaat komt (``tailscale_route``).
        """
        host = (self.headers.get("Host") or "").strip()
        lan = getattr(self.server, "lan_route", None)
        if lan is not None:
            # Het LAN/VPN-luisteradres (``lan_route``): nooit de localhost-regels, en nooit doorgestuurd.
            if tailscale_module.forwarded(self.headers) or not lan.matches_host(host) \
                    or not lan.peer_allowed(self.client_address[0]):
                self.log_refusal(f"lan-route geweigerd: host={host!r} peer={self.client_address[0]!r}")
                return None
            return "lan"
        # Via Serve komt de verbinding óók van 127.0.0.1, en elk proces op deze Mac (ook
        # de modelsandbox, die 443 bereikt) kan daar ``Host: localhost`` meesturen. Een
        # doorgestuurde aanvraag is daarom nooit ``local``, wat zijn Host ook zegt.
        proxied = tailscale_module.forwarded(self.headers)
        if not proxied:
            if not host:
                return "local"
            match = _HOST_PORT.match(host)
            if match is not None and match.group(1).lower() in _ALLOWED_HOSTS:
                return "local"
        route = self.server.tailscale
        if route is not None and route.matches_host(host):
            refusal = route.refusal(self.client_address[0], self.headers)
            if refusal is None:
                return "tailscale"
            self.log_refusal(f"tailscale-route geweigerd: {refusal} "
                             f"(xff={self.headers.get('X-Forwarded-For')!r})")
        elif proxied:
            self.log_refusal(f"doorgestuurde aanvraag geweigerd: host={host!r} "
                             f"(xff={self.headers.get('X-Forwarded-For')!r})")
        return None

    def _host_allowed(self) -> bool:
        self.route = self._route()
        return self.route is not None

    def _send(self, status: HTTPStatus, body: bytes, content_type: str, *, cache: str = "no-store",
              cookie: str | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        if cookie is not None:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache)
        if self.close_connection:
            self.send_header("Connection", "close")
        for key, value in _SECURITY_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, status: HTTPStatus, payload: dict) -> None:
        self._send(status, json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
                   "application/json; charset=utf-8")

    def _reject(self, status: HTTPStatus, message: str, reason: str = "rejected", **extra) -> None:
        self.close_connection = True
        self._json(status, {"ok": False, "reason": reason, "message": message,
                            "generation": self.server.generation, **extra})

    # ------------------------------------------------------------- GET

    def do_GET(self) -> None:  # noqa: N802
        if not self._host_allowed():
            self._reject(HTTPStatus.FORBIDDEN, "Alleen bereikbaar via localhost.")
            return
        parts = urlsplit(self.path)
        route = parts.path
        if self.route == "lan" and not self._lan_gate(parts):
            return
        if route in self.server.static:
            body, kind = self.server.static[route]
            # Alleen de pagina zelf draagt het actiecookie; geen enkel ander GET-antwoord.
            cookie = (f"{ACTION_COOKIE}={self.server.action_token}; Path=/; HttpOnly; SameSite=Strict"
                      + ("; Secure" if self.route == "tailscale" else "")
                      if route == "/" else None)
            self._send(HTTPStatus.OK, body, kind, cache="no-store" if route == "/" else "no-cache", cookie=cookie)
        elif route == "/healthz":
            self._json(HTTPStatus.OK, self.server.health())
        elif route == "/api/versie":
            self._json(HTTPStatus.OK, {"ok": True, **build_info()})
        elif route == "/api/dashboard":
            self._dashboard(parse_qs(parts.query, keep_blank_values=False))
        elif route == REPORT_ROUTE:
            self._report(parse_qs(parts.query, keep_blank_values=False))
        elif route == ACTION_REVIEW_ROUTE:
            self._action_review(parse_qs(parts.query, keep_blank_values=False))
        elif route == ACTION_HISTORY_ROUTE:
            self._action_history(parse_qs(parts.query, keep_blank_values=False))
        elif route == JOBS_ROUTE:
            self._jobs_overview(parse_qs(parts.query, keep_blank_values=False))
        elif route == DISCUSSIONS_ROUTE:
            query = parse_qs(parts.query, keep_blank_values=False)
            status, answer = (self.server.discussions.get(query['conversation_id'][0]) if query.get('conversation_id') else self.server.discussions.overview())
            self._json(HTTPStatus(status), {**answer, 'actions_enabled': not self.server.read_only, 'meta': {'users': self.server.jobs_users}, 'generation': self.server.generation})
        elif route == INTEGRATION_ROUTE:
            self._json(HTTPStatus.OK, {"ok": True, **self.server.integration.meta(), "generation": self.server.generation,
                                       "actions_enabled": not self.server.read_only})
        else:
            self._json(HTTPStatus.NOT_FOUND, {"error": "Onbekend adres.", "status": 404})

    def _lan_gate(self, parts) -> bool:
        """Op het LAN-adres: zonder toegangscookie niets. ``/?key=...`` zet het cookie en stuurt door naar ``/``.

        Op de tailnet-route met ``identity`` mag een uitdrukkelijk genoemd eigen apparaat ook zonder cookie
        (``lan_route.IdentityCheck``); lukt die controle niet, dan blijft de sleutel nodig."""
        lan = self.server.lan_route
        offered = (parse_qs(parts.query).get("key") or [""])[0] if parts.path == "/" and self.command in ("GET", "HEAD") else ""
        if offered:
            # Een eenmalige bootstrap-link (``monitor2-ctl tailnet-url``) wordt alleen op GET ingewisseld, nooit op HEAD.
            if not lan.key_ok(offered) and not (self.command == "GET" and lan_module.redeem_bootstrap(offered, lan.label)):
                self.log_refusal(f"{lan.label}-route: onjuiste of verlopen toegangssleutel/-link")
                self._reject(HTTPStatus.FORBIDDEN, "Onjuiste toegangssleutel.", reason="bad_key")
                return False
            self.close_connection = True
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/")
            self.send_header("Set-Cookie", f"{lan_module.KEY_COOKIE}={lan.key}; Path=/; HttpOnly; SameSite=Strict; Max-Age=31536000")
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.send_header("Cache-Control", "no-store")
            for key, value in _SECURITY_HEADERS.items():
                self.send_header(key, value)
            self.end_headers()
            return False
        try:
            jar = SimpleCookie(self.headers.get("Cookie") or "")
        except CookieError:
            jar = SimpleCookie()
        morsel = jar.get(lan_module.KEY_COOKIE)
        if morsel is not None and lan.key_ok(morsel.value):
            return True
        check = getattr(self.server, "identity_check", None)
        peer, port = self.client_address[0], self.client_address[1]
        if check is not None and check.check(peer, port, on_refusal=lambda why: self.log_refusal(
                f"{lan.label}-route: tailscale-identiteit geweigerd: {why} peer={peer!r}")):
            return True
        self._reject(HTTPStatus.UNAUTHORIZED, "Toegang vereist: open Monitor 2 met de volledige toegangslink.", reason="lan_key_required")
        return False

    def do_HEAD(self) -> None:  # noqa: N802
        self.do_GET()

    def _dashboard(self, query: dict) -> None:
        selections: dict[str, str | None] = {}
        for node in self.server.nodes:
            selected = (query.get("sel." + node.id) or [None])[0]
            if selected is not None and not r2_adapter.TASK_ID.match(selected):
                self._json(HTTPStatus.BAD_REQUEST, {"error": f"Ongeldige keuze voor {node.id}."})
                return
            selections[node.id] = selected
        self._json(HTTPStatus.OK, self.server.dashboard(selections))

    def _report(self, query: dict) -> None:
        """Het eindrapport van één afgeronde taak (``runner2_result``). Alleen lezen: geen cookie, geen mutatie."""
        node_id = (query.get("node") or [""])[0]
        task_id = (query.get("task") or [""])[0]
        backend = self.server.backends.get(node_id)
        if backend is None:
            self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "reason": "bad_node", "message": "Onbekende machine."})
            return
        if not r2_adapter.TASK_ID.match(task_id):
            self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "reason": "invalid_input", "message": "Ongeldige taak."})
            return
        status, answer = backend.report(task_id)
        answer = {**answer, "generation": self.server.generation, "node": node_id}
        self._json(HTTPStatus(status) if 100 <= status < 600 else HTTPStatus.BAD_GATEWAY, answer)

    def _action_review(self, query: dict) -> None:
        """Het opschoonvoorstel van één machine (DRY RUN van het actiebeheer). Alleen lezen: geen cookie, geen mutatie, geen sluiten."""
        node_id = (query.get("node") or [""])[0]
        backend = self.server.backends.get(node_id)
        review = getattr(backend, "action_review", None)
        if backend is None or review is None:
            self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "reason": "bad_node", "message": "Onbekende machine."})
            return
        status, answer = review((query.get("refresh") or [""])[0] == "1")
        answer = {**answer, "generation": self.server.generation, "node": node_id}
        self._json(HTTPStatus(status) if 100 <= status < 600 else HTTPStatus.BAD_GATEWAY, answer)

    def _action_history(self, query: dict) -> None:
        """De volledige actiehistorie van één machine (open en afgehandeld). Alleen lezen: geen cookie, geen mutatie, geen sluiten."""
        node_id = (query.get("node") or [""])[0]
        backend = self.server.backends.get(node_id)
        history = getattr(backend, "action_history", None)
        if backend is None or history is None:
            self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "reason": "bad_node", "message": "Onbekende machine."})
            return
        status, answer = history()
        answer = {**answer, "generation": self.server.generation, "node": node_id}
        self._json(HTTPStatus(status) if 100 <= status < 600 else HTTPStatus.BAD_GATEWAY, answer)

    def _jobs_overview(self, query: dict) -> None:
        """Alleen lezen: het overzicht van de opdrachtwachtrij plus ``meta`` voor het formulier; met ``job=`` één opdracht."""
        job_id = (query.get("job") or [""])[0]
        if job_id and not _JOB_ID.match(job_id):
            self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "reason": "invalid_input", "message": "Ongeldig opdracht-id."})
            return
        status, answer = self.server.jobs.job(job_id) if job_id else self.server.jobs.overview()
        meta = self.server.jobs_meta(answer if status == 200 and not job_id else None)
        if status != 200:
            answer = {**answer, "ok": False}
        self._json(HTTPStatus(status) if 100 <= status < 600 else HTTPStatus.BAD_GATEWAY,
                   {**answer, "meta": meta, "generation": self.server.generation,
                    "actions_enabled": not self.server.read_only})

    # ------------------------------------------------------------ POST

    def do_POST(self) -> None:  # noqa: N802
        route = urlsplit(self.path).path
        if route not in POST_ROUTES:
            self._unsupported()
            return
        if not self._host_allowed():
            self._reject(HTTPStatus.FORBIDDEN, "Alleen bereikbaar via localhost.")
            return
        if self.route == "lan" and not self._lan_gate(urlsplit(self.path)):
            return
        payload = self._read_body(MAX_JOBS_BODY_BYTES if route in (JOBS_ROUTE, DISCUSSIONS_ROUTE)
                                  else MAX_BODY_BYTES if route == ACTION_ROUTE else MAX_SMALL_BODY_BYTES)
        if payload is None:
            return
        if self.server.read_only:
            self._reject(HTTPStatus.FORBIDDEN, "Monitor 2 draait zonder bediening (--read-only).")
            return
        if not isinstance(payload.get("nonce"), str) or not _NONCE.match(payload["nonce"]):
            self._reject(HTTPStatus.BAD_REQUEST, "Deze knopdruk draagt geen geldig eenmalig kenmerk; "
                         "ververs de pagina.", reason="bad_nonce")
            return
        if route == ACTION_ROUTE:
            self._action(payload)
        elif route == JOBS_ROUTE:
            self._jobs_action(payload)
        elif route == DISCUSSIONS_ROUTE:
            try:
                request = discussions_backend.clean(payload, self.server.jobs_users)
            except ValueError as exc:
                self._reject(HTTPStatus.BAD_REQUEST, str(exc), reason='invalid_input')
                return
            status, answer = self.server.discussions.act(request)
            self._json(HTTPStatus(status), {**answer, 'generation': self.server.generation})
        elif route == INTEGRATION_ROUTE:
            status, answer = self.server.integration.act(payload, route=getattr(self, "route", "local"))
            self._json(HTTPStatus(status), {**answer, "generation": self.server.generation})
        elif route == NOTIFY_ROUTE:
            self._notifications(payload)
        else:
            self._notification_test(payload)

    def _action(self, payload: dict) -> None:
        """Geeft één knopdruk door aan de Runner-2-controller van die node. Verder niets."""
        node_id = payload.get("node")
        backend = self.server.backends.get(node_id) if isinstance(node_id, str) else None
        if backend is None:
            self._reject(HTTPStatus.BAD_REQUEST, "Onbekende node.", reason="bad_node")
            return
        body = {key: payload[key] for key in ACTION_FIELDS if key in payload}
        if any(not isinstance(value, str) for value in body.values()):
            self._reject(HTTPStatus.BAD_REQUEST, "Onleesbaar verzoek.", reason="bad_field")
            return
        if body.get("action") not in r2_adapter.ACTIONS:
            self._reject(HTTPStatus.BAD_REQUEST, "Onbekende actie.", reason="bad_action")
            return
        if not r2_adapter.TASK_ID.match(body.get("task", "")):
            self._reject(HTTPStatus.BAD_REQUEST, "Een knopdruk hoort bij precies één taak.", reason="no_target")
            return
        if len(body.get("text", "")) > MAX_TEXT_CHARS or len(body.get("reason", "")) > 500:
            self._reject(HTTPStatus.BAD_REQUEST, "De tekst is te lang.", reason="too_long")
            return
        fields = {key: body[key] for key in ("text", "reason") if key in body}
        status, answer = backend.action(body["action"], body["task"], **fields)
        answer = {**answer, "generation": self.server.generation, "node": node_id}
        self._json(HTTPStatus(status) if 100 <= status < 600 else HTTPStatus.BAD_GATEWAY, answer)

    def _jobs_action(self, payload: dict) -> None:
        """Geeft één handeling door aan de opdrachtwachtrij van Runner 2. Velden en gebruiker worden alleen gecontroleerd."""
        request, problem = clean_job_request(payload, self.server.jobs_users)
        if request is None:
            reason, message = problem
            self._reject(HTTPStatus.REQUEST_ENTITY_TOO_LARGE if reason == "too_long" else HTTPStatus.BAD_REQUEST,
                         message, reason=reason)
            return
        fields = request["fields"]
        auto = str(fields.get("repository", "")).strip().lower() == AUTO_REPOSITORY
        if request["action"] == "plan" and not auto:
            self._reject(HTTPStatus.BAD_REQUEST, "Een voorstel is er alleen bij Automatisch bepalen.", reason="bad_field")
            return
        if request["action"] in ("add", "plan"):
            self.server.repo_choices()             # eerst de laatste lezing van de machinekaarten verwerken
            if auto:
                # Runner 2 bepaalt de repository (``runner2.repos``); een Runner 2 die dat niet kent zou "auto" als pad nemen.
                if not self.server.repo_catalog.auto_available():
                    self._reject(HTTPStatus.CONFLICT, "Automatisch bepalen is hier nog niet beschikbaar (de Runner 2 op de "
                                 "control plane kent de repositorykeuze niet of antwoordt niet). Kies zelf een repository.",
                                 reason="auto_unavailable")
                    return
                fields = {**fields, "repository": AUTO_REPOSITORY}
            elif request["action"] == "add":
                # Alleen naar een expliciet gekozen repository die voor die Runner in de lijst staat (``jobs_repos``).
                problem = self.server.repo_catalog.check(fields.get("node", ""), fields.get("repository", ""))
                if problem is not None:
                    self._reject(HTTPStatus.CONFLICT, problem, reason="repo_not_allowed")
                    return
        if not auto:
            fields = {k: v for k, v in fields.items() if k not in ("split", "project_repos")}   # alleen bij Automatisch
        status, answer = self.server.jobs.act(request["action"], fields, request["by"])
        if request["action"] == "add" and status == 200 and not auto:
            self.server.repo_catalog.note_used(fields["node"], fields["repository"])
        answer = {**answer, "generation": self.server.generation}
        self._json(HTTPStatus(status) if 100 <= status < 600 else HTTPStatus.BAD_GATEWAY, answer)

    def _notifications(self, payload: dict) -> None:
        if not isinstance(payload.get("on"), bool):
            self._reject(HTTPStatus.BAD_REQUEST, "Meldingen staan aan of uit.", reason="bad_state")
            return
        try:
            self.server.notifications.set(payload["on"], actor=f"monitor2-{self.server.server_address[1]}")
        except OSError as exc:
            self._reject(HTTPStatus.CONFLICT, f"De schakelaar kon niet worden opgeslagen: "
                         f"{exc.__class__.__name__}.", reason="notify_write_failed")
            return
        self._json(HTTPStatus.OK, {"ok": True, "generation": self.server.generation,
                                   "notifications": self.server.notifications.snapshot()})

    def _notification_test(self, _payload: dict) -> None:
        try:
            self.server.notifications.send_test(actor=f"monitor2-{self.server.server_address[1]}")
        except OSError as exc:
            self._reject(HTTPStatus.SERVICE_UNAVAILABLE, f"Testmail niet verstuurd: {exc}",
                         reason="notify_test_failed")
            return
        self._json(HTTPStatus.OK, {"ok": True, "message": "Testmail verstuurd."})

    def _read_body(self, limit: int) -> dict | None:
        if (self.headers.get("Transfer-Encoding") or "").strip():
            self._reject(HTTPStatus.LENGTH_REQUIRED, "Alleen een verzoek met Content-Length.")
            return None
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        if length < 0 or length > limit:
            self._reject(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Het verzoek is te groot.")
            return None
        try:
            raw = self.rfile.read(length) if length else b""
        except OSError:
            self._reject(HTTPStatus.BAD_REQUEST, "Het verzoek kon niet worden gelezen.")
            return None
        if not secrets.compare_digest(self._action_cookie(), self.server.action_token):
            self._reject(HTTPStatus.FORBIDDEN, "Dit verzoek komt niet van Monitor 2. Ververs de pagina.",
                         reason="session_invalid")
            return None
        refusal = self._browser_origin_refusal()
        if refusal is not None:
            self._reject(HTTPStatus.FORBIDDEN, "Verzoek van een andere herkomst wordt niet uitgevoerd.",
                         reason=refusal)
            return None
        if (self.headers.get("Content-Type") or "").split(";")[0].strip().lower() != "application/json":
            self._reject(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "Alleen application/json.")
            return None
        try:
            body = json.loads(raw.decode("utf-8")) if raw else None
        except (ValueError, UnicodeDecodeError):
            self._reject(HTTPStatus.BAD_REQUEST, "Onleesbaar verzoek.")
            return None
        if not isinstance(body, dict):
            self._reject(HTTPStatus.BAD_REQUEST, "Het verzoek moet een JSON-object zijn.")
            return None
        return body

    def _action_cookie(self) -> str:
        try:
            jar = SimpleCookie(self.headers.get("Cookie") or "")
        except CookieError:
            return ""
        morsel = jar.get(ACTION_COOKIE)
        return morsel.value if morsel is not None else ""

    def _browser_origin_refusal(self) -> str | None:
        """Waarom dit geen knopdruk van de eigen pagina is, of ``None``. Fail-closed.

        Een browser stuurt bij elke POST een ``Origin``; ontbreekt die, dan is het
        geen browser en gebeurt er niets. De herkomst moet precies de eigen zijn:
        ``http``, dezelfde hostnaam en poort als het ``Host``-kopje (ook via een
        SSH-doorsturing, waar beide de doorgestuurde poort noemen). Een andere dienst
        op localhost is een andere herkomst. ``Sec-Fetch-Site``, als de browser het
        stuurt, moet ``same-origin`` zijn.

        Via de Tailscale Serve-route (``self.route == "tailscale"``) is de eigen herkomst
        precies ``https://<ts.net-naam>`` (poort 443); verder dezelfde regels.
        """
        origin = (self.headers.get("Origin") or "").strip()
        if not origin:
            return "origin_required"
        if getattr(self, "route", None) == "tailscale":
            try:
                parts = urlsplit(origin)
                origin_port = parts.port or 443
            except ValueError:
                return "origin_mismatch"
            if parts.scheme != "https" or parts.path or parts.query or parts.fragment \
                    or (parts.hostname or "") != self.server.tailscale.host or origin_port != 443 \
                    or parts.username is not None or parts.password is not None:
                return "origin_mismatch"
            fetch_site = (self.headers.get("Sec-Fetch-Site") or "").strip().lower()
            if fetch_site and fetch_site != "same-origin":
                return "cross_site"
            return None
        host = (self.headers.get("Host") or "").strip().lower()
        match = _HOST_PORT.match(host) if host else None
        allowed = self.server.lan_route.hosts if getattr(self, "route", None) == "lan" else _ALLOWED_HOSTS
        if match is None or match.group(1) not in allowed:
            return "host_required"
        try:
            parts = urlsplit(origin)
            origin_port = parts.port or 80
        except ValueError:
            return "origin_mismatch"
        origin_host = (parts.netloc.rsplit(":", 1)[0] if parts.port else parts.netloc).lower()
        if parts.scheme != "http" or parts.path or parts.query or parts.fragment \
                or origin_host != match.group(1) or origin_port != int(match.group(2) or 80):
            return "origin_mismatch"
        fetch_site = (self.headers.get("Sec-Fetch-Site") or "").strip().lower()
        if fetch_site and fetch_site != "same-origin":
            return "cross_site"
        return None

    def _unsupported(self) -> None:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if 0 < length <= 1_048_576:
            self.rfile.read(length)
        self.close_connection = True
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.send_header("Allow", "POST" if urlsplit(self.path).path in POST_ROUTES else "GET, HEAD")
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        for key, value in _SECURITY_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()

    do_PUT = _unsupported
    do_PATCH = _unsupported
    do_DELETE = _unsupported
    do_OPTIONS = _unsupported

    def log_refusal(self, message: str) -> None:
        """Een geweigerde Serve-aanvraag komt altijd in het logboek, ook met ``--quiet``."""
        sys.stderr.write("%s [monitor2] %s\n" % (self.log_date_time_string(), message))

    def log_message(self, fmt: str, *args) -> None:
        if getattr(self.server, "quiet", False):
            return
        sys.stderr.write("%s [monitor2] %s\n" % (self.log_date_time_string(), fmt % args))

    def log_error(self, fmt: str, *args) -> None:
        """Fouten (send_error: een misvormd verzoek, 414) altijd, ook met ``--quiet`` (MON-08)."""
        sys.stderr.write("%s [monitor2] %s\n" % (self.log_date_time_string(), fmt % args))


def build_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, **kwargs) -> Monitor2Server:
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError(f"Monitor 2 bindt alleen op localhost, niet op {host!r}.")
    return Monitor2Server((host, port), **kwargs)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((DEFAULT_HOST, 0))
        return sock.getsockname()[1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="obf-monitor2",
                                     description="Monitor 2: de Studio-OBF-monitor op Claude Runner 2.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=int(os.environ.get("OBF_MONITOR2_PORT", DEFAULT_PORT)))
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--read-only", dest="read_only", action="store_true", default=True)
    parser.add_argument("--enable-actions", dest="read_only", action="store_false")
    parser.add_argument("--runner2-src", metavar="MAP",
                        help="de map met het Runner-2-pakket (standaard $OBF_RUNNER2_SRC of ~/StudioOBF-wt-runner2)")
    args = parser.parse_args(argv)
    if args.runner2_src:
        os.environ[r2_adapter.SOURCE_ENV] = args.runner2_src
    # Register before binding the socket: a service stop can arrive while backends initialize.
    stopping = threading.Event()
    serve_ready = threading.Event()
    serving = False
    server = None

    def stop(signum, frame):  # noqa: ARG001
        if stopping.is_set():
            return
        stopping.set()
        sys.stderr.write("[monitor2] SIGTERM: netjes stoppen\n")

        def shutdown_when_ready():
            serve_ready.wait()
            if serving and server is not None:
                server.shutdown()
        threading.Thread(target=shutdown_when_ready, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    try:
        server = build_server(args.host, args.port, quiet=args.quiet, read_only=args.read_only, tailscale=False, nuc_health=False)
    except (OSError, ValueError) as exc:
        serve_ready.set()
        sys.stderr.write(f"[monitor2] kan niet starten op {args.host}:{args.port}: {exc}\n")
        return 2
    if stopping.is_set():
        serve_ready.set()
        server.close_sources()
        server.server_close()
        return 0
    sources = ", ".join(f"{node.id}={server.backends[node.id].kind}" for node in server.nodes)
    sys.stderr.write(f"[monitor2] luistert op http://{args.host}:{server.server_address[1]} (nodes: {sources})\n")
    sys.stderr.flush()

    if server.nuc_health is not None:
        server.nuc_health.start()
    lan_stop = threading.Event()
    lan_servers: list[Monitor2LanServer] = []
    if r2_adapter.local_node_id() == "gerard":
        def observe_chains():
            while not lan_stop.wait(4.0):
                try:
                    server.poll_chain_notifications()
                except Exception as exc:
                    sys.stderr.write(f"[monitor2] achtergrondcontrole meldingen mislukt: {exc.__class__.__name__}\n")
        threading.Thread(target=observe_chains, daemon=True, name="monitor2-chain-notifications").start()
    for lan in ():
        if lan is not None:
            threading.Thread(target=_serve_lan, args=(server, lan, lan_stop, lan_servers), daemon=True, name=f"monitor2-{lan.label}").start()
    try:
        serving = True
        serve_ready.set()
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        lan_stop.set()
        for lan_server in lan_servers:
            lan_server.shutdown()
            lan_server.server_close()
        server.close_sources()
        server.server_close()
    return 0


def _serve_lan(primary: Monitor2Server, lan: lan_module.LanRoute, stop: threading.Event, started: list) -> None:
    """Het LAN- of tailnetadres openen en bedienen. Is het adres er nog niet (netwerk komt later op, of Tailscale is nog niet
    aangemeld), dan elke 10 s opnieuw; de localhost-server draait ondertussen gewoon door."""
    announced = False
    while not stop.is_set():
        route = lan.resolved()
        try:
            if route is None:
                raise OSError(f"geen adres op interface {lan.interface}")
            lan_server = Monitor2LanServer(primary, route)
        except OSError as exc:
            if not announced:
                sys.stderr.write(f"[monitor2] {lan.label}-adres {(route or lan).bind or lan.interface}:{lan.port} nog niet beschikbaar ({exc}); "
                                 "ik probeer het elke 10 s opnieuw\n")
                sys.stderr.flush()
                announced = True
            stop.wait(10.0)
            continue
        started.append(lan_server)
        how = "toegangssleutel of Tailscale-identiteit" if route.identity is not None else "toegangssleutel"
        sys.stderr.write(f"[monitor2] luistert ook op http://{route.bind}:{route.port} ({route.label}, met {how})\n")
        sys.stderr.flush()
        lan_server.serve_forever(poll_interval=0.5)
        return


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
