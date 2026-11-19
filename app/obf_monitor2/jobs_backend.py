"""De dunne laag van Monitor 2 naar de opdrachtwachtrij van Runner 2 (``runner2_jobs``).

De wachtrij zelf -- opslag, volgorde, afhankelijkheden, goedkeuring, de dispatcher die de
volgende opdracht start -- leeft in Runner 2 op de control plane. Monitor 2 heeft hier
geen eigen toestand en geen eigen logica: dit bestand roept alleen de controller-MCP-tool
``runner2_jobs`` aan en vertaalt de foutcodes naar HTTP-statussen en Nederlandse tekst.

Waar de wachtrij leeft (de *control-plane-node*):

* ``OBF_JOBS_NODE`` (MAC / GERARD / LINUX / WINDOWS) als die is gezet;
* anders de ``primary_host`` van ``runner2.cplane.role()`` als deze machine ``standby`` is
  (de control plane is dan overgedragen);
* anders deze machine zelf (lokaal, in dit proces).

Lokaal gaat de aanroep via ``runner2.controller_mcp.call_tool``, voor een andere node via
``runner2.remote.call`` (ssh, met dezelfde korte timeout als ``r2_adapter``).
"""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import os
import threading
import time
from typing import Any, Callable

from . import r2_adapter

log = logging.getLogger("obf_monitor2")

TOOL = "runner2_jobs"
REPOS_TOOL = "runner2_repos"
NODE_ENV = "OBF_JOBS_NODE"
JOB_NODES = ("MAC", "GERARD", "LINUX", "WINDOWS")

#: Zo lang een overzicht hergebruikt wordt (de pagina pollt elke 4 s, meerdere tabbladen).
CACHE_SECONDS = 2.0

#: Runner-2-foutcode -> HTTP-status. Alles wat hier niet staat is 503, zonder stacktrace.
STATUS_BY_CODE = {"invalid_input": 400, "not_found": 404, "wrong_state": 409, "limit": 409, "queue_corrupt": 503,
                  "not_control_plane": 503, "node_unreachable": 503, "runner2_unavailable": 503,
                  # repository=auto: geen fout van Runner 2 maar een vraag of voorstel; er is dan niets aangemaakt
                  "repo_choice_needed": 409, "repo_unknown": 409, "repo_split_confirm": 409}

DONE_TEXT = {
    "add": "Opdracht in de wachtrij gezet.", "move": "Opdracht verplaatst.", "set_node": "Opdracht naar een andere Runner gezet.",
    "set_deps": "Voorgangers aangepast.", "hold": "Opdracht wordt vastgehouden.", "release": "Opdracht vrijgegeven.",
    "approve": "Opdracht goedgekeurd.", "reject": "Opdracht geweigerd.", "cancel": "Annuleren aangevraagd.",
    "check": "Controle vastgelegd.", "retry": "Opdracht opnieuw in de wachtrij gezet.", "resume": "Hervatten aangevraagd.",
    "archive": "Opdracht gearchiveerd.", "pause_queue": "Wachtrij gepauzeerd.", "resume_queue": "Wachtrij hervat.",
    "plan": "Voorstel van Runner 2.", "set_prompt": "Opdrachttekst opgeslagen.",
}

#: Foutcode van ``queue_text`` -> HTTP-status (de codes van Runner 2 zelf staan in ``STATUS_BY_CODE``). Rest is 503.
TEXT_STATUS_BY_CODE = {"invalid_input": 400, "forbidden": 403, "not_found": 404, "wrong_state": 409, "rejected": 409,
                       "confirm_mismatch": 409, "not_control_plane": 503}


class JobsCallError(Exception):
    """Een fout van Runner 2 (of van het bereiken ervan) met zijn eigen code en tekst."""

    def __init__(self, code: str, message: str, data: dict | None = None):
        super().__init__(message)
        self.code, self.message, self.data = code, message, data   # data: de vraag of het voorstel bij repository=auto


def control_node(env: dict | None = None, role: Callable[[], dict | None] | None = None) -> str | None:
    """De node waar de wachtrij leeft, of ``None`` = deze machine. Zie de modulekop."""
    value = ((env if env is not None else os.environ).get(NODE_ENV) or "").strip().upper()
    if value in JOB_NODES:
        return value
    try:
        info = (role or _cplane_role)()
    except Exception:  # noqa: BLE001 -- zonder leesbare rol is deze machine de control plane
        return None
    if isinstance(info, dict) and info.get("role") == "standby":
        host = info.get("primary_host")
        if isinstance(host, str) and host.strip().upper() in JOB_NODES:
            return host.strip().upper()
    return None


def _cplane_role() -> dict | None:
    r2_adapter.load_runner2_modules()
    return importlib.import_module("runner2.cplane").role()


def _default_call(node: str | None, tool: str, args: dict) -> dict:
    """Eén aanroep van ``runner2_jobs``: lokaal in dit proces of via het node-transport."""
    if node is None:
        try:
            r2_adapter.load_runner2_modules()
            mcp = importlib.import_module("runner2.controller_mcp")
        except ImportError as exc:
            raise JobsCallError("runner2_unavailable", f"Runner 2 is op deze machine niet beschikbaar: {exc}") from None
        text, is_error = mcp.call_tool(tool, args)
        try:
            data = json.loads(text)
        except ValueError:
            raise JobsCallError("runner2_unavailable", "Runner 2 gaf een onleesbaar antwoord.") from None
        if is_error:
            data = data if isinstance(data, dict) else {}
            raise JobsCallError(str(data.get("error") or "runner2_unavailable"), str(data.get("message") or ""),
                                data.get("data") if isinstance(data.get("data"), dict) else None)
        return data
    try:
        remote = r2_adapter.load_runner2_remote()
    except ImportError as exc:
        raise JobsCallError("runner2_unavailable", f"Het Runner-2-nodetransport is niet beschikbaar: {exc}") from None
    # Alleen dit proces: één hangende ssh-aanroep mag de pagina niet een minuut vasthouden.
    remote.CALL_TIMEOUT_S = min(float(getattr(remote, "CALL_TIMEOUT_S", r2_adapter.REMOTE_CALL_TIMEOUT_S)),
                                r2_adapter.REMOTE_CALL_TIMEOUT_S)
    return remote.call(node, tool, args)


class JobsBackend:
    """``overview()`` en ``act()`` over de opdrachtwachtrij; geen eigen toestand behalve een cache van 2 s."""

    def __init__(self, *, call: Callable[[str, dict], dict] | None = None, clock: Callable[[], float] = time.monotonic,
                 cache_seconds: float = CACHE_SECONDS, env: dict | None = None,
                 role: Callable[[], dict | None] | None = None, text_editor: Callable[..., dict] | None = None):
        # ``call(tool, args)`` is voor tests; zonder dat kiest de backend zelf lokaal of remote.
        # ``text_editor`` (voor tests) vervangt ``queue_text.edit_prompt``, het wijzigen van een opdrachttekst.
        self._call = call
        self._text_editor = text_editor
        self._clock, self._cache_seconds = clock, cache_seconds
        self._env, self._role = env, role
        self._cache: tuple[float, dict] | None = None
        self._lock = threading.Lock()          # single-flight voor het overzicht
        self._action_lock = threading.Lock()   # één handeling tegelijk

    # -- wie is de control plane

    def control_node(self) -> str | None:
        return control_node(self._env, self._role)

    def control_label(self) -> str:
        """Voor de pagina: de node waar de wachtrij leeft, of ``LOKAAL``."""
        return self.control_node() or "LOKAAL"

    def invalidate(self) -> None:
        self._cache = None

    # -- aanroepen

    def _invoke(self, args: dict) -> dict:
        if self._call is not None:
            return self._call(TOOL, args)
        return _default_call(self.control_node(), TOOL, args)

    def _failure(self, exc: Exception, node: str | None = None) -> tuple[int, dict]:
        code = getattr(exc, "code", None)
        message = getattr(exc, "message", None) or ""
        if not isinstance(code, str):
            # Onverwacht: nooit een stacktrace naar de pagina, wel de soort fout.
            log.warning("runner2_jobs mislukt: %s", exc.__class__.__name__)
            return 503, {"ok": False, "reason": "runner2_unavailable",
                         "message": f"Runner 2 gaf een fout ({exc.__class__.__name__}); er is niets uitgevoerd."}
        if code == "invalid_input" and "onbekende tool" in message.lower():
            where = node or self.control_label()
            return 501, {"ok": False, "reason": "not_supported",
                         "message": f"Deze Runner 2 kent de opdrachtwachtrij nog niet (oudere versie op {where}); "
                                    "werk Runner 2 daar eerst bij."}
        status = STATUS_BY_CODE.get(code, 503)
        out = {"ok": False, "reason": code if code in STATUS_BY_CODE else "runner2_unavailable",
               "message": message or "Runner 2 gaf een fout."}
        data = getattr(exc, "data", None)
        if isinstance(data, dict) and code in ("repo_choice_needed", "repo_unknown", "repo_split_confirm"):
            out["data"] = data
        return status, out

    def repos_catalog(self) -> dict | None:
        """De gevonden repositories per node en de projectenlijst (``runner2_repos`` catalog); alleen lezen.
        ``None`` = deze Runner 2 kent de repositorykeuze nog niet of is niet bereikbaar (dan geen "Automatisch bepalen")."""
        try:
            data = self._call(REPOS_TOOL, {"action": "catalog"}) if self._call is not None \
                else _default_call(self.control_node(), REPOS_TOOL, {"action": "catalog"})
        except Exception as exc:  # noqa: BLE001
            log.info("runner2_repos catalog niet beschikbaar: %s", exc.__class__.__name__)
            return None
        return data if isinstance(data, dict) and isinstance(data.get("nodes"), dict) else None

    def notification_snapshot(self) -> dict | None:
        """Fresh primary read, all obligations (not the UI's 25 finished jobs).

        Reuses Runner's own proof predicate. No reconcile, transaction, remote
        verifier or model call; an unsupported/remote primary fails closed.
        """
        if self._call is not None or self.control_node() is not None:
            return None
        try:
            r2_adapter.load_runner2_modules()
            jobs = importlib.import_module("runner2.jobs")
            proof = importlib.import_module("runner2.integration_check")
            config = importlib.import_module("runner2.config")
            cfg = config.load_config()
            data = jobs.load()
            nodes = {node: {"paused": data["paused"]["nodes"].get(node), "jobs": []} for node in ("GERARD", "LINUX")}
            # Match the dispatcher's distinction: durable workflow obligations
            # block the whole node; legacy reviews only matter to successors
            # that actually depend on them. Old reviewed history is not a new
            # obligation merely because today's config enables proof checking.
            dependencies = {dep for job in data["jobs"].values() if job.get("status") in ("queued", "starting", "running")
                            for dep in (job.get("depends_on") or [])}
            for job in data["jobs"].values():
                obligation = job.get("integration_obligation") or {}
                bound = obligation.get("node", job.get("node"))
                targets = {n for n in (bound, job.get("node")) if n in nodes}
                if not targets:
                    continue
                required = bool(obligation) or bool(job.get("integration_workflow") and job.get("started_at")) or bool(
                    job.get("id") in dependencies and job.get("started_at") and proof.required(job, cfg))
                satisfied = proof.satisfied(job)
                if job.get("archived") and not (required and not satisfied):
                    continue
                view = {key: job.get(key) for key in ("id", "task_id", "node", "title", "run", "status", "started_at",
                        "ended_at", "check", "last_error", "held", "outcome", "integration_obligation")}
                state, why = jobs.label(data, job)
                for target in targets:
                    # A move cannot hide the destination queue, nor transfer
                    # proof bound to the original node into destination success.
                    nodes[target]["jobs"].append({**view, "requires_integration": required and target == bound,
                        "integration_satisfied": satisfied and target == bound, "state": state, "why": why})
            return {"version": data["version"], "paused_all": data["paused"].get("all"), "nodes": nodes}
        except Exception:
            log.warning("volledige meldingsstatus niet te lezen", exc_info=True)
            return None

    def overview(self) -> tuple[int, dict]:
        """Het hele overzicht (``action=list``). Een geslaagde lezing wordt 2 s hergebruikt, een fout nooit."""
        with self._lock:
            now = self._clock()
            if self._cache is not None and 0 <= now - self._cache[0] < self._cache_seconds:
                return 200, self._cache[1]
            try:
                data = self._invoke({"action": "list"})
            except Exception as exc:  # noqa: BLE001
                self._cache = None
                return self._failure(exc)
            if not isinstance(data, dict):
                return 503, {"ok": False, "reason": "runner2_unavailable", "message": "Runner 2 gaf een onleesbaar antwoord."}
            self._cache = (now, data)
            return 200, data

    def job(self, job_id: str) -> tuple[int, dict]:
        """Eén opdracht met volledige prompt en log (voor het detailvenster). Niet gecachet."""
        try:
            data = self._invoke({"action": "list", "job_id": job_id})
        except Exception as exc:  # noqa: BLE001
            return self._failure(exc)
        if not isinstance(data, dict):
            return 503, {"ok": False, "reason": "runner2_unavailable", "message": "Runner 2 gaf een onleesbaar antwoord."}
        job = data.get("job")
        if isinstance(job, dict) and isinstance(job.get("prompt"), str):
            # De pagina stuurt dit terug bij een tekstwijziging, zodat een intussen gewijzigde tekst niet stil verdwijnt.
            data = {**data, "job": {**job, "prompt_sha256": hashlib.sha256(job["prompt"].encode("utf-8")).hexdigest()}}
        return 200, data

    def _edit_text(self) -> Callable[..., dict] | None:
        if self._text_editor is not None:
            return self._text_editor
        if self._call is not None:
            return None   # een nagebootste wachtrij (tests) heeft geen eigen opslag om te wijzigen
        from . import queue_text   # laat: haalt Runner 2 binnen, en alleen nodig bij een tekstwijziging
        return queue_text.edit_prompt

    def set_prompt(self, fields: dict, by: str) -> tuple[int, dict]:
        """Wijzigt de opdrachttekst van één wachtende opdracht (alleen op de machine waar de wachtrij leeft).

        De wijziging gaat door Runner 2's eigen transactie (``queue_text``): zelfde lock, back-up, audit en behoudcontrole
        als de andere handelingen; actieve of afgeronde opdrachten worden geweigerd, volgorde en voorgangers blijven staan."""
        node = self.control_node()
        editor = self._edit_text()
        if node is not None or editor is None:
            where = f" (de wachtrij leeft op {node})" if node else ""
            return 501, {"ok": False, "reason": "not_supported",
                         "message": "De tekst van een opdracht kan alleen gewijzigd worden op de machine waar de wachtrij "
                                    f"zelf leeft{where}; open Monitor 2 daar."}
        with self._action_lock:
            try:
                data = editor(fields.get("job_id"), fields.get("prompt"), fields.get("old_sha256"), by=by)
            except Exception as exc:  # noqa: BLE001
                self.invalidate()
                return self._text_failure(exc)
            self.invalidate()
        message = DONE_TEXT["set_prompt"]
        if data.get("approval_reset"):
            message += " De goedkeuring gold voor de oude tekst en moet opnieuw gegeven worden."
        return 200, {"ok": True, "action": "set_prompt", "job_id": data.get("job_id"), "approval_reset": bool(data.get("approval_reset")),
                     "message": message}

    def _text_failure(self, exc: Exception) -> tuple[int, dict]:
        code = getattr(exc, "code", None)
        message = getattr(exc, "message", None) or ""
        if not isinstance(code, str):
            log.warning("tekstwijziging mislukt: %s", exc.__class__.__name__)
            return 503, {"ok": False, "reason": "runner2_unavailable",
                         "message": f"De tekst kon niet worden opgeslagen ({exc.__class__.__name__}); controleer de opdracht."}
        errors = (getattr(exc, "data", None) or {}).get("errors")
        if code == "rejected" and isinstance(errors, list) and errors:
            message = "; ".join(str(item) for item in errors[:3])
        status = TEXT_STATUS_BY_CODE.get(code) or STATUS_BY_CODE.get(code, 503)
        reason = code if code in TEXT_STATUS_BY_CODE or code in STATUS_BY_CODE else "runner2_unavailable"
        return status, {"ok": False, "reason": reason, "message": message or "De tekst kon niet worden opgeslagen."}

    def act(self, action: str, fields: dict, by: str) -> tuple[int, dict]:
        """Eén handeling. De cache wordt altijd ongeldig, ook na een fout (de stand kan toch veranderd zijn)."""
        if action == "set_prompt":
            return self.set_prompt(fields, by)
        args = {**fields, "action": action, "by": by}
        with self._action_lock:
            try:
                data = self._invoke(args)
            except Exception as exc:  # noqa: BLE001
                self.invalidate()
                return self._failure(exc)
            self.invalidate()
        data = data if isinstance(data, dict) else {}
        count = len(data.get("jobs") or [])
        if data.get("duplicate"):
            message = "Deze opdracht stond al in de wachtrij (dubbel verzonden); er is niets toegevoegd."
        elif action == "add" and data.get("split") and count > 1:
            message = f"{count} opdrachten in de wachtrij gezet, één per repository."
        else:
            message = DONE_TEXT.get(action, "Doorgegeven.")
        return 200, {"ok": True, "action": action, **data, "message": message}
