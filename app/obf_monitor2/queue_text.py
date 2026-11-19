"""Veilig de tekst (``prompt`` en ``supervisor_opts.project_goal``) van vastgehouden Runner 2-opdrachten wijzigen.

De wachtrij-API van Runner 2 (``runner2_jobs``) kent geen handeling om de tekst van een bestaande opdracht te wijzigen
(rapport ``docs/rapporten/2026-10-16-wachtrijteksten``). Deze module doet dat beheerwerk wel, maar alleen langs de
eigen transactie van Runner 2 (``runner2.jobs._txn``) en met dezelfde zekerheden als de andere handelingen:

* **Atomair**: alle wijzigingen van één plan staan in één transactie en één ``atomic_write`` van ``queue.json``; óf
  alles, óf niets. Runner 2 bewaart de vorige generatie in ``queue.prev.json``.
* **Locking**: dezelfde bestandslock als de dispatcher en ``runner2_jobs`` (``jobs/.lock``) en het overdrachtshek
  (``HANDED_OVER.json``); de dispatcher kan een opdracht dus nooit half gewijzigd zien of starten.
* **Concurrency**: elk plan noemt per opdracht de sha256 van de huidige prompt; is de prompt intussen anders, dan
  wordt het hele plan geweigerd. De bevestigingscode (``plan_sha``) komt uit de preview, dus een verouderde of
  andere preview past nooit.
* **Alleen wachtende opdrachten**: ``status == queued``; actief (starting/running), afgerond, geannuleerd of met een
  openstaande annulering wordt geweigerd. De batch-CLI eist daarbovenop ``held`` (``require_held``); de bewerking vanuit
  Monitor 2 (``edit_prompt``) staat ook een wachtende, niet-vastgehouden opdracht toe. Dat is race-vrij omdat de
  dispatcher een opdracht onder dezelfde lock op ``starting`` zet: onder de lock is ``queued`` dus nog niet geclaimd.
* **Behoud**: id, status, held, volgorde, voorgangers, request_ids, historie (het log krijgt er één regel bij) en
  alle overige velden blijven gelijk; dat wordt vóór het opslaan per veld gecontroleerd. Alleen een al gegeven
  goedkeuring vervalt (zoals bij ``set_deps``/``set_node``): die gold voor de oude tekst.
* **Vooraf een back-up** (fsync, nooit overschreven) van de oorspronkelijke records én een kopie van de hele
  ``queue.json``, geschreven onder de lock vóór het opslaan; mislukt die, dan wordt er niets gewijzigd.
* **Audit**: een regel ``set_prompt`` in Runner 2's ``jobs/audit.jsonl`` en een logregel per opdracht.
* **Herstel**: ``restore_plan`` maakt van een back-up een omgekeerd plan dat langs precies dezelfde route gaat.
* **Geautoriseerd**: alleen een naam uit Runner 2's ``jobs_approvers`` (de ingestelde goedkeurders), alleen op de
  primaire control plane, en alleen met de bevestigingscode van de preview.

Gebruik: ``scripts/obf_queue_text.py`` (preview, apply, restore-preview, restore, verify).
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any

from . import r2_adapter

ACTION = "set_prompt"
BACKUP_DIRNAME = "tekst-backups"
BACKUP_KIND = "runner2_queue_text_backup"
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
#: Velden die een plan mag veranderen. ``log`` krijgt één regel, ``approval`` alleen van goedgekeurd naar open.
TEXT_FIELDS = ("prompt", "supervisor_opts.project_goal")


class TextEditError(Exception):
    def __init__(self, code: str, message: str, data: dict | None = None):
        super().__init__(message)
        self.code, self.message, self.data = code, message, data or {}


def sha(text: str | None) -> str | None:
    return None if text is None else hashlib.sha256(text.encode("utf-8")).hexdigest()


def runner2_jobs() -> Any:
    """``runner2.jobs`` (en daarmee ``config``/``store``), uit een importeerbare Runner 2 of de bronmap."""
    r2_adapter.load_runner2_modules()
    return importlib.import_module("runner2.jobs")


# --- plannen ---

def normalize(raw: Any) -> list[dict]:
    """Een plan: ``[{job_id, old_sha256, prompt, project_goal[, old_project_goal_sha256]}]``. Accepteert ook
    ``voorstel.json`` (``jobs`` met ``oud_sha256``). ``project_goal`` ``None`` = de opdracht heeft geen project_goal
    (en krijgt er ook geen)."""
    items = raw.get("jobs") if isinstance(raw, dict) else raw
    if not isinstance(items, list) or not items:
        raise TextEditError("invalid_input", "het plan bevat geen opdrachten")
    out, seen = [], set()
    for i, it in enumerate(items):
        if not isinstance(it, dict):
            raise TextEditError("invalid_input", f"plan[{i}] is geen object")
        jid = it.get("job_id")
        old = it.get("old_sha256", it.get("oud_sha256"))
        prompt, goal = it.get("prompt"), it.get("project_goal")
        if not isinstance(jid, str) or not re.fullmatch(r"J-[0-9a-f]{12}", jid):
            raise TextEditError("invalid_input", f"plan[{i}]: job_id ontbreekt of is ongeldig")
        if jid in seen:
            raise TextEditError("invalid_input", f"{jid} staat twee keer in het plan")
        if not isinstance(old, str) or not SHA_RE.match(old):
            raise TextEditError("invalid_input", f"{jid}: old_sha256 (sha256 van de huidige prompt) ontbreekt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise TextEditError("invalid_input", f"{jid}: nieuwe prompt ontbreekt")
        if goal is not None and (not isinstance(goal, str) or not goal.strip()):
            raise TextEditError("invalid_input", f"{jid}: project_goal moet tekst of null zijn")
        old_goal = it.get("old_project_goal_sha256")
        if old_goal is not None and (not isinstance(old_goal, str) or not SHA_RE.match(old_goal)):
            raise TextEditError("invalid_input", f"{jid}: old_project_goal_sha256 is geen sha256")
        seen.add(jid)
        out.append({"job_id": jid, "old_sha256": old, "prompt": prompt, "project_goal": goal,
                    "old_project_goal_sha256": old_goal})
    return out


def plan_sha(edits: list[dict]) -> str:
    """De bevestigingscode: hangt af van de opdrachten, hun huidige tekst en de nieuwe tekst."""
    canon = [[e["job_id"], e["old_sha256"], sha(e["prompt"]), sha(e["project_goal"]), e.get("old_project_goal_sha256")]
             for e in edits]
    return hashlib.sha256(json.dumps(canon, separators=(",", ":")).encode()).hexdigest()


def _goal(job: dict) -> str | None:
    return (job.get("supervisor_opts") or {}).get("project_goal")


def validate(d: dict, edits: list[dict], jobs: Any, *, node: str | None = None,
             expect_count: int | None = None, require_held: bool = True) -> list[str]:
    """Alle redenen om het plan tegen deze wachtrij te weigeren (leeg = toepasbaar)."""
    errs = []
    if expect_count is not None and len(edits) != expect_count:
        errs.append(f"het plan bevat {len(edits)} opdrachten, verwacht {expect_count}")
    for e in edits:
        jid = e["job_id"]
        job = d["jobs"].get(jid)
        if job is None:
            errs.append(f"{jid}: bestaat niet")
            continue
        if node is not None and job["node"] != node:
            errs.append(f"{jid}: staat op {job['node']}, niet op {node}")
        if job["status"] != jobs.QUEUED:
            errs.append(f"{jid}: status {job['status']}; alleen een wachtende opdracht kan een andere tekst krijgen")
        elif require_held and not job.get("held"):
            errs.append(f"{jid}: niet vastgehouden; houd de opdracht eerst vast (een vrijgegeven opdracht kan elk moment starten)")
        if job.get("cancel_requested"):
            errs.append(f"{jid}: er is een annulering aangevraagd")
        if jid not in d["order"].get(job["node"], []):
            errs.append(f"{jid}: staat niet in de volgorde van {job['node']}")
        if sha(job["prompt"]) != e["old_sha256"]:
            errs.append(f"{jid}: de prompt is gewijzigd sinds het plan is gemaakt (sha256 klopt niet)")
        if len(e["prompt"]) > jobs.MAX_PROMPT or len(e["project_goal"] or "") > jobs.MAX_PROMPT:
            errs.append(f"{jid}: nieuwe tekst langer dan {jobs.MAX_PROMPT} tekens")
        cur_goal = _goal(job)
        if e.get("old_project_goal_sha256") and sha(cur_goal) != e["old_project_goal_sha256"]:
            errs.append(f"{jid}: project_goal is gewijzigd sinds het plan is gemaakt (sha256 klopt niet)")
        if (cur_goal is None) != (e["project_goal"] is None):
            errs.append(f"{jid}: project_goal " + ("ontbreekt in het plan" if e["project_goal"] is None
                                                   else "hoort niet bij deze opdracht (heeft er nu geen)"))
        if e["prompt"] == job["prompt"] and e["project_goal"] == cur_goal:
            errs.append(f"{jid}: het plan verandert niets")
    return errs


def _flat(d: Any, prefix: str = "") -> dict:
    if not isinstance(d, dict) or not d:
        return {prefix.rstrip("."): d}
    out = {}
    for k, v in d.items():
        out.update(_flat(v, f"{prefix}{k}."))
    return out


def changed_fields(before: dict, after: dict) -> list[str]:
    fb, fa = _flat(before), _flat(after)
    return sorted(k for k in set(fb) | set(fa) if fb.get(k, ...) != fa.get(k, ...))


def _kept_log(log: list, k: int, max_log: int) -> list:
    """Wat er van ``log`` overblijft na ``k`` keer ``runner2.jobs._log``."""
    cur = list(log)
    for _ in range(k):
        cur = cur[-(max_log - 1):] + [None]
    return cur[:-k]


def check_only_text_changed(before: dict, after: dict, ids: list[str], max_log: int = 80) -> list[str]:
    """Behoudcontrole over de hele wachtrij: buiten de opdrachten in het plan niets, daarbinnen alleen de tekst,
    één extra logregel en (alleen goedgekeurd -> open) de goedkeuring."""
    errs = []
    for key in set(before) | set(after):
        if key in ("jobs", "version", "updated"):
            continue
        if before.get(key) != after.get(key):
            errs.append(f"wachtrijveld {key} veranderd")
    if set(before["jobs"]) != set(after["jobs"]):
        errs.append("er zijn opdrachten bijgekomen of verdwenen")
    for jid in set(before["jobs"]) & set(after["jobs"]):
        b, a = before["jobs"][jid], after["jobs"][jid]
        if jid not in ids:
            if a != b:
                errs.append(f"{jid}: veranderd maar staat niet in het plan")
            continue
        blog, alog = b.get("log") or [], a.get("log") or []
        # het log krijgt er één of twee regels bij (tekst gewijzigd, eventueel "goedkeuring vervalt"); de rest blijft,
        # alleen ingekort zoals runner2.jobs._log dat doet (maximaal max_log regels)
        if not any(len(alog) == len(_kept_log(blog, k, max_log)) + k and alog[:-k] == _kept_log(blog, k, max_log)
                   for k in (1, 2)):
            errs.append(f"{jid}: historie (log) niet behouden")
        rest_b = {k: v for k, v in b.items() if k not in ("log", "approval")}
        rest_a = {k: v for k, v in a.items() if k not in ("log", "approval")}
        extra = [f for f in changed_fields(rest_b, rest_a) if f not in TEXT_FIELDS]
        if extra:
            errs.append(f"{jid}: verandert ook {extra}")
        ab, aa = b.get("approval"), a.get("approval")
        if ab != aa and not ((ab or {}).get("state") == "approved" and (aa or {}).get("state") == "open"):
            errs.append(f"{jid}: goedkeuring onverwacht veranderd")
    return errs


# --- autorisatie ---

def authorize(by: Any, jobs: Any) -> str:
    """Alleen een naam uit ``jobs_approvers`` van Runner 2 en alleen op de primaire control plane."""
    try:
        name = jobs._name(by)
    except jobs.JobsError as exc:
        raise TextEditError("invalid_input", exc.message) from None
    approvers = jobs._approvers(jobs.config.load_config())
    if name.lower() not in [a.lower() for a in approvers]:
        raise TextEditError("forbidden", f"alleen {', '.join(approvers)} kunnen de tekst van een opdracht wijzigen")
    try:
        role = importlib.import_module("runner2.cplane").role()
    except Exception:  # noqa: BLE001 -- zonder rolinformatie: de wachtrij hieronder is de lokale (oude opstelling)
        role = None
    if isinstance(role, dict) and role.get("role") == "standby":
        raise TextEditError("not_control_plane", f"deze machine is standby; wijzig de wachtrij op {role.get('primary_host')}")
    return name


# --- preview, toepassen, controleren, herstellen ---

def _summary(d: dict, edits: list[dict]) -> list[dict]:
    rows = []
    for e in edits:
        job = d["jobs"].get(e["job_id"]) or {}
        rows.append({"job_id": e["job_id"], "title": job.get("title"), "node": job.get("node"),
                     "status": job.get("status"), "held": job.get("held"), "old_sha256": e["old_sha256"],
                     "new_sha256": sha(e["prompt"]), "old_len": len(job.get("prompt") or ""), "new_len": len(e["prompt"]),
                     "project_goal": "wijzigt" if e["project_goal"] is not None and e["project_goal"] != _goal(job)
                     else ("ongewijzigd" if e["project_goal"] is not None else "geen")})
    return rows


def preview(raw: Any, *, node: str | None = None, expect_count: int | None = None, jobs: Any = None,
            require_held: bool = True) -> dict:
    """Dry-run: alleen lezen. Geeft wat er zou veranderen, de fouten en de bevestigingscode voor ``apply``."""
    jobs = jobs or runner2_jobs()
    edits = normalize(raw)
    try:
        d = jobs.load()
    except jobs.JobsError as exc:
        raise TextEditError(exc.code, exc.message) from None
    errs = validate(d, edits, jobs, node=node, expect_count=expect_count, require_held=require_held)
    return {"ok": not errs, "dry_run": True, "plan_sha": plan_sha(edits), "queue_version": d.get("version"),
            "count": len(edits), "project_goals": sum(1 for e in edits if e["project_goal"] is not None),
            "errors": errs, "jobs": _summary(d, edits), "handed_over": bool(jobs.handed_over())}


def default_backup_dir(jobs: Any) -> Path:
    return jobs.jobs_dir() / BACKUP_DIRNAME


def _write_exclusive(path: Path, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())


def write_backup(backup_dir: Path, d: dict, queue_bytes: bytes, edits: list[dict], *, by: str, psha: str) -> Path:
    """Back-up vóór het opslaan: de oorspronkelijke records en een kopie van de hele ``queue.json``. Nooit overschreven."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    base = backup_dir / f"tekst-{stamp}-{psha[:12]}"
    queue_copy = base.with_name(base.name + ".queue.json")
    _write_exclusive(queue_copy, queue_bytes.decode("utf-8"))
    data = {"kind": BACKUP_KIND, "made_at": stamp, "by": by, "plan_sha": psha, "queue_version": d.get("version"),
            "queue_sha256": hashlib.sha256(queue_bytes).hexdigest(), "queue_copy": queue_copy.name,
            "jobs": [{"job_id": e["job_id"], "record": copy.deepcopy(d["jobs"][e["job_id"]]),
                      "new_prompt_sha256": sha(e["prompt"]), "new_project_goal_sha256": sha(e["project_goal"])}
                     for e in edits]}
    path = base.with_name(base.name + ".json")
    _write_exclusive(path, json.dumps(data, ensure_ascii=False, indent=1) + "\n")
    try:   # de map zelf ook naar schijf, zodat de back-up een crash overleeft
        dfd = os.open(backup_dir, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass
    return path


def _on_disk(jobs: Any, edits: list[dict]) -> str:
    """Na een fout: wat staat er echt? ``applied`` (alle nieuwe teksten), ``unchanged`` (alle oude) of ``mixed``."""
    try:
        d = jobs.load()
    except Exception:  # noqa: BLE001
        return "unreadable"
    new = [sha((d["jobs"].get(e["job_id"]) or {}).get("prompt")) == sha(e["prompt"]) for e in edits]
    old = [sha((d["jobs"].get(e["job_id"]) or {}).get("prompt")) == e["old_sha256"] for e in edits]
    return "applied" if all(new) else "unchanged" if all(old) else "mixed"


def apply(raw: Any, *, by: Any, confirm: str, node: str | None = None, expect_count: int | None = None,
          backup_dir: Path | None = None, jobs: Any = None, note: str = "", require_held: bool = True) -> dict:
    """Past een plan atomair toe. Weigert alles (en wijzigt niets) zodra één opdracht niet klopt."""
    jobs = jobs or runner2_jobs()
    name = authorize(by, jobs)
    edits = normalize(raw)
    psha = plan_sha(edits)
    if confirm != psha:
        raise TextEditError("confirm_mismatch", "de bevestigingscode hoort niet bij dit plan; draai eerst de preview "
                            "en geef de code die daar staat", {"plan_sha": psha})
    backup_dir = Path(backup_dir) if backup_dir else default_backup_dir(jobs)
    ids = [e["job_id"] for e in edits]
    q_path = jobs.jobs_dir() / "queue.json"
    box_ref: dict = {}
    approval_reset: list[str] = []
    try:
        with jobs._txn() as box:
            d = box["d"]
            errs = validate(d, edits, jobs, node=node, expect_count=expect_count, require_held=require_held)
            if errs:
                raise TextEditError("rejected", f"{len(errs)} reden(en) om te weigeren; er is niets gewijzigd", {"errors": errs})
            before = copy.deepcopy(d)
            backup = write_backup(backup_dir, d, q_path.read_bytes() if q_path.exists() else b"{}", edits, by=name, psha=psha)
            box_ref["backup"] = backup
            for e in edits:
                job = d["jobs"][e["job_id"]]
                old = sha(job["prompt"])
                goal_changed = e["project_goal"] is not None and e["project_goal"] != _goal(job)
                job["prompt"] = e["prompt"]
                if e["project_goal"] is not None:
                    job["supervisor_opts"]["project_goal"] = e["project_goal"]
                appr = job.get("approval") or {}
                if appr.get("required") and appr.get("state") == "approved":
                    approval_reset.append(e["job_id"])
                jobs._reset_approval(job, name, "tekst gewijzigd")
                jobs._log(job, name, f"tekst gewijzigd door {name} (prompt {old[:12]} -> {sha(e['prompt'])[:12]}"
                          + (", project_goal mee" if goal_changed else "")
                          + f"; back-up {backup.name}" + (f"; {note}" if note else "") + ")")
            errs = check_only_text_changed(before, d, ids, jobs.MAX_JOB_LOG)
            if errs:
                raise TextEditError("invariant", "behoudcontrole faalt; er is niets gewijzigd", {"errors": errs})
            box["dirty"] = True
            jobs._audit(ACTION, f"tekst van {len(ids)} opdracht(en) gewijzigd door {name}", job_ids=ids, by=name,
                        plan_sha=psha, backup=str(backup),
                        changes=[{"job_id": e["job_id"], "old": e["old_sha256"], "new": sha(e["prompt"]),
                                  "project_goal": e["project_goal"] is not None} for e in edits])
    except TextEditError:
        raise
    except jobs.JobsError as exc:
        raise TextEditError(exc.code, exc.message, {"on_disk": _on_disk(jobs, edits)}) from None
    except Exception as exc:  # noqa: BLE001 -- fout tijdens back-up, opslaan of audit: zeg eerlijk wat er nu staat
        state = _on_disk(jobs, edits)
        msg = ("toegepast, maar daarna ging iets mis (auditregel?)" if state == "applied"
               else "niets gewijzigd" if state == "unchanged" else f"wachtrij {state}; controleer en herstel")
        raise TextEditError("apply_failed", f"{exc.__class__.__name__}: {exc}; {msg}",
                            {"on_disk": state, "backup": str(box_ref.get("backup") or "")}) from None
    result = verify(edits, jobs=jobs, require_held=require_held)
    if not result["ok"]:
        # Na het opslaan iets anders op schijf dan bedoeld: terug naar de back-up, langs dezelfde veilige route.
        restored = None
        try:
            rplan = restore_plan(backup)
            restored = apply(rplan, by=name, confirm=plan_sha(normalize(rplan)), backup_dir=backup_dir, jobs=jobs,
                             note="automatisch herstel na mislukte controle", require_held=require_held)["backup"]
        except Exception as exc:  # noqa: BLE001
            restored = f"herstel mislukt: {getattr(exc, 'message', exc)}"
        raise TextEditError("verify_failed", "controle na het opslaan faalt; teruggezet uit de back-up",
                            {"errors": result["errors"], "backup": str(backup), "restore": str(restored)})
    return {"ok": True, "plan_sha": psha, "changed": len(ids), "project_goals": sum(1 for e in edits if e["project_goal"] is not None),
            "job_ids": ids, "backup": str(backup), "by": name, "approval_reset": approval_reset}


def verify(raw: Any, *, jobs: Any = None, require_held: bool = True) -> dict:
    """Staan de nieuwe teksten in de wachtrij, en zijn de opdrachten nog wachtend (en vastgehouden)?"""
    jobs = jobs or runner2_jobs()
    edits = normalize(raw)
    d = jobs.load()
    errs = []
    for e in edits:
        job = d["jobs"].get(e["job_id"])
        if job is None:
            errs.append(f"{e['job_id']}: bestaat niet")
            continue
        if job["prompt"] != e["prompt"]:
            errs.append(f"{e['job_id']}: prompt is niet de nieuwe tekst")
        if e["project_goal"] is not None and _goal(job) != e["project_goal"]:
            errs.append(f"{e['job_id']}: project_goal is niet de nieuwe tekst")
        # Zonder ``require_held`` kan de dispatcher de opdracht direct na het opslaan (met de nieuwe tekst) gestart hebben;
        # dat is geen fout en mag niet tot een herstel leiden. Alleen de vastgehouden batch moet nog wachtend zijn.
        if require_held and (job["status"] != jobs.QUEUED or not job.get("held")):
            errs.append(f"{e['job_id']}: niet meer wachtend en vastgehouden ({job['status']}, held={job.get('held')})")
    return {"ok": not errs, "errors": errs, "count": len(edits)}


def load_backup(path: Path) -> dict:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("kind") != BACKUP_KIND or not isinstance(data.get("jobs"), list):
        raise TextEditError("invalid_input", f"{path} is geen back-up van een tekstwijziging")
    return data


def restore_plan(path: Path) -> dict:
    """Het omgekeerde plan uit een back-up: van de nieuwe tekst terug naar de oorspronkelijke records."""
    data = load_backup(path)
    jobs_out = []
    for it in data["jobs"]:
        rec = it["record"]
        jobs_out.append({"job_id": it["job_id"], "old_sha256": it["new_prompt_sha256"], "prompt": rec["prompt"],
                         "project_goal": (rec.get("supervisor_opts") or {}).get("project_goal"),
                         "old_project_goal_sha256": it.get("new_project_goal_sha256")})
    return {"jobs": jobs_out, "from_backup": str(path)}


# --- één opdracht, vanuit Monitor 2 ---

def edit_prompt(job_id: Any, prompt: Any, old_sha256: Any, *, by: Any, jobs: Any = None,
                backup_dir: Path | None = None) -> dict:
    """De opdrachttekst (``prompt``) van één wachtende opdracht wijzigen, langs dezelfde veilige route als ``apply``.

    Alleen ``status == queued`` (vastgehouden of niet); ``old_sha256`` is de sha256 van de prompt zoals de gebruiker die
    zag, zodat een intussen gewijzigde tekst niet stil wordt overschreven. Volgorde, voorgangers, vasthouden, node,
    project_goal en historie blijven staan (behoudcontrole); een al gegeven goedkeuring vervalt (``approval_reset``)."""
    jobs = jobs or runner2_jobs()
    if not isinstance(prompt, str):
        raise TextEditError("invalid_input", "de nieuwe tekst ontbreekt")
    try:
        d = jobs.load()
    except jobs.JobsError as exc:
        raise TextEditError(exc.code, exc.message) from None
    job = d["jobs"].get(job_id) if isinstance(job_id, str) else None
    if job is None:
        raise TextEditError("not_found", f"opdracht {job_id} bestaat niet")
    goal = _goal(job)
    plan = [{"job_id": job_id, "old_sha256": old_sha256, "prompt": prompt, "project_goal": goal,
             "old_project_goal_sha256": sha(goal) if goal is not None else None}]
    edits = normalize(plan)
    result = apply(plan, by=by, confirm=plan_sha(edits), backup_dir=backup_dir, jobs=jobs, require_held=False,
                   note="via Monitor 2")
    return {**result, "job_id": job_id, "new_sha256": sha(prompt)}
