"""Bewijsgerichte afsluiting van open actiepunten (besluit 43 van Beheerder, 23 oktober 2026).

Een open actiepunt wordt administratief gesloten ALS EN ALLEEN ALS aantoonbaar is dat het besloten, uitgevoerd, gecontroleerd en
(waar relevant) geïntegreerd is. Die vier stappen zijn aparte dimensies in `item["lifecycle"]` en worden nooit uit elkaar afgeleid:

* `besluit`     genomen | niet_nodig | ontbreekt   - `genomen`: een besluit uit het beslissingenregister noemt dit actiepunt
                (relatie `genoemd-in-besluit`, zekerheid `bewezen`); `niet_nodig`: een technisch vervolgpunt (door de claim zo benoemd,
                geen vraag, geen beschermde categorie). Anders `ontbreekt`: een mens beslist en het punt blijft open.
* `uitvoering`  wacht | bezig | gedaan | mislukt   - uit de status van de gekoppelde opdracht. Een wachtende of lopende opdracht is
                NOOIT uitgevoerd; ook een besluit niet.
* `controle`    wacht | ok | mislukt               - `jobs/check` van de opdracht (integrator, mens of ChatGPT); `afgekeurd` = mislukt.
* `integratie`  n.v.t. | wacht | ok               - waar de opdracht een integratieketen heeft (`integration_workflow` of node in
                `jobs_integration_check_nodes`): het bewijs van de standalone integrator (`integration_check.satisfied`: ontvangstbewijs,
                grant, resultaat, tests, auditketen en Git). Een ontvangstbewijs alleen is geen bewijs.

De koppeling tussen punt en opdracht is een expliciete claim (`claim`), nooit afgeleid uit tekst. Een claim bewijst niets: het bewijs
komt uit wachtrij, integrator en register en wordt bij elke ronde opnieuw getoetst. Geen claim = het punt wordt niet aangeraakt.

Veiligheid:
* alleen open punten met een claim; nooit supervisie (dossiers/judge), nooit een opdracht die ketens of een supervisor draait;
* de opdracht moet na het punt zijn aangemaakt en mag niet de taak zijn die het punt zelf meldde;
* sluiten gebeurt onder de statelock van het punt, na een tweede toetsing met de dan actuele wachtrij; een punt dat intussen door
  iemand anders is gesloten of gewijzigd wordt niet aangeraakt (geen dubbele sluiting); de ronde draait onder een niet-blokkerende lock;
* stroomonderbreker (`action_closure_max` per ronde), hash-geketend auditspoor met vooraf (`close_intent`) en achteraf (`closed`);
* niets wordt verwijderd: het punt behoudt tekst, reden, historie en claim; `resolved_*`, `auto_resolution` en `history` leggen vast
  wat, wanneer en op grond waarvan; `undo` zet een punt terug naar open en trekt de claim in (anders sluit de volgende ronde het opnieuw).
"""
import hashlib
import json
import os
import re
import time
from contextlib import contextmanager, ExitStack, nullcontext
from pathlib import Path

from . import actionreview, actions, config, integration_check, jobs, store
from .filelock import lock_exclusive

VERSION = 1
BY = "actieafsluiting"
AID = re.compile(r"^A-[0-9a-f]{10}$")
CHECK_OK_SOURCES = ("integrator", "mens", "chatgpt")
SOORTEN = ("technisch",)
SUMMARY_ITEMS = 20
GENESIS = "0" * 64
_QUESTION = re.compile(r"(?i)\?|^\s*(?:mag|mogen|moet|moeten|wil|wilt|kun|kunt|kan|zal|zullen|welke?|wanneer|hoe|op welk)\b")

VERDICTS = ("close", "wait", "doubt")


class ClosureError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code, self.message = code, message


def home():
    return config.r2_home() / "actionclosure"


_clip = actions._clip


def _sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


# --- auditspoor: append-only, hash-geketend ---

def _audit(rec: dict) -> None:
    path = home() / "audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    prev = GENESIS
    if path.exists():
        lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        if lines:
            prev = hashlib.sha256(lines[-1].encode()).hexdigest()
    rec = {"at": store.now(), "node": config.local_node(), **rec, "prev": prev}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
        f.flush()
        os.fsync(f.fileno())


def read_audit() -> list:
    path = home() / "audit.jsonl"
    out = []
    for line in path.read_text(encoding="utf-8").splitlines() if path.exists() else []:
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def verify_audit() -> list:
    """Problemen in de auditketen (leeg = intact)."""
    path = home() / "audit.jsonl"
    problems, prev = [], GENESIS
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines() if path.exists() else [], 1):
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            problems.append(f"regel {n}: geen geldige JSON")
            break
        if rec.get("prev") != prev:
            problems.append(f"regel {n}: keten verbroken")
            break
        prev = hashlib.sha256(line.encode()).hexdigest()
    return problems


@contextmanager
def _run_lock():
    path = home() / "run.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        try:
            lock_exclusive(f, blocking=False)
        except OSError:
            raise ClosureError("busy", "er loopt al een afsluitronde op deze node") from None
        yield


def _recover_audit():
    """Reconcile interrupted intent/state/audit writes under the run lock.

    An intent is never treated as success: durable action state must agree with
    the exact claim or the closer's stored digest and identity.
    """
    problems = verify_audit()
    if problems:
        raise ClosureError("audit_invalid", "; ".join(problems))
    records = read_audit()
    claims = {(r.get("action_id"), (r.get("claim") or {}).get("id")) for r in records if r.get("kind") == "claim"}
    closed = {(r.get("action_id"), r.get("claim"), r.get("digest")) for r in records if r.get("kind") == "closed"}
    approved = {r.get("evidence_id") for r in records if r.get("kind") == "evidence_approved"}
    for r in records:
        if r.get("kind") == "evidence_approval_intent" and r.get("evidence_id") not in approved:
            from . import actionevidence
            try:
                packet = actionevidence.load(r["evidence_id"])
                if (packet.get("action_id") == r.get("action_id") and packet.get("review") == r.get("review")
                        and packet["review"].get("candidate_sha256") == r.get("candidate_sha256")):
                    _audit({"kind": "evidence_approved", "action_id": r["action_id"], "evidence_id": r["evidence_id"], "recovered": True})
                    approved.add(r["evidence_id"])
            except (ValueError, OSError, KeyError):
                pass
        if r.get("kind") not in ("claim_intent", "close_intent"):
            continue
        try:
            st = store.load(r["task_id"])
            it = next(i for i in st.get("human_actions") or [] if i.get("id") == r["action_id"])
        except (OSError, ValueError, KeyError, StopIteration):
            continue
        if r["kind"] == "claim_intent":
            cl = r["claim"]; key = (r["action_id"], cl["id"])
            if key not in claims and it.get("closure_claim") == cl:
                _audit({"kind": "claim", "action_id": r["action_id"], "task_id": r["task_id"], "claim": cl, "recovered": True})
                claims.add(key)
        else:
            auto = it.get("auto_resolution") or {}; key = (r["action_id"], r["claim"], r["digest"])
            if (key not in closed and it.get("status") == actions.RESOLVED and it.get("resolved_by") == BY
                    and auto.get("kind") == "evidence" and auto.get("closure_id") == r["claim"] and auto.get("digest") == r["digest"]):
                _audit({"kind": "closed", "action_id": r["action_id"], "task_id": r["task_id"], "claim": r["claim"],
                        "job_id": r.get("job_id"), "digest": r["digest"], "recovered": True})
                closed.add(key)


# --- register (besluit 49): alleen lezen ---

def load_register(directory=None) -> dict | None:
    """Het beslissingenregister, of None als het niet beschikbaar is (dan blijft `besluit` onbewezen en sluit er niets)."""
    from . import beslissingen
    try:
        target = directory or config.load_config().get("action_register_directory") or beslissingen.REGISTER_DIR
        register, audit = beslissingen.load(target)
        if beslissingen.validate(register) or beslissingen.check_audit(register, audit):
            return None
        return register
    except Exception:   # noqa: BLE001 -- ontbrekend of onleesbaar register mag nooit tot sluiten leiden
        return None


def named_in_decision(register: dict | None, aid: str, nr) -> dict | None:
    """Bewijs dat besluit `nr` dit actiepunt noemt (relatie genoemd-in-besluit, zekerheid bewezen), of None."""
    if not isinstance(register, dict) or isinstance(nr, bool) or not isinstance(nr, int):
        return None
    for b in register.get("besluiten") or []:
        if b.get("nr") == nr:
            for a in b.get("acties") or []:
                if a.get("id") == aid and a.get("relatie") == "genoemd-in-besluit" and a.get("zekerheid") == "bewezen":
                    return {"besluit": nr, "relatie": a["relatie"], "zekerheid": a["zekerheid"], "bron": a.get("bron")}
    return None


# --- claim: de expliciete koppeling tussen een actiepunt en de opdracht die het uitvoert ---

def _norm_besluit(value):
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 9999:
        raise ClosureError("invalid_input", "besluit moet een heel getal zijn (nummer in het beslissingenregister)")
    return value


def claim(action_id: str, job_id: str | None, by: str, besluit=None, soort: str | None = None, note: str = "", evidence_id=None) -> dict:
    with _run_lock():
        return _claim(action_id, job_id, by, besluit, soort, note, evidence_id)


def _claim(action_id: str, job_id: str | None, by: str, besluit=None, soort: str | None = None, note: str = "", evidence_id=None,
           preserve_existing=False) -> dict:
    """Koppelt een open actiepunt aan de opdracht die het moet uitvoeren. Bewijst niets; sluit niets. Idempotent."""
    if not isinstance(action_id, str) or not AID.fullmatch(action_id):
        raise ClosureError("invalid_input", "action_id moet de vorm A-xxxxxxxxxx hebben")
    if job_id is not None and (not isinstance(job_id, str) or not jobs.JOB_RE.fullmatch(job_id)):
        raise ClosureError("invalid_input", "job_id moet de vorm J-xxxxxxxxxxxx hebben")
    besluit = _norm_besluit(besluit)
    if soort not in (None, *SOORTEN):
        raise ClosureError("invalid_input", f"soort moet leeg of een van {SOORTEN} zijn")
    if besluit is None and soort is None:
        raise ClosureError("invalid_input", "geef een besluit (nummer in het register) of soort=technisch op: zonder een van beide "
                                            "is er nooit grond om het punt te sluiten")
    if job_id is not None and job_id not in jobs.load()["jobs"]:
        raise ClosureError("unknown_job", f"opdracht {job_id} staat niet in de wachtrij van deze node")
    for st0 in store.all_tasks():
        if not any(isinstance(i, dict) and i.get("id") == action_id for i in st0.get("human_actions") or []):
            continue
        with store._flock(st0["task_id"]):
            st = store.load(st0["task_id"])
            it = next(i for i in st["human_actions"] if isinstance(i, dict) and i.get("id") == action_id)
            if it.get("status") != actions.OPEN:
                raise ClosureError("not_open", f"actiepunt {action_id} is niet meer open")
            old = it.get("closure_claim")
            if preserve_existing and old:
                return {"action_id": action_id, "claim": old, "changed": False}
            if evidence_id:
                from . import actionevidence
                queue = jobs.load()
                verified = actionevidence.verify(evidence_id, it, st, queue["jobs"].get(job_id), config.load_config(), load_register())
                if not verified["ok"]:
                    raise ClosureError("invalid_evidence", "; ".join(verified["reasons"]))
                if verified["packet"].get("job_id") != job_id:
                    raise ClosureError("invalid_evidence", "evidence job differs from claim")
            elif job_id is None:
                raise ClosureError("invalid_input", "een claim zonder opdracht vraagt beoordeeld duplicaatbewijs")
            new = {"job_id": job_id, "besluit": besluit, "soort": soort, "by": _clip(by, 80), "note": _clip(note, 300) or None}
            if evidence_id:
                new["evidence_id"] = evidence_id
            new["id"] = "CL-" + _sha([action_id, job_id, besluit, soort, evidence_id])[:10]
            if old and not old.get("revoked") and {k: old.get(k) for k in ("job_id", "besluit", "soort", "evidence_id")} == \
                    {"job_id": job_id, "besluit": besluit, "soort": soort, "evidence_id": evidence_id}:
                return {"action_id": action_id, "claim": old, "changed": False}
            new["at"] = store.now()
            h = {"at": new["at"], "event": "closure_claimed", "claim": new["id"], "job_id": job_id, "by": new["by"]}
            if old:
                h.update(replaces=old.get("id"), was={k: old.get(k) for k in ("job_id", "besluit", "soort", "revoked")})
            it["closure_claim"] = new
            it.setdefault("history", []).append(h)
            _audit({"kind": "claim_intent", "action_id": action_id, "task_id": st["task_id"], "claim": new})
            store.save(st)
            _audit({"kind": "claim", "action_id": action_id, "task_id": st["task_id"], "claim": new})
            return {"action_id": action_id, "claim": new, "changed": True}
    raise ClosureError("unknown_action", f"actiepunt {action_id} staat niet op deze node")


# --- toetsing: een zuivere functie van state, wachtrij, register en config ---

def integration_relevant(job: dict, cfg: dict) -> bool:
    return bool(job.get("integration_workflow")) or job.get("node") in (cfg.get("jobs_integration_check_nodes") or [])


def lifecycle_complete(lc: dict | None) -> bool:
    """Alle vier dimensies sluitend (besluit, uitvoering, controle, integratie). Enige toegestane sluitgrond naast een expliciete mensbeslissing."""
    try:
        return (lc["besluit"]["status"] in ("genomen", "niet_nodig") and lc["uitvoering"]["status"] in ("gedaan", "n.v.t.")
                and lc["controle"]["status"] == "ok" and lc["integratie"]["status"] in ("ok", "n.v.t."))
    except (KeyError, TypeError):
        return False


def _dim(status: str, **kw) -> dict:
    return {"status": status, **{k: v for k, v in kw.items() if v not in (None, [], "")}}


def assess(item: dict, st: dict, job: dict | None, cfg: dict, register: dict | None, guard: dict | None = None) -> dict:
    """Beoordeelt één open punt met claim. Geeft {lifecycle, verdict, reasons, protected, evidence}. Wijzigt niets."""
    cl = item.get("closure_claim") or {}
    aid, reasons, hard = item["id"], [], []
    prot = actionreview.protected(item, st, guard)
    lc = {"besluit": _dim("ontbreekt"), "uitvoering": _dim("wacht"), "controle": _dim("wacht"), "integratie": _dim("wacht"),
          "job": cl.get("job_id"), "claim": cl.get("id")}

    from . import actionevidence
    verified = actionevidence.verify(cl.get("evidence_id"), item, st, job, cfg, register)
    if not verified["ok"]:
        hard.extend("volledige-scopebewijs: " + r for r in verified["reasons"])
    packet = verified.get("packet") or {}
    if verified["ok"] and packet.get("kind") == "duplicate":
        survivor = packet["survivor"]
        lc.update(besluit=_dim("niet_nodig", reden="administratief duplicaat; oorspronkelijke vraag blijft open"),
                  uitvoering=_dim("n.v.t.", reden="geen uitvoering geclaimd"),
                  controle=_dim("ok", bewijs=packet["review"]), integratie=_dim("n.v.t."), survivor=survivor)
        return {"lifecycle": lc, "verdict": "close", "reasons": [], "protected": prot,
                "digest": _sha([actionreview.fingerprint(item), cl, verified["digest"]])}

    # 1. besluit
    proof = named_in_decision(register, aid, cl.get("besluit")) if cl.get("besluit") is not None else None
    if verified["ok"] and (packet.get("decision") or {}).get("kind") == "primary":
        lc["besluit"] = _dim("genomen", bewijs={"besluit": cl.get("besluit"), "evidence_id": cl.get("evidence_id"),
                                               "primaire_bronnen": packet["decision"]["sources"]})
    elif proof:
        lc["besluit"] = _dim("genomen", bewijs=proof)
    elif cl.get("soort") == "technisch" and not prot and not _QUESTION.search(item.get("text") or ""):
        lc["besluit"] = _dim("niet_nodig", reden="technisch vervolgpunt: geen vraag en geen beschermde categorie")
    else:
        why = ("besluit niet (als 'genoemd-in-besluit') in het register gevonden" if cl.get("besluit") is not None else
               "geen besluit opgegeven" if cl.get("soort") != "technisch" else
               "een vraag of beschermd punt vraagt een besluit van een mens")
        lc["besluit"] = _dim("ontbreekt", reden=why)
        reasons.append("besluit ontbreekt: " + why)

    # 2. geschiktheid van de opdracht
    if job is None:
        hard.append("opdracht niet gevonden in de wachtrij")
    else:
        if job.get("continuation") or job.get("supervisor"):
            hard.append("opdracht is een keten of supervisor: dat sluit nooit automatisch")
        if (job.get("created_at") or "") < (item.get("created_at") or ""):
            hard.append("opdracht is ouder dan het punt: kan er niet de uitvoering van zijn")
        if (item.get("source") or {}).get("task_id") in jobs_task_ids(job) | {job.get("task_id")}:
            hard.append("dezelfde taak meldde dit punt: een taak sluit haar eigen vraag niet")
    if prot:
        hard.append("beschermde categorie: sluit nooit automatisch (" + ", ".join(prot) + ")")

    # 3. uitvoering, controle, integratie (alleen uit de wachtrij)
    relevant = False
    if job is not None:
        status = job.get("status")
        text = (job.get("outcome") or {}).get("text")
        if status == jobs.DONE and (job.get("outcome") or {}).get("kind") in (None, "done"):
            lc["uitvoering"] = _dim("gedaan", bron=f"opdracht {job['id']} klaar (taak {job.get('task_id')}, run {job.get('run')})")
        elif status in jobs.ACTIVE_STATES:
            lc["uitvoering"] = _dim("bezig", bron=f"opdracht {job['id']} loopt")
            reasons.append("opdracht loopt nog")
        elif status == jobs.QUEUED:
            lc["uitvoering"] = _dim("wacht", bron=f"opdracht {job['id']} staat in de wachtrij; niets uitgevoerd")
            reasons.append("opdracht staat nog in de wachtrij: niet uitgevoerd")
        else:
            lc["uitvoering"] = _dim("mislukt", bron=f"opdracht {job['id']}: {status}" + (f" ({text})" if text else ""))
            hard.append(f"opdracht is {status}, niet afgerond")
        chk = job.get("check") or {}
        relevant = integration_relevant(job, cfg)
        if lc["uitvoering"]["status"] == "gedaan":
            if chk.get("state") == "afgekeurd":
                lc["controle"] = _dim("mislukt", bron=f"afgekeurd door {chk.get('by')} ({chk.get('source')})", notitie=chk.get("note"))
                hard.append("de controle van de opdracht is afgekeurd")
            elif chk.get("state") == "ok" and chk.get("source") in CHECK_OK_SOURCES:
                lc["controle"] = _dim("ok", bron=f"gecontroleerd door {chk.get('by')} ({chk.get('source')}) op {chk.get('at')}")
            else:
                reasons.append("opdracht is klaar maar nog niet gecontroleerd (jobs/check)")
        else:
            lc["controle"] = _dim("wacht", bron="uitvoering is nog niet gedaan")
        if not relevant:
            lc["integratie"] = _dim("n.v.t.", reden="opdracht heeft geen integratieketen")
        elif integration_check.satisfied(job):
            ev = chk["evidence"]
            lc["integratie"] = _dim("ok", bewijs={k: ev.get(k) for k in ("request", "grant", "commit", "audit_hash")},
                                   bron="bewijs van de standalone integrator (ontvangstbewijs, grant, tests, audit, Git)")
        else:
            lc["integratie"] = _dim("wacht", reden="integratiebewijs van de integrator ontbreekt of is niet sluitend")
            if lc["uitvoering"]["status"] == "gedaan":
                reasons.append("geïntegreerd is nog niet bewezen door de integrator")

    ok = (lc["besluit"]["status"] in ("genomen", "niet_nodig") and lc["uitvoering"]["status"] == "gedaan"
          and lc["controle"]["status"] == "ok" and lc["integratie"]["status"] in ("ok", "n.v.t.") and not hard)
    if ok:
        verdict = "close"
    elif hard or lc["besluit"]["status"] == "ontbreekt":
        verdict = "doubt"        # een mens moet kijken / beslissen; blijft open
    else:
        verdict = "wait"         # niets tegenstrijdigs; het bewijs is er nog niet
    evidence = _sha([actionreview.fingerprint(item), cl.get("id"), job and [job.get("id"), job.get("run"), job.get("task_id"), job.get("status"), job.get("check")],
                     lc["besluit"], proof, verified.get("digest")])
    return {"lifecycle": lc, "verdict": verdict, "reasons": hard + reasons, "protected": prot, "digest": evidence}


def jobs_task_ids(job: dict) -> set:
    return {jobs.task_id_for(job["id"], r) for r in range(1, int(job.get("run") or 1) + 1)}


def evaluate(states: list, queue: dict, cfg: dict, register: dict | None, guard: dict | None = None, node: str | None = None) -> dict:
    """Het rapport voor één node: elk open punt met een (niet-ingetrokken) claim, met zijn vier dimensies en uitslag."""
    out = {"version": VERSION, "node": node or config.local_node(), "generated_at": store.now(), "entries": [], "counts": {}}
    for st in states:
        if not isinstance(st, dict) or not st.get("task_id"):
            continue
        for it in st.get("human_actions") or []:
            cl = it.get("closure_claim") if isinstance(it, dict) else None
            if not isinstance(cl, dict) or cl.get("revoked") or not it.get("id") or it.get("status") != actions.OPEN:
                continue
            a = assess(it, st, queue["jobs"].get(cl.get("job_id")), cfg, register, guard)
            out["entries"].append({"id": it["id"], "task_id": st["task_id"], "text": actions._clip(it.get("text"), 160),
                                   "job_id": cl.get("job_id"), **a})
    out["entries"].sort(key=lambda e: (e["task_id"], e["id"]))
    out["counts"] = {v: sum(1 for e in out["entries"] if e["verdict"] == v) for v in VERDICTS}
    out["counts"]["claimed"] = len(out["entries"])
    return out


def summary(rep: dict) -> dict:
    c = rep["counts"]
    return {"node": rep["node"], "generated_at": rep["generated_at"], "counts": c,
            "headline": f"{rep['node']}: {c['claimed']} gekoppeld · te sluiten {c['close']} · wacht op bewijs {c['wait']} · vraagt een mens {c['doubt']}",
            "entries": [{"id": e["id"], "job_id": e["job_id"], "verdict": e["verdict"], "reasons": e["reasons"],
                         "besluit": e["lifecycle"]["besluit"]["status"], "uitvoering": e["lifecycle"]["uitvoering"]["status"],
                         "controle": e["lifecycle"]["controle"]["status"], "integratie": e["lifecycle"]["integratie"]["status"]}
                        for e in rep["entries"][:SUMMARY_ITEMS]]}


def render(rep: dict) -> str:
    lines = ["Bewijsgerichte afsluiting (niets wordt hier gesloten; dit is het rapport)", "", summary(rep)["headline"]]
    for e in rep["entries"]:
        lc = e["lifecycle"]
        lines.append(f"  - {e['id']} [{e['verdict']}] via {e['job_id']}: besluit {lc['besluit']['status']} · uitvoering "
                     f"{lc['uitvoering']['status']} · controle {lc['controle']['status']} · integratie {lc['integratie']['status']}")
        for r in e["reasons"]:
            lines.append(f"        · {r}")
    return "\n".join(lines) + "\n"


# --- uitvoeren ---

def _lc_core(lc: dict) -> dict:
    return {k: v for k, v in lc.items() if k != "bijgewerkt"}


def _sync_lifecycle(e: dict) -> bool:
    """Legt de vier dimensies vast op het punt zelf als ze veranderd zijn (met een historieregel). True = geschreven."""
    with store._flock(e["task_id"]):
        st = store.load(e["task_id"])
        it = next((i for i in st.get("human_actions") or [] if isinstance(i, dict) and i.get("id") == e["id"]), None)
        if not it or it.get("status") != actions.OPEN or (it.get("closure_claim") or {}).get("id") != e["lifecycle"]["claim"]:
            return False
        if _lc_core(it.get("lifecycle") or {}) == _lc_core(e["lifecycle"]):
            return False
        lc = dict(e["lifecycle"], bijgewerkt=store.now())
        it["lifecycle"] = lc
        it.setdefault("history", []).append({"at": lc["bijgewerkt"], "event": "lifecycle", "job_id": lc["job"],
                                             **{k: lc[k]["status"] for k in ("besluit", "uitvoering", "controle", "integratie")}})
        store.save(st)
        return True


def _note(lc: dict, job: dict) -> str:
    if lc.get("survivor"):
        return "Administratief duplicaat: " + lc["survivor"]["action_id"] + " blijft open met de volledige vraag. Geen uitvoering of productbesluit geclaimd. Bewijs en historie bewaard."
    b, i = lc["besluit"], lc["integratie"]
    parts = [f"besluit {b['status']}" + (f" (register {b['bewijs']['besluit']})" if b.get("bewijs") else ""),
             f"uitgevoerd door opdracht {job['id']} (taak {job.get('task_id')})", f"gecontroleerd ({lc['controle'].get('bron')})",
             f"integratie {i['status']}" + (f" {i['bewijs']['request']} commit {i['bewijs']['commit']}" if i.get("bewijs") else "")]
    return "Bewijsgericht afgesloten: " + "; ".join(parts) + ". Historie en claim blijven bewaard."


def _close_one(e: dict, cfg: dict, register, guard, by: str, refresh_register=False) -> str | None:
    """Sluit één punt onder de statelock na een tweede toetsing. None = gesloten, anders de reden van overslaan."""
    with ExitStack() as locks:
        task_ids = {e["task_id"]}
        survivor = e["lifecycle"].get("survivor")
        if survivor:
            task_ids.add(survivor["task_id"])
        for task_id in sorted(task_ids):
            locks.enter_context(store._flock(task_id))
        try:
            st = store.load(e["task_id"])
            queue = jobs.load()
        except (OSError, ValueError, jobs.JobsError):
            return "taak of wachtrij niet leesbaar"
        it = next((i for i in st.get("human_actions") or [] if isinstance(i, dict) and i.get("id") == e["id"]), None)
        if not it or it.get("status") != actions.OPEN:
            return "al gesloten of verdwenen"
        cl = it.get("closure_claim") or {}
        if cl.get("id") != e["lifecycle"]["claim"] or cl.get("revoked"):
            return "claim veranderd of ingetrokken"
        job = queue["jobs"].get(cl.get("job_id"))
        if refresh_register:
            register = load_register()
        guard = actionreview.guard_from_dossiers()
        a = assess(it, st, job, cfg, register, guard)
        if a["verdict"] != "close" or a["digest"] != e["digest"]:
            return "bewijs veranderd sinds de toetsing"
        at = store.now()
        note = _note(a["lifecycle"], job)
        _audit({"kind": "close_intent", "action_id": e["id"], "task_id": e["task_id"], "claim": cl["id"], "job_id": (job or {}).get("id"),
                "lifecycle": a["lifecycle"], "digest": a["digest"],
                "before": {k: it.get(k) for k in ("status", "resolved_at", "resolved_by", "resolved_note")}})
        auto = {"kind": "evidence", "closure_id": cl["id"], "job_id": (job or {}).get("id"), "lifecycle": a["lifecycle"], "digest": a["digest"],
                "at": at, "by": by}
        it.update(status=actions.RESOLVED, resolved_at=at, resolved_by=BY, resolved_note=actions._clip(note, actions.NOTE_CHARS),
                  auto_resolution=auto, lifecycle=dict(a["lifecycle"], bijgewerkt=at))
        it.setdefault("history", []).append({"at": at, "event": "evidence_closed", "closure_id": cl["id"], "job_id": (job or {}).get("id"), "by": by})
        store.save(st)
        _audit({"kind": "closed", "action_id": e["id"], "task_id": e["task_id"], "claim": cl["id"], "job_id": (job or {}).get("id"), "digest": a["digest"]})
    return None


def run(cfg: dict | None = None, by: str = BY, apply: bool | None = None, register=None) -> dict:
    """Eén afsluitronde op deze node. `apply=False` (of `action_closure_apply = false`) = alleen rapport, niets wordt geschreven."""
    cfg = cfg or config.load_config()
    apply = bool(cfg.get("action_closure_apply", True)) if apply is None else apply
    if not (jobs.jobs_dir() / "queue.json").exists():
        return {"mode": "apply" if apply else "report", "report": evaluate([], jobs._empty(), cfg, None), "closed": [], "skipped": [],
                "synced": [], "breaker": None}
    fresh_register = register is None
    max_close = int(cfg.get("action_closure_max", 10))
    with ExitStack() as locks:
        locks.enter_context(_run_lock() if apply else nullcontext())
        if apply:
            _recover_audit()
        if apply and fresh_register:
            # Register mutations use this same lock; deferred state cannot change
            # between the fresh assessment and the durable close.
            from . import beslissingen
            directory = config.load_config().get("action_register_directory") or beslissingen.REGISTER_DIR
            try:
                handle = locks.enter_context(open(Path(directory) / ".lock", "a+"))
                lock_exclusive(handle)
            except OSError:
                register = None
            else:
                register = load_register()
        elif fresh_register:
            register = load_register()
        queue, guard = jobs.load(), actionreview.guard_from_dossiers()
        rep = evaluate(store.all_tasks(), queue, cfg, register, guard)
        res = {"mode": "apply" if apply else "report", "report": rep, "closed": [], "skipped": [], "synced": [], "breaker": None}
        if not apply:
            return res
        closing = [e for e in rep["entries"] if e["verdict"] == "close"]
        if len(closing) > max_close:
            res["breaker"] = f"{len(closing)} punten zouden sluiten; meer dan {max_close} per ronde. Er wordt niets gesloten tot een mens dit heeft bekeken."
            _audit({"kind": "breaker", "ids": [e["id"] for e in closing], "max": max_close})
        for e in rep["entries"]:
            if e["verdict"] == "close" and not res["breaker"]:
                why = _close_one(e, cfg, register, guard, by, refresh_register=fresh_register)
                (res["skipped"].append({"id": e["id"], "why": why}) if why else res["closed"].append(e["id"]))
            elif _sync_lifecycle(e):
                res["synced"].append(e["id"])
        if res["closed"] or res["skipped"]:
            _audit({"kind": "run", "closed": res["closed"], "skipped": res["skipped"]})
    return res


def undo(action_ids: list, by: str, reason: str) -> dict:
    """Zet door bewijs gesloten punten terug naar open en trekt de claim in (idempotent). Een punt dat sindsdien door iemand anders
    is gesloten of gewijzigd blijft zoals het is. De historie blijft bewaard; er wordt niets verwijderd."""
    if not reason or not str(reason).strip():
        raise ClosureError("invalid_input", "geef een reden voor het terugzetten")
    want = {i for i in action_ids or [] if isinstance(i, str)}
    result = {"reopened": [], "untouched": [], "unknown": []}
    with _run_lock():
        found = set()
        for st0 in store.all_tasks():
            if not any(isinstance(i, dict) and i.get("id") in want for i in st0.get("human_actions") or []):
                continue
            with store._flock(st0["task_id"]):
                st = store.load(st0["task_id"])
                changed = False
                for it in st.get("human_actions") or []:
                    if not isinstance(it, dict) or it.get("id") not in want:
                        continue
                    found.add(it["id"])
                    auto = it.get("auto_resolution") or {}
                    if it.get("status") != actions.RESOLVED or it.get("resolved_by") != BY or auto.get("kind") != "evidence":
                        result["untouched"].append(it["id"])
                        continue
                    at = store.now()
                    it.pop("auto_resolution")
                    for k in ("resolved_at", "resolved_by", "resolved_note"):
                        it.pop(k, None)
                    it["status"] = actions.OPEN
                    if isinstance(it.get("closure_claim"), dict):
                        it["closure_claim"]["revoked"] = {"by": _clip(by, 80), "at": at, "reason": _clip(reason, 300)}
                    it.setdefault("history", []).append({"at": at, "event": "evidence_reopened", "by": _clip(by, 80),
                                                         "reason": _clip(reason, 300), "was": auto})
                    result["reopened"].append(it["id"])
                    changed = True
                if changed:
                    store.save(st)
        result["unknown"] = sorted(want - found)
        _audit({"kind": "undo", "by": by, "reason": _clip(reason, 300), **result})
    return result


# --- watch-haak ---

def due(cfg: dict, now: float | None = None) -> bool:
    secs = float(cfg.get("action_closure_interval_s", 120))
    if secs <= 0:
        return False
    try:
        last = (home() / "last-run").stat().st_mtime
    except OSError:
        return True
    return (now or time.time()) - last >= secs


def tick(cfg: dict) -> dict | None:
    """Watch-haak: hoogstens elke `action_closure_interval_s` (0 = uit) een ronde, alleen op de primaire control plane."""
    from . import cplane
    if not due(cfg) or not cplane.is_primary():
        return None
    home().mkdir(parents=True, exist_ok=True)
    (home() / "last-run").write_text(store.now())
    try:
        res = run(cfg)
    except ClosureError:
        return None
    return res if (res["closed"] or res["skipped"] or res["synced"] or res["breaker"]) else None
