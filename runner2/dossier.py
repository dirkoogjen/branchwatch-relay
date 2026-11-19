"""Supervisiedossier: het duurzame geheugen van de externe (ChatGPT-)supervisor, per project op schijf.

Een nieuwe ChatGPT-sessie moet uitsluitend vanuit dit dossier + de Runner-2-status kunnen doorwerken. Daarom:
- één atomair snapshot (dossier.json) plus de vorige generatie (dossier.prev.json);
- een append-only, hash-geketend auditlog (audit.jsonl) naast het snapshot, via een outbox zodat een crash tussen
  snapshot en log altijd te herstellen is;
- schema-versie + migraties, integriteitscontrole en een expliciete reparatieroute;
- geen secrets: alle vrije tekst gaat door `redact` voordat hij op schijf komt.
Dit module bevat geen beleid; het beslissen en starten staat in `supervision`.
"""
import copy
import hashlib
import json
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path

from . import config, store
from .filelock import lock_exclusive, replace

SCHEMA_VERSION = 1
GENESIS = "0" * 64
LIVE = ("planned", "starting", "started")
REDACTED = "[GEREDIGEERD]"

# version -> functie die een dossier van die versie naar versie+1 brengt (zie migrate)
MIGRATIONS: dict = {}


class DossierError(Exception):
    """Dossier onleesbaar/onbruikbaar; `code` is een stabiele foutcode, `message` is hersteladvies in gewoon Nederlands."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code, self.message = code, message


# --- redactie: geen secrets in dossier of log ---

_SECRET_PATTERNS = [re.compile(p, re.S) for p in (
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)",
    r"\beyJ[\w-]{8,}\.[\w-]{8,}\.[\w-]{8,}",
    r"\bsk-[A-Za-z0-9_\-]{12,}",
    r"\bgh[pousr]_[A-Za-z0-9]{20,}",
    r"\bgithub_pat_[A-Za-z0-9_]{20,}",
    r"\bxox[abprs]-[A-Za-z0-9\-]{10,}",
    r"\bAKIA[0-9A-Z]{16}\b",
    r"(?i)\bBearer\s+[A-Za-z0-9._~+/\-]{16,}=*",
    r"(?i)\b(?:password|passwd|wachtwoord|secret|api[_-]?key|access[_-]?token|auth[_-]?token|token)\s*[=:]\s*\S+",
)]


def redact(text):
    if not isinstance(text, str):
        return text
    for p in _SECRET_PATTERNS:
        text = p.sub(REDACTED, text)
    return text


def redact_deep(obj):
    if isinstance(obj, str):
        return redact(obj)
    if isinstance(obj, list):
        return [redact_deep(i) for i in obj]
    if isinstance(obj, dict):
        return {k: redact_deep(v) for k, v in obj.items()}
    return obj


def has_secret(obj) -> bool:
    return redact_deep(obj) != obj


# --- paden, lock ---

def project_dir(pid: str) -> Path:
    if not isinstance(pid, str) or not re.fullmatch(r"P-[0-9a-f]{12}", pid):
        raise DossierError("invalid_project_id", f"project_id {pid!r} is ongeldig (verwacht P- plus 12 hexcijfers)")
    return config.supervision_dir() / pid


def dossier_path(pid: str) -> Path:
    return project_dir(pid) / "dossier.json"


def prev_path(pid: str) -> Path:
    return project_dir(pid) / "dossier.prev.json"


def audit_path(pid: str) -> Path:
    return project_dir(pid) / "audit.jsonl"


def project_ids() -> list:
    root = config.supervision_dir()
    return sorted(p.name for p in root.glob("P-*") if p.is_dir() and re.fullmatch(r"P-[0-9a-f]{12}", p.name)) if root.exists() else []


@contextmanager
def lock(pid: str):
    d = project_dir(pid)
    d.mkdir(parents=True, exist_ok=True)
    with open(d / ".lock", "w") as f:
        lock_exclusive(f)
        yield


def now() -> str:
    return store.now()


def _canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def sha(obj) -> str:
    return hashlib.sha256((obj if isinstance(obj, str) else _canon(obj)).encode()).hexdigest()


# --- nieuw dossier ---

def blank(project_id: str, title: str, goal: str, targets: list, limits: dict, master_list: dict | None = None,
          backlog: list | None = None, knowledge: dict | None = None, deadline: str | None = None) -> dict:
    t = now()
    goal = redact(goal)
    return {
        "schema_version": SCHEMA_VERSION, "project_id": project_id, "title": redact(title), "created_at": t, "updated_at": t,
        "revision": 0,
        "goal": {"version": 1, "text": goal, "sha256": sha(goal), "amendments": []},
        "master_list": redact_deep(master_list or {"refs": [], "text": ""}),
        "targets": targets, "limits": limits, "deadline": deadline,
        "control": {"state": "active", "reason": "", "at": t, "by": "register"},
        "integrity": {"status": "ok", "code": "", "message": "", "at": t},
        "knowledge": redact_deep(knowledge or {"architecture": [], "beheerder_decisions": []}),
        "status_items": [], "backlog": redact_deep(backlog or []), "blockers": [],
        "runner_actions": {}, "assignments": [], "decisions": [], "events": [],
        "counters": {"event_seq": 0, "no_progress": 0, "assignments_total": 0, "blocker_seq": 0},
        "checkpoint": {"revision": 0, "at": t, "event_id": None, "decision_n": 0, "assignment_id": None, "audit_seq": 0,
                       "audit_head": GENESIS},
        "audit": {"seq": 0, "head": GENESIS, "outbox": []},
    }


# --- validatie ---

_REQUIRED = ("schema_version", "project_id", "goal", "targets", "limits", "control", "integrity", "backlog", "blockers",
             "assignments", "decisions", "events", "counters", "checkpoint", "audit", "revision", "knowledge")


def _dups(items, key="id") -> list:
    seen, out = set(), []
    for i in items:
        v = i.get(key)
        if v in seen:
            out.append(v)
        seen.add(v)
    return out


def validate(d, pid: str) -> list:
    """Geeft een lijst problemen ({code, message}); leeg = in orde. Pure functie, raakt geen schijf aan."""
    def bad(code, message):
        return [{"code": code, "message": message}]
    if not isinstance(d, dict):
        return bad("dossier_invalid", "dossier.json bevat geen object")
    missing = [k for k in _REQUIRED if k not in d]
    if missing:
        return bad("dossier_invalid", f"velden ontbreken: {', '.join(missing)}")
    v = d["schema_version"]
    if not isinstance(v, int) or isinstance(v, bool):
        return bad("dossier_invalid", "schema_version is geen geheel getal")
    if v > SCHEMA_VERSION:
        return bad("schema_unsupported", f"dossier heeft schema {v}, deze Runner 2 kent hooguit {SCHEMA_VERSION}; update Runner 2")
    if v < SCHEMA_VERSION and v not in MIGRATIONS:
        return bad("schema_unsupported", f"geen migratie vanaf schema {v}")
    out = []
    if d["project_id"] != pid:
        out += bad("project_mismatch", f"dossier hoort bij {d['project_id']}, niet bij map {pid}")
    try:
        if d["goal"]["sha256"] != sha(d["goal"]["text"]):
            out += bad("goal_tampered", "het projectdoel past niet bij zijn controlegetal; het doel is buiten de procedure gewijzigd")
        for name in ("events", "assignments", "decisions", "backlog", "blockers"):
            key = {"assignments": "assignment_id", "decisions": "n"}.get(name, "id")
            if dups := _dups(d[name], key):
                out += bad("duplicate_ids", f"dubbele {name}-id's: {dups[:3]}")
        live = [a for a in d["assignments"] if a.get("status") in LIVE]
        if len(live) > 1:
            out += bad("multiple_live_assignments", f"{len(live)} gelijktijdig lopende opdrachten; maximaal één per project")
        if d["counters"]["assignments_total"] != len(d["assignments"]):
            out += bad("counters_inconsistent", "teller assignments_total klopt niet met de opdrachtenlijst")
        ids = {a["assignment_id"] for a in d["assignments"]}
        for e in d["events"]:
            if e.get("assignment_id") and e["assignment_id"] not in ids:
                out += bad("event_orphan", f"event {e['id']} verwijst naar onbekende opdracht {e['assignment_id']}")
        if d["checkpoint"]["revision"] != d["revision"]:
            out += bad("checkpoint_inconsistent", "laatste checkpoint hoort niet bij deze revisie")
        if d["checkpoint"]["audit_seq"] != d["audit"]["seq"]:
            out += bad("checkpoint_inconsistent", "checkpoint wijst naar een andere auditpositie dan het dossier")
    except (KeyError, TypeError, AttributeError) as e:
        return bad("dossier_invalid", f"structuur onjuist ({type(e).__name__}: {e})")
    return out


# --- auditlog ---

def _record_hash(rec: dict) -> str:
    return sha({k: v for k, v in rec.items() if k != "hash"})


def make_record(seq: int, prev: str, kind: str, summary: str, data: dict) -> dict:
    rec = {"seq": seq, "at": now(), "kind": kind, "summary": redact(summary)[:400], "data": redact_deep(data), "prev": prev}
    rec["hash"] = _record_hash(rec)
    return rec


def read_audit(pid: str) -> tuple:
    """(records, problem) - problem is None of {code, message}. Controleert de keten binnen het bestand."""
    p = audit_path(pid)
    if not p.exists():
        return [], None
    recs = []
    for n, line in enumerate(p.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            # een afgebroken laatste regel is een crash tijdens het schrijven; alleen de allerlaatste regel is verontschuldigbaar
            if n == len(p.read_text().splitlines()):
                break
            return recs, {"code": "audit_corrupt", "message": f"auditlog regel {n} is onleesbaar"}
        if rec.get("hash") != _record_hash(rec):
            return recs, {"code": "audit_corrupt", "message": f"auditlog regel {n}: controlegetal klopt niet"}
        if recs and (rec["prev"] != recs[-1]["hash"] or rec["seq"] != recs[-1]["seq"] + 1):
            return recs, {"code": "audit_corrupt", "message": f"auditlog regel {n}: keten onderbroken"}
        recs.append(rec)
    return recs, None


def _append_audit(pid: str, recs: list) -> None:
    p = audit_path(pid)
    p.parent.mkdir(parents=True, exist_ok=True)
    existing = p.read_text() if p.exists() else ""
    if existing and not existing.endswith("\n"):   # afgebroken laatste regel weghalen
        existing = existing[: existing.rfind("\n") + 1]
        store.atomic_write(p, existing)
    with open(p, "a") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def audit_status(pid: str, d: dict) -> tuple:
    """Vergelijkt het auditlog met het snapshot. Geeft (problem|None, te_flushen_records)."""
    recs, problem = read_audit(pid)
    if problem:
        return problem, []
    file_seq = recs[-1]["seq"] if recs else 0
    need, head = d["audit"]["seq"], d["audit"]["head"]
    if file_seq == need:
        have = recs[-1]["hash"] if recs else GENESIS
        if have != head:
            return {"code": "audit_corrupt", "message": "laatste auditregel past niet bij het dossier"}, []
        return None, []
    if file_seq > need:
        return {"code": "audit_ahead", "message": f"auditlog loopt voor op het dossier (log {file_seq}, dossier {need}); "
                "het dossier is waarschijnlijk teruggezet"}, []
    pending = [r for r in d["audit"]["outbox"] if r["seq"] > file_seq]
    contiguous = [r["seq"] for r in pending] == list(range(file_seq + 1, need + 1))
    if recs:
        anchored = bool(pending) and pending[0]["prev"] == recs[-1]["hash"]
    else:   # leeg/afwezig log: begint bij het begin, of met een expliciete reseal-regel
        anchored = bool(pending) and (pending[0]["prev"] == GENESIS or pending[0]["kind"] == "reseal")
    if not pending or not anchored or (not contiguous and pending[0]["kind"] != "reseal") or pending[-1]["hash"] != head:
        return {"code": "audit_missing", "message": f"auditlog mist regels {file_seq + 1}..{need} die niet meer in de outbox staan"}, []
    return None, pending


# --- lezen, opslaan ---

def _read_json(path: Path):
    try:
        return json.loads(path.read_text()), None
    except OSError as e:
        return None, DossierError("dossier_missing", f"{path.name} niet leesbaar: {e}")
    except ValueError as e:
        return None, DossierError("dossier_corrupt", f"{path.name} is geen geldige JSON ({e})")


def read(pid: str) -> tuple:
    """Lock-vrij lezen voor status/Monitor. Geeft (dossier|None, problems). Geen reparatie, geen audit-flush."""
    d, err = _read_json(dossier_path(pid))
    if err:
        return None, [{"code": err.code, "message": err.message + "; herstel via runner2_supervision_admin action=repair"}]
    if isinstance(d, dict) and isinstance(d.get("schema_version"), int) and d["schema_version"] < SCHEMA_VERSION:
        return d, []   # migratie gebeurt bij de eerstvolgende schrijfbeurt; lezen blijft mogelijk
    return d, validate(d, pid)


def load_checked(pid: str) -> tuple:
    """Onder de lock: lezen, migreren, auditlog gelijktrekken (outbox flushen). Geeft (dossier|None, problems)."""
    d, err = _read_json(dossier_path(pid))
    if err:
        return None, [{"code": err.code, "message": err.message + "; herstel via runner2_supervision_admin action=repair"}]
    if isinstance(d, dict) and isinstance(d.get("schema_version"), int) and d["schema_version"] < SCHEMA_VERSION \
            and d["schema_version"] in MIGRATIONS:
        d = migrate(pid, d)
    problems = validate(d, pid)
    if problems:
        return d, problems
    problem, pending = audit_status(pid, d)
    if problem:
        return d, [problem]
    if pending or d["audit"]["outbox"]:
        if pending:
            _append_audit(pid, pending)
        d["audit"]["outbox"] = []
        _write(pid, d, keep_prev=False)
    return d, []


def _write(pid: str, d: dict, keep_prev: bool = True) -> None:
    d = redact_deep(d)
    path = dossier_path(pid)
    if keep_prev and path.exists():
        try:
            old = path.read_text()
            if not validate(json.loads(old), pid):
                store.atomic_write(prev_path(pid), old)
        except (OSError, ValueError):
            pass
    store.atomic_write(path, json.dumps(d, indent=1, ensure_ascii=False))


def save(pid: str, d: dict, records: list) -> dict:
    """Schrijft het snapshot (met de nieuwe auditregels in de outbox) en daarna pas het auditlog; crash-veilig.

    `records` = [(kind, summary, data)]. Volgorde: (1) snapshot met outbox, (2) auditlog bijwerken, (3) outbox legen."""
    a = d["audit"]
    new = []
    for kind, summary, data in records:
        rec = make_record(a["seq"] + 1, a["head"], kind, summary, data)
        a["seq"], a["head"] = rec["seq"], rec["hash"]
        new.append(rec)
    a["outbox"] = a["outbox"] + new
    d["revision"] += 1
    d["updated_at"] = now()
    last = d["decisions"][-1]["n"] if d["decisions"] else 0
    d["checkpoint"] = {"revision": d["revision"], "at": d["updated_at"], "event_id": (d["events"][-1]["id"] if d["events"] else None),
                       "decision_n": last, "assignment_id": (d["assignments"][-1]["assignment_id"] if d["assignments"] else None),
                       "audit_seq": a["seq"], "audit_head": a["head"]}
    problems = validate(d, pid)
    if problems:
        raise DossierError("refused_inconsistent_write", f"opslaan geweigerd, dossier zou inconsistent worden: {problems[0]['message']}")
    _write(pid, d)
    _append_audit(pid, a["outbox"])
    a["outbox"] = []
    _write(pid, d, keep_prev=False)
    return d


def create(pid: str, d: dict) -> dict:
    """Eerste opslag van een nieuw dossier (onder de lock); weigert te overschrijven."""
    if dossier_path(pid).exists():
        raise DossierError("exists", f"dossier {pid} bestaat al")
    return save(pid, d, [("register", f"project geregistreerd: {d['title'][:80]}", {"goal_sha": d["goal"]["sha256"]})])


# --- migratie ---

def migrate(pid: str, d: dict) -> dict:
    """Brengt een ouder dossier stap voor stap naar SCHEMA_VERSION; bewaart eerst een back-up. Onder de lock."""
    v0 = d["schema_version"]
    store.atomic_write(project_dir(pid) / f"dossier.v{v0}.bak.json", json.dumps(d, indent=1, ensure_ascii=False))
    d = copy.deepcopy(d)
    while d["schema_version"] < SCHEMA_VERSION:
        step = MIGRATIONS[d["schema_version"]]
        d = step(d)
        d["schema_version"] += 1
    if not validate(d, pid):
        problem, pending = audit_status(pid, d)
        if not problem:
            if pending:
                _append_audit(pid, pending)
                d["audit"]["outbox"] = []
            save(pid, d, [("migrated", f"schema {v0} -> {SCHEMA_VERSION}", {"from": v0, "to": SCHEMA_VERSION})])
    return d


# --- reparatie (alleen op expliciet verzoek; start nooit zelf iets) ---

def _adopt_audit_tail(pid: str, d: dict) -> list:
    """De vorige generatie loopt achter op het (append-only) log. Als het log een geldige keten vormt die door de vorige generatie
    loopt, schrijven we een 'restore'-regel achter het log en laten de snapshot daarnaar wijzen; de verloren regels blijven leesbaar.
    Geeft de verloren regels terug (leeg als er niets te adopteren viel)."""
    recs, problem = read_audit(pid)
    seq, head = d["audit"]["seq"], d["audit"]["head"]
    if problem or not recs or recs[-1]["seq"] <= seq:
        return []
    anchor = next((r for r in recs if r["seq"] == seq), None)
    if (seq and (not anchor or anchor["hash"] != head)) or (not seq and head != GENESIS):
        return []
    lost = [r for r in recs if r["seq"] > seq]
    rec = make_record(recs[-1]["seq"] + 1, recs[-1]["hash"], "restore", "dossier teruggezet naar een eerdere generatie; auditregels erna zijn niet in het dossier verwerkt",
                      {"restored_to_seq": seq, "lost_seqs": [r["seq"] for r in lost], "lost_summaries": [r["summary"][:120] for r in lost][-10:]})
    d["audit"].update(seq=rec["seq"], head=rec["hash"], outbox=[rec])
    d["revision"] += 1
    d["checkpoint"].update(revision=d["revision"], audit_seq=rec["seq"], audit_head=rec["hash"], at=now())
    return lost


def repair(pid: str, mode: str, confirm: bool = False) -> dict:
    """mode=restore_prev: corrupt dossier.json vervangen door de vorige generatie.
    mode=reseal: auditlog dat niet meer klopt opzij zetten en een nieuwe keten laten beginnen (confirm=true vereist).
    mode=accept: een vastgelegde integriteitsblokkade (reconcile) opheffen nadat die beoordeeld is."""
    with lock(pid):
        if mode == "restore_prev":
            d, _ = _read_json(prev_path(pid))
            if d is None or validate(d, pid):
                raise DossierError("no_valid_prev", "er is geen geldige vorige generatie om naar terug te gaan")
            lost = _adopt_audit_tail(pid, d)
            store.atomic_write(dossier_path(pid), json.dumps(d, indent=1, ensure_ascii=False))
            d, problems = load_checked(pid)
            if problems:   # blijft dan zichtbaar als blokkade
                return {"restored": True, "problems": problems}
            save(pid, d, [("repair", "dossier teruggezet naar de vorige generatie", {"mode": mode, "lost_records": len(lost)})])
            return {"restored": True, "problems": [], "lost_records": [r["summary"] for r in lost][-10:]}
        if mode == "reseal":
            if not confirm:
                raise DossierError("confirm_required", "reseal zet het auditlog opzij en begint een nieuwe keten: geef confirm=true")
            d, err = _read_json(dossier_path(pid))
            if err or validate(d, pid):
                raise DossierError("dossier_invalid", "eerst het dossier zelf herstellen (restore_prev); reseal werkt alleen op een geldig dossier")
            a = d["audit"]
            rec = make_record(a["seq"] + 1, a["head"], "reseal", "auditlog opnieuw begonnen na een niet-herstelbare afwijking",
                              {"previous_head": a["head"]})
            a["seq"], a["head"], a["outbox"] = rec["seq"], rec["hash"], [rec]
            d["revision"] += 1
            d["checkpoint"].update(revision=d["revision"], audit_seq=a["seq"], audit_head=a["head"], at=now())
            _write(pid, d)   # eerst het snapshot met de reseal-regel in de outbox, dan pas het oude log opzij
            p = audit_path(pid)
            if p.exists():
                replace(p, p.with_name(f"audit.{int(time.time())}.corrupt.jsonl"))
            _append_audit(pid, [rec])
            a["outbox"] = []
            _write(pid, d, keep_prev=False)
            return {"resealed": True, "audit_seq": a["seq"]}
    raise DossierError("invalid_mode", f"onbekende reparatiemodus {mode!r}")
