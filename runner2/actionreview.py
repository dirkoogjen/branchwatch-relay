"""Actiebeheer: open menselijke actiepunten nalopen en aantoonbaar overbodige punten aanwijzen (fase 1: alleen DRY RUN).

Het plan is een zuivere functie van de taakstates van één node (`plan`): vaste regels, geen externe aanroep, geen AI.
Elk open punt komt in precies één bak:

* `would_close`    zou sluiten: alleen met een vaste regel, bron-id's en controleerbare feiten (zie RULES);
* `review`         twijfel: misschien overbodig (bijna gelijke tekst, leest als mededeling). Blijft open; eventueel een
                   Claude Opus-advies (`opus_packet`), maar dat advies alleen is NOOIT genoeg om te sluiten;
* `needs_decision` vereist beslissing: al het andere, waaronder elk beschermd punt (PROTECTED) - ook als een regel past.

Veiligheid (ook voor de latere fase 2, `apply`/`undo`, die standaard uit staat en alleen lokaal draait):
* beschermd zijn supervisie, beveiliging, toegang, gegevens verwijderen, integratie/publicatie, onopgeloste fouten en
  kosten, plus elk punt met `new_info` (nieuwe omstandigheid bij een besluit);
* nooit sluiten op grond van iets dat zelf (automatisch) gesloten is: het bewijs-item moet open blijven (dubbel) of door een
  mens/controller met een inhoudelijke notitie gesloten zijn (al beslist); geen ketens (cascade);
* alleen dezelfde node en dezelfde repository; een remote antwoord van een andere node dan gevraagd wordt geweigerd;
* stroomonderbreker: meer dan `action_review_max_close` of de helft van de open punten = niets sluiten;
* `apply` berekent het plan opnieuw onder een lock en sluit alleen wat nog exact hetzelfde is (vingerafdruk + bewijs),
  schrijft vooraf en achteraf naar de auditlog en is idempotent; `undo(plan_id)` zet die punten weer open.
"""
import hashlib
import json
import os
import re
import time
from contextlib import ExitStack, contextmanager
from datetime import datetime

from . import actions, config, store
from .filelock import lock_exclusive

VERSION = 1
BY = "actiebeheer"
WOULD_CLOSE, REVIEW, DECIDE = "would_close", "review", "needs_decision"
BUCKETS = (WOULD_CLOSE, REVIEW, DECIDE)
OPUS_MODEL = "claude-opus-5-5"
SUMMARY_ITEMS = 20
LOOSE_SHARED, LOOSE_JACCARD = 4, 0.2     # 'mogelijk dubbel' (alleen aanwijzing; op echte data ~1 op 6 terecht)
FRACTION_FLOOR = 5          # de 'helft van de open punten'-grens telt pas boven zoveel sluitingen (kleine nodes)

RULES = {
    "exact_duplicate": "Dubbel: precies dezelfde vraag staat al eerder open in dezelfde repository; die blijft open.",
    "topic_duplicate_same_run": "Dubbel: dezelfde vraag (zelfde onderwerp) staat al eerder open in dezelfde run; die blijft open.",
    "already_decided": "Al beslist: precies deze vraag is eerder in dezelfde repository beantwoord.",
    "integration_completed": "De integratiemelding is achterhaald: exact deze taak is aantoonbaar gecontroleerd geïntegreerd.",
}
# Regels die vroeger automatisch sloten: nu alleen een sluitvoorstel voor een mens (tekst- of onderwerpgelijkheid is geen sluitbewijs).
CLOSING_PROPOSAL_RULES = ("exact_duplicate", "topic_duplicate_same_run", "already_decided")
REVIEW_REASONS = {
    "similar_open": "Lijkt op een ander open punt van dezelfde run, maar de tekst is niet gelijk.",
    "similar_decided": "Lijkt op een eerder beantwoorde vraag in dezelfde repository, maar de tekst is niet gelijk.",
    "asked_again": "Dezelfde vraag is eerder beantwoord, maar daarna opnieuw gesteld: misschien is er iets veranderd.",
    "notice": "Leest als mededeling over afgerond werk, niet als vraag of opdracht.",
    "exact_duplicate": "Sluitvoorstel voor een mens: precies dezelfde tekst staat al eerder open in dezelfde repository; tekstgelijkheid is geen sluitbewijs.",
    "topic_duplicate_same_run": "Sluitvoorstel voor een mens: zelfde onderwerp als een eerder open punt in dezelfde run; gelijk onderwerp is geen sluitbewijs.",
    "already_decided": "Sluitvoorstel voor een mens: een gelijke vraag is eerder beantwoord, maar dat besluit noemt dit punt niet.",
    "evidence_closing": "Het bewijs-item zou in dezelfde ronde zelf worden gesloten; volgende ronde opnieuw bekijken.",
}

# Woordstammen op de gevouwen tekst (kleine letters, zonder accenten). Liever te veel beschermd dan één keer te veel gesloten.
PROTECTED = (
    ("supervisie", r"supervis|\bjudge\b|\bdossier"),
    ("beveiliging", r"beveilig|security|wachtwoord|password|geheim|secret|\btoken|sleutel|\bkeys?\b|certificaat|\btls\b|\bssl\b|"
                    r"firewall|\b2fa\b|\bmfa\b|kwetsba|versleutel|encrypt|\bcve\b"),
    ("toegang", r"toegang|access|\binlog|\blogin|aanmeld|account|rechten|permissi|\bssh\b|\bsudo\b|\badmin|tailscale|\bvpn\b|"
                r"uitnodig|machtig"),
    ("gegevens verwijderen", r"verwijder|\bwis(sen|t)?\b|delete|\bremove|weggooi|purge|prune|\bdrop\b|leegma|vernietig|overschrijf|"
                             r"\brm -|reset --hard|opruim|opschoon"),
    ("integratie/publicatie", r"integr|samenvoeg|\bmerge|\bmain\b|hoofdlijn|hoofdversie|publice|publicat|\blive\b|uitrol|deploy|"
                              r"release|\bpush|productie|\bpreview"),
    ("onopgeloste fout", r"\bfout|\bfaal|faalt|mislukt|\berror|crash|kapot|\bbug|storing|werkt niet|exception|traceback"),
    ("kosten", r"betaal|kosten|abonnement|factuur|aankoop|\bkoop|prijs|\bbudget|licentie"),
)
_PROTECTED = tuple((name, re.compile(rx)) for name, rx in PROTECTED)
# Een gesloten punt telt alleen als besluit als de notitie inhoud heeft en niet zelf 'dubbel/verouderd' is.
_NOT_A_DECISION = re.compile(r"duplica|dubbel|verouderd|achterhaald|vervallen|outdated|obsolete|opgeschoond|opgeruimd|"
                             r"gedekt door|blijft open|blijven open|administratief|geen inhoudelijk besluit")


class ReviewError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code, self.message = code, message


def _clip(text, n: int) -> str:
    return actions._clip(text, n)


def _sha(obj) -> str:
    return hashlib.sha1(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def fingerprint(item: dict) -> str:
    """Wat er van een punt niet mag veranderen tussen plan en uitvoering."""
    src = item.get("source") or {}
    return _sha([item.get("id"), item.get("status"), item.get("text"), item.get("reason"), item.get("details"), item.get("created_at"), src.get("task_id"), src.get("role"),
                 item.get("topic"), item.get("new_info"), item.get("resolved_by"), item.get("resolved_at"),
                 item.get("resolved_note")])[:16]


def _key(item: dict) -> tuple:
    return (item.get("created_at") or "", item.get("id") or "")


def _run_key(st: dict) -> tuple:
    for key in ("supervisor_id", "chain_id"):
        if st.get(key):
            return (key, st[key])
    return ("task_id", st.get("task_id"))


def protected(item: dict, st: dict, guard: dict | None = None) -> list:
    """Beschermde categorieën van een punt (leeg = niet beschermd)."""
    guard = guard or {}
    src = item.get("source") or {}
    out = []
    if (item.get("id") in guard.get("ids", ()) or src.get("task_id") in guard.get("task_ids", ())
            or (st.get("chain_id") and st["chain_id"] in guard.get("chain_ids", ())) or src.get("role") == "judge"):
        out.append("supervisie")
    hay = actions._fold(" ".join(str(item.get(k) or "") for k in ("text", "reason", "details", "topic")))
    for name, rx in _PROTECTED:
        if name not in out and rx.search(hay):
            out.append(name)
    if item.get("new_info"):
        out.append("nieuwe omstandigheid")
    return out


def _is_decision(r: dict) -> bool:
    """Mag dit gesloten punt als besluit gelden? Niet als het (automatisch) als dubbel/verouderd is gesloten."""
    by = str(r.get("resolved_by") or "")
    note = actions._fold(str(r.get("resolved_note") or ""))
    return (r.get("status") == actions.RESOLVED and not r.get("auto_resolution") and not by.startswith(BY)
            and len(note.strip()) >= 3 and not _NOT_A_DECISION.search(note))


def _decision_names(decision: dict, item: dict) -> bool:
    """Bewezen besluit voor dit punt: de notitie van de mens/controller noemt het id van het te sluiten punt."""
    return bool(item.get("id")) and str(item["id"]) in str(decision.get("resolved_note") or "")


def _epoch(ts) -> float | None:
    try:
        return datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%S%z").timestamp()
    except ValueError:
        return None


def _before(a, b) -> bool:
    """Ligt tijdstip a aantoonbaar vóór b? Onleesbare tijden tellen als 'ja' (dan sluit de regel niet)."""
    ea, eb = _epoch(a), _epoch(b)
    return ea is None or eb is None or ea < eb


def _topics_conflict(a: dict, b: dict) -> bool:
    ta, tb = actions._slug(a.get("topic")), actions._slug(b.get("topic"))
    return bool(ta and tb and ta != tb)


def _ref(item: dict, st: dict) -> dict:
    src = item.get("source") or {}
    out = {"id": item.get("id"), "task_id": st.get("task_id") or src.get("task_id"), "status": item.get("status"),
           "created_at": item.get("created_at"), "text": _clip(item.get("text"), 160), "fingerprint": fingerprint(item)}
    if item.get("status") == actions.RESOLVED:
        out.update(resolved_at=item.get("resolved_at"), resolved_by=item.get("resolved_by"),
                   resolved_note=_clip(item.get("resolved_note"), 200))
    return out


def _entry(item: dict, st: dict, node: str, bucket: str, rule: str | None, label: str, evidence: dict | None = None,
           facts: list | None = None, prot: list | None = None, hint: str | None = None) -> dict:
    out = {"id": item["id"], "node": node, "bucket": bucket, "rule": rule, "label": label,
           "text": _clip(item.get("text"), 300), "task_id": st.get("task_id") or src.get("task_id"),
           "repo": st.get("repo") or "", "created_at": item.get("created_at"), "fingerprint": fingerprint(item),
           "protected": prot or [], "facts": facts or [], "evidence": evidence}
    if hint:
        out["hint"] = hint
    return out


def plan(states: list, node: str, guard: dict | None = None, policy: dict | None = None) -> dict:
    """Het DRY-RUN-plan voor één node. Wijzigt niets. Deterministisch: dezelfde states geven hetzelfde plan_id."""
    policy = dict(policy or {})
    max_close = int(policy.get("max_close", 25))
    max_fraction = float(policy.get("max_fraction", 0.5))
    pairs = []          # (state, item) voor elk geldig punt
    for st in states:
        if not isinstance(st, dict) or not st.get("task_id"):
            continue
        for it in st.get("human_actions") or []:
            if isinstance(it, dict) and it.get("id") and it.get("status") in (actions.OPEN, actions.RESOLVED):
                pairs.append((st, it))
    pairs.sort(key=lambda p: _key(p[1]))
    norm = {id(it): actions.normalize(it.get("text")) for _, it in pairs}
    opened = [(st, it) for st, it in pairs if it["status"] == actions.OPEN]
    by_repo_open: dict = {}
    by_repo_decided: dict = {}
    for st, it in pairs:
        repo = st.get("repo") or ""
        if not repo:
            continue
        if it["status"] == actions.OPEN:
            by_repo_open.setdefault(repo, []).append((st, it))
        elif _is_decision(it):
            by_repo_decided.setdefault(repo, []).append((st, it))

    entries = []
    for st, it in opened:
        repo = st.get("repo") or ""
        prot = protected(it, st, guard)
        hit = review = None
        if repo and not it.get("new_info"):
            # regel 1: al beslist (exact of zelfde topic), op een besluit van een mens/controller
            for rst, r in by_repo_decided.get(repo, []):
                if _topics_conflict(it, r):
                    continue
                exact = norm[id(r)] == norm[id(it)] and norm[id(it)]
                topic = actions._slug(it.get("topic")) and actions._slug(it.get("topic")) == actions._slug(r.get("topic"))
                if (exact or topic) and not _before(r.get("resolved_at"), it.get("created_at")):
                    how = "exact dezelfde tekst" if exact else f"hetzelfde onderwerp ({actions._slug(it.get('topic'))})"
                    if not _decision_names(r, it):
                        # gelijke tekst/onderwerp is geen bewijs: alleen een sluitvoorstel voor een mens
                        if review is None:
                            review = ("already_decided", r, rst)
                        continue
                    hit = ("already_decided", r, rst, [
                        f"{how} als {r['id']} (taak {rst['task_id']}), gesloten op {r.get('resolved_at')} door {r.get('resolved_by')}",
                        f"antwoord: {_clip(r.get('resolved_note'), 200)}"])
                    break
                if review is None and (exact or topic or actions.similar(it.get("text") or "", r.get("text") or "")):
                    review = ("asked_again" if exact or topic else "similar_decided", r, rst)
            # regel 2: dubbel van een ouder open punt (exact in de repo; zelfde topic binnen de run)
            if hit is None:
                for sst, s in by_repo_open.get(repo, []):
                    if _key(s) >= _key(it):
                        break
                    if _topics_conflict(it, s):
                        continue
                    if norm[id(s)] == norm[id(it)] and norm[id(it)]:
                        if review is None:
                            review = ("exact_duplicate", s, sst)
                        continue
                    same_run = _run_key(sst) == _run_key(st)
                    if same_run and actions._slug(it.get("topic")) and actions._slug(it.get("topic")) == actions._slug(s.get("topic")):
                        if review is None:
                            review = ("topic_duplicate_same_run", s, sst)
                        continue
                    if review is None and same_run and actions.similar(it.get("text") or "", s.get("text") or ""):
                        review = ("similar_open", s, sst)
        if hit:
            rule, ev, est, facts = hit
            evidence = {"kind": "survivor" if ev["status"] == actions.OPEN else "decision", "item": _ref(ev, est)}
            if prot:
                entries.append(_entry(it, st, node, DECIDE, None, "Beschermd: " + ", ".join(prot), evidence, facts, prot,
                                      hint=f"Past op regel '{rule}', maar beschermde punten sluiten nooit automatisch."))
            else:
                entries.append(_entry(it, st, node, WOULD_CLOSE, rule, RULES[rule], evidence, facts))
            continue
        if review is None and actions._NOTICE.search(it.get("text") or "") and "?" not in (it.get("text") or ""):
            review = ("notice", None, None)
        if review:
            why, ev, est = review
            evidence = {"kind": "candidate", "item": _ref(ev, est)} if ev else None
            if prot:
                entries.append(_entry(it, st, node, DECIDE, None, "Beschermd: " + ", ".join(prot), evidence, prot=prot,
                                      hint="Twijfelgeval: " + REVIEW_REASONS[why]))
            else:
                entries.append(_entry(it, st, node, REVIEW, why, REVIEW_REASONS[why], evidence))
            continue
        entries.append(_entry(it, st, node, DECIDE, None, ("Beschermd: " + ", ".join(prot)) if prot else "Open: alleen een mens kan dit afhandelen.",
                              prot=prot))

    # losse aanwijzing 'mogelijk dubbel' (zelfde node, elke repo) voor mens of Opus; verandert de bak nooit
    toks = {id(it): actions.tokens(it.get("text")) for _, it in opened}
    by_id = {it["id"]: it for _, it in opened}
    for e in entries:
        if e["bucket"] == WOULD_CLOSE:
            continue
        ta = toks[id(by_id[e["id"]])]
        for _, o in opened:
            tb = toks[id(o)]
            shared = len(ta & tb)
            if o["id"] != e["id"] and shared >= LOOSE_SHARED and shared / len(ta | tb) >= LOOSE_JACCARD:
                e["possible_duplicate_of"] = o["id"]
                break

    # cascade: een punt waarop een sluiting steunt, mag in dezelfde ronde niet zelf gesloten worden
    closing = {e["id"] for e in entries if e["bucket"] == WOULD_CLOSE}
    for e in entries:
        if e["bucket"] == WOULD_CLOSE and e["evidence"]["item"]["id"] in closing:
            e.update(bucket=REVIEW, rule="evidence_closing", label=REVIEW_REASONS["evidence_closing"])
    out = {"version": VERSION, "mode": "dry_run", "plan_id": None, "node": node, "generated_at": store.now(),
           "policy": {"max_close": max_close, "max_fraction": max_fraction}, "breaker": None, "counts": {"open": len(opened)}}
    for b in BUCKETS:
        out[b] = [e for e in entries if e["bucket"] == b]
    return _seal(out)


def _seal(p: dict) -> dict:
    """Sortering, tellingen, stroomonderbreker en plan_id uit de bakken (ook na `enforce_guard`)."""
    for b in BUCKETS:
        p[b] = sorted(p[b], key=lambda e: (e["created_at"] or "", e["id"]))
    p["counts"] = {"open": p["counts"]["open"], **{b: len(p[b]) for b in BUCKETS}}
    closing, opened = sorted(p[WOULD_CLOSE], key=lambda e: e["id"]), p["counts"]["open"]
    max_close, max_fraction = p["policy"]["max_close"], p["policy"]["max_fraction"]
    p["breaker"] = None
    if len(closing) > max_close or (len(closing) > FRACTION_FLOOR and len(closing) > max_fraction * opened):
        p["breaker"] = {"code": "too_many", "message": f"{len(closing)} van {opened} open punten zouden sluiten; dat is meer dan "
                        f"{max_close} of {int(max_fraction * 100)} %. Er wordt niets gesloten tot een mens dit heeft bekeken."}
    p["plan_id"] = "AR-" + _sha([p["node"], [(e["id"], e["rule"], e["evidence"]["item"]["id"], e["evidence"]["item"]["fingerprint"],
                                              e["fingerprint"]) for e in closing]])[:12]
    return p


# --- supervisie-bescherming (dossiers staan op de control plane) ---

def guard_from_dossiers() -> dict:
    """Actie-id's, taak- en keten-id's die bij een supervisieproject horen. Alleen lezen; leeg zonder dossiers."""
    from . import dossier
    out = {"ids": set(), "task_ids": set(), "chain_ids": set(), "errors": []}
    try:
        root = config.supervision_dir()
        if root.exists():
            list(root.iterdir())  # an unreadable inventory is not an empty inventory
        pids = dossier.project_ids()
    except OSError as exc:
        out["errors"].append(type(exc).__name__)
        return out
    for pid in pids:
        d, problems = dossier.read(pid)
        if problems or not d:
            out["errors"].append(pid + ": supervision dossier unavailable/invalid")
            continue
        out["ids"] |= set((d.get("runner_actions") or {}).keys())
        for a in d.get("assignments") or []:
            if a.get("task_id"):
                out["task_ids"].add(a["task_id"])
            if a.get("chain_id"):
                out["chain_ids"].add(a["chain_id"])
    return out


def enforce_guard(p: dict, guard: dict) -> dict:
    """Verplaatst punten die (volgens de dossiers van de control plane) bij supervisie horen naar 'vereist beslissing'."""
    ids, tids = guard.get("ids") or set(), guard.get("task_ids") or set()
    moved = False
    for b in (WOULD_CLOSE, REVIEW):
        keep = []
        for e in p.get(b) or []:
            if e["id"] in ids or e.get("task_id") in tids:
                e = dict(e, bucket=DECIDE, protected=sorted(set(e.get("protected") or []) | {"supervisie"}),
                         label="Beschermd: supervisie", hint=f"Hoort bij een supervisieproject; was '{b}'.")
                p[DECIDE].append(e)
                moved = True
            else:
                keep.append(e)
        p[b] = keep
    return _seal(p) if moved else p


# --- samenvatting voor Monitor 2 / CLI ---

def _short(e: dict) -> dict:
    out = {"id": e["id"], "text": e["text"], "label": e["label"], "task_id": e["task_id"], "created_at": e["created_at"]}
    if e.get("evidence"):
        out["evidence_id"] = e["evidence"]["item"]["id"]
    if e.get("protected"):
        out["protected"] = e["protected"]
    if e.get("hint"):
        out["hint"] = e["hint"]
    if e.get("possible_duplicate_of"):
        out["possible_duplicate_of"] = e["possible_duplicate_of"]
    return out


def summary(p: dict) -> dict:
    """Compact blok 'zou sluiten / vereist beslissing' (alleen lezen) voor Monitor 2."""
    c = p["counts"]
    head = f"{p['node']}: {c['open']} open · zou sluiten {c[WOULD_CLOSE]} · vereist beslissing {c[DECIDE] + c[REVIEW]}"
    if c[REVIEW]:
        head += f" (waarvan {c[REVIEW]} twijfelgeval{'len' if c[REVIEW] != 1 else ''})"
    return {"node": p["node"], "plan_id": p["plan_id"], "mode": p["mode"], "generated_at": p["generated_at"], "counts": c,
            "headline": head, "breaker": p.get("breaker"),
            "closing_enabled": bool(p.get("closing_enabled")),
            "would_close": [_short(e) for e in p[WOULD_CLOSE][:SUMMARY_ITEMS]],
            "review": [_short(e) for e in p[REVIEW][:SUMMARY_ITEMS]],
            "needs_decision": [_short(e) for e in p[DECIDE][:SUMMARY_ITEMS]],
            "possible_duplicates": sum(1 for e in p[DECIDE] + p[REVIEW] if e.get("possible_duplicate_of"))}


def render(report: dict) -> str:
    """Leesbare tekst (Nederlands) van een plan of van `review_all`."""
    plans = report.get("plans") or ([report] if report.get("plan_id") else [])
    lines = ["Actiebeheer - DRY RUN (er is niets gesloten)", ""]
    for p in plans:
        lines.append(summary(p)["headline"])
        if p.get("breaker"):
            lines.append(f"  STROOMONDERBREKER: {p['breaker']['message']}")
        for b, title in ((WOULD_CLOSE, "Zou sluiten"), (REVIEW, "Twijfel (blijft open; mens of Opus-advies)"),
                         (DECIDE, "Vereist beslissing")):
            if not p[b]:
                continue
            lines.append(f"  {title} ({len(p[b])}):")
            for e in p[b][: (None if b != DECIDE else 15)]:
                ev = f" [bewijs {e['evidence']['item']['id']}]" if e.get("evidence") else ""
                ev += f" [mogelijk dubbel van {e['possible_duplicate_of']}]" if e.get("possible_duplicate_of") else ""
                lines.append(f"    - {e['id']} {e['text'][:110]}{ev}")
                if b != DECIDE:
                    lines.append(f"        {e['label']}")
                    for f in e.get("facts") or []:
                        lines.append(f"        · {f}")
            if b == DECIDE and len(p[b]) > 15:
                lines.append(f"    … en nog {len(p[b]) - 15}")
        lines.append("")
    for node, err in (report.get("errors") or {}).items():
        lines.append(f"{node}: niet beoordeeld - {err}")
    return "\n".join(lines).rstrip() + "\n"


# --- lokaal en over de nodes ---

def policy_from(cfg: dict) -> dict:
    return {"max_close": int(cfg.get("action_review_max_close", 25)), "max_fraction": float(cfg.get("action_review_max_fraction", 0.5))}


def local_plan(cfg: dict | None = None) -> dict:
    cfg = cfg or config.load_config()
    states, guard = store.all_tasks(), guard_from_dossiers()
    p = plan(states, config.local_node(), guard, policy_from(cfg))
    by_task = {st["task_id"]: st for st in states}
    moved = []
    for bucket in (REVIEW, DECIDE):
        keep = []
        for entry in p[bucket]:
            state = by_task[entry["task_id"]]
            original = _find(state, entry["id"])
            proof = _integration_notice_proof(original, state, guard)
            if proof:
                evidence = {"kind": "integration", "item": {"id": proof["request"], "task_id": state["task_id"],
                            "fingerprint": _sha(proof)}}
                moved.append(dict(entry, bucket=WOULD_CLOSE, rule="integration_completed", protected=[],
                                  label=RULES["integration_completed"], evidence=evidence,
                                  facts=[f"Taak {state['task_id']}: {proof['request']}, grant {proof['grant']}, commit {proof['commit']}" ]))
            else:
                keep.append(entry)
        p[bucket] = keep
    p[WOULD_CLOSE].extend(moved)
    _seal(p)
    p["closing_enabled"] = bool(cfg.get("action_review_apply"))
    return p


def _integration_notice_proof(item: dict, state: dict, guard: dict) -> dict | None:
    """Only Monitor-generated notices, never a task's content/approval question. Verify complete local integrator evidence."""
    if not item or item.get("new_info") or state.get("status") != "completed":
        return None
    if "supervisie" in protected(item, state, guard):
        return None
    source = item.get("source") or {}
    tid, repo = state.get("task_id"), state.get("repo")
    if source.get("task_id") != tid or source.get("role") != "task":
        return None
    match = re.fullmatch(r"integratie-(protected-paths|tests-failed|too-large)-([0-9a-f]{12})", item.get("topic") or "")
    details = item.get("details") or ""
    if not match or not details.startswith("Bron: Monitor 2 automatische integratie (niet door de taak zelf gemeld);"):
        return None
    fields = {part.strip() for part in details.split(";")}
    if not {f"aanvraag R-{match[2]}", f"reden {match[1].replace('-', '_')}", f"taak {tid}"} <= fields:
        return None
    try:
        from . import integration_check
        proof = integration_check.verify({"task_id": tid, "repo": repo, "node": config.local_node()})
        if (proof.get("ok") is True and proof.get("task_id") == tid and proof.get("repo") == repo
                and proof.get("node") == config.local_node() and proof.get("request") and proof.get("grant")
                and proof.get("commit") and proof.get("audit_hash") and proof.get("tests")):
            return proof
    except Exception:  # unavailable verifier or incomplete evidence: keep the notice open
        pass
    return None


def review_all(nodes: list | None = None, call=None) -> dict:
    """DRY RUN over alle nodes vanaf de control plane. Een node met een oude Runner 2 of een antwoord van een andere node
    wordt niet beoordeeld (en nooit geraden). Supervisie-dossiers van deze machine beschermen ook punten van andere nodes."""
    from . import remote
    from .controller import ControllerError
    call = call or remote.call
    local = config.local_node()
    wanted = [n.upper() for n in (nodes or [local, *remote.nodes().keys()])]
    guard = guard_from_dossiers()
    out = {"mode": "dry_run", "generated_at": store.now(), "plans": [], "errors": {}}
    for node in dict.fromkeys(wanted):
        try:
            if node == local:
                p = local_plan()
            else:
                p = call(node, "runner2_actions", {"review": True})
                if not isinstance(p, dict) or p.get("mode") != "dry_run" or "plan_id" not in p:
                    raise ReviewError("unsupported", "deze node kent de dry run nog niet (oude Runner 2-versie)")
                if p.get("node") != node:
                    raise ReviewError("wrong_node", f"antwoord kwam van node {p.get('node')!r} in plaats van {node}")
            out["plans"].append(enforce_guard(p, guard))
        except ControllerError as e:
            msg = e.message or e.code
            if e.code == "invalid_input" and "review" in msg:
                msg = "deze node kent de dry run nog niet (oude Runner 2-versie)"
            out["errors"][node] = msg
        except ReviewError as e:
            out["errors"][node] = e.message
    out["totals"] = {k: sum(p["counts"][k] for p in out["plans"]) for k in ("open", *BUCKETS)}
    return out


# --- Claude Opus-advies voor twijfelgevallen (optioneel, nooit genoeg om te sluiten) ---

OPUS_INSTRUCTION = (
    "Je beoordeelt één open actiepunt van Runner 2 voor een niet-programmerende gebruiker. Zeg alleen of het punt waarschijnlijk "
    "overbodig is (dubbel, al beslist of mededeling) en waarom, met de id's waarop je steunt. Je advies sluit niets: een mens "
    "beslist. Twijfel je, antwoord dan 'houden'. Antwoord als JSON: {\"advies\": \"houden\" | \"waarschijnlijk_overbodig\", "
    "\"waarom\": \"...\", \"steunt_op\": [\"A-...\"]}.")


def opus_candidates(p: dict) -> list:
    """Twijfelgevallen die een Opus-advies kunnen krijgen: de bak 'twijfel' en punten met een 'mogelijk dubbel'-aanwijzing."""
    return [e for e in p[REVIEW]] + [e for e in p[DECIDE] if e.get("possible_duplicate_of")]


def opus_packet(entry: dict) -> dict:
    """Alles wat een Opus-beoordeling van een twijfelgeval nodig heeft. Wordt niet automatisch verstuurd."""
    return {"model": OPUS_MODEL, "instruction": OPUS_INSTRUCTION,
            "item": {k: entry.get(k) for k in ("id", "node", "repo", "task_id", "text", "created_at", "label", "protected")},
            "candidate": (entry.get("evidence") or {}).get("item"), "possible_duplicate_of": entry.get("possible_duplicate_of")}


def attach_ai_advice(p: dict, advice: dict) -> dict:
    """Hangt AI-advies {action_id: {advies, waarom, steunt_op}} aan twijfelgevallen. Verplaatst NOOIT iets naar 'zou sluiten'."""
    for e in opus_candidates(p):
        a = advice.get(e["id"])
        if isinstance(a, dict):
            e["ai_advice"] = {"model": OPUS_MODEL, "advies": _clip(a.get("advies"), 40), "waarom": _clip(a.get("waarom"), 300),
                              "steunt_op": [str(x)[:20] for x in (a.get("steunt_op") or [])][:5],
                              "effect": "alleen advies aan de gebruiker; sluit niets"}
    return p


# --- rapport bewaren en planning (standaard uit) ---

def home():
    return config.r2_home() / "actionreview"


def save_report(p: dict) -> str:
    path = home() / f"latest-{p['node']}.json"
    store.atomic_write(path, json.dumps(p, indent=1, ensure_ascii=False))
    _audit({"kind": "dry_run", "plan_id": p["plan_id"], "node": p["node"], "counts": p["counts"],
            "would_close": [e["id"] for e in p[WOULD_CLOSE]], "breaker": bool(p.get("breaker"))})
    return str(path)


def due(cfg: dict, now: float | None = None) -> bool:
    """Is er een geplande DRY RUN aan de beurt? `action_review_interval_h = 0` (standaard) = nooit."""
    hours = float(cfg.get("action_review_interval_h") or 0)
    if hours <= 0:
        return False
    try:
        marker = "last-cleanup" if cfg.get("action_review_apply") else f"latest-{config.local_node()}.json"
        last = (home() / marker).stat().st_mtime
    except OSError:
        return True
    return (now or time.time()) - last >= hours * 3600


def covered_by_reassess(cfg: dict) -> bool:
    """Op de primaire control plane maakt de dagelijkse herbeoordeling (besluit 47, `actionreassess`) het lokale dry-run-rapport mee:
    een tweede dagelijkse scheduler daarvoor zou dubbel werk zijn."""
    from . import cplane
    return float(cfg.get("action_reassess_interval_h") or 0) > 0 and cplane.is_primary()


def tick(cfg: dict) -> str | None:
    """Dagelijkse lokale administratie. Alleen expliciet ingeschakeld beleid past het opnieuw getoetste plan toe.
    De volledige herbeoordeling vervangt alleen de rapportmodus; sluiting is een afzonderlijke, lokale ronde."""
    enabled = bool(cfg.get("action_review_apply"))
    if not due(cfg) or (covered_by_reassess(cfg) and not enabled):
        return None
    plan = local_plan(cfg)
    path = save_report(plan)
    if enabled:
        store.atomic_write(home() / "last-cleanup", store.now())
    if enabled and plan[WOULD_CLOSE] and not plan.get("breaker"):
        apply(plan, confirm=plan["plan_id"], cfg=cfg)
        path = save_report(local_plan(cfg))
    return path


# --- auditlog ---

def _audit(rec: dict) -> None:
    path = home() / "audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    rec = {"at": store.now(), "node": config.local_node(), **rec}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
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


# --- fase 2: sluiten en terugzetten (standaard uit; niet via CLI of MCP bereikbaar) ---

@contextmanager
def _run_lock():
    path = home() / "run.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        try:
            lock_exclusive(f, blocking=False)
        except OSError:
            raise ReviewError("busy", "er loopt al een actiebeheer-ronde op deze node") from None
        yield


def _find(st: dict, aid: str) -> dict | None:
    return next((i for i in st.get("human_actions") or [] if isinstance(i, dict) and i.get("id") == aid), None)


def _age_s(ts: str) -> float:
    e = _epoch(ts)
    return float("inf") if e is None else time.time() - e


def apply(doc: dict, confirm: str | None, by: str = BY, cfg: dict | None = None) -> dict:
    """Sluit de 'zou sluiten'-punten van een DRY RUN-plan. Alleen als `action_review_apply = true`, `confirm == plan_id`,
    het plan van deze node is, jong genoeg is en de stroomonderbreker niet sprong. Idempotent en race-veilig."""
    cfg = cfg or config.load_config()
    if not cfg.get("action_review_apply"):
        raise ReviewError("disabled", "automatisch sluiten staat uit (fase 1: alleen dry run); daarvoor is aparte toestemming nodig")
    if not isinstance(doc, dict) or doc.get("version") != VERSION or doc.get("mode") != "dry_run":
        raise ReviewError("invalid_plan", "geen geldig actiebeheer-plan")
    if confirm != doc.get("plan_id"):
        raise ReviewError("not_confirmed", "bevestig met precies het plan_id van het bekeken plan")
    if doc.get("node") != config.local_node():
        raise ReviewError("wrong_node", f"plan is van node {doc.get('node')}, dit is {config.local_node()}")
    if _age_s(doc["generated_at"]) > float(cfg.get("action_review_max_plan_age_s", 3600)):
        raise ReviewError("stale_plan", "het plan is te oud; maak een nieuwe dry run")
    if doc.get("breaker"):
        raise ReviewError("breaker", doc["breaker"]["message"])
    result = {"plan_id": doc["plan_id"], "node": doc["node"], "closed": [], "already_closed": [], "skipped": []}
    with _run_lock():
        fresh = local_plan(cfg)
        if fresh.get("breaker"):
            raise ReviewError("breaker", fresh["breaker"]["message"])
        now_ok = {e["id"]: e for e in fresh[WOULD_CLOSE]}
        _audit({"kind": "apply_start", "plan_id": doc["plan_id"], "by": by, "ids": [e["id"] for e in doc[WOULD_CLOSE]]})
        for e in doc[WOULD_CLOSE]:
            f = now_ok.get(e["id"])
            same = f and (f["rule"], f["evidence"]["item"]["id"], f["fingerprint"], f["evidence"]["item"]["fingerprint"]) == \
                         (e["rule"], e["evidence"]["item"]["id"], e["fingerprint"], e["evidence"]["item"]["fingerprint"])
            if not same:
                why = "al gesloten" if _closed_by_plan(e, doc["plan_id"]) else "veranderd sinds de dry run"
                (result["already_closed"] if why == "al gesloten" else result["skipped"]).append({"id": e["id"], "why": why})
                continue
            outcome = _close_one(e, doc["plan_id"], by)
            (result["closed"] if outcome is None else result["skipped"]).append(e["id"] if outcome is None else {"id": e["id"], "why": outcome})
        _audit({"kind": "apply_end", "plan_id": doc["plan_id"], "by": by, **{k: result[k] for k in ("closed", "already_closed", "skipped")}})
    return result


def _closed_by_plan(e: dict, plan_id: str) -> bool:
    try:
        it = _find(store.load(e["task_id"]), e["id"])
    except (OSError, ValueError):
        return False
    return bool(it and (it.get("auto_resolution") or {}).get("plan_id") == plan_id and it.get("status") == actions.RESOLVED)


def _close_one(e: dict, plan_id: str, by: str) -> str | None:
    """Sluit één punt onder de locks van de betrokken taken (vaste volgorde). None = gesloten, anders de reden van overslaan."""
    if e["evidence"]["kind"] == "integration":
        return _close_integration_notice(e, plan_id, by)
    ev = e["evidence"]["item"]
    tids = sorted({e["task_id"], ev["task_id"]})
    with ExitStack() as stack:
        for tid in tids:
            stack.enter_context(store._flock(tid))
        try:
            st = store.load(e["task_id"])
            est = st if ev["task_id"] == e["task_id"] else store.load(ev["task_id"])
        except (OSError, ValueError):
            return "taak niet gevonden op deze node"
        if st.get("task_id") != e["task_id"]:
            return "verkeerde taak"
        it, proof = _find(st, e["id"]), _find(est, ev["id"])
        if not it or it.get("status") != actions.OPEN or fingerprint(it) != e["fingerprint"]:
            return "punt veranderd of al gesloten"
        if not proof or fingerprint(proof) != ev["fingerprint"]:
            return "bewijs veranderd"
        if e["evidence"]["kind"] == "survivor" and proof.get("status") != actions.OPEN:
            return "het blijvende punt is niet meer open"
        if e["evidence"]["kind"] == "decision" and not _is_decision(proof):
            return "het besluit geldt niet meer"
        note = f"Automatisch gesloten door actiebeheer ({plan_id}): {RULES[e['rule']]} Bewijs: {ev['id']} (taak {ev['task_id']})."
        _audit({"kind": "close_intent", "plan_id": plan_id, "id": e["id"], "task_id": e["task_id"], "rule": e["rule"],
                "evidence": ev["id"], "before": {k: it.get(k) for k in ("status", "resolved_at", "resolved_by", "resolved_note")}})
        auto = {"plan_id": plan_id, "rule": e["rule"], "evidence": ev["id"], "evidence_task": ev["task_id"], "at": store.now(),
                "by": by, "fingerprint": e["fingerprint"]}
        it.update(status=actions.RESOLVED, resolved_at=auto["at"], resolved_by=BY, resolved_note=_clip(note, actions.NOTE_CHARS),
                  auto_resolution=auto)
        it.setdefault("history", []).append({"at": auto["at"], "event": "auto_closed", "plan_id": plan_id, "by": by})
        store.save(st)
        _audit({"kind": "closed", "plan_id": plan_id, "id": e["id"], "task_id": e["task_id"], "rule": e["rule"], "evidence": ev["id"]})
    return None


def _close_integration_notice(e: dict, plan_id: str, by: str) -> str | None:
    with store._flock(e["task_id"]):
        state = store.load(e["task_id"])
        item = _find(state, e["id"])
        if not item or item.get("status") != actions.OPEN or fingerprint(item) != e["fingerprint"]:
            return "punt veranderd of al gesloten"
        proof = _integration_notice_proof(item, state, guard_from_dossiers())
        if not proof or _sha(proof) != e["evidence"]["item"]["fingerprint"]:
            return "integratiebewijs veranderd of ontbreekt"
        before = {k: item.get(k) for k in ("status", "resolved_at", "resolved_by", "resolved_note")}
        _audit({"kind": "close_intent", "plan_id": plan_id, "id": e["id"], "task_id": e["task_id"],
                "rule": e["rule"], "evidence": proof, "before": before})
        at = store.now()
        note = f"Administratief gesloten: integratie van {e['task_id']} bewezen met {proof['request']}, {proof['grant']}, commit {proof['commit']}."
        item.update(status=actions.RESOLVED, resolved_at=at, resolved_by=BY, resolved_note=_clip(note, actions.NOTE_CHARS),
                    auto_resolution={"plan_id": plan_id, "rule": e["rule"], "at": at, "by": by,
                                     "evidence": proof, "fingerprint": e["fingerprint"]})
        item.setdefault("history", []).append({"at": at, "event": "auto_closed", "plan_id": plan_id, "by": by})
        store.save(state)
        _audit({"kind": "closed", "plan_id": plan_id, "id": e["id"], "task_id": e["task_id"],
                "rule": e["rule"], "evidence": proof})
    return None


def undo(plan_id: str, by: str, ids: list | None = None) -> dict:
    """Zet punten die dit plan automatisch sloot weer open (idempotent). Een punt dat sindsdien door iemand anders is gewijzigd
    (andere resolved_by of plan) blijft zoals het is. Werkt ook als automatisch sluiten uit staat."""
    if not isinstance(plan_id, str) or not plan_id.startswith("AR-"):
        raise ReviewError("invalid_input", "plan_id ontbreekt of is ongeldig")
    want = set(ids) if ids else None
    result = {"plan_id": plan_id, "reopened": [], "untouched": []}
    with _run_lock():
        for st0 in store.all_tasks():
            hits = [i for i in st0.get("human_actions") or [] if isinstance(i, dict)
                    and (i.get("auto_resolution") or {}).get("plan_id") == plan_id and (want is None or i.get("id") in want)]
            if not hits:
                continue
            with store._flock(st0["task_id"]):
                st = store.load(st0["task_id"])
                changed = False
                for it in st.get("human_actions") or []:
                    if not isinstance(it, dict) or (it.get("auto_resolution") or {}).get("plan_id") != plan_id:
                        continue
                    if want is not None and it.get("id") not in want:
                        continue
                    if it.get("status") != actions.RESOLVED or it.get("resolved_by") != BY:
                        result["untouched"].append(it["id"])
                        continue
                    auto = it.pop("auto_resolution")
                    for k in ("resolved_at", "resolved_by", "resolved_note"):
                        it.pop(k, None)
                    it["status"] = actions.OPEN
                    it.setdefault("history", []).append({"at": store.now(), "event": "reopened", "plan_id": plan_id, "by": by,
                                                         "was": auto})
                    result["reopened"].append(it["id"])
                    changed = True
                if changed:
                    store.save(st)
        _audit({"kind": "undo", "plan_id": plan_id, "by": by, **{k: result[k] for k in ("reopened", "untouched")}})
    return result
