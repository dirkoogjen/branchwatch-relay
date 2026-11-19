"""Reviewed full-scope evidence for actionclosure; never creates queue jobs.

Finding sources is research, not proof. An operator seals the exact candidate hash
only after a separate reviewer checked every action field and the primary sources.
The software verifies identity, coverage, bytes, policy, execution and integration;
it cannot infer semantic completeness from prose. Unreviewed/legacy claims fail closed.
Approved files are content-addressed. They do not grant any operational capability.
"""
import copy
import hashlib
import json
import os
import re
from contextlib import nullcontext
from pathlib import Path

from . import actionreview, actions, config, integration_check as ic, jobs, store

EID = re.compile(r"E-[0-9a-f]{64}")
SCOPE_FIELDS = ("text", "reason", "details")
ACTION_FIELDS = ("id", "text", "reason", "details", "topic", "new_info", "source", "status", "created_at")
JOB_FIELDS = ("id", "task_id", "run", "node", "repo", "title", "prompt", "created_at", "status", "outcome", "check",
              "held", "approval", "archived", "cancel_requested", "continuation", "supervisor", "require_check",
              "integration_workflow", "integration_obligation", "integration_disposition")


class EvidenceError(ValueError):
    pass


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def action_digest(item, st):
    return digest({"item": {k: item.get(k) for k in ACTION_FIELDS}, "task_id": st.get("task_id"), "repo": st.get("repo")})


def job_digest(job):
    return digest({k: job.get(k) for k in JOB_FIELDS})


def approved_dir():
    return config.r2_home() / "actionevidence" / "approved"


def _need(ok, why):
    if not ok:
        raise EvidenceError(why)


def _item(tid, aid):
    _need(isinstance(tid, str) and bool(re.fullmatch(r"R2-[0-9a-f]{12}", tid)), "invalid task identity")
    st = store.load(tid)
    items = [i for i in st.get("human_actions") or [] if isinstance(i, dict) and i.get("id") == aid]
    _need(len(items) == 1, "action missing or ambiguous")
    return st, items[0]


def _deferred(register, aid):
    return any(a.get("id") == aid and a.get("relatie") == "open-actie:UITGESTELD"
               for b in (register or {}).get("besluiten") or [] for a in b.get("acties") or [])


def _sources(p, st):
    sources = p.get("sources")
    _need(isinstance(sources, list) and 1 <= len(sources) <= 40, "primary sources missing")
    out = {}
    roots = (Path(st["repo"]).resolve(), config.r2_home().resolve())
    for s in sources:
        _need(isinstance(s, dict) and isinstance(s.get("id"), str) and s["id"] not in out, "invalid/duplicate source id")
        path = Path(s.get("path") or "")
        _need(path.is_absolute() and any(path.resolve(strict=True).is_relative_to(r) for r in roots), "source outside repository/Runner evidence roots")
        raw = ic.raw(path)
        _need(hashlib.sha256(raw).hexdigest() == s.get("sha256"), "source bytes changed")
        quote = s.get("quote")
        _need(isinstance(quote, str) and quote.strip() and quote in raw.decode("utf-8"), "source quote not found")
        _need(s.get("role") in ("decision", "execution", "tests", "duplicate"), "unknown source role")
        out[s["id"]] = s
    return out


def validate(p, item, st, job, cfg, register, *, reviewed=True):
    """Validate all proof afresh; exceptions are converted to a conservative blocker."""
    try:
        _need(isinstance(p, dict) and p.get("version") == 1, "invalid evidence version")
        _need(p.get("kind") in ("completed", "duplicate"), "invalid evidence kind")
        _need(isinstance(register, dict), "decision register unavailable; deferred state unknown")
        _need(item.get("status") == actions.OPEN, "action no longer open")
        _need((p.get("action_id"), p.get("task_id"), p.get("node"), p.get("repo")) ==
              (item.get("id"), st.get("task_id"), config.local_node(), st.get("repo")), "action/node/repository identity changed")
        _need(p.get("action_sha256") == action_digest(item, st), "action scope changed")
        guard = actionreview.guard_from_dossiers()
        _need(not guard.get("errors"), "supervision protection inventory unavailable/invalid")
        _need(not actionreview.protected(item, st, guard), "protected action")
        _need(not _deferred(register, item["id"]), "deferred approval/action")
        _need(p.get("remaining") == [] and p.get("contradictions") == [], "remaining or contradictory evidence")
        _need(isinstance(p.get("prepared_by"), str) and p["prepared_by"].strip(), "researcher missing")
        src = _sources(p, st)
        covered = set()
        for c in p.get("coverage") or []:
            k = c.get("field")
            _need(k in SCOPE_FIELDS and k not in covered and bool(item.get(k)), "invalid/duplicate scope field")
            _need(c.get("requirement") == item[k] and c.get("finding") == "complete", "incomplete action requirement")
            _need(isinstance(c.get("sources"), list) and c["sources"] and all(s in src for s in c["sources"]), "coverage sources missing")
            covered.add(k)
        _need(covered == {k for k in SCOPE_FIELDS if item.get(k)}, "action field not covered")
        if reviewed:
            review = p.get("review") or {}
            base = {k: v for k, v in p.items() if k != "review"}
            _need(review.get("candidate_sha256") == digest(base), "review does not cover candidate bytes")
            _need(review.get("full_scope") is True and review.get("by") and review.get("by") != p["prepared_by"]
                  and review.get("at") and review.get("note"), "independent full-scope review missing")
            for tid in (st["task_id"], job and job.get("task_id")):
                _need(not tid or tid not in review["by"], "source/executing task cannot review itself")
        if p["kind"] == "duplicate":
            survivor = p.get("survivor") or {}
            st2, i2 = _item(survivor.get("task_id"), survivor.get("action_id"))
            _need(i2["id"] != item["id"] and st2.get("repo") == st.get("repo"), "duplicate must have a different survivor in same repo")
            _need(i2.get("status") == actions.OPEN and not i2.get("auto_resolution"), "survivor must remain open")
            cl = i2.get("closure_claim") or {}
            _need(not cl or cl.get("revoked"), "survivor itself is claimed; no cascades")
            _need(not actionreview.protected(i2, st2, actionreview.guard_from_dossiers()) and not _deferred(register, i2["id"]), "survivor protected/deferred")
            _need(survivor.get("sha256") == action_digest(i2, st2), "survivor scope changed")
            _need(actionreview._before(i2.get("created_at"), item.get("created_at")) and
                  actionreview._epoch(i2.get("created_at")) is not None, "survivor must be older")
            _need(not p.get("job_id") and not p.get("job_sha256"), "duplicate cannot claim execution")
            _need(any(s["role"] == "duplicate" for s in src.values()), "primary duplicate comparison missing")
        else:
            _need(job is not None and p.get("job_id") == job.get("id") and p.get("job_sha256") == job_digest(job), "job identity/scope changed")
            _need(job.get("node") == config.local_node() and job.get("repo") == st.get("repo"), "job wrong node/repository")
            _need(job.get("status") == jobs.DONE and (job.get("outcome") or {}).get("kind") in (None, "done"), "execution not completed")
            _need(not any(job.get(k) for k in ("held", "approval", "cancel_requested", "continuation", "supervisor")), "job held/gated/chain")
            _need(job.get("task_id") != st["task_id"] and job.get("task_id") != (item.get("source") or {}).get("task_id"), "same task cannot prove itself")
            _need(actionreview._epoch(job.get("created_at")) is not None and
                  not actionreview._before(job.get("created_at"), item.get("created_at")), "job predates action")
            chk = job.get("check") or {}
            _need(chk.get("state") == "ok" and chk.get("source") in ("integrator", "mens", "chatgpt"), "execution not checked")
            roles = {s["role"] for s in src.values()}
            _need({"execution", "tests"} <= roles, "execution/test source missing")
            dec = p.get("decision") or {}
            from .actionclosure import _QUESTION
            if dec.get("kind") == "not_required":
                _need(not _QUESTION.search(item.get("text") or "") and dec.get("reason"), "question needs primary decision")
            else:
                _need(dec.get("kind") == "primary" and dec.get("sources") and
                      all(s in src and src[s]["role"] == "decision" for s in dec["sources"]), "primary decision missing")
            if bool(job.get("integration_workflow")) or job.get("node") in (cfg.get("jobs_integration_check_nodes") or []):
                _need(ic.satisfied(job), "stored integration evidence missing")
                live = ic.fetch(job)
                _need(ic.valid_reply(live, job), "live integration evidence invalid")
                expected = chk["evidence"]
                _need(all(live.get(k) == expected.get(k) for k in ("request", "grant", "commit", "audit_hash", "tests")), "integration identity changed")
        return {"ok": True, "digest": digest(p), "kind": p["kind"], "packet": p, "reasons": []}
    except (ValueError, OSError, KeyError, TypeError, AttributeError) as e:
        return {"ok": False, "reasons": [str(e)[:250] or type(e).__name__]}


def load(eid):
    _need(isinstance(eid, str) and bool(EID.fullmatch(eid)), "full-scope evidence missing")
    p = ic.read(approved_dir() / (eid + ".json"))
    _need(eid == "E-" + digest(p), "approved evidence changed")
    return p


def _approval_logged(eid):
    from . import actionclosure as ac
    _need(not ac.verify_audit(), "approval audit chain invalid")
    _need(any(r.get("kind") == "evidence_approved" and r.get("evidence_id") == eid for r in ac.read_audit()),
          "approval audit incomplete; no claim allowed")


def verify(eid, item, st, job, cfg, register):
    try:
        p = load(eid)
        _approval_logged(eid)
        return validate(p, item, st, job, cfg, register)
    except (ValueError, OSError, TypeError) as e:
        return {"ok": False, "reasons": [str(e)[:250]]}


def approve(candidate, expected_sha256, by, note, *, cfg=None):
    """Local operator acknowledgment of an independently reviewed exact candidate.

    Does not make a claim, close an action, run tests or create/start a job.
    Not exposed through task RPC/MCP. R2_TASK_ID explicitly forbids task use.
    """
    from . import actionclosure as ac
    _need(not os.environ.get("R2_TASK_ID"), "tasks cannot seal reviews")
    _need(isinstance(candidate, dict) and "review" not in candidate and digest(candidate) == expected_sha256, "candidate differs from reviewed hash")
    _need(isinstance(by, str) and by.strip() and by != candidate.get("prepared_by") and isinstance(note, str) and note.strip(), "separate reviewer and substantive review required")
    cfg = config.load_config() if cfg is None else cfg
    with ac._run_lock():
        st, it = _item(candidate.get("task_id"), candidate.get("action_id"))
        job = jobs.load()["jobs"].get(candidate.get("job_id"))
        reg = ac.load_register()
        result = validate(candidate, it, st, job, cfg, reg, reviewed=False)
        _need(result["ok"], "; ".join(result["reasons"]))
        # Lost replies and interrupted writes retry the same approval, never create
        # a second eligible proof just because the wall clock advanced.
        for existing_path in sorted(approved_dir().glob("E-*.json")):
            existing = load(existing_path.stem)
            review = existing.get("review") or {}
            if (review.get("candidate_sha256"), review.get("by"), review.get("note")) == (expected_sha256, by, note):
                valid = validate(existing, it, st, job, cfg, reg)
                _need(valid["ok"], "; ".join(valid["reasons"]))
                if not any(r.get("kind") == "evidence_approved" and r.get("evidence_id") == existing_path.stem for r in ac.read_audit()):
                    ac._audit({"kind": "evidence_approved", "action_id": it["id"], "evidence_id": existing_path.stem, "recovered": True})
                return {"id": existing_path.stem, "path": str(existing_path)}
        p = copy.deepcopy(candidate)
        p["review"] = {"candidate_sha256": expected_sha256, "by": by, "note": note, "at": store.now(), "full_scope": True}
        result = validate(p, it, st, job, cfg, reg)
        _need(result["ok"], "; ".join(result["reasons"]))
        eid = "E-" + digest(p); path = approved_dir() / (eid + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        ac._audit({"kind": "evidence_approval_intent", "action_id": it["id"], "evidence_id": eid, "candidate_sha256": expected_sha256, "review": p["review"]})
        store.atomic_write(path, json.dumps(p, sort_keys=True, ensure_ascii=False, indent=1) + "\n")
        ac._audit({"kind": "evidence_approved", "action_id": it["id"], "evidence_id": eid})
        return {"id": eid, "path": str(path)}


def transfer(cfg=None, *, apply=False, register=None):
    """Bounded, idempotent reassessment-to-claim handoff; queue is read-only."""
    from . import actionclosure as ac, cplane
    cfg = config.load_config() if cfg is None else cfg
    register = ac.load_register() if register is None else register
    out = {"eligible": [], "claimed": [], "blocked": [], "breaker": None}
    if not approved_dir().exists():
        return out
    if apply:
        _need(cplane.is_primary(), "handoff only on primary control plane")
    with ac._run_lock() if apply else nullcontext():
        if apply:
            ac._recover_audit()
        candidates = {}
        queue = jobs.load()
        for path in sorted(approved_dir().glob("E-*.json")):
            try:
                p = load(path.stem); _approval_logged(path.stem); st, it = _item(p.get("task_id"), p.get("action_id"))
                if it.get("status") != actions.OPEN:
                    continue
                old = it.get("closure_claim") or {}
                if old:
                    if old.get("evidence_id") != path.stem or old.get("revoked"):
                        out["blocked"].append({"id": it["id"], "reason": "existing or revoked claim preserved"})
                    continue
                v = validate(p, it, st, queue["jobs"].get(p.get("job_id")), cfg, register)
                _need(v["ok"], "; ".join(v["reasons"]))
                candidates.setdefault(it["id"], []).append((path.stem, p))
            except (ValueError, OSError, KeyError, TypeError) as e:
                out["blocked"].append({"evidence_id": path.stem, "reason": str(e)[:250]})
        selected = []
        for aid, choices in candidates.items():
            if len(choices) != 1:
                out["blocked"].append({"id": aid, "reason": "multiple reviewed proofs; operator must choose"})
            else:
                selected.append(choices[0]); out["eligible"].append(aid)
        limit = int(cfg.get("action_evidence_transfer_max", 3))
        if len(selected) > limit or limit < 1:
            out["breaker"] = f"{len(selected)} reviewed claims exceed limit {limit}; nothing transferred"
            return out
        if apply:
            for eid, p in selected:
                try:
                    r = ac._claim(p["action_id"], p.get("job_id"), "herbeoordeling", soort="technisch", evidence_id=eid,
                                  note="Volledige scope en primaire bronnen onafhankelijk beoordeeld", preserve_existing=True)
                    if r["changed"]:
                        out["claimed"].append(p["action_id"])
                except ac.ClosureError as e:
                    out["blocked"].append({"id": p["action_id"], "reason": e.message})
    return out


def main(argv=None):
    """Operator-only local interface; no task/RPC exposure."""
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_mutually_exclusive_group(required=True)
    commands.add_argument("--approve", metavar="CANDIDATE_JSON")
    commands.add_argument("--transfer", action="store_true")
    parser.add_argument("--sha256", help="exact candidate hash independently reviewed")
    parser.add_argument("--by", help="independent reviewer identity")
    parser.add_argument("--note", help="substantive review finding")
    parser.add_argument("--apply", action="store_true", help="transfer verified claims; default is read-only")
    args = parser.parse_args(argv)
    try:
        if args.approve:
            _need(not args.apply and args.sha256 and args.by and args.note, "approval requires --sha256, --by and --note; no --apply")
            result = approve(ic.read(args.approve), args.sha256, args.by, args.note)
        else:
            _need(not os.environ.get("R2_TASK_ID"), "task context cannot transfer operator proofs")
            result = transfer(apply=args.apply)
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0
    except (EvidenceError, OSError, ValueError) as e:
        print(json.dumps({"ok": False, "reason": str(e)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
