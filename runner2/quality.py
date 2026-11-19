"""Risicogestuurd kwaliteits-/mijlpaalbeleid van de supervisor (geen urenlange brede audits na elke keten).

Beleid in het kort:
- Gewone ketens (`feature`, `repair`) draaien alleen gerichte tests voor gewijzigde modules en directe afhankelijkheden.
- De judge bepaalt na een keten het review- en testoppervlak uit de WERKELIJK gewijzigde bestanden sinds de laatste
  kwaliteitsmijlpaal; een aparte review-keten (`quality`, scope `change_scoped`) blijft change- en dependency-scoped.
- Een brede controle (scope `broad`, volledige repository-/testsuite) vraagt een concrete brede-impactreden
  (BROAD_RISKS) plus motivering en valt onder een eigen budget; nooit automatisch.
- Bevindingen van een review leiden eerst tot een gerichte reparatie (`repair`) met relevante hertests.
- Opeenvolgende audit-only ketens worden geweigerd; kwaliteitsketens tellen mee in anti-churn/no-progress.
De afdwinging gebeurt in `validate_plan` (judge-tool én supervisor) en in de ketenlimieten; de prompts leggen het uit.
"""
import re

from . import gitops

KINDS = ("feature", "quality", "repair")
SCOPES = ("targeted", "change_scoped", "broad")
RISKS = ("shared_core", "architecture", "migration", "security_boundary", "cross_cutting_regression",
         "release_milestone", "user_request")
# Alleen deze redenen rechtvaardigen een volledige repository-/testsuite; een securitygrens alleen leidt tot
# een change-scoped review van precies die paden, niet tot een brede controle.
BROAD_RISKS = tuple(r for r in RISKS if r != "security_boundary")
# Risico's die een kwaliteitsmijlpaal ook bij een kleinere serie rechtvaardigen.
HIGH_RISKS = tuple(r for r in RISKS if r != "user_request")
KIND_SCOPES = {"feature": ("targeted", "broad"), "repair": ("targeted", "broad"), "quality": ("change_scoped", "broad")}
WILDCARDS = {"*", "**", ".", "./", "/", "repo", "repository", "all", "alles", "everything", "heel", "hele repo"}
MAX_TARGETS = 30
MAX_DEPENDENCIES = 10
MIN_RATIONALE = 15
CHANGED_SHOWN = 60

# Padhints voor het judge-prompt; uitsluitend aanwijzingen, de judge beslist.
HINTS = (
    ("migration", re.compile(r"migrat|alembic|schema|\.sql$|/versions?/", re.I)),
    ("security_boundary", re.compile(r"auth|secret|token|crypt|credential|permission|sandbox|security|keyring", re.I)),
    ("shared_core", re.compile(r"(^|/)(core|common|shared|base|platform)(/|_|\.)|(^|/)__init__\.py$|(^|/)config", re.I)),
)


def limits_from_config(cfg: dict, max_quality_chains=None, full_verification: bool = False) -> dict:
    return {"max_quality_chains": cfg["quality_default_max_chains"] if max_quality_chains is None else max_quality_chains,
            "max_broad": cfg["quality_max_broad"], "min_commits": cfg["quality_min_commits"],
            "review_after_chains": cfg["quality_review_after_chains"],
            "review_after_commits": cfg["quality_review_after_commits"],
            "chain_max_hours": cfg["quality_chain_max_hours"], "chain_max_tasks": cfg["quality_chain_max_tasks"],
            "broad_chain_max_hours": cfg["quality_broad_chain_max_hours"], "full_verification": bool(full_verification)}


def limits_of(sup: dict) -> dict:
    if sup.get("quality_limits"):
        return sup["quality_limits"]
    from . import config
    return limits_from_config(config.load_config())


def fresh_state(root_commit: str) -> dict:
    return {"quality_chains": 0, "broad_chains": 0, "chains_since_quality": 0, "commits_since_quality": 0,
            "commits_since_broad": 0, "milestone_commit": root_commit}


def state_of(sup: dict) -> dict:
    return sup.setdefault("quality", fresh_state(sup["root_commit"]))


def kind_of(entry: dict) -> str:
    return entry.get("kind") or "feature"


def scope_of(entry: dict) -> str:
    return (entry.get("verification") or {}).get("scope") or "targeted"


def _strs(value) -> list:
    return [v.strip() for v in value if isinstance(v, str) and v.strip()] if isinstance(value, list) else []


def normalize(args: dict) -> dict:
    """Het gekozen plan voor de volgende keten (met defaults: gewone feature-keten, gerichte tests)."""
    v = args.get("verification") if isinstance(args.get("verification"), dict) else {}
    kind = args.get("chain_kind") or "feature"
    scope = v.get("scope") or ("change_scoped" if kind == "quality" else "targeted")
    plan = {"kind": kind,
            "verification": {"scope": scope, "rationale": str(v.get("rationale") or "").strip()[:600],
                             "targets": _strs(v.get("targets"))[:MAX_TARGETS],
                             "dependencies": _strs(v.get("dependencies"))[:MAX_DEPENDENCIES],
                             "dependency_rationale": str(v.get("dependency_rationale") or "").strip()[:400],
                             "risk_factors": [r for r in _strs(v.get("risk_factors")) if r in RISKS]}}
    if kind == "repair":
        plan["findings"] = [f[:400] for f in _strs(args.get("findings"))[:10]]
    return plan


def changed_paths(sup: dict, tip: str | None = None) -> list | None:
    """Bestanden die sinds de laatste kwaliteitsmijlpaal werkelijk veranderden; None als git dat niet kan zeggen."""
    tip = tip or sup["chains"][-1].get("end_commit")
    base = state_of(sup).get("milestone_commit") or sup["root_commit"]
    if not tip:
        return None
    try:
        out = gitops.git(sup["repo"], "diff", "--name-only", f"{base}..{tip}").stdout
    except (RuntimeError, OSError):
        return None
    return sorted({p.strip() for p in out.splitlines() if p.strip()})


def _covered(target: str, changed: list) -> bool:
    """Een target is gedekt als het een gewijzigd bestand is of een (niet-toplevel) map met een gewijzigd bestand."""
    t = target.strip().strip("/")
    if t in changed:
        return True
    return t.count("/") >= 1 and any(p.startswith(t + "/") for p in changed)


def validate_plan(args: dict, sup: dict | None = None, changed: list | None = None) -> str | None:
    """Foutmelding (Nederlands, voor de judge) of None. Zonder `sup` alleen de statische controles."""
    kind = args.get("chain_kind") or "feature"
    if kind not in KINDS:
        return f"chain_kind moet een van {', '.join(KINDS)} zijn"
    raw = args.get("verification")
    if raw is not None and not isinstance(raw, dict):
        return "verification moet een object zijn (scope, rationale, targets, risk_factors)"
    if "findings" in args and not isinstance(args["findings"], list):
        return "findings moet een lijst van teksten zijn"
    for key in ("targets", "dependencies", "risk_factors"):
        if raw and key in raw and not isinstance(raw[key], list):
            return f"verification.{key} moet een lijst van teksten zijn"
    plan = normalize(args)
    v = plan["verification"]
    scope = v["scope"]
    if scope not in SCOPES:
        return f"verification.scope moet een van {', '.join(SCOPES)} zijn"
    if scope not in KIND_SCOPES[kind]:
        return (f"chain_kind {kind} gebruikt verification.scope {' of '.join(KIND_SCOPES[kind])}, niet {scope}: gewone ketens draaien "
                "alleen gerichte tests; een review-keten is change-scoped; breed (volledige suite) alleen met een brede-impactreden")
    if raw and any(r not in RISKS for r in _strs(raw.get("risk_factors"))):
        return f"verification.risk_factors moeten uit {', '.join(RISKS)} komen"
    for t in v["targets"] + v["dependencies"]:
        if t.lower() in WILDCARDS or len(t) > 200:
            return f"target '{t[:40]}' is te breed of te lang: noem concrete gewijzigde bestanden, modules of tests"
    if kind == "repair" and not plan["findings"]:
        return "chain_kind repair vraagt findings: de concrete reviewbevindingen die gericht gerepareerd moeten worden"
    if kind == "quality":
        if len(v["rationale"]) < MIN_RATIONALE:
            return "een kwaliteitsketen vraagt verification.rationale: waarom nu een review en waarom dit oppervlak"
        if scope == "change_scoped" and not v["targets"]:
            return "een change-scoped kwaliteitsketen vraagt verification.targets: de gewijzigde bestanden/mappen die je laat reviewen"
        if v["dependencies"] and len(v["dependency_rationale"]) < MIN_RATIONALE:
            return "verification.dependencies (ongewijzigde code) vraagt dependency_rationale: welke concrete afhankelijkheid maakt dat nodig"
    if scope == "broad":
        broad = [r for r in v["risk_factors"] if r in BROAD_RISKS]
        if not broad:
            return (f"een brede controle (volledige repository-/testsuite) vraagt in verification.risk_factors een concrete brede impact: "
                    f"{', '.join(BROAD_RISKS)}; een securitygrens alleen rechtvaardigt een change-scoped review")
        if len(v["rationale"]) < MIN_RATIONALE:
            return "een bredere/duurdere controle vraagt verification.rationale: motiveer waarom gerichte tests niet volstaan"
    if sup is None:
        return None
    return _validate_state(plan, sup, changed)


def _validate_state(plan: dict, sup: dict, changed: list | None) -> str | None:
    kind, v = plan["kind"], plan["verification"]
    lim, st = limits_of(sup), state_of(sup)
    if kind == "quality":
        if kind_of(sup["chains"][-1]) == "quality":
            return ("er volgde al een kwaliteitsketen op de vorige keten: geen tweede audit achter elkaar. Laat bevindingen "
                    "eerst gericht repareren (chain_kind repair), doe featurewerk of beslis project_complete")
        if st["quality_chains"] >= lim["max_quality_chains"]:
            return f"het budget van {lim['max_quality_chains']} kwaliteitsketens is op; ga door met gericht featurewerk of rond af"
        if st["commits_since_quality"] < 1:
            return "er zijn geen nieuwe commits sinds de vorige kwaliteitsmijlpaal: er valt niets te reviewen"
        if st["commits_since_quality"] < lim["min_commits"] and not any(r in HIGH_RISKS for r in v["risk_factors"]):
            return (f"de serie is te klein voor een aparte review ({st['commits_since_quality']} commits sinds de laatste mijlpaal, "
                    f"minimaal {lim['min_commits']}) en er is geen hoog risico opgegeven ({', '.join(HIGH_RISKS)})")
        if v["scope"] == "change_scoped" and changed is not None:
            for t in v["targets"]:
                if not _covered(t, changed):
                    return (f"target '{t[:80]}' is geen gewijzigd bestand of gewijzigde submap sinds de laatste mijlpaal: audits "
                            "onderzoeken geen onaangeraakte code zonder concrete reden (noem het onder dependencies mét dependency_rationale)")
    if v["scope"] == "broad":
        if st["broad_chains"] >= lim["max_broad"]:
            return f"het budget van {lim['max_broad']} brede controle(s) is op; werk met gerichte tests"
        if st["commits_since_broad"] < 1:
            return "er zijn geen nieuwe commits sinds de vorige brede controle: nog een volledige run heeft geen zin"
        if "user_request" in v["risk_factors"] and not lim["full_verification"] \
                and not any(r in BROAD_RISKS and r != "user_request" for r in v["risk_factors"]):
            return "user_request is alleen geldig als de gebruiker bij de start een volledige controle vroeg (full_verification)"
    return None


def record_chain_end(sup: dict, entry: dict) -> None:
    """Bijwerken van de teller sinds de laatste mijlpaal/brede controle wanneer een keten definitief eindigt."""
    st = state_of(sup)
    commits = entry.get("commits") or 0
    if kind_of(entry) == "quality":
        st.update(chains_since_quality=0, commits_since_quality=0, milestone_commit=entry.get("end_commit") or st["milestone_commit"])
    else:
        st["chains_since_quality"] += 1
        st["commits_since_quality"] += commits
    st["commits_since_broad"] = 0 if scope_of(entry) == "broad" else st["commits_since_broad"] + commits


def record_planned(sup: dict, plan: dict) -> None:
    st = state_of(sup)
    if plan["kind"] == "quality":
        st["quality_chains"] += 1
    if plan["verification"]["scope"] == "broad":
        st["broad_chains"] += 1


def review_due(sup: dict) -> bool:
    lim, st = limits_of(sup), state_of(sup)
    if kind_of(sup["chains"][-1]) == "quality" or st["commits_since_quality"] < 1 or st["quality_chains"] >= lim["max_quality_chains"]:
        return False
    return st["chains_since_quality"] >= lim["review_after_chains"] or st["commits_since_quality"] >= lim["review_after_commits"]


def chain_limits(sup: dict, entry: dict, limits: dict) -> dict:
    """Tijd-/schakelbudget van een keten: een kwaliteitsketen krijgt het (kleinere) controlebudget."""
    if kind_of(entry) != "quality":
        return limits
    lim = limits_of(sup)
    hours = lim["broad_chain_max_hours"] if scope_of(entry) == "broad" else lim["chain_max_hours"]
    return dict(limits, max_hours=min(limits["max_hours"], hours), max_tasks=min(limits["max_tasks"], lim["chain_max_tasks"]))


def hints(paths: list) -> list:
    out = []
    for name, rx in HINTS:
        n = sum(1 for p in paths if rx.search(p))
        if n:
            out.append(f"{name}: {n}")
    return out


def judge_block(sup: dict) -> str:
    lim, st = limits_of(sup), state_of(sup)
    changed = changed_paths(sup)
    lines = ["KWALITEITSBELEID (risicogestuurd; geen brede audit of volledige suite na elke keten):",
             f"- Sinds de laatste kwaliteitsmijlpaal: {st['chains_since_quality']} ketens, {st['commits_since_quality']} commits; "
             f"sinds de laatste brede controle: {st['commits_since_broad']} commits.",
             f"- Budget: kwaliteitsketens {st['quality_chains']}/{lim['max_quality_chains']} gebruikt (elk max {lim['chain_max_hours']} u, "
             f"{lim['chain_max_tasks']} schakels), brede controles {st['broad_chains']}/{lim['max_broad']} gebruikt (max "
             f"{lim['broad_chain_max_hours']} u)."]
    if changed is None:
        lines.append("- Gewijzigde bestanden: niet vast te stellen (git); werk met git diff --name-only in je worktree.")
    else:
        shown = changed[:CHANGED_SHOWN]
        extra = f" (+{len(changed) - CHANGED_SHOWN} meer)" if len(changed) > CHANGED_SHOWN else ""
        lines.append(f"- Werkelijk gewijzigd sinds de laatste mijlpaal ({len(changed)} bestanden){extra}: " + ", ".join(shown))
        if h := hints(changed):
            lines.append("- Padhints voor risico (aanwijzing, jij beslist): " + "; ".join(h))
    if review_due(sup):
        lines.append("- De serie is substantieel genoeg voor een aparte, change-scoped review-mijlpaal; kies die alleen als het "
                     "risico dat rechtvaardigt, anders gewoon gericht featurewerk.")
    lines.append(
        "REGELS: (1) Gewone ketens (feature/repair): alleen gerichte tests voor gewijzigde modules en hun directe afhankelijkheden. "
        "(2) Bepaal review-/testoppervlak uit de werkelijk gewijzigde bestanden, afhankelijkheden, migraties, securitygrenzen en "
        "regressierisico. (3) Een aparte review (chain_kind quality, verification.scope change_scoped) blijft change-scoped: noem "
        "targets uit de gewijzigde bestanden; ongewijzigde code alleen via dependencies met dependency_rationale; nooit twee kwaliteitsketens "
        "achter elkaar. (4) Volledige repository-/testsuite (verification.scope broad) ALLEEN bij concrete brede impact: "
        f"{', '.join(BROAD_RISKS)} (user_request alleen als de gebruiker dat bij de start vroeg), met motivering in verification.rationale. "
        "(5) Bevindingen: eerst gerichte reparatie (chain_kind repair met findings) en relevante hertests, niet automatisch een volledige suite. "
        "(6) Draai zelf in deze beoordeling alleen gerichte controles. Veiligheidsregels blijven altijd gelden.")
    return "\n".join(lines)


def assignment_block(entry: dict) -> str:
    """Testbeleid voor de ketenopdracht (naast de vaste veiligheidsregels)."""
    kind, v = kind_of(entry), entry.get("verification") or {}
    scope = scope_of(entry)
    risks = ", ".join(v.get("risk_factors") or []) or "geen opgegeven"
    if kind == "quality":
        lines = [
            "Dit is een KWALITEITSKETEN (review-mijlpaal), geen featurewerk. Review change-scoped: alleen de gewijzigde bestanden "
            "hieronder en hun directe afhankelijkheden; lees geen ongewijzigde code zonder concrete reden en voer geen brede audit uit.",
            f"Reviewoppervlak: {', '.join(v.get('targets') or []) or '(zie opdracht)'}",
            f"Rationale: {v.get('rationale', '')}"]
        if v.get("dependencies"):
            lines.append(f"Extra (ongewijzigde) afhankelijkheden: {', '.join(v['dependencies'])} — reden: {v.get('dependency_rationale', '')}")
        lines.append("Bevindingen repareer je gericht (kleine, relevante fixes) en je hertest alleen wat raakt aan die fixes; "
                     "geef in summary de bevindingen (open en gerepareerd) per bestand. Zonder bevindingen: zeg dat expliciet en doe niets erbij.")
    elif kind == "repair":
        lines = ["Dit is een REPARATIEKETEN voor concrete reviewbevindingen; los alleen deze op en draai gerichte hertests voor wat je raakt "
                 "en zijn directe afhankelijkheden:", *[f"- {f}" for f in entry.get("findings") or []]]
    else:
        lines = ["Testbeleid: draai alleen gerichte tests voor de modules die je wijzigt en hun directe afhankelijkheden."]
    if scope == "broad":
        lines.append(f"Brede controle toegestaan (volledige repository-/testsuite), want brede impact: {risks}. "
                     f"Motivering: {v.get('rationale', '')}. Houd je aan het tijdbudget van deze keten.")
    else:
        lines.append("Draai GEEN volledige repository- of testsuite; breder testen doe je alleen als deze opdracht dat expliciet toestaat.")
    return "\n".join(lines)


def describe_plan(plan_or_entry: dict) -> dict:
    """Compact: welk soort keten volgt, met welk scope en welke motivering."""
    v = plan_or_entry.get("verification") or {}
    out = {"kind": kind_of(plan_or_entry), "scope": v.get("scope") or "targeted",
           "rationale": (v.get("rationale") or "")[:300], "targets": (v.get("targets") or [])[:6]}
    if v.get("risk_factors"):
        out["risk_factors"] = v["risk_factors"]
    if plan_or_entry.get("findings"):
        out["findings"] = [f[:150] for f in plan_or_entry["findings"][:5]]
    return out


def describe_state(sup: dict) -> dict:
    lim, st = limits_of(sup), state_of(sup)
    return {"quality_chains": f"{st['quality_chains']}/{lim['max_quality_chains']}",
            "broad_checks": f"{st['broad_chains']}/{lim['max_broad']}",
            "chains_since_quality": st["chains_since_quality"], "commits_since_quality": st["commits_since_quality"],
            "review_due": review_due(sup)}
