"""Voltooiingscontrole (completion gate) voor autonome ketens.

Claude bepaalt wat een schakel heeft afgerond; de controller bepaalt of de KETEN mag stoppen. Meldt een schakel
`goal_reached`, dan accepteert Runner 2 dat niet op zijn woord maar start een aparte, onafhankelijke controletaak
(rol `review`, worktree op de tip, gewone Runner 2-taak) die het ketendoel naast roadmap/backlog/documentatie van de
repository legt. Dit bestand bevat alleen de pure delen: de deterministische backlogscan (eigen bewijs van de controller),
de prompt, het beslisschema en de validatie. De toestandsmachine zit in `chains.py`.

Beslissingen van de controle:
- goal_complete   het ketendoel is aantoonbaar gereed (met bewijs) -> de keten mag eindigen.
- work_remaining  er is nog zinvol, uitvoerbaar werk binnen het ketendoel -> `goal_reached` wordt geweigerd, volgende schakel.
- human_blocked   geen uitvoerbaar werk meer door een echte menselijke blokkade -> een gewone vraag (waiting_for_human).

Niet-blokkerende menselijke actiepunten dwingen niets af en houden ander uitvoerbaar werk niet tegen.
"""
import hashlib
import re

from . import gitops

VERDICTS = ("goal_complete", "work_remaining", "human_blocked")
# Waarom een item uit de scan geen resterend uitvoerbaar werk is. human_blocked alleen bij een echte blokkade.
DISMISS = ("done", "out_of_scope", "not_actionable", "human_followup", "human_blocked")
MAX_SCAN_ITEMS = 30
MAX_SCAN_FILES = 40
MAX_FILE_BYTES = 262144
ITEM_CHARS = 160
REASON_CHARS = 1500
NEXT_STEP_CHARS = 1500
PROMPT_CHARS = 30000

_NAME = re.compile(r"(roadmap|backlog|todo|to-do|plan|status|project[_-]?state|tasks?|checklist|milestone|fases?|sprint)", re.I)
_EXT = (".md", ".txt", ".rst", ".markdown")
_SKIP = ("node_modules/", ".venv/", "venv/", "vendor/", "site-packages/", "dist/", "build/", ".git/", "CHANGELOG", "RELEASE_NOTES")
_CHECKBOX = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+\[ \]\s+(\S.*)$")
_TODO = re.compile(r"^\s*(?:[-*+]\s+)?(?:TODO|OPEN|NOG TE DOEN)\s*[:\-]\s*(\S.*)$", re.I)
_OPEN_STATUS = frozenset({"todo", "to do", "open", "pending", "planned", "gepland", "te doen", "nog te doen", "nog open",
                          "niet gestart", "not started", "backlog", "in progress", "in uitvoering", "wip", "next", "volgende"})
_PRIORITY = ("roadmap", "backlog", "todo", "plan", "status", "project_state")


def _clip(text, n: int) -> str:
    text = " ".join(str(text or "").split())
    return text if len(text) <= n else text[: n - 1] + "…"


def item_id(path: str, text: str) -> str:
    return "B" + hashlib.sha1(f"{path}\0{' '.join(text.lower().split())}".encode()).hexdigest()[:8]


def _candidate(path: str) -> bool:
    low = path.lower()
    if not low.endswith(_EXT) or any(s.lower() in low for s in _SKIP):
        return False
    base = low.rsplit("/", 1)[-1]
    return bool(_NAME.search(base)) or (low.startswith("docs/") and bool(_NAME.search(low)))


def _rank(path: str) -> tuple:
    low = path.lower()
    return (next((i for i, k in enumerate(_PRIORITY) if k in low), len(_PRIORITY)), low.count("/"), low)


def _open_items(path: str, text: str) -> list:
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        m = _CHECKBOX.match(line) or _TODO.match(line)
        body = m.group(1).strip() if m else None
        if not body and line.lstrip().startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if any(c.strip("*_` ").lower() in _OPEN_STATUS for c in cells[1:]) and cells and not set(cells[0]) <= set("-: "):
                body = " | ".join(c for c in cells if c)
        if body:
            out.append({"id": item_id(path, body), "file": path, "line": n, "text": _clip(body, ITEM_CHARS)})
    return out


def scan_backlog(worktree, commit: str, extra_files: list | None = None) -> dict:
    """Open items in roadmap-/backlog-/statusbestanden op exact `commit` (niet de werkmap): onafhankelijk bewijs van de
    controller. Geeft {"files", "items" (max MAX_SCAN_ITEMS), "total_open", "truncated", "error"?}."""
    try:
        names = gitops.git(worktree, "ls-tree", "-r", "--name-only", commit).stdout.splitlines()
    except RuntimeError as e:
        return {"files": [], "items": [], "total_open": 0, "truncated": False, "error": _clip(e, 200)}
    extra = [f for f in extra_files or [] if f in names]
    files = sorted({n for n in names if _candidate(n)} | set(extra), key=_rank)[:MAX_SCAN_FILES]
    items, scanned = [], []
    for path in files:
        try:
            r = gitops.git(worktree, "show", f"{commit}:{path}", check=False)
        except RuntimeError:
            continue
        if r.returncode or len(r.stdout) > MAX_FILE_BYTES:
            continue
        scanned.append(path)
        items.extend(_open_items(path, r.stdout))
    seen, unique = set(), []
    for it in items:
        if it["id"] not in seen:
            seen.add(it["id"])
            unique.append(it)
    return {"files": scanned, "items": unique[:MAX_SCAN_ITEMS], "total_open": len(unique), "truncated": len(unique) > MAX_SCAN_ITEMS}


# --- beslisschema (de sessietool van de controletaak) ---

_STR = {"type": "string"}
_LIST = {"type": "array", "items": _STR}


def finish_tool(open_actions_schema: dict) -> dict:
    return {
        "name": "runner_finish",
        "description": "Geef je onafhankelijke oordeel over de vraag of het ketendoel werkelijk af is. Beëindig daarna je beurt.",
        "inputSchema": {"type": "object", "required": ["verdict", "reason"], "properties": {
            "verdict": {"type": "string", "enum": list(VERDICTS),
                        "description": "goal_complete | work_remaining | human_blocked"},
            "reason": {**_STR, "description": "onderbouwing van je oordeel"},
            "evidence": {**_LIST, "description": "verplicht bij goal_complete: wat je zelf controleerde en waaruit blijkt dat het doel af is"},
            "remaining_tasks": {"type": "array", "description":
                                "verplicht bij work_remaining: het zinvolle, door een agent uitvoerbare werk dat binnen het ketendoel nog resteert",
                                "items": {"type": "object", "required": ["title", "source"], "properties": {
                                    "title": _STR, "source": {**_STR, "description": "bestand/sectie waaruit dit blijkt"},
                                    "scan_id": {**_STR, "description": "id uit de BACKLOGSCAN als dit een gescand item is"}}}},
            "next_step": {**_STR, "description": "verplicht bij work_remaining: concrete, zelfstandig uitvoerbare opdracht voor de volgende schakel"},
            "dismissed": {"type": "array", "description":
                          "verplicht voor ELK item uit de BACKLOGSCAN dat niet in remaining_tasks staat",
                          "items": {"type": "object", "required": ["id", "category", "reason"], "properties": {
                              "id": _STR, "category": {"type": "string", "enum": list(DISMISS)}, "reason": _STR}}},
            "human_blockers": {**_LIST, "description": "verplicht bij human_blocked: wat alleen een mens kan beslissen of doen"},
            "question": {**_STR, "description": "verplicht bij human_blocked: de concrete vraag aan de mens"},
            "open_actions": open_actions_schema, "summary": _STR}}}


def validate_decision(args: dict, scan_ids: list) -> str | None:
    """Foutmelding (Nederlands, voor Claude) of None. Gebruikt door de sessietool én door de controller bij het toepassen."""
    def text(v):
        return isinstance(v, str) and bool(v.strip())

    def objects(key):
        v = args.get(key)
        return v if isinstance(v, list) else None if v is not None else []

    v = args.get("verdict")
    if v not in VERDICTS:
        return f"verdict moet een van {', '.join(VERDICTS)} zijn"
    if not text(args.get("reason")):
        return "reason (onderbouwing) is verplicht"
    remaining, dismissed = objects("remaining_tasks"), objects("dismissed")
    if remaining is None or dismissed is None:
        return "remaining_tasks en dismissed moeten lijsten zijn"
    for t in remaining:
        if not (isinstance(t, dict) and text(t.get("title")) and text(t.get("source"))):
            return "elk punt in remaining_tasks vraagt een title en een source"
    for d in dismissed:
        if not (isinstance(d, dict) and text(d.get("id")) and d.get("category") in DISMISS and text(d.get("reason"))):
            return f"elk punt in dismissed vraagt id, reden en een category uit {', '.join(DISMISS)}"
    unknown = [d["id"] for d in dismissed if d["id"] not in scan_ids]
    if unknown:
        return f"dismissed bevat ids die niet in de BACKLOGSCAN staan: {', '.join(unknown[:5])}"
    accounted = {d["id"] for d in dismissed} | {t.get("scan_id") for t in remaining if t.get("scan_id")}
    missing = [i for i in scan_ids if i not in accounted]
    if missing:
        return ("de BACKLOGSCAN-items " + ", ".join(missing[:8]) + (" …" if len(missing) > 8 else "")
                + " zijn niet verantwoord: zet ze in remaining_tasks (met scan_id) of in dismissed (met category en reden)")
    if v == "goal_complete":
        ev = args.get("evidence")
        if not (isinstance(ev, list) and any(text(e) for e in ev)):
            return "goal_complete vraagt evidence: een lijst van wat je zelf controleerde en waaruit blijkt dat het ketendoel af is"
        if remaining:
            return "goal_complete kan niet samengaan met remaining_tasks; kies work_remaining als er uitvoerbaar werk resteert"
        if any(d["category"] == "human_blocked" for d in dismissed):
            return ("een item dat alleen een mens kan afronden en het ketendoel blokkeert, betekent niet 'af': kies human_blocked "
                    "(of categorie human_followup als het doel er niet van afhangt)")
    elif v == "work_remaining":
        if not remaining:
            return "work_remaining vraagt minstens één punt in remaining_tasks (title + source)"
        if not text(args.get("next_step")):
            return "work_remaining vraagt een concrete next_step voor de volgende schakel"
    else:
        blockers = args.get("human_blockers")
        if not (isinstance(blockers, list) and any(text(b) for b in blockers)):
            return "human_blocked vraagt human_blockers: wat alleen een mens kan beslissen of doen"
        if not text(args.get("question")):
            return "human_blocked vraagt een concrete question aan de mens"
        if remaining:
            return ("human_blocked kan niet samengaan met remaining_tasks: is er nog uitvoerbaar werk dat niet op een mens wacht, "
                    "kies dan work_remaining; een blokkade houdt ander werk niet tegen")
        if args.get("open_actions"):
            return "human_blocked blokkeert al en is zelf de menselijke actie: laat open_actions weg (alleen voor niet-blokkerende punten)"
    return None


def fingerprint(args: dict) -> str:
    """Stabiele vingerafdruk van het resterende werk: dezelfde afwijzing twee keer achter elkaar is een patstelling."""
    keys = sorted(" ".join(str(t.get("scan_id") or t.get("title") or "").lower().split()) for t in args.get("remaining_tasks") or [])
    return hashlib.sha1("\n".join(keys).encode()).hexdigest()[:12]


# --- prompt ---

def gate_prompt(ch: dict, link: dict, scan: dict, links_text: str, open_human: list) -> str:
    parts = [
        f"[Voltooiingscontrole van keten {ch['chain_id']} na schakel {link['link']}]",
        f"KETENDOEL (overkoepelend, wat uiteindelijk af moet zijn):\n{ch['goal']}",
        f"BEWERING VAN DE SCHAKEL: schakel {link['link']} meldt dat dit doel volledig bereikt is. Samenvatting van die schakel: "
        f"{_clip(link.get('summary'), 600) or '(geen)'}",
        f"VERLOOP:\n{links_text}",
        f"Je worktree staat op de tip van de keten (commit {str(link.get('end_commit'))[:10]}): precies het gecommitte resultaat.",
    ]
    if scan.get("error"):
        parts.append(f"BACKLOGSCAN: mislukt ({scan['error']}); zoek zelf in de repository naar roadmap, backlog, projectstatus en documentatie.")
    else:
        head = (f"BACKLOGSCAN (door de controller zelf bepaald, niet door de schakel): {scan['total_open']} open item(s) in "
                f"{len(scan['files'])} roadmap-/backlog-/statusbestand(en)"
                + (f"; hieronder de eerste {len(scan['items'])}" if scan["truncated"] else "") + ".")
        lines = [f"- {i['id']} ({i['file']}:{i['line']}): {i['text']}" for i in scan["items"]]
        parts.append(head + ("\n" + "\n".join(lines) if lines else "\nGeen open items gevonden in de gescande bestanden: " + (", ".join(scan["files"][:12]) or "geen")))
    if open_human:
        parts.append("OPENSTAANDE MENSELIJKE ACTIEPUNTEN (niet-blokkerend achtergelaten; ze dwingen NIETS af en houden ander werk niet tegen):\n"
                     + "\n".join(f"- {_clip(i.get('text'), 200)}" for i in open_human[:12]))
    parts.append(
        "TAAK: beoordeel ONAFHANKELIJK of het KETENDOEL werkelijk af is. Neem de bewering van de schakel niet over: lees de roadmap, "
        "backlog, projectstatus en documentatie in de repository, bekijk git log/diff en draai zo nodig gerichte tests. Werk alleen "
        "binnen het ketendoel: items daarbuiten zijn out_of_scope. Verantwoord ELK item uit de BACKLOGSCAN: staat het nog open en "
        "is het zinvol uitvoerbaar door een agent, zet het dan in remaining_tasks (met scan_id); anders in dismissed met een "
        "category (done, out_of_scope, not_actionable, human_followup = niet-blokkerende actie voor een mens waarvan het doel niet "
        "afhangt, human_blocked = alleen een mens kan het afronden en het doel hangt ervan af) en een reden.\n"
        "Beslis via mcp__r2__runner_finish:\n"
        "- work_remaining: er is nog zinvol, uitvoerbaar werk binnen het ketendoel (ook als andere punten op een mens wachten). Geef "
        "remaining_tasks en een concrete next_step.\n"
        "- goal_complete: alleen als het ketendoel aantoonbaar gereed is; geef evidence van wat je zelf controleerde.\n"
        "- human_blocked: er is GEEN uitvoerbaar werk meer omdat alleen een mens verder kan (beslissing, credential, toegang). Geef "
        "human_blockers en een question.\n"
        "Een niet-blokkerend punt voor een mens is nooit een reden om 'niet klaar' te zeggen, en nooit een reden om ander "
        "uitvoerbaar werk te laten liggen. Verzin geen werk om door te gaan en bevestig geen doel dat niet af is.")
    text = "\n\n".join(parts)
    return text if len(text) <= PROMPT_CHARS else text[:PROMPT_CHARS - 40] + "\n[…prompt ingekort…]"
