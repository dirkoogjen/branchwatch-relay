"""Open menselijke actiepunten: beslissingen of handelingen die een taak achterlaat zonder erop te wachten.

Een blokkerende vraag (runner_ask, needs_human van de judge) is GEEN actiepunt: die zet de taak op waiting_for_human en
hoort bij WACHT OP ACTIE. Een actiepunt is wat een afgeronde of doorlopende run achterlaat terwijl het werk verdergaat of
klaar is. Het wordt structureel vastgelegd (nooit afgeleid uit samenvattingstekst) in `state["human_actions"]` van de
taak die het meldde, via het veld `open_actions` van runner_finish:

    {"id": "A-…", "text": "korte interne omschrijving", "reason": "waarom een mens dit moet beslissen",
     "status": "open" | "resolved", "created_at": …, "resolved_at": …, "resolved_note": …, "resolved_by": …,
     "source": {"task_id", "role": "task" | "judge", "chain_id"?, "supervisor_id"?}}

Het id is stabiel (hash van taak + tekst; bij integratievragen taak + aanvraag/reden-topic): opnieuw verwerken van hetzelfde
signaal maakt geen dubbel punt en een eenmaal gesloten punt gaat niet terug naar open. Taken zonder `human_actions` tellen als nul.

`text` is de korte vraag voor een niet-programmerende gebruiker (1-2 zinnen, gewoon Nederlands); `reason` en het optionele
`details` zijn de technische toelichting (Monitor 2 toont ze onder 'Details'). Optioneel draagt een punt een stabiele `topic`
(slug van de vraag) en `new_info` (alleen als er een nieuwe omstandigheid is die een eerder besluit niet dekt).

Besluiten: gesloten punten met hun `resolved_note` zijn de duurzame besliscontext (geen tweede opslag). Voordat een nieuw
punt wordt opgeslagen vergelijkt `merged` het met open en gesloten punten van dezelfde run (supervisor, anders keten, anders
taak) en met gesloten punten van dezelfde repo: exact gelijke tekst, dezelfde topic of een bijna gelijke formulering (conservatieve
tokenvergelijking, geen externe aanroep) wordt onderdrukt en vastgelegd in `state["suppressed_actions"]` (waarom, en tegen welk punt).
"""
import hashlib
import re
import unicodedata

from . import store

OPEN, RESOLVED = "open", "resolved"
MAX_PER_FINISH = 10
TEXT_CHARS = 300
REASON_CHARS = 400
NOTE_CHARS = 300
DETAILS_CHARS = 600
TOPIC_CHARS = 60
RAW_TEXT_MAX = 350          # langer is geen korte vraag: Claude moet het inkorten en de rest in details zetten
SUPPRESSED_KEEP = 30
DECISION_LINES = 15


def _clip(value, n: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= n else text[: n - 1] + "…"


def _integration_topic(topic) -> str:
    value = _slug(topic)
    return value if re.fullmatch(r"integratie-[a-z][a-z0-9-]*-[0-9a-f]{12}", value) else ""


def make_id(task_id: str, text: str, topic: str | None = None) -> str:
    # Controller decisions are scoped to one request/reason, independent of wording.
    identity = _integration_topic(topic) or ' '.join(str(text).lower().split())
    return "A-" + hashlib.sha1(f"{task_id}\0{identity}".encode()).hexdigest()[:10]


def _slug(value) -> str:
    return re.sub(r"[^a-z0-9]+", "-", _fold(str(value or ""))).strip("-")[:TOPIC_CHARS]


def parse(raw) -> list:
    """Nette [{text, reason, details?, topic?, new_info?}] uit het veld `open_actions` van runner_finish; rommel valt weg."""
    out, seen = [], set()
    for item in raw if isinstance(raw, list) else []:
        obj = item if isinstance(item, dict) else {}
        text = _clip(item if isinstance(item, str) else obj.get("text"), TEXT_CHARS)
        identity = _integration_topic(obj.get("topic")) or text.lower()
        if not text or identity in seen:
            continue
        seen.add(identity)
        entry = {"text": text, "reason": _clip(obj.get("reason"), REASON_CHARS)}
        for key, value in (("details", _clip(obj.get("details"), DETAILS_CHARS)), ("topic", _slug(obj.get("topic"))),
                           ("new_info", _clip(obj.get("new_info"), REASON_CHARS))):
            if value:
                entry[key] = value
        out.append(entry)
    return out[:MAX_PER_FINISH]


# --- leesbaarheid: de primaire tekst is voor een niet-programmerende gebruiker ---

_TECH = (
    (re.compile(r"\b(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])[0-9a-f]{7,40}\b"), "een commit-hash"),
    (re.compile(r"\b(?:r2|runner2|origin|feature|fix|claude|codex)/[\w./-]+"), "een branchnaam"),
    (re.compile(r"\b(?:R2|C|A|PRUN|CTRL|NOD)-[0-9a-f]{6,}\b"), "een intern id"),
    (re.compile(r"(?<![\w-])H\d{1,2}\b"), "een intern kenmerk als H1/H2"),
    (re.compile(r"(?i)\btls[- ]?terminator|\bdns[- ]records?\b|\bwsgi\b|\breverse[- ]proxy\b"), "technisch jargon"),
)
_NOTICE = re.compile(r"(?i)^\W*(?:ter info(?:rmatie)?|fyi|nb\b|let op\b|samenvatting|afgerond|voltooid|opgeruimd|gesloten|"
                     r"geen actie (?:nodig|vereist))|\bgeen actie (?:nodig|vereist)\b")


def lint(text: str) -> str | None:
    """Reden waarom `text` geen goede primaire tekst voor een mens is, of None."""
    raw = str(text or "")
    if len(" ".join(raw.split())) > RAW_TEXT_MAX:
        return f"is te lang (max ca. {RAW_TEXT_MAX} tekens): schrijf één of twee zinnen en zet de rest in details"
    for pattern, what in _TECH:
        if pattern.search(raw):
            return f"bevat {what}: gebruik gewone woorden en zet technische informatie in details"
    if _NOTICE.search(raw):
        return "leest als verslag of mededeling over afgerond werk: dat hoort in summary; open_actions is alleen voor wat een mens nog moet beslissen of doen"
    return None


def validate(raw) -> str | None:
    """Foutmelding (Nederlands, voor Claude) of None voor het veld `open_actions` van runner_finish."""
    if raw is None:
        return None
    if not isinstance(raw, list):
        return "open_actions moet een lijst zijn van {text, reason}"
    if len(raw) > MAX_PER_FINISH:
        return f"open_actions mag hoogstens {MAX_PER_FINISH} punten bevatten"
    for item in raw:
        text = item if isinstance(item, str) else (item or {}).get("text") if isinstance(item, dict) else None
        if not isinstance(text, str) or not text.strip():
            return "elk punt in open_actions vraagt een niet-lege text"
        if problem := lint(text):
            return (f"open_actions-tekst {_clip(text, 60)!r} {problem}. Schrijf text als korte vraag in gewoon Nederlands voor een "
                    "niet-programmerende gebruiker, en alleen voor iets wat die zelf moet beslissen of doen.")
    return None


# --- deduplicatie: conservatieve normalisatie, geen externe aanroep ---

_STOP = frozenset("""de het een en of maar want dat die dit deze er ook nog wel niet geen wat wie wij jij hij zij ons uw mijn
voor van met op in uit bij naar aan als om te tot door over onder zonder tussen na dan nu al is zijn was waren wordt worden
word kan kunnen moet moeten mag mogen zal zullen heeft hebben had hadden wil willen welke waar hoe wanneer waarom kies bepaal
beslis beslist graag eventueel bijvoorbeeld standaard""".split())
_LEADER = re.compile(r"^\s*[A-Za-z]{1,3}\d{1,3}\s*[:\-–]\s*")


def _fold(text: str) -> str:
    return unicodedata.normalize("NFKD", text.lower()).encode("ascii", "ignore").decode()


def normalize(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", _fold(_LEADER.sub("", str(text or "")))))


def _stem(token: str) -> str:
    if len(token) > 6 and token.endswith("en"):
        return token[:-2]
    if len(token) > 4 and token.endswith("s"):
        return token[:-1]
    return token


def tokens(text: str) -> frozenset:
    return frozenset(_stem(t) for t in normalize(text).split() if len(t) > 2 and t not in _STOP)


def _leader(text: str) -> str:
    m = re.match(r"\s*([A-Za-z]{1,3}\d{1,3})\s*[:\-–]", str(text or ""))
    return m.group(1).lower() if m else ""


def similar(a: str, b: str, strict: bool = True) -> bool:
    """Bijna dezelfde formulering: genoeg gedeelde inhoudswoorden én een hoge overlap (of de ene tekst zit vrijwel geheel in de
    andere, of beide dragen hetzelfde projectkenmerk als 'H3:' met enige overlap). `strict=False` geldt alleen voor punten die
    dezelfde afronding net sluit (een samenvattend punt over het zojuist gesloten werk)."""
    ta, tb = tokens(a), tokens(b)
    if not ta or not tb:
        return False
    shared = len(ta & tb)
    if not strict:
        return shared >= 2 and shared / len(ta | tb) >= 0.34
    if shared >= 4 and shared / len(ta | tb) >= 0.5:
        return True
    if shared >= 5 and shared / min(len(ta), len(tb)) >= 0.75:
        return True
    return bool(_leader(a)) and _leader(a) == _leader(b) and shared >= 3


def _match(new: dict, cand: dict, resolving: bool) -> str | None:
    """Waarom `cand` hetzelfde punt is als `new` ('exact' | 'topic' | 'similar'), of None."""
    scope, other_scope = _integration_topic(new.get("topic")), _integration_topic(cand.get("topic"))
    if scope or other_scope:
        return "topic" if scope and scope == other_scope else None
    if normalize(new["text"]) == normalize(cand.get("text")):
        return "exact"
    resolved = cand.get("status") == RESOLVED or resolving
    if new.get("new_info") and resolved:
        return None                       # uitdrukkelijk een nieuwe omstandigheid: alleen exacte gelijkheid blokkeert
    topic, other = new.get("topic"), _slug(cand.get("topic"))
    if topic and other:
        return "topic" if topic == other else None      # beide benoemen hun vraag: de topic is leidend
    return "similar" if similar(new["text"], cand.get("text") or "", strict=not resolving) else None


def scope_items(state: dict, all_states: list) -> list:
    """Punten waartegen een nieuw punt van `state` wordt vergeleken: alles uit dezelfde run (supervisor, anders keten, anders
    taak) plus de gesloten punten (= besluiten) van andere taken in dezelfde repo."""
    run = {s["task_id"] for s in run_states(state, all_states)} | {state["task_id"]}
    out = []
    for st in all_states:
        same_run = st["task_id"] in run
        if not same_run and (not state.get("repo") or st.get("repo") != state["repo"]):
            continue
        for it in st.get("human_actions") or []:
            if isinstance(it, dict) and it.get("id") and (same_run or it.get("status") == RESOLVED):
                out.append(it)
    return out


def _resolving_ids(task_id: str, finish: dict | None, context: list) -> set:
    ids = {i for i in (finish or {}).get("resolve_actions") or [] if isinstance(i, str)}
    ids |= {i["id"] for i in context if i.get("resolved_by") == f"task {task_id}"}
    return ids


def merged(state: dict, finish: dict | None, context: list | None = None) -> tuple | None:
    """(nieuwe lijst `human_actions`, nieuwe suppressed-regels) voor deze taak na een runner_finish, of None bij niets te doen.

    Bestaande punten (ook gesloten) blijven staan. Een nieuw punt dat al open staat, al is beantwoord of een afronding van
    zojuist gesloten punten samenvat, wordt niet opgeslagen maar met reden in de suppressed-regels gezet."""
    new = parse((finish or {}).get("open_actions"))
    if not new:
        return None
    tid = state["task_id"]
    items = list(state.get("human_actions") or [])
    have = {i["id"] for i in items}
    context = [c for c in context or [] if c.get("id") not in have] + items
    resolving = _resolving_ids(tid, finish, context)
    source = {"task_id": tid, "role": "judge" if state.get("supervisor_role") == "judge" else "task"}
    for key in ("chain_id", "supervisor_id"):
        if state.get(key):
            source[key] = state[key]
    log = []
    for n in new:
        aid = make_id(tid, n["text"], n.get("topic"))
        if aid in have:
            continue
        hit = None
        for cand in sorted(context, key=lambda c: (c.get("status") != OPEN, c.get("created_at") or "")):
            if cand.get("id") == aid:
                continue
            why = _match(n, cand, cand.get("id") in resolving)
            if why:
                hit = (cand, why)
                break
        if hit:
            cand, why = hit
            status = RESOLVED if cand.get("status") == RESOLVED or cand.get("id") in resolving else OPEN
            log.append({"at": store.now(), "text": n["text"], "topic": n.get("topic"), "match": why,
                        "why": "already_resolved" if status == RESOLVED else "duplicate_open",
                        "matched_id": cand.get("id"), "matched_text": _clip(cand.get("text"), 120),
                        "matched_note": _clip(cand.get("resolved_note"), 160) or None})
            continue
        item = {"id": aid, "text": n["text"], "reason": n["reason"], "status": OPEN, "created_at": store.now(), "source": dict(source)}
        item.update({k: n[k] for k in ("details", "topic", "new_info") if n.get(k)})
        items.append(item)
        have.add(aid)
        context.append(item)
    return items, log


def record(task_id: str, finish: dict | None) -> int:
    """Legt de `open_actions` van een runner_finish vast in de taakstate (onder de statelock). Geeft het aantal nieuwe punten.

    Onderdrukte punten (dubbel, al beantwoord) worden niet geteld maar staan in `suppressed_actions` van de taak."""
    if not parse((finish or {}).get("open_actions")):
        return 0
    others = [s for s in store.all_tasks() if s.get("task_id") != task_id]
    with store._flock(task_id):
        st = store.load(task_id)
        if st["status"] == "stopped":
            return 0
        before = len(st.get("human_actions") or [])
        res = merged(st, finish, scope_items(st, others + [st]))
        if res is None:
            return 0
        items, log = res
        seen = {(e.get("text"), e.get("why")) for e in st.get("suppressed_actions") or []}
        log = [e for e in log if (e["text"], e["why"]) not in seen]
        if len(items) == before and not log:
            return 0
        st["human_actions"] = items
        if log:
            st["suppressed_actions"] = ((st.get("suppressed_actions") or []) + log)[-SUPPRESSED_KEEP:]
        store.save(st)
        return len(items) - before


# --- lezen ---

def _items(states: list, status: str | None = OPEN) -> list:
    out = []
    for st in states:
        for it in st.get("human_actions") or []:
            if isinstance(it, dict) and it.get("id") and (status is None or it.get("status") == status):
                out.append(it)
    return sorted(out, key=lambda i: (i.get("created_at") or "", i["id"]))


def open_items(states: list) -> list:
    return _items(states, OPEN)


def run_states(state: dict, all_states: list) -> list:
    """Taken die bij dezelfde run horen: dezelfde supervisor, anders dezelfde keten, anders alleen de taak zelf."""
    for key in ("supervisor_id", "chain_id"):
        if state.get(key):
            return [s for s in all_states if s.get(key) == state[key]]
    return [state]


def brief(item: dict) -> dict:
    out = {"id": item["id"], "text": item.get("text"), "reason": item.get("reason") or None, "status": item.get("status"),
           "created_at": item.get("created_at"), "source": item.get("source") or {}}
    for key in ("details", "topic"):
        if item.get(key):
            out[key] = item[key]
    if item.get("status") == RESOLVED:
        out.update(resolved_at=item.get("resolved_at"), resolved_by=item.get("resolved_by"),
                   resolved_note=item.get("resolved_note"))
    return out


def describe(states: list, include_resolved: bool = False) -> dict:
    """Compact blok voor status/result: aantal open punten en de punten zelf (interne tekst + bron)."""
    items = _items(states, None if include_resolved else OPEN)
    return {"open": sum(1 for i in items if i.get("status") == OPEN),
            "resolved": sum(1 for i in _items(states, RESOLVED)),
            "suppressed": sum(len(st.get("suppressed_actions") or []) for st in states),
            "items": [brief(i) for i in items]}


# --- besliscontext voor de volgende schakel / de judge ---

def decisions_block(supervisor_id: str | None = None, chain_id: str | None = None, include_open: bool = True) -> str:
    """Prompttekst met de al genomen besluiten (gesloten punten mét hun antwoord) en de nog openstaande punten van deze run.

    Leeg als er niets is. De taak moet dit niet opnieuw aan een mens vragen."""
    key, value = ("supervisor_id", supervisor_id) if supervisor_id else ("chain_id", chain_id)
    if not value:
        return ""
    states = [s for s in store.all_tasks() if s.get(key) == value]
    # automatisch gesloten punten (actiebeheer) zijn geen besluit: hun vraag staat nog open elders of is daar beslist
    decided = [i for i in _items(states, RESOLVED) if i.get("resolved_note") and not i.get("auto_resolution")][-DECISION_LINES:]
    opened = _items(states, OPEN)[:DECISION_LINES] if include_open else []
    parts = []
    if decided:
        parts.append("AL BESLOTEN OF AFGEHANDELD (vraag dit niet opnieuw aan de gebruiker; neem het als uitgangspunt):\n" + "\n".join(
            f"- {_clip(i.get('text'), 160)} → {_clip(i.get('resolved_note'), 240)}" for i in decided))
    if opened:
        parts.append("AL OPEN ALS ACTIEPUNT (meld het niet nogmaals):\n" + "\n".join(f"- {_clip(i.get('text'), 160)}" for i in opened))
    if parts:
        parts.append("Meld in open_actions alleen een werkelijk nieuwe vraag die hierboven niet is beantwoord, en alleen als een mens "
                     "het moet beslissen of doen. Hoort een nieuwe vraag bij een eerder besluit maar dekt dat besluit haar niet, "
                     "vul dan new_info in.")
    return "\n\n".join(parts)


# --- sluiten ---

def resolve(ids: list, note: str = "", by: str = "controller", candidates: list | None = None) -> dict:
    """Sluit open punten op id (idempotent). `candidates` beperkt het zoeken tot die taken (id's of states).

    Geeft {"resolved": [ids], "already_resolved": [ids], "unknown": [ids]}."""
    wanted = [i for i in dict.fromkeys(ids or []) if isinstance(i, str) and i]
    tids = [c["task_id"] if isinstance(c, dict) else c for c in (candidates if candidates is not None else store.all_tasks())]
    result = {"resolved": [], "already_resolved": [], "unknown": []}
    found = set()
    for tid in tids:
        try:
            probe = store.load(tid)
        except (OSError, ValueError):
            continue
        if not any(isinstance(i, dict) and i.get("id") in wanted for i in probe.get("human_actions") or []):
            continue
        with store._flock(tid):     # lees-wijzig-schrijf: een parallelle beurt mag dit niet overschrijven
            st = store.load(tid)
            changed = False
            for it in st.get("human_actions") or []:
                if not isinstance(it, dict) or it.get("id") not in wanted:
                    continue
                found.add(it["id"])
                if it.get("status") == OPEN:
                    it.update(status=RESOLVED, resolved_at=store.now(), resolved_by=_clip(by, 80),
                              resolved_note=_clip(note, NOTE_CHARS) or None)
                    result["resolved"].append(it["id"])
                    changed = True
                else:
                    result["already_resolved"].append(it["id"])
            if changed:
                store.save(st)
    result["unknown"] = [i for i in wanted if i not in found]
    return result
