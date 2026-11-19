"""Meldingen over menselijke blokkades en het dagoverzicht (besluit 48 van Beheerder, 23 oktober 2026).

Dit is GEEN tweede systeem naast de notifier: de notifier (`notifier.py`, draait al always-on) blijft de enige afzender en bezit ledger,
outbox en afnemers. Deze module bevat alleen de regels: wat is een echte blokkade door een mens, hoe wordt een melding stabiel en
gededupliceerd, hoe wordt iets gezuiverd en hoe ziet het dagoverzicht eruit. Er is geen timer en geen scheduler: de directe meldingen
volgen de notifier-ronde (elke `interval_s`, 15 s, plus de wake-up van de watch); het dagoverzicht volgt de dagelijkse herbeoordeling
(besluit 46/47, `actionreassess`, de enige dagelijkse scheduler) doordat de notifier een nieuw rapport van die ronde opmerkt.

Drie klassen (veld `class` van een conditie):

  blokkade   Runner 2 komt aantoonbaar niet verder zonder dat een mens beslist of antwoordt: een taak op `waiting_for_human` met een
             vraag, een supervisieproject dat op Beheerder wacht (WACHT OP BEHEERDER), een bereikte limiet die Beheerder moet verruimen, of een
             geëscaleerd event/integriteitsblokkade die Beheerder moet vrijgeven. DIRECT gemeld, pas na een stabiele waarneming.
  storing    technisch, geen mensenbesluit (node onbereikbaar, supervisor pakt een event al uren niet op): direct gemeld zoals vroeger.
  intern     alleen bewaard (outbox/ledger) en zichtbaar in `monitor.json`, nooit extern verstuurd (bv. een supervisor die een event
             nog moet claimen).

Een OPEN ACTIE (`human_action`, `state["human_actions"]`) is nadrukkelijk GEEN blokkade: het werk gaat door of is klaar. Zulke punten
worden nooit direct gemeld en nooit als "geblokkeerd" aangeduid; ze komen hooguit in het dagoverzicht.
"""
import re

BLOCK, INFRA, INTERNAL = "blokkade", "storing", "intern"
STABLE_S = 15.0           # een blokkade moet minstens zo lang én bij STABLE_ROUNDS opeenvolgende rondes zichtbaar zijn (zelfde vloer als Monitor 2: 15 s, 2 waarnemingen)
STABLE_ROUNDS = 2
RESOLVE_GRACE_S = 120.0   # een blokkade telt pas als opgelost als ze zo lang weg is; komt ze eerder terug dan is het dezelfde melding
MAX_DIRECT_PER_ROUND = 5  # meer nieuwe blokkades in één ronde worden in één bundelmelding verstuurd
SUPERVISOR_STALE_H = 4.0  # een supervisie-event dat zo lang door niemand is opgepakt wordt een storingsmelding (geen mensenbesluit)
DIGEST_ITEMS = 5          # zoveel werkelijk nieuwe punten staan met tekst in het dagoverzicht
DIGEST_BASELINE_S = 6 * 3600   # eerste keer dat de notifier het dagoverzicht ziet: een rapport dat ouder is wordt bewaard maar niet verstuurd
HUMAN_SUPERVISION = {"waiting_human": "WACHT OP BEHEERDER", "limit": "LIMIET BEREIKT", "blocked": "SUPERVISIE GEBLOKKEERD"}

# --- zuiveren: geen geheimen, paden, tokens of toegangslinks in een melding ---

_SECRET_PATTERNS = (
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)", re.S),
    re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"(?i)\b(password|passwd|wachtwoord|secret|token|api[_-]?key|apikey|authorization|credentials?|hmac|sleutel|key)\b\s*[:=]\s*\S+"),
    re.compile(r"\b(?:sk|pk|rk|ghp|gho|ghs|github_pat|xox[abprs]|AKIA|ASIA)[-_A-Za-z0-9]{12,}"),
    re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s/@:]+:[^\s/@]+@\S+"),            # url met gebruiker:wachtwoord
    re.compile(r"(?i)\bhttps?://\S*[?&#](?:key|token|sig|signature|access|auth)\S*"),   # toegangslink met sleutel
    re.compile(r"\b[A-Fa-f0-9]{32,}\b"),
    re.compile(r"\b[A-Za-z0-9+/_-]{40,}={0,2}"),
)
_PATH = re.compile(r"(?:~(?=/)|/home/[^\s/]+|/Users/[^\s/]+|C:\\Users\\[^\s\\]+|/root|/etc|/var/lib|/run/user)(?:[/\\][^\s,;:)\"']*)?")


def redact(text, limit: int = 200) -> str:
    """Haalt alles weg dat op een geheim, een toegangslink of een pad naar het systeem lijkt en kort af. Regeleinden worden spaties."""
    clean = " ".join(str(text or "").split())
    for pattern in _SECRET_PATTERNS:
        clean = pattern.sub("[verborgen]", clean)
    clean = _PATH.sub("[pad]", clean)
    return clean if len(clean) <= limit else clean[: limit - 1] + "…"


# --- classificatie van supervisieprojecten en taken ---

def supervision_conditions(projects: list, now: float, epoch, stale_h: float) -> tuple:
    """(condities, bedekte taak-id's). Een taak die al via zijn supervisieproject als blokkade is gemeld krijgt geen tweede melding."""
    out, covered = [], set()
    for p in projects:
        if not p.get("attention"):
            continue
        pid, st = p["project_id"], p["state"]
        ev = p.get("event") or {}
        title = redact(f"{p['label']} · {p.get('title') or pid}", 120)
        text = redact(p.get("next_step"))
        if st in HUMAN_SUPERVISION:
            ids = ev.get("id") or ",".join(b["id"] for b in p.get("blockers_open") or []) or \
                (((p.get("assignment") or {}).get("observed") or {}).get("current_task") or (p.get("assignment") or {}).get("task_id") or "")
            out.append({"kind": "supervision", "class": BLOCK, "key": f"{pid}:{st}:{ids}", "title": title, "text": text})
            a = p.get("assignment") or {}
            covered.update(x for x in (a.get("task_id"), (a.get("observed") or {}).get("current_task")) if x)
            continue
        out.append({"kind": "supervision", "class": INTERNAL, "key": f"{pid}:{st}:{ev.get('id', '')}", "title": title, "text": text})
        if st == "needed" and ev.get("created_at"):
            try:
                age_h = (now - epoch(ev["created_at"])) / 3600
            except (ValueError, TypeError):
                continue
            if age_h >= stale_h:
                out.append({"kind": "supervision_stale", "class": INFRA, "key": f"{pid}:{ev.get('id', '')}",
                            "title": redact(f"SUPERVISIE STAAT STIL · {p.get('title') or pid}", 120),
                            "text": redact(f"Geen supervisor heeft dit event in {age_h:.0f} uur opgepakt. Dit is geen menselijke beslissing; de supervisor-koppeling controleren. {text}")})
    return out, covered


def task_conditions(states: list, covered: set) -> list:
    out = []
    for s in states:
        if s["status"] == "waiting_for_human" and s.get("question") and s["task_id"] not in covered:
            out.append({"kind": "task_question", "class": BLOCK, "key": f"{s['task_id']}:{s['turn']}",
                        "title": redact(f"WACHT OP ACTIE · {s.get('title') or s['task_id']}", 120),
                        "text": redact((s["question"] or {}).get("question"))})
    return out


# --- dagoverzicht ---

def mens_total(counts: dict, klassen: dict) -> int:
    """Aantal open punten waarvoor een mens iets moet beslissen of doen (klassen met vlag `mens`)."""
    return sum(int(counts.get(k) or 0) for k, (_, mens) in klassen.items() if mens)


def digest_text(rep: dict, klassen: dict, blockers: list, items: int = DIGEST_ITEMS) -> tuple:
    """(titel, tekst) van het dagoverzicht uit het rapport van de dagelijkse herbeoordeling. Niets hiervan is een blokkade."""
    counts = rep.get("counts") or {}
    mens = mens_total(counts, klassen)
    new = rep.get("nieuw_voor_beheerder") or []
    parts = [f"{rep.get('totaal_open', 0)} open acties; daarvan vragen {mens} een menselijke beslissing of handeling"
             f" ({len(new)} werkelijk nieuw). Dit zijn geen blokkades: Runner 2 werkt door."]
    skipped = [n for n, v in (rep.get("nodes") or {}).items() if v.get("status") != "ok"]
    if skipped:
        parts.append("Niet beoordeeld: " + ", ".join(sorted(skipped)) + ".")
    for x in new[:items]:
        parts.append(f"Nieuw ({x.get('node')}): {redact(x.get('text'), 140)}")
    if len(new) > items:
        parts.append(f"... en {len(new) - items} andere nieuwe punten.")
    if blockers:
        parts.append(f"Nog geblokkeerd door een menselijke beslissing (al direct gemeld): {len(blockers)}. "
                     + "; ".join(redact(b.get("title"), 80) for b in blockers[:items]))
    return "DAGOVERZICHT · menselijke beslissingen", " ".join(parts)
