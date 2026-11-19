"""Periodieke herbeoordeling van de resterende open acties op alle nodes (besluit 46 van Beheerder, 23 oktober 2026).

Vult de bewijsgerichte afsluiting (besluit 43, `actionclosure`) aan; vervangt of dupliceert die niet. Deze module SLUIT NOOIT iets,
draagt alleen vooraf onafhankelijk beoordeelde bewijspakketten als claims over; maakt geen opdracht aan, start niets en roept geen betaalde dienst of Claude aan. Ze leest en schrijft alleen
haar eigen bestanden onder `~/.runner2/actionreassess/`. Sluiten blijft voorbehouden aan `actionclosure` (claim + bewijs) en aan een mens.

Eén ronde (alleen op de primaire control plane, eenmaal per dag op `action_reassess_daily_at`, standaard 05:00; besluit 47):

1. Per node (GERARD lokaal; MAC en LINUX via de bestaande read-only aanroep `runner2_actions review`) de open punten ophalen met de bestaande
   regels van het actiebeheer (`actionreview`: dubbel, al beslist, beschermde categorieën). Een node die niet bereikbaar is of een oude
   Runner heeft, wordt overgeslagen: de bekende stand van die node blijft ongewijzigd staan en er wordt nooit iets geraden.
2. Lokaal de vier dimensies uit de bewijsgerichte afsluiting (besluit / uitvoering / controle / integratie) voor punten met een claim.
   Zonder claim (en op andere nodes) blijft uitvoering, controle en integratie `onbekend`: ze worden nooit afgeleid.
3. Het beslissingenregister (besluit 49) als bron voor "al besloten / al getrieerd".
4. Elk open punt krijgt één klasse (KLASSEN). `mens_nieuw` is uitsluitend: een punt dat een mens moet beslissen, dat in het register
   niet voorkomt, dat is ontstaan nadat het register is aangemaakt (eerder gemelde punten staan al open in Monitor 2) en dat in een
   eerdere ronde nog niet is voorgelegd. Bij een ontbrekend register wordt niets als nieuw voorgelegd.
5. Veilig technisch vervolgwerk wordt VOORBEREID (concept met opdrachttekst en claim-opdracht), niet in de wachtrij gezet: alleen een punt dat
   het register als TECHNISCH kent, geen vraag is, niet beschermd is en nog geen claim of opdracht heeft. Een voorstel heeft een stabiel
   id (één per punt), controleert de ruimte in de wachtrij (maximum `jobs.MAX_OPEN_JOBS`) en een bestaande opdracht die het punt al noemt.

Geschiedenis: het grootboek (`ledger.json`) bewaart per punt eerste/laatste keer gezien, klasse met wijzigingsgeschiedenis, voorgelegd-op en
"niet meer open" (nooit verwijderd); `rondes.jsonl` en het hash-geketende `audit.jsonl` zijn append-only.
"""
import hashlib
import json
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from . import actionclosure, actionreview, actions, config, jobs, store
from .filelock import lock_exclusive

VERSION = 1
BY = "herbeoordeling"
GENESIS = "0" * 64
LEDGER_HISTORY = 20
SUMMARY_ITEMS = 20
PROPOSAL_TEXT = 300
MIN_GAP_S = 3 * 3600   # een ronde die minder dan drie uur vóór het dagmoment startte (inhaalronde) telt voor dat dagmoment mee: één ronde per dag

# klasse -> (korte uitleg, heeft een mens nodig)
KLASSEN = {
    "nader_onderzoek": ("Onvoldoende bewijs voor een veilige classificatie; blijft open.", False),
    "mogelijk_dubbel": ("Mogelijk dubbel; tekstgelijkenis is geen sluitbewijs.", False),
    "bewijs_sluit": ("Bewijs voor besluit, uitvoering, controle en integratie is sluitend; de bewijsgerichte afsluiting sluit dit zelf.", False),
    "in_uitvoering": ("Gekoppeld aan een opdracht; het bewijs is er nog niet (wacht, loopt of is nog niet gecontroleerd).", False),
    "claim_twijfel": ("Gekoppeld aan een opdracht, maar er is iets tegenstrijdigs of een besluit ontbreekt: een mens moet kijken.", True),
    "dubbel_of_beslist": ("Dubbel van een ouder open punt of al beslist (actiebeheer); sluit pas als een mens dat toestaat.", False),
    "twijfel": ("Lijkt overbodig of is een mededeling (actiebeheer), maar de zekerheid ontbreekt: blijft open.", True),
    "afgehandeld_volgens_register": ("Het register noemt dit punt beantwoord, vervallen of dubbel (afgeleid, geen sluitend bewijs); blijft open.", False),
    "uitgesteld": ("Bewust uitgesteld volgens het register; wordt niet opnieuw voorgelegd.", False),
    "besloten_wacht_op_uitvoering": ("Een besluit noemt dit punt; er is nog geen opdracht aan gekoppeld. Uitvoering is onbekend.", False),
    "technisch_voorbereid": ("Veilig technisch vervolgwerk: een concept-opdracht is voorbereid (niet in de wachtrij gezet).", False),
    "technisch_open": ("Het register kent dit als technisch, maar het is niet veilig zelfstandig uit te voeren (vraag, beschermd of nieuwe omstandigheid).", True),
    "mens_bekend": ("Een mens moet dit beslissen of doen; het register kent dit punt al, dus het wordt niet opnieuw voorgelegd.", True),
    "mens_nieuw": ("Een mens moet dit beslissen of doen en het is nog niet voorgelegd of getrieerd: werkelijk nieuw.", True),
}
# veilige hoofdcategorie per klasse: volledig en afgeleid van de klasse (die zelf op bewijs rust), nooit van tekstgelijkenis
CATEGORIEEN = {
    "mens_nieuw": "beslissing_beheerder", "mens_bekend": "beslissing_beheerder", "claim_twijfel": "beslissing_beheerder", "twijfel": "beslissing_beheerder",
    "technisch_open": "beslissing_beheerder",
    "bewijs_sluit": "sluitbaar_met_bewijs", "dubbel_of_beslist": "sluitvoorstel_mens",
    "in_uitvoering": "loopt", "besloten_wacht_op_uitvoering": "loopt", "technisch_voorbereid": "loopt",
    "afgehandeld_volgens_register": "register_triage", "uitgesteld": "register_triage",
    "nader_onderzoek": "onbeslist", "mogelijk_dubbel": "onbeslist",
}
PLAN_BUCKETS = (actionreview.WOULD_CLOSE, actionreview.REVIEW, actionreview.DECIDE)

# relaties in het register waarmee een punt al is getrieerd (besluit 50) en wat dat betekent
_AFGEHANDELD = ("open-actie:BEANTWOORD", "open-actie:VERVALLEN", "open-actie:DUBBEL")
_UITGESTELD = ("open-actie:UITGESTELD",)
_TECHNISCH = ("open-actie:TECHNISCH",)
_MENS = ("open-actie:HANDELING_BEHEERDER", "open-actie:OPEN_BESLISSING", "open-actie:WACHT_OP_TRIAGE", "open-actie:ONDUIDELIJK")
_DECISION_WORDS = re.compile(r"(?i)\b(?:akkoord|toestemming|goedkeur\w*|beslissing\s+(?:nodig|van|door)|keuze\s+nodig|wil je|wilt u)\b")
_APPROVAL_WORDS = re.compile(r"(?i)\b(?:akkoord|bezwaar|goedkeuring|toestemming|eens|ok|oke|oké)\b")
_RESPONSE_REQUEST = re.compile(
    r"(?i)\b(?:graag\s+(?:uw|je|jouw)\s+(?:reactie|akkoord|goedkeuring|antwoord|besluit|toestemming|feedback)"
    r"|(?:wacht|wachten|wachtend)\s+op\s+(?:uw|je|jouw)\s+(?:reactie|akkoord|goedkeuring|antwoord|besluit|toestemming)"
    r"|(?:uw|je|jouw)\s+(?:reactie|akkoord|goedkeuring|antwoord)\s+(?:is\s+)?(?:gewenst|nodig|gevraagd|vereist)"
    r"|laat\s+(?:het\s+)?(?:me|ons|mij)\s+weten)\b")
# Technisch onderwerp met 'akkoord' als testuitkomst ('Test moet akkoord zijn?') is geen vraag aan een mens, tenzij er een persoon wordt aangesproken.
_TECH_SUBJECT_OK = re.compile(r"(?i)^\s*(?:de\s+|het\s+|alle\s+)?(?:tests?|testen|build|check|checks|controle|pipeline|ci|lint|linter|code|output|resultaat)\b[^?]*\bakkoord\b")
_PERSON = re.compile(r"(?i)\b(?:u|uw|je|jij|jou|jouw|jullie|beheerder|gebruiker)\b")
# Een technische vraag over een proces ('Waarom faalt de test?', 'Is de build mislukt?') is werk voor de uitvoerder, geen beslissing voor een mens.
# Alleen als er een technisch onderwerp is, geen persoon wordt aangesproken, geen toestemming/keuze wordt gevraagd en geen beschermd onderwerp
# (productie, live, uitrol, geld, toegang, verwijderen, publiceren) genoemd wordt. Twijfel ⇒ blijft een zichtbare vraag.
# De uitzondering geldt alleen voor een zuivere waarom-/wat-/status-vraag (begint met waarom, wat, is, zijn, slaagt, faalt, draait, werkt ...).
_TECH_NOUN = re.compile(r"(?i)\b(?:tests?|testen|testrun|build|builds|check|checks|pipeline|ci|lint|linter|compilatie|compiler|stacktrace|traceback|logs?|exitcode)\b")
_TECH_PREDICATE = re.compile(r"(?i)\b(?:faalt|falen|faalde|mislukt|mislukken|mislukte|slaagt|slagen|geslaagd|rood|groen|crasht|hangt|draait|werkt|loopt\s+vast|time-?out)\b")
_NEEDS_HUMAN = re.compile(
    r"(?i)\b(?:ik|we|wij|mijn|ons|mag|mogen|moet|moeten|kan|akkoord|doorgaan|door\s+te\s+gaan|release\w*|vrijgave|vrijgeven|klant\w*|opdrachtgever\w*|bevestig\w*|bezwaar|gaan|toestemming|goedkeur\w*|keuze|kies\w*|beslis\w*|wil|wilt|kunt|kunnen|zou|zullen|"
    r"productie|prod|live|uitrol\w*|deploy\w*|geld|kosten|betaal\w*|toegang|geheim\w*|wachtwoord|token|verwijder\w*|publiceer\w*|publicatie|push\w*|merge\w*|main)\b")
_TN = _TECH_NOUN.pattern.replace("(?i)", "", 1)
_TP = _TECH_PREDICATE.pattern.replace("(?i)", "", 1)
_ART = r"(?:(?:de|het|alle)\s+)?"
_VERB = r"(?:slaagt|slagen|faalt|falen|draait|werkt|hangt|crasht)"
# Volledig verankerde allowlist: alleen deze vormen zijn een technische statusvraag; al het andere met een vraagteken blijft een zichtbare vraag.
_TECH_PURE_FORM = re.compile(
    r"(?i)^\s*(?:"
    rf"(?:is|zijn|was)\s+{_ART}{_TN}\s+(?:(?:nog|weer)\s+)?{_TP}"
    rf"|{_VERB}\s+{_ART}{_TN}(?:\s+(?:nog|weer))?"
    rf"|waarom\s+{_TP}\s+{_ART}{_TN}"
    rf"|waarom\s+(?:is|zijn|was)\s+{_ART}{_TN}\s+{_TP}"
    r")\s*\?\s*$")
_URL_OR_QUERY = re.compile(r"https?://\S+|\S*\?\S+")


class ReassessError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code, self.message = code, message


def home():
    return config.r2_home() / "actionreassess"


_clip = actions._clip


def _sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


# --- auditspoor: append-only, hash-geketend ---

def _audit(rec: dict) -> None:
    path = home() / "audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    # Parkeren kan buiten de rondelock gebeuren. Houd lezen van de vorige hash en append daarom onder één eigen bestandslock.
    with open(path, "a+", encoding="utf-8") as f:
        lock_exclusive(f)
        f.seek(0)
        text = f.read()
        lines = text.splitlines()
        problems = _audit_problems(lines)
        if problems:
            raise ReassessError("audit_beschadigd", "auditspoor niet uitgebreid: " + "; ".join(problems))
        nonempty = [ln for ln in lines if ln.strip()]
        prev = hashlib.sha256(nonempty[-1].encode()).hexdigest() if nonempty else GENESIS
        rec = {"at": store.now(), "node": config.local_node(), **rec, "prev": prev}
        if text and not text.endswith("\n"):
            f.write("\n")
        f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
        f.flush()
        os.fsync(f.fileno())
        store.fsync_dir(path.parent)


def read_audit() -> list:
    path = home() / "audit.jsonl"
    out = []
    for line in path.read_text(encoding="utf-8").splitlines() if path.exists() else []:
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def _audit_problems(lines: list) -> list:
    problems, prev = [], GENESIS
    for n, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            problems.append(f"regel {n}: geen geldige JSON")
            break
        if not isinstance(rec, dict):
            problems.append(f"regel {n}: geen object")
            break
        if rec.get("prev") != prev:
            problems.append(f"regel {n}: keten verbroken")
            break
        prev = hashlib.sha256(line.encode()).hexdigest()
    return problems


def verify_audit() -> list:
    """Problemen in de auditketen (leeg = intact)."""
    try:
        lines = (home() / "audit.jsonl").read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        lines = []
    except (OSError, UnicodeError):
        return ["auditspoor is niet leesbaar"]
    return _audit_problems(lines)


@contextmanager
def _run_lock():
    path = home() / "run.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        try:
            lock_exclusive(f, blocking=False)
        except OSError:
            raise ReassessError("busy", "er loopt al een herbeoordeling op deze node") from None
        yield


# --- grootboek en voorstellen (nooit verwijderen) ---

def _load(name: str, default: dict) -> dict:
    path = home() / name
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(d, dict):
            raise ValueError("geen object")
        return d
    except FileNotFoundError:
        return default
    except (OSError, ValueError):
        raise ReassessError("unreadable", f"{path} is onleesbaar; de herbeoordeling schrijft niets tot een mens dit heeft hersteld") from None


def _rondes_bewijs() -> int:
    """Hoogste rondenummer dat andere duurzame bestanden (rondelog, laatste rapport) kennen; 0 als er niets is."""
    best = 0
    try:
        for ln in (home() / "rondes.jsonl").read_text(encoding="utf-8").splitlines():
            try:
                best = max(best, int(json.loads(ln).get("ronde") or 0))
            except (ValueError, AttributeError, TypeError):
                pass
    except OSError:
        pass
    try:
        best = max(best, int((json.loads((home() / "latest.json").read_text(encoding="utf-8")) or {}).get("ronde") or 0))
    except (OSError, ValueError, AttributeError, TypeError):
        pass
    return best


def _historie_bewijs() -> list:
    """Duurzame sporen van eerdere verwerking, los van het grootboek. Leeg = werkelijk verse installatie. Een spoor dat niet gelezen
    kan worden telt als aanwezig (fail-closed). Een gewone afwezigheid (FileNotFoundError) is geen spoor."""
    out = []
    for name in ("ledger-commit.json", "gebeurtenissen-archief.jsonl", "gebeurtenissen-archief.spiegel.jsonl", "rondes.jsonl", "latest.json", "voorstellen.json"):
        try:
            if (home() / name).stat().st_size > 0:
                out.append(name)
        except FileNotFoundError:
            pass
        except OSError:
            out.append(name)
    try:
        if verify_audit() or any(r.get("kind") in ("ronde", "ronde_vastgelegd", "gebeurtenis_uitgesteld_verwerkt", "gebeurtenissen_gearchiveerd", "archief_hersteld") for r in read_audit()):
            out.append("audit.jsonl")
    except OSError:
        out.append("audit.jsonl")
    return out


def _verified_audit() -> list:
    """Lees en controleer één momentopname onder dezelfde lock als de schrijvers."""
    path = home() / "audit.jsonl"
    try:
        with path.open("r", encoding="utf-8") as f:
            lock_exclusive(f)
            lines = f.read().splitlines()
    except FileNotFoundError:
        return []
    except (OSError, UnicodeError):
        raise ReassessError("audit_beschadigd", "auditspoor is niet leesbaar; niets verwerkt") from None
    problems = _audit_problems(lines)
    if problems:
        raise ReassessError("audit_beschadigd", "; ".join(problems))
    return [json.loads(ln) for ln in lines if ln.strip()]


def _committed_audit(audit: list, current_transaction: str | None = None) -> list:
    committed = {r.get("transaction") for r in audit if r.get("kind") == "ronde_vastgelegd" and r.get("commit_protocol") == 2}
    if current_transaction:
        committed.add(current_transaction)
    return [r for r in audit if r.get("commit_protocol") != 2 or r.get("transaction") in committed]


def _commit_path():
    return home() / "ledger-commit.json"


def _commit_payload(record: dict) -> dict:
    return {k: record[k] for k in ("ledger", "latest", "voorstellen", "round_record", "sleutels") if k in record}


def _read_commit(audit: list) -> dict | None:
    try:
        record = json.loads(_commit_path().read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        raise ReassessError("grootboek_commit_onbetrouwbaar", "het duurzame herstelbeeld is onleesbaar; niets verwerkt") from None
    return _validate_commit(record, audit)


def _validate_commit(record: dict, audit: list) -> dict:
    if not isinstance(record, dict) or not isinstance(record.get("ledger"), dict):
        raise ReassessError("grootboek_commit_onbetrouwbaar", "ongeldig duurzaam herstelbeeld; niets verwerkt")
    ledger = record["ledger"]
    if (record.get("version") != 2 or type(ledger.get("rondes")) is not int or ledger["rondes"] < 0
            or not isinstance(ledger.get("acties"), dict) or not isinstance(ledger.get("gebeurtenissen", {}), dict)
            or not isinstance(record.get("transaction"), str) or not record["transaction"]
            or record.get("digest") != _sha(_commit_payload(record))):
        raise ReassessError("grootboek_commit_onbetrouwbaar", "duurzaam herstelbeeld klopt niet met zijn inhoudsbewijs; niets verwerkt")
    kind = "grootboek_anker" if record.get("anchor") else "ronde"
    if not any(r.get("kind") == kind and r.get("transaction") == record["transaction"]
               and r.get("content_digest") == record["digest"] and r.get("ledger_digest") == _sha(ledger)
               and r.get("previous_digest") == record.get("previous_digest") and r.get("ronde") == ledger["rondes"] for r in audit):
        raise ReassessError("grootboek_commit_onbetrouwbaar", "herstelbeeld heeft geen passend intact auditbewijs; niets verwerkt")
    return record


def _ensure_commit_anchor(ledger: dict) -> None:
    if _commit_path().exists():
        return
    record = {"version": 2, "anchor": True, "transaction": uuid.uuid4().hex, "previous_digest": None, "ledger": ledger}
    record["digest"] = _sha(_commit_payload(record))
    _audit({"kind": "grootboek_anker", "commit_protocol": 2, "transaction": record["transaction"], "ronde": ledger["rondes"],
            "content_digest": record["digest"], "ledger_digest": _sha(ledger), "previous_digest": None})
    _save("ledger-commit.json", record)  # vóór transfer of andere bijwerkingen


def _check_recovery_outputs(record: dict) -> None:
    try:
        if record.get("anchor"):
            raise ValueError("anker is geen nieuwe ronde")
        for name, key in (("latest.json", "latest"), ("voorstellen.json", "voorstellen")):
            if json.loads((home() / name).read_text(encoding="utf-8")) != record[key]:
                raise ValueError("rapport of voorstellen wijken af")
        rounds = [json.loads(ln) for ln in (home() / "rondes.jsonl").read_text(encoding="utf-8").splitlines() if ln.strip()]
        if record["round_record"] not in rounds:
            raise ValueError("ronde ontbreekt")
    except (OSError, ValueError, KeyError, TypeError):
        raise ReassessError("grootboek_commit_onbetrouwbaar", "rapport, voorstellen of rondelog passen niet bij het herstelbeeld; niets verwerkt") from None


def _resolve_commit(ledger: dict, audit: list, archive_repair: bool) -> tuple:
    record = _read_commit(audit)
    if record is None:
        v2 = [r for r in audit if r.get("commit_protocol") == 2]
        anchors = [r for r in v2 if r.get("kind") == "grootboek_anker"]
        if anchors and len(anchors) == len(v2) and all(r.get("ledger_digest") == _sha(ledger)
                and r.get("ronde") == ledger["rondes"] for r in anchors):
            # Crash tijdens bootstrap, vóór enige bijwerking: het bestaande ledger bewijst exact het geaudite anker.
            anchor = anchors[-1]
            record = {"version": 2, "anchor": True, "transaction": anchor["transaction"], "previous_digest": None, "ledger": ledger}
            record["digest"] = _sha(_commit_payload(record))
            _validate_commit(record, audit)
            _save("ledger-commit.json", record)
        elif ledger.get("commit_protocol") == 2 or v2:
            raise ReassessError("grootboek_commit_onbetrouwbaar", "duurzaam herstelbeeld ontbreekt naast versie-2-historie; niets verwerkt")
        else:
            return ledger, None
    candidate = record["ledger"]
    if candidate["rondes"] == ledger["rondes"]:
        # De inhoudskruiscontrole geeft eerst de specifieke fout voor ontbrekende events; de digest beschermt ook de andere velden.
        _controleer_audit(ledger, _committed_audit(audit, record["transaction"]))
        if _sha(candidate) != _sha(ledger):
            raise ReassessError("grootboek_onvolledig", "het grootboek wijkt af van het duurzame inhoudsbewijs; niets verwerkt")
    elif (candidate["rondes"] == ledger["rondes"] + 1 and record.get("previous_digest") == _sha(ledger)
          and not any(r.get("kind") == "ronde_vastgelegd" and r.get("transaction") == record["transaction"] for r in audit)):
        _check_recovery_outputs(record)
        _controleer_audit(candidate, _committed_audit(audit, record["transaction"]))
        if archive_repair:
            return candidate, record  # eerst archief herstellen; promoveer het grootboek nog niet
        _archived_keys(candidate)
        _save("ledger.json", candidate)  # exact duurzaam herstelbeeld, geen transfer/evaluatie/herverwerking
        ledger = candidate
    else:
        raise ReassessError("grootboek_verouderd", "grootboek en duurzaam herstelbeeld spreken elkaar tegen; niets verwerkt")
    if not record.get("anchor") and not any(r.get("kind") == "ronde_vastgelegd" and r.get("transaction") == record["transaction"] for r in audit):
        _check_recovery_outputs(record)
        if archive_repair:
            return ledger, record
        _archived_keys(ledger)
        _audit({"kind": "ronde_vastgelegd", "ronde": ledger["rondes"], "commit_protocol": 2, "transaction": record["transaction"],
                "ledger_digest": _sha(ledger), "sleutels": record.get("sleutels", [])})
        audit = _verified_audit()
    return ledger, record

def load_ledger(*, archive_repair: bool = False) -> dict:
    """Het grootboek. Ontbreekt `ledger.json` terwijl er andere duurzame historie is (archief, spiegel, rondelog, rapport, auditspoor), dan is
    dat een verdwenen grootboek en géén verse installatie: ReassessError `grootboek_ontbreekt`, voordat enig event wordt beoordeeld.
    Een leeg of ouder grootboek naast een recenter rondenummer elders is evenmin betrouwbaar (`grootboek_verouderd`).
    Alleen `restore_ledger` met aantoonbaar complete bron maakt dit ongedaan."""
    default = {"version": VERSION, "rondes": 0, "acties": {}}
    path = home() / "ledger.json"
    try:
        path.stat()
    except FileNotFoundError:
        bewijs = _historie_bewijs()
        if bewijs:
            _verified_audit()  # voeg nooit iets toe aan een beschadigd spoor
            _audit({"kind": "grootboek_ontbreekt", "bewijs": bewijs})
            raise ReassessError("grootboek_ontbreekt",
                                f"{path} ontbreekt terwijl er historie is ({', '.join(bewijs)}); er wordt niets verwerkt tot `r2 reassess grootboek-herstel --bron <back-up>` "
                                "een aantoonbaar complete back-up heeft teruggezet") from None
        return default
    except OSError:
        raise ReassessError("unreadable", f"{path} is onleesbaar; de herbeoordeling schrijft niets tot een mens dit heeft hersteld") from None
    d = _load("ledger.json", default)
    if not (isinstance(d.get("acties"), dict) and isinstance(d.get("gebeurtenissen", {}), dict)
            and isinstance(d.get("rondes"), int) and not isinstance(d.get("rondes"), bool) and d["rondes"] >= 0):
        raise ReassessError("unreadable", f"{path} heeft een ongeldige structuur; de herbeoordeling schrijft niets tot een mens dit heeft hersteld")
    audit = _verified_audit()
    audit_rounds = {r["ronde"] for r in audit if r.get("kind") == "ronde" and type(r.get("ronde")) is int and r["ronde"] > 0}
    if len({n for n in audit_rounds if n <= d["rondes"]}) != d["rondes"]:
        raise ReassessError("audit_onvolledig", "auditspoor mist rondes van het grootboek; niets verwerkt")
    d, record = _resolve_commit(d, audit, archive_repair)
    audit = _verified_audit()
    committed = _committed_audit(audit, record["transaction"] if record else None)
    rounds = {r["ronde"] for r in committed if r.get("kind") == "ronde" and type(r.get("ronde")) is int and r["ronde"] > 0}
    if len({n for n in rounds if n <= d["rondes"]}) != d["rondes"]:
        raise ReassessError("audit_onvolledig", "auditspoor bewijst niet elke ronde van het grootboek; niets verwerkt tot de historie is hersteld")
    vastgelegd = max([r["ronde"] for r in committed if r.get("kind") == "ronde_vastgelegd" and type(r.get("ronde")) is int] or [0])
    if d["rondes"] < vastgelegd:
        raise ReassessError("grootboek_verouderd", "het grootboek is ouder dan de laatst vastgelegde ronde; niets verwerkt")
    ahead = [r for r in audit if r.get("kind") == "ronde" and type(r.get("ronde")) is int and r["ronde"] > d["rondes"]]
    newer = max([r["ronde"] for r in ahead] + [_rondes_bewijs()])
    if newer > d["rondes"]:
        # Alleen een aantoonbaar v2-voorbereiding vóór het commitpunt mag opnieuw worden geëvalueerd.
        if not (record and newer == d["rondes"] + 1 and ahead and all(r.get("commit_protocol") == 2
                and r.get("previous_digest") == _sha(d) and r["ronde"] == newer for r in ahead)):
            raise ReassessError("grootboek_verouderd", "voorbereide en vastgelegde historie zijn niet ondubbelzinnig te onderscheiden; niets verwerkt")
    _controleer_audit(d, committed)
    return d


def _controleer_audit(d: dict, audit: list | None = None) -> None:
    """Kruiscontrole met het hash-geketende auditspoor: elk event dat in een ronde met nummer <= `rondes` is verwerkt moet in de snelle index
    of in archief/spiegel staan. Anders is het grootboek een leeg of afgekapt exemplaar naast bestaande historie (`grootboek_onvolledig`).
    Rondes boven `rondes` zijn een crash vóór het commitpunt en horen herverwerkt te worden. Leniënt gelezen archief: de strikte controle
    tegen het manifest doet `_archived_keys`; de kruiscontrole hier draait ook mét manifest; hier mag een beschadigd archief het herstelpad niet blokkeren."""
    rondes = d["rondes"]
    audit = _committed_audit(_verified_audit()) if audit is None else audit
    vast = set()
    for r in audit:
        if r.get("kind") == "ronde_vastgelegd" and isinstance(r.get("ronde"), int) and r["ronde"] <= rondes:
            vast |= {k for k in (r.get("sleutels") or []) if isinstance(k, str) and _KEY_RE.match(k)}
    # ook met manifest: het manifest bewijst alleen het archief, niet dat de index nog alle recente sleutels heeft
    need = set(vast)
    for r in audit:
        k = r.get("gebeurtenis") if r.get("kind") == "ronde" else r.get("sleutel") if r.get("kind") == "gebeurtenis_uitgesteld_verwerkt" else None
        if isinstance(k, str) and _KEY_RE.match(k) and isinstance(r.get("ronde"), int) and r["ronde"] <= rondes:
            need.add(k)
    need -= set(d.get("gebeurtenissen") or {})
    if not need:
        return
    for archive_source, src in ((True, _read_archive), (False, lambda: _mirror_path().read_bytes() if _mirror_path().exists() else b"")):
        try:
            keys, bad = _parse_archive(src())
        except OSError:
            continue
        if bad or (isinstance(d.get("archief"), dict) and not _fits(keys, d)):
            if archive_source:
                return   # het archief zelf blokkeert via `_archived_keys`; houd `repair_archive` bereikbaar
            continue     # een onbruikbare spiegel bewijst niets en mag ontbrekende recente sleutels niet verbergen
        need -= keys
    if need:
        _audit({"kind": "grootboek_onvolledig", "rondes": rondes, "ontbrekend": len(need)})
        raise ReassessError("grootboek_onvolledig", f"het grootboek (ronde {rondes}) mist {len(need)} event(s) die het auditspoor als verwerkt kent; niets verwerkt tot een mens "
                            "`r2 reassess grootboek-herstel --bron <back-up>` met een complete back-up heeft gedaan")


def load_proposals() -> dict:
    return _load("voorstellen.json", {"version": VERSION, "voorstellen": {}})


def _save(name: str, doc: dict) -> None:
    store.atomic_write(home() / name, json.dumps(doc, indent=1, ensure_ascii=False, sort_keys=True))


# --- register ---

def register_index(register: dict | None) -> dict | None:
    """action_id -> [{nr, relatie, zekerheid}] uit het beslissingenregister; None als het register ontbreekt."""
    if not isinstance(register, dict):
        return None
    out: dict = {}
    for b in register.get("besluiten") or []:
        for a in b.get("acties") or []:
            if a.get("id"):
                out.setdefault(a["id"], []).append({"nr": b.get("nr"), "relatie": a.get("relatie"), "zekerheid": a.get("zekerheid")})
    return out


def _rels(rels: list | None, names: tuple) -> list:
    return [r for r in rels or [] if r["relatie"] in names]


# --- classificatie: een zuivere functie van planentry, closure-entry en register ---

def levensloop(closure: dict | None, rels: list | None, register_ok: bool, local: bool) -> dict:
    """De vier dimensies, apart. Wat niet uit een claim volgt blijft `onbekend`; nooit afgeleid uit tekst of register-triage."""
    if closure:
        lc = closure["lifecycle"]
        return {k: lc[k]["status"] for k in ("besluit", "uitvoering", "controle", "integratie")} | {"bron": "bewijsgerichte afsluiting (claim)"}
    named = _rels(rels, ("genoemd-in-besluit",))
    besluit = "genomen" if any(r["zekerheid"] == "bewezen" for r in named) else "onbekend" if not register_ok else "ontbreekt"
    why = "niet gekoppeld aan een opdracht" if local else "andere node: claims worden alleen op de control plane beoordeeld"
    return {"besluit": besluit, "uitvoering": "onbekend", "controle": "onbekend", "integratie": "onbekend", "bron": why}


def safe_technical(entry: dict, rels: list | None, closure: dict | None) -> list:
    """Redenen waarom dit punt NIET veilig zelfstandig uitvoerbaar is (leeg = veilig). Positief bewijs uit het register is vereist."""
    why = []
    if not _rels(rels, _TECHNISCH):
        why.append("het register kent dit punt niet als technisch vervolgwerk")
    if _rels(rels, _MENS + _UITGESTELD + _AFGEHANDELD):
        why.append("het register noemt het als menselijke handeling, uitgesteld of afgehandeld")
    if closure:
        why.append("er is al een opdracht aan gekoppeld")
    if entry.get("protected"):
        why.append("beschermde categorie: " + ", ".join(entry["protected"]))
    text = entry.get("text") or ""
    if actionclosure._QUESTION.search(text) or _DECISION_WORDS.search(text):
        why.append("leest als vraag of keuze")
    if entry.get("bucket") != actionreview.DECIDE:
        why.append(f"actiebeheer plaatst het in '{entry.get('bucket')}'")
    return why


def classify(entry: dict, closure: dict | None, rels: list | None, register_ok: bool, since: str | None = None) -> dict:
    """Klasse van één open punt. Volgorde: bewijs > actiebeheer > register > mens. Sluit of wijzigt niets."""
    bucket = entry["bucket"]
    reasons = []
    if closure:
        v = closure["verdict"]
        reasons = list(closure.get("reasons") or [])
        klasse = {"close": "bewijs_sluit", "wait": "in_uitvoering", "doubt": "claim_twijfel"}[v]
        if klasse == "bewijs_sluit" and not actionclosure.lifecycle_complete(closure.get("lifecycle")):
            klasse, reasons = "claim_twijfel", reasons + ["sluitoordeel zonder volledig bewijs voor besluit, uitvoering, controle en integratie"]
        return {"klasse": klasse, "redenen": reasons}
    if bucket == actionreview.WOULD_CLOSE or (bucket == actionreview.REVIEW and entry.get("rule") in actionreview.CLOSING_PROPOSAL_RULES):
        return {"klasse": "dubbel_of_beslist", "redenen": [entry.get("label") or ""] + list(entry.get("facts") or [])}
    if bucket == actionreview.REVIEW:
        return {"klasse": "twijfel", "redenen": [entry.get("label") or ""]}
    if _rels(rels, _UITGESTELD):
        return {"klasse": "uitgesteld", "redenen": ["register: " + _rels(rels, _UITGESTELD)[0]["relatie"]]}
    if _rels(rels, _AFGEHANDELD):
        r = _rels(rels, _AFGEHANDELD)[0]
        return {"klasse": "afgehandeld_volgens_register", "redenen": [f"register (besluit {r['nr']}, {r['zekerheid']}): {r['relatie']}"]}
    if _rels(rels, _TECHNISCH):
        why = safe_technical(entry, rels, closure)
        return {"klasse": "technisch_open" if why else "technisch_voorbereid", "redenen": why}
    named = _rels(rels, ("genoemd-in-besluit",))
    if named and not entry.get("protected") and not actionclosure._QUESTION.search(entry.get("text") or ""):
        return {"klasse": "besloten_wacht_op_uitvoering", "redenen": [f"besluit {named[0]['nr']} noemt dit punt ({named[0]['zekerheid']})"]}
    # Fail-closed: alleen positief bewijs maakt van een punt een menselijke vraag. Een beschermde categorie, een
    # onzekere registerverwijzing of een ontbrekende registratie alleen bewijst dat niet.
    human = _rels(rels, _MENS)
    question = is_human_question(entry.get("text") or "")
    if human and any(r["zekerheid"] == "bewezen" for r in human):
        r = next(r for r in human if r["zekerheid"] == "bewezen")
        return {"klasse": "mens_bekend", "redenen": [f"register (besluit {r['nr']}, bewezen): {r['relatie']}"],
                "menselijk_bewijs": "vraag+register" if question else "register_bewezen"}
    if question:
        if human:
            return {"klasse": "mens_bekend", "redenen": [f"vraag; register: {human[0]['relatie']} (besluit {human[0]['nr']})"], "menselijk_bewijs": "vraag"}
        if not register_ok:
            return {"klasse": "mens_bekend", "redenen": ["vraag; register niet beschikbaar: er wordt niets als nieuw voorgelegd"], "menselijk_bewijs": "vraag"}
        if since and (entry.get("created_at") or "") < since:
            return {"klasse": "mens_bekend", "redenen": [f"vraag; stond al open toen het register werd aangemaakt ({since}) en is toen als gewoon actiepunt gemeld"], "menselijk_bewijs": "vraag"}
        return {"klasse": "mens_nieuw", "redenen": ["vraag aan een mens; niet in het register en nog niet eerder voorgelegd"], "menselijk_bewijs": "vraag"}
    if entry.get("possible_duplicate_of"):
        return {"klasse": "mogelijk_dubbel", "redenen": ["mogelijk dubbele tekst; geen sluitbewijs"]}
    return {"klasse": "nader_onderzoek", "redenen": ["onvoldoende onafhankelijk bewijs voor actuele menselijke beslissing"]}


_BEWEZEN_MENS = ("open-actie:BEANTWOORD", "open-actie:VERVALLEN", "open-actie:DUBBEL")


def closing_ground(klasse: str, rels: list | None) -> dict:
    """Of een rij op bewijs mag sluiten, en op welke grond. Tekstgelijkenis, een mogelijk-dubbel-vlag, een actiebeheer-bak of een
    afgeleide registerrelatie zijn nooit een grond. Grond is alleen: volledig bewijs (klasse bewijs_sluit) of een bewezen mensbeslissing."""
    if klasse == "bewijs_sluit":
        return {"mag_sluiten": True, "grond": "bewijs"}
    human = [r for r in _rels(rels, _BEWEZEN_MENS) if r["zekerheid"] == "bewezen"]
    if human and klasse in ("afgehandeld_volgens_register",):
        return {"mag_sluiten": True, "grond": "mensbeslissing", "besluit": human[0]["nr"]}
    return {"mag_sluiten": False, "grond": None}


def is_human_question(text: str) -> bool:
    """Positieve herkenning van een menselijke vraag of reactieverzoek. Een vraagteken telt alleen als echt vraagteken (geen URL of
    querystring: 'pad?id=3'). Een echt vraagteken volstaat, ook kort ('Mag dit?', 'Kunt u kiezen?', 'Welke optie?', 'Wie beslist?', 'Akkoord?'); een expliciet
    reactieverzoek ('Graag uw reactie op dit voorstel') ook; zonder vraagteken is vraagvorm of beslissingswoord plus ≥ 3 woorden nodig. 'Test moet akkoord zijn?' (technisch onderwerp, geen persoon) is geen menselijke beslissing. Een modaal werkwoord midden in een
    technische mededeling ('De test moet slagen') is geen vraag."""
    clean = _URL_OR_QUERY.sub(" ", text or "")
    words = len(re.findall(r"\w{2,}", clean))
    if _RESPONSE_REQUEST.search(clean):
        return True
    if _TECH_SUBJECT_OK.search(clean) and not _PERSON.search(clean):
        return False
    if _TECH_PURE_FORM.search(clean) and not _PERSON.search(clean) and not _NEEDS_HUMAN.search(clean):
        return False
    # Een echt vraagteken is zichtbaar, ook bij een korte vraag ('Mag dit?', 'Wie beslist?', 'Akkoord?'): liever een vraag te veel op de lijst
    # dan een menselijke vraag die verdwijnt.
    if "?" in clean and words >= 1:
        return True
    if not (actionclosure._QUESTION.search(clean) or _DECISION_WORDS.search(clean)):
        return False
    return words >= 3


# --- voorbereiding van technisch vervolgwerk ---

def proposal_id(action_id: str) -> str:
    return "PV-" + hashlib.sha1(f"voorstel\0{action_id}".encode()).hexdigest()[:10]


def draft_prompt(entry: dict) -> str:
    return (
        f"Technisch vervolgwerk bij open actiepunt {entry['id']} (node {entry['node']}).\n\n"
        f"Het punt: {entry.get('text')}\n\n"
        "Werk uitsluitend aan dit punt, in kleine veilige stappen. Onderzoek eerst wat er al is en wat al gedaan is, zodat er geen dubbel werk ontstaat. "
        "Doe niets met toegang, geheimen, verwijderen, publicatie of kosten; stuit je daarop, meld dan een open_action in plaats van door te gaan. "
        "Voer gerichte tests uit, commit op de taakbranch en gebruik de bestaande integratieketen. Rapporteer wat is gedaan, getest en geïntegreerd, "
        "elk apart.")


def queue_room(cfg: dict) -> dict:
    """Ruimte in de opdrachtwachtrij (alleen lezen). `ok` False = geen voorstel naar de wachtrij zetten."""
    reserve = int(cfg.get("action_reassess_queue_reserve", 30))
    try:
        q = jobs.load()
    except jobs.JobsError as e:
        return {"leesbaar": False, "ok": False, "reden": e.message[:200], "max": jobs.MAX_OPEN_JOBS}
    open_ = sum(1 for j in q["jobs"].values() if j["status"] not in jobs.END_STATES)
    return {"leesbaar": True, "open": open_, "max": jobs.MAX_OPEN_JOBS, "ruimte": jobs.MAX_OPEN_JOBS - open_, "reserve": reserve,
            "ok": jobs.MAX_OPEN_JOBS - open_ > reserve, "_jobs": q["jobs"]}


def existing_job_for(action_id: str, queue_jobs: dict) -> str | None:
    """Een open opdracht die dit punt al noemt (dubbele taken voorkomen)."""
    for jid, j in sorted(queue_jobs.items()):
        if j["status"] not in jobs.END_STATES and action_id in " ".join(str(j.get(k) or "") for k in ("prompt", "title")):
            return jid
    return None


def prepare(entry: dict, room: dict, props: dict, now: str) -> dict:
    """Legt het concept vast (idempotent per punt). Zet nooit iets in de wachtrij."""
    pid = proposal_id(entry["id"])
    fp = _sha([entry["id"], entry.get("text")])
    jid = existing_job_for(entry["id"], room.get("_jobs") or {})
    status = ("in_wachtrij" if jid else "voorbereid" if room.get("ok") else "wacht_op_ruimte")
    old = props["voorstellen"].get(pid)
    rec = {"id": pid, "action_id": entry["id"], "node": entry["node"], "repo": entry.get("repo") or None,
           "title": ("Vervolgwerk: " + _clip(entry.get("text"), 80))[:120], "prompt": draft_prompt(entry), "fingerprint": fp,
           "status": status, "bestaande_opdracht": jid,
           "volgende_stap": (f"opdracht {jid} bestaat al: koppel met `r2 actions --claim {entry['id']} --job {jid} --technisch`" if jid else
                             "voeg het concept toe met runner2_jobs (add) op de genoemde node en koppel het daarna met "
                             f"`r2 actions --claim {entry['id']} --job J-… --technisch`; dat sluit niets"),
           "created_at": (old or {}).get("created_at") or now, "updated_at": now,
           "history": list((old or {}).get("history") or [])}
    if not old:
        rec["history"].append({"at": now, "event": "voorbereid"})
    else:
        if old.get("fingerprint") != fp:
            rec["history"].append({"at": now, "event": "tekst_gewijzigd", "was": old.get("fingerprint")})
        if old.get("status") != status:
            rec["history"].append({"at": now, "event": "status", "van": old.get("status"), "naar": status})
    rec["history"] = rec["history"][-LEDGER_HISTORY:]
    props["voorstellen"][pid] = rec
    return rec


# --- een ronde ---

def collect(cfg: dict, call=None, nodes=None) -> dict:
    """Plannen per node (actiebeheer, alleen lezen); een onbereikbare node of oude Runner staat onder `errors`.
    Standaard de nodes uit `action_reassess_nodes` (MAC, LINUX, GERARD); een lege lijst = alle geconfigureerde nodes."""
    return actionreview.review_all(nodes or list(cfg.get("action_reassess_nodes") or []) or None, call)


def _closure_entries(cfg: dict, register) -> tuple:
    try:
        res = actionclosure.run(cfg, apply=False, register=register if register is not None else {})
    except actionclosure.ClosureError as e:
        return {}, f"afsluitronde bezet: {e.message}"
    except Exception as e:   # noqa: BLE001 -- zonder closure-info blijven uitvoering/controle onbekend; nooit gokken
        return {}, f"afsluiting niet beoordeeld ({type(e).__name__})"
    return {e["id"]: e for e in res["report"]["entries"]}, None


def evaluate(cfg: dict, plans: dict, closure: dict, register: dict | None, ledger: dict, props: dict, now: str, local: str,
             persist: bool = True) -> dict:
    """Het rapport van één ronde uit verzamelde gegevens. Met `persist` worden grootboek en voorstellen bijgewerkt (in `ledger`/`props`)."""
    idx = register_index(register)
    ok = idx is not None
    since = (register or {}).get("aangemaakt") if ok else None
    room = queue_room(cfg)
    entries, per_node = [], {}
    for p in plans["plans"]:
        node = p["node"]
        per_node[node] = {"status": "ok", "open": p["counts"]["open"]}
        for b in PLAN_BUCKETS:
            for e in p[b]:
                entries.append(dict(e, node=node, bucket=b))
    for node, msg in plans["errors"].items():
        per_node[node] = {"status": "niet_beoordeeld", "fout": _clip(msg, 200)}
    entries.sort(key=lambda e: (e["node"], e["created_at"] or "", e["id"]))
    rows = []
    for e in entries:
        rels = idx.get(e["id"]) if ok else None
        cl = closure.get(e["id"]) if e["node"] == local else None
        c = classify(e, cl, rels, ok, since)
        row = {"id": e["id"], "node": e["node"], "task_id": e.get("task_id"), "repo": e.get("repo") or None, "created_at": e.get("created_at"),
               "text": _clip(e.get("text"), 160), "is_vraag": is_human_question(e.get("text") or ""), "klasse": c["klasse"], "mens": KLASSEN[c["klasse"]][1], "categorie": CATEGORIEEN[c["klasse"]],
               "menselijk_bewijs": c.get("menselijk_bewijs"), "sluiting": closing_ground(c["klasse"], rels), "redenen": c["redenen"],
               "levensloop": levensloop(cl, rels, ok, e["node"] == local), "register": rels or []}
        if c["klasse"] == "technisch_voorbereid":
            row["voorstel"] = prepare(e, room, props, now)["id"] if persist else proposal_id(e["id"])
        rows.append(row)
    current = {r["id"] for r in rows if r.get("voorstel")}
    if persist:
        for p in props["voorstellen"].values():
            if p["status"] in ("voorbereid", "wacht_op_ruimte") and p["action_id"] not in current and \
                    per_node.get(p["node"], {}).get("status") == "ok":
                p["status"] = "vervallen"
                p["history"] = (p["history"] + [{"at": now, "event": "vervallen",
                                                  "reden": "punt is niet meer open of niet meer veilig technisch"}])[-LEDGER_HISTORY:]
    new = [r for r in rows if r["klasse"] == "mens_nieuw" and not (ledger["acties"].get(r["id"]) or {}).get("voorgelegd_at")]
    counts = {k: sum(1 for r in rows if r["klasse"] == k) for k in KLASSEN}
    categorie_counts = {k: sum(1 for r in rows if r["categorie"] == k) for k in sorted(set(CATEGORIEEN.values()))}
    beslissingen = decisions(rows, ledger)
    changed = [{"id": r["id"], "van": (ledger["acties"].get(r["id"]) or {}).get("klasse"), "naar": r["klasse"]}
               for r in rows if (ledger["acties"].get(r["id"]) or {}).get("klasse") not in (None, r["klasse"])]
    room_pub = {k: v for k, v in room.items() if k != "_jobs"}
    rep = {"version": VERSION, "generated_at": now, "node": local, "sluit_niets": True, "register_beschikbaar": ok,
           "nodes": per_node, "totaal_open": len(rows), "counts": counts, "categorie_counts": categorie_counts,
           "beslissingen": beslissingen, "wachtrij": room_pub,
           "nieuw_voor_beheerder": [{k: r[k] for k in ("id", "node", "text", "created_at")} for r in new],
           "voorbereid": [{k: v for k, v in p.items() if k in ("id", "action_id", "node", "title", "status", "bestaande_opdracht")}
                          for p in sorted(props["voorstellen"].values(), key=lambda p: p["id"])
                          if p["action_id"] in current and p["status"] in ("voorbereid", "wacht_op_ruimte", "in_wachtrij")] if persist else
                      [{"action_id": r["id"], "id": r["voorstel"], "node": r["node"]} for r in rows if r.get("voorstel")],
           "gewijzigd_sinds_vorige_ronde": changed, "entries": rows}
    return rep


_TWIJFEL_KLASSEN = ("twijfel", "claim_twijfel", "mogelijk_dubbel", "nader_onderzoek")


def decisions(rows: list, ledger: dict) -> list:
    """Actuele beslissingenlijst: punten met positief bewijs van een menselijke vraag of bewezen menselijke registerrelatie, plus
    punten die als twijfel zijn geclassificeerd maar zelf een menselijke vraag zijn (`bewijs` = 'vraag_in_twijfel', met de klasse erbij):
    zo verdwijnt een vraag niet in een twijfelbak. `nieuw` = nog nooit voorgelegd (eenmalig gemeld); twijfelvragen zijn nooit `nieuw`
    (ze veranderen geen melding of klasse) maar staan wel zichtbaar in de lijst. Sluiten doet de lijst niet."""
    out = []
    for r in rows:
        if r["klasse"] in ("mens_nieuw", "mens_bekend") and r.get("menselijk_bewijs"):
            out.append({"id": r["id"], "node": r["node"], "text": r["text"], "created_at": r["created_at"], "bewijs": r["menselijk_bewijs"],
                        "klasse": r["klasse"],
                        "nieuw": r["klasse"] == "mens_nieuw" and not (ledger["acties"].get(r["id"]) or {}).get("voorgelegd_at")})
        elif r["klasse"] in _TWIJFEL_KLASSEN and r.get("is_vraag"):
            out.append({"id": r["id"], "node": r["node"], "text": r["text"], "created_at": r["created_at"], "bewijs": "vraag_in_twijfel",
                        "klasse": r["klasse"], "nieuw": False})
    return out


def _update_ledger(ledger: dict, rep: dict, now: str) -> None:
    """Grootboek bijwerken. Een node die niet beoordeeld is wordt niet aangeraakt; een punt verdwijnt nooit, het krijgt `niet_meer_open_sinds`."""
    seen = set()
    for r in rep["entries"]:
        seen.add(r["id"])
        rec = ledger["acties"].setdefault(r["id"], {"node": r["node"], "eerste_keer_gezien": now, "klasse_historie": [], "rondes": 0})
        rec.pop("niet_meer_open_sinds", None)
        if rec.get("klasse") != r["klasse"]:
            rec["klasse_historie"] = (rec.get("klasse_historie") or [])[-(LEDGER_HISTORY - 1):] + [
                {"at": now, "van": rec.get("klasse"), "naar": r["klasse"], "levensloop": r["levensloop"]}]
            rec["klasse_sinds"] = now
        rec.update(node=r["node"], klasse=r["klasse"], laatste_keer_gezien=now, rondes=int(rec.get("rondes") or 0) + 1,
                   levensloop=r["levensloop"])
        if r["klasse"] == "mens_nieuw" and not rec.get("voorgelegd_at"):
            rec["voorgelegd_at"] = now
    ok_nodes = {n for n, v in rep["nodes"].items() if v["status"] == "ok"}
    for aid, rec in ledger["acties"].items():
        if aid not in seen and rec.get("node") in ok_nodes and not rec.get("niet_meer_open_sinds"):
            rec["niet_meer_open_sinds"] = now
    ledger["rondes"] = int(ledger.get("rondes") or 0) + 1
    ledger["bijgewerkt"] = now


def headline(rep: dict) -> str:
    c, w = rep["counts"], rep["wachtrij"]
    nodes = ", ".join(f"{n} {v['open'] if v['status'] == 'ok' else 'niet beoordeeld'}" for n, v in sorted(rep["nodes"].items()))
    return (f"{rep['totaal_open']} open ({nodes}) · nieuw voor Beheerder {len(rep['nieuw_voor_beheerder'])} · voorbereid {len(rep['voorbereid'])} · "
            f"in uitvoering {c['in_uitvoering']} · sluitbaar met bewijs {c['bewijs_sluit']} · wachtrij {w.get('open', '?')}/{w['max']}")


def run(cfg: dict | None = None, persist: bool = True, call=None, nodes=None, register=None, now: str | None = None) -> dict:
    """Eén herbeoordeling. `persist=False` = alleen berekenen (CLI-rapport): niets wordt geschreven en niets als voorgelegd gemarkeerd."""
    cfg = cfg or config.load_config()
    now = now or store.now()
    register = register if register is not None else actionclosure.load_register()
    local = config.local_node()
    if persist:
        with _run_lock():
            return _round(cfg, True, call, nodes, register, now, local)
    return _round(cfg, False, call, nodes, register, now, local)


EVENT_KEEP = 500   # alleen de omvang van de snelle index in het grootboek; een oudere sleutel verdwijnt nooit maar gaat naar het archief


def _archive_path():
    return home() / "gebeurtenissen-archief.jsonl"


def _mirror_path():
    return home() / "gebeurtenissen-archief.spiegel.jsonl"


_KEY_RE = re.compile(r"^[0-9a-f]{24}$")
_SALVAGE_RE = re.compile(rb'"sleutel"\s*:\s*"([0-9a-f]{24})"')


def _parse_archive(raw: bytes) -> tuple:
    """(sleutels, beschadigde_regels[(nummer, bytes)]). Een geldige laatste regel zonder newline is geldig; elke andere regel die niet
    als {"sleutel": <24 hex>} te lezen is, is beschadigd."""
    keys, bad = set(), []
    for i, ln in enumerate(raw.split(b"\n")):
        if not ln.strip():
            continue
        try:
            k = json.loads(ln.decode("utf-8"))["sleutel"]
            if not (isinstance(k, str) and _KEY_RE.match(k)):
                raise ValueError("sleutel")
            keys.add(k)
        except (ValueError, KeyError, TypeError):
            bad.append((i + 1, ln))
    return keys, bad


def _read_archive() -> bytes:
    try:
        return _archive_path().read_bytes()
    except FileNotFoundError:
        return b""
    except OSError as e:
        _audit({"kind": "archief_beschadigd", "fout": type(e).__name__})
        raise ReassessError("archief_beschadigd", f"{_archive_path()} is onleesbaar; verwerking stopt tot een mens dit heeft hersteld") from None


def _manifest(committed: set) -> dict:
    """Duurzaam bewijs in het grootboek (atomisch opgeslagen, ander bestand dan het archief): aantal en vingerafdruk van alle sleutels die
    uit de snelle index zijn en dus alleen nog in het archief staan. Een verloren of aangetaste archiefregel is daardoor aantoonbaar."""
    return {"aantal": len(committed), "digest": hashlib.sha256("\n".join(sorted(committed)).encode()).hexdigest()}


def _fits(keys: set, ledger: dict) -> bool:
    """Klopt de sleutelverzameling met het bewijs? Sleutels die (nog) in het grootboek staan tellen niet mee: die zijn al op twee plekken
    bekend en het archief mag ze hebben (crash tussen archiveren en grootboek). Zonder manifest (oud grootboek) is er niets te bewijzen."""
    man = ledger.get("archief")
    if not isinstance(man, dict):
        return True
    return _manifest(keys - set(ledger.get("gebeurtenissen") or {})) == {"aantal": man.get("aantal"), "digest": man.get("digest")}


def _archived_keys(ledger: dict | None = None) -> set:
    """Sleutels van events die uit de snelle index zijn gehaald. Duurzaam beleid: een verwerkt event blijft voor altijd herkenbaar
    (geen verloop), zodat een herhaling na het 500e event geen tweede ronde of tweede melding geeft.
    Fail-closed op twee manieren: (1) elke beschadigde regel (ook een afgebroken staart) stopt de verwerking; (2) ook een ogenschijnlijk
    schoon archief moet kloppen met het manifest in het grootboek, anders is verwerkingsgeschiedenis weg en zou een bekend event als nieuw
    gelden. Beide: auditregel en ReassessError tot `repair_archive` de geschiedenis aantoonbaar volledig heeft hersteld."""
    ledger = load_ledger() if ledger is None else ledger
    raw = _read_archive()
    keys, bad = _parse_archive(raw)
    if bad:
        torn = not raw.endswith(b"\n") and bad[-1][0] == len(raw.split(b"\n"))
        _audit({"kind": "archief_staart_afgebroken" if torn else "archief_beschadigd", "regels": [n for n, _ in bad][:20]})
        raise ReassessError("archief_beschadigd", f"{_archive_path()} is beschadigd (regel {bad[0][0]}); verwerking stopt tot `r2 reassess archief-herstel` de geschiedenis aantoonbaar heeft hersteld")
    if not _fits(keys, ledger):
        _audit({"kind": "verwerkingsgeschiedenis_onvolledig", "in_archief": len(keys), "verwacht": (ledger.get("archief") or {}).get("aantal")})
        raise ReassessError("verwerkingsgeschiedenis_onvolledig",
                            "het archief van verwerkte events klopt niet met het bewijs in het grootboek (regel kwijt?); verwerking stopt tot "
                            "`r2 reassess archief-herstel` (eventueel met --bron) de geschiedenis aantoonbaar heeft hersteld")
    return keys


def _salvage(raw: bytes) -> set:
    return {m.decode() for m in _SALVAGE_RE.findall(raw)}


def _audit_keys(audit: list | None = None) -> set:
    """Alleen vastgelegde transactie-events; voorbereidingen van afgebroken pogingen zijn geen bewijs van verwerking."""
    audit = _committed_audit(_verified_audit()) if audit is None else audit
    out = set()
    for r in audit:
        keys = list(r.get("sleutels") or []) if r.get("kind") == "ronde_vastgelegd" else []
        keys += [r.get("gebeurtenis"), r.get("sleutel") if r.get("kind") == "gebeurtenis_uitgesteld_verwerkt" else None]
        out |= {k for k in keys if isinstance(k, str) and _KEY_RE.match(k)}
    return out


def repair_archive(bronnen: list | None = None) -> dict:
    """Herstel zonder herverwerking en zonder stilzwijgend verlies. Eerst wordt een kandidaat-sleutelverzameling samengesteld uit de
    leesbare regels, de sleutels die uit beschadigde bytes te redden zijn, het spiegelbestand, de extra `bronnen` (bestanden, bv. een
    back-up of een eerder afgezonderd bestand) en zo nodig het auditspoor. Alleen als die verzameling EXACT klopt met het manifest in het
    grootboek wordt het archief (na een volledige kopie van het origineel, fsync) atomisch vervangen. Klopt het niet, dan verandert er
    niets aan het archief: de verwerking blijft geblokkeerd (`geblokkeerd: True`, aantal ontbrekende sleutels) tot een mens ontbrekende
    sleutels aanlevert. Afzonderen alleen is dus nooit voldoende. Herhaald herstel op een gezond archief doet niets."""
    with _run_lock():
        ledger = load_ledger(archive_repair=True)
        audit = _verified_audit()
        commit = _read_commit(audit)
        proven_audit = _committed_audit(audit, commit["transaction"] if commit else None)
        raw = _read_archive()
        keys, bad = _parse_archive(raw)
        if not bad and _fits(keys, ledger):
            return {"hersteld": False, "sleutels": len(keys)}
        inledger = set(ledger.get("gebeurtenissen") or {})
        man = ledger.get("archief")
        # Betrouwbare bronnen (geldige regels, spiegel, --bron) staan los van sleutels die alleen uit beschadigde bytes te redden zijn:
        # die laatste komen pas bij als de betrouwbare set niet volstaat en gaan anders alleen naar het afzonderingsbestand.
        reliable = set(keys)
        confirmers = set()      # spiegel en --bron bevestigen zelfstandig; een geldige archiefregel buiten die set wordt niet blind vertrouwd
        try:
            confirmers |= _parse_archive(_mirror_path().read_bytes())[0] if _mirror_path().exists() else set()
        except OSError:
            pass
        for b in bronnen or []:
            try:
                confirmers |= _salvage(Path(b).read_bytes())
            except OSError as e:     # vóór enige wijziging: een onbruikbare bron is een fout, geen stille overslag
                raise ReassessError("bron_onleesbaar", f"--bron {b} is niet te lezen ({type(e).__name__}); niets gewijzigd") from None
        reliable |= confirmers
        salvaged = set()
        for _, ln in bad:
            salvaged |= _salvage(ln)
        chosen = None
        union = reliable | salvaged
        if isinstance(man, dict):    # zonder bewijs valt een beschadiging nooit als volledig te bewijzen
            audit = _audit_keys(proven_audit) - inledger
            for tier in (reliable, confirmers, reliable | salvaged, confirmers | salvaged, reliable | audit, union | audit, audit):
                if _fits(tier, ledger):
                    chosen = tier
                    break
            union = union | audit
        if chosen is None:
            have_n = len(union - inledger)
            want = int(man.get("aantal") or 0) if isinstance(man, dict) else None
            ontbrekend = max(0, want - have_n) if want is not None else None
            overtollig = max(0, have_n - want) if want is not None else None
            afwijkend = want is not None and have_n == want
            _audit({"kind": "archief_herstel_onvolledig", "ontbrekend": ontbrekend, "overtollig": overtollig, "manifest": isinstance(man, dict),
                    "beschadigde_regels": len(bad)})
            return {"hersteld": False, "geblokkeerd": True, "ontbrekend": ontbrekend, "overtollig": overtollig, "afwijkend": afwijkend,
                    "manifest": isinstance(man, dict),
                    "melding": ("verwerkingsgeschiedenis niet aantoonbaar volledig; niets gewijzigd, verwerking blijft geblokkeerd. "
                                + ("Het archief is afwijkend of verdacht (even veel sleutels als verwacht, maar andere): lever een betrouwbare volledige back-up of het spiegelbestand aan via --bron"
                                   if afwijkend else "Er zijn overtollige of onbevestigde sleutels: lever een betrouwbare volledige bron aan via --bron"
                                   if overtollig else "Lever de ontbrekende sleutels aan via --bron"))}
        cand = set(chosen) | (reliable & inledger) | (set(keys) & inledger)
        path = _archive_path()
        quarantine = None
        if raw:
            quarantine = path.with_name(path.name + f".beschadigd.{time.time_ns()}")
            with open(quarantine, "wb") as f:      # eerst een volledige, duurzame kopie van het origineel
                f.write(raw)
                f.flush()
                os.fsync(f.fileno())
            store.fsync_dir(path.parent)
        good = [ln for ln in raw.split(b"\n") if ln.strip() and _parse_archive(ln)[0] and _parse_archive(ln)[0] <= cand]
        have = {k for ln in good for k in _parse_archive(ln)[0]}
        out = [ln.decode("utf-8") for ln in good] + [json.dumps({"sleutel": k, "hersteld": True}) for k in sorted(cand - have)]
        text = "".join(ln + "\n" for ln in out)
        store.atomic_write(path, text)
        store.atomic_write(_mirror_path(), text)
        _audit({"kind": "archief_hersteld", "afgezonderd": quarantine.name if quarantine else None, "bytes": len(raw), "sleutels": len(cand),
                "teruggezet": len(cand - have), "beschadigde_regels": len(bad)})
        return {"hersteld": True, "afgezonderd": quarantine.name if quarantine else None, "sleutels": len(cand), "teruggezet": len(cand - have)}


def restore_ledger(bron: str) -> dict:
    """Zet een verdwenen grootboek terug uit een back-up, maar alleen als de historie daarmee aantoonbaar COMPLEET is; anders blijft de
    verwerking geblokkeerd en verandert er niets. Bewijs: (1) de back-up is een leesbaar grootboek; (2) het archief of het spiegelbestand
    klopt exact met het manifest in de back-up (minus de snelle index); (3) elke sleutel in het auditspoor staat in de back-up-index of in
    dat archief (geen event verwerkt ná de back-up); (4) het rondenummer van de back-up loopt niet achter op rondelog en rapport.
    Een bestaand grootboek wordt nooit overschreven. Een werkelijk verse installatie (geen enkele historie) heeft niets te herstellen.
    Een kopie van de bron blijft naast het grootboek staan; het terugzetten zelf is atomisch (een onderbreking laat het grootboek afwezig,
    dus nog steeds geblokkeerd, en herhalen kan). Geparkeerde events blijven onaangeroerd en worden daarna gededupliceerd."""
    with _run_lock():
        path = home() / "ledger.json"
        if os.path.lexists(path):
            return {"hersteld": False, "reden": "grootboek is aanwezig; er wordt niets overschreven"}
        if not _historie_bewijs():
            return {"hersteld": False, "reden": "verse installatie: geen blijvende historie, niets te herstellen"}
        try:
            raw = Path(bron).read_bytes()
            source = json.loads(raw.decode("utf-8"))
            source_record = source if isinstance(source, dict) and source.get("version") == 2 and "ledger" in source else None
            led = source["ledger"] if source_record else source
            if not (isinstance(led, dict) and isinstance(led.get("acties"), dict) and isinstance(led.get("gebeurtenissen", {}), dict)
                    and isinstance(led.get("rondes"), int) and not isinstance(led["rondes"], bool) and led["rondes"] >= 0):
                raise ValueError("geen grootboek")
        except (OSError, ValueError) as e:
            raise ReassessError("bron_onleesbaar", f"--bron {bron} is geen leesbaar grootboek ({type(e).__name__}); niets gewijzigd") from None
        # Een leniënt gelezen of ingekort auditspoor kan ontbrekende sleutels verbergen. Herstel vereist een intacte keten
        # én alle rondes tot de back-up; zonder dat bewijs wordt ook geen weigering aan een beschadigde keten toegevoegd.
        audit, current_record = None, None
        try:
            audit = _verified_audit()
            source_record = _validate_commit(source_record, audit) if source_record else None
            try:
                current_record = _read_commit(audit)
            except ReassessError:
                if not source_record:
                    raise
                current_record = None
            record = source_record or current_record
            if led.get("commit_protocol") == 2 and not record:
                raise ReassessError("grootboek_commit_onbetrouwbaar", "v2-back-up vereist een passend duurzaam herstelbeeld")
            if record and _sha(record["ledger"]) != _sha(led):
                raise ReassessError("grootboek_commit_onbetrouwbaar", "back-up wijkt af van het duurzame herstelbeeld")
            if source_record and not source_record.get("anchor"):
                _check_recovery_outputs(source_record)
            committed = _committed_audit(audit, record["transaction"] if record else None)
            rounds = {r["ronde"] for r in committed if r.get("kind") == "ronde" and type(r.get("ronde")) is int and r["ronde"] > 0}
            problems = []
            if len({n for n in rounds if n <= led["rondes"]}) != led["rondes"]:
                problems.append("auditspoor bewijst niet elke ronde van de back-up")
            if led["rondes"] != max(rounds | {_rondes_bewijs()}):
                problems.append("rondenummer van de back-up komt niet overeen met de duurzame historie")
        except ReassessError as e:
            problems = [str(e)]
        if problems:
            if audit is not None and current_record:
                n = current_record["ledger"]["rondes"]
                have = {r["ronde"] for r in audit if r.get("kind") == "ronde" and type(r.get("ronde")) is int and 0 < r["ronde"] <= n}
                if len(have) == n:
                    _audit({"kind": "grootboek_herstel_geweigerd", "bron": str(bron), "problemen": problems})
            return {"hersteld": False, "geblokkeerd": True, "problemen": problems,
                    "melding": "auditspoor, herstelbeeld of rondenummer is niet aantoonbaar compleet; niets gewijzigd"}
        idx = set(led.get("gebeurtenissen") or {})
        man = led.get("archief")
        akeys, abad = _parse_archive(_read_archive())
        try:
            mkeys, mbad = _parse_archive(_mirror_path().read_bytes()) if _mirror_path().exists() else (set(), [(0, b"")])
        except OSError:
            mkeys, mbad = set(), [(0, b"")]
        proven = None
        for keys, bad in ((akeys, abad), (mkeys, mbad)):
            rest = keys - idx
            if not bad and (_fits(keys, led) if isinstance(man, dict) else not rest) and (rest or not isinstance(man, dict) or man.get("aantal") == 0):
                proven = rest
                break
        problems = []
        if proven is None:
            problems.append("archief en spiegelbestand kloppen geen van beide exact met het manifest in de back-up (of de back-up kent geen manifest)")
        else:
            extra = _audit_keys(committed) - idx - proven
            if extra:
                problems.append(f"{len(extra)} event(s) in het auditspoor staan niet in de back-up (verwerkt na de back-up)")
        vastgelegd = max([r["ronde"] for r in audit if r.get("kind") == "ronde_vastgelegd" and isinstance(r.get("ronde"), int)] or [0])
        if int(led["rondes"]) < vastgelegd:
            problems.append(f"de back-up (ronde {led['rondes']}) is ouder dan de laatst vastgelegde ronde {vastgelegd}")
        if int(led["rondes"]) + 1 < _rondes_bewijs():
            problems.append(f"de back-up (ronde {led['rondes']}) loopt achter op ronde {_rondes_bewijs()}")
        if problems:
            _audit({"kind": "grootboek_herstel_geweigerd", "bron": str(bron), "problemen": problems})
            return {"hersteld": False, "geblokkeerd": True, "problemen": problems,
                    "melding": "historie niet aantoonbaar compleet; niets gewijzigd, verwerking blijft geblokkeerd. Lever een recentere complete back-up aan via --bron"}
        copy = path.with_name(path.name + f".bron.{time.time_ns()}")
        with open(copy, "wb") as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        store.fsync_dir(path.parent)
        if source_record and (not current_record or current_record != source_record):
            if _commit_path().exists():
                damaged = _commit_path().with_name("ledger-commit.json.beschadigd." + str(time.time_ns()))
                with damaged.open("wb") as f:
                    f.write(_commit_path().read_bytes())
                    f.flush()
                    os.fsync(f.fileno())
                store.fsync_dir(damaged.parent)
            _save("ledger-commit.json", source_record)
        _save("ledger.json", led)
        _audit({"kind": "grootboek_hersteld", "bron": str(bron), "kopie": copy.name, "rondes": led["rondes"], "index": len(idx), "gearchiveerd": len(proven)})
        return {"hersteld": True, "kopie": copy.name, "rondes": led["rondes"], "index": len(idx), "gearchiveerd": len(proven)}


def _archive_events(old: dict, ledger: dict | None = None) -> set | None:
    """Voeg te verdringen events toe aan het archief (append + fsync, plus een spiegelbestand) VOORDAT ze uit het grootboek gaan; een crash
    daartussen laat ze op twee plekken staan, wat onschadelijk is. Groei: precies één compacte regel per event, nooit een tweede voor
    dezelfde sleutel. Er wordt nooit iets uit het archief verwijderd: ontbreekt het afsluitende newline van een geldige laatste regel, dan
    wordt dat eerst toegevoegd. Geeft alle archiefsleutels terug (None als er niets te archiveren viel); de aanroeper legt daarmee het
    manifest vast in het grootboek."""
    if not old:
        return None
    known = _archived_keys(ledger)
    new = {k: v for k, v in old.items() if k not in known}
    home().mkdir(parents=True, exist_ok=True)
    if new:
        for path in (_archive_path(), _mirror_path()):
            with open(path, "a+b") as f:
                f.seek(0)
                data = f.read()
                f.seek(0, os.SEEK_END)
                if data and not data.endswith(b"\n"):   # geldige laatste regel zonder newline: scheiden, niets wegsnijden
                    f.write(b"\n")
                for k, v in new.items():
                    rec = {"sleutel": k, **{x: v.get(x) for x in ("soort", "ref", "ronde", "at")}}
                    f.write((json.dumps(rec, ensure_ascii=False) + "\n").encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
            store.fsync_dir(path.parent)
    _audit({"kind": "gebeurtenissen_gearchiveerd", "aantal": len(new), "sleutels": sorted(new)[:50]})
    return known | set(new)


def event_seen(ledger: dict, key: str) -> dict | None:
    """Het record van een al verwerkt event (snelle index of archief), of None. Een archief dat niet aantoonbaar volledig is geeft een
    ReassessError, nooit None."""
    rec = (ledger.get("gebeurtenissen") or {}).get(key)
    if rec:
        return rec
    return {"gearchiveerd": True} if key in _archived_keys(ledger) else None


def event_key(event: dict) -> str:
    return _sha({"soort": str(event.get("soort") or ""), "ref": str(event.get("ref") or "")})[:24]


def on_event(cfg: dict | None, event: dict, call=None, nodes=None, register=None, now: str | None = None) -> dict:
    """Eventgestuurde herbeoordeling (bv. opdracht klaar, controle, integratie, nieuw besluit). Idempotent: hetzelfde event
    (soort + ref) geeft nooit een tweede ronde of een tweede melding; de herhaling komt wel in het auditspoor. Elke verwerkte
    gebeurtenis staat met sleutel, ronde en beslissingenlijst-hash in grootboek en audit."""
    if not str(event.get("soort") or "").strip() or not str(event.get("ref") or "").strip():
        raise ReassessError("bad_event", "een gebeurtenis heeft een soort en een ref nodig")
    cfg = cfg or config.load_config()
    now = now or store.now()
    key = event_key(event)
    register = register if register is not None else actionclosure.load_register()
    with _run_lock():
        seen = event_seen(load_ledger(), key)
        if seen:
            _audit({"kind": "gebeurtenis_herhaald", "sleutel": key, "soort": event["soort"], "ref": event["ref"], "ronde": seen.get("ronde")})
            return {"al_verwerkt": True, "sleutel": key, "ronde": seen.get("ronde"), "rapport": latest()}
        rep = _round(cfg, True, call, nodes, register, now, config.local_node(), event=dict(event, sleutel=key))
        rep["al_verwerkt"] = False
        return rep


def _wachtend_path():
    """Map met één bestand per wachtend event (`<sleutel>.json`): gelijktijdige schrijvers delen geen bestand, dus er is niets om
    tegelijk aan te appenden of weg te hernoemen."""
    return home() / "wachtend"


def _park_event(event: dict, key: str, reden: str) -> None:
    """Een event dat de lock niet kreeg wordt duurzaam vastgelegd (tijdelijk bestand, fsync, atomaire rename, fsync van de map) en in het
    auditspoor gemeld; de volgende ronde verwerkt het. Dezelfde sleutel overschrijft hetzelfde bestand: nooit een dubbel."""
    d = _wachtend_path()
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / f".{key}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp"
    doc = {"sleutel": key, "soort": event["soort"], "ref": event["ref"], "at": store.now(), "reden": reden}
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(doc, ensure_ascii=False))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, d / f"{key}.json")
    store.fsync_dir(d)


def parked_keys() -> list:
    """Sleutels van de nog niet verwerkte wachtende events (alleen lezen)."""
    d = _wachtend_path()
    return sorted(p.stem for p in d.glob("*.json")) if d.is_dir() else []


def _take_parked() -> tuple:
    """(events, bestanden). NIETS wordt verwijderd: de aanroeper verwijdert de bestanden pas (`_release_parked`) nadat het grootboek
    met die events is opgeslagen. Crasht de ronde daartussen, dan staan ze er bij de herstart nog en dedupliceert het grootboek ze.
    Draait onder de run-lock. Ook restanten van het oude formaat (`wachtend.jsonl`, `wachtend.*.taken`) worden meegenomen; een
    onleesbaar bestand wordt apart gezet (`.onleesbaar`) en in het auditspoor gemeld, nooit stilzwijgend weggegooid."""
    out, files = {}, []
    d = _wachtend_path()
    legacy = [p for p in [home() / "wachtend.jsonl", *sorted(home().glob("wachtend.*.taken"))] if p.is_file()]
    for p in (sorted(d.glob("*.json")) if d.is_dir() else []) + legacy:
        try:
            lines = [p.read_text(encoding="utf-8")] if p.parent == d else p.read_text(encoding="utf-8").splitlines()
            for ln in lines:
                if ln.strip():
                    rec = json.loads(ln)
                    if not (isinstance(rec, dict) and rec["sleutel"] and rec["soort"] and rec["ref"]):
                        raise ValueError("onvolledig")
                    out.setdefault(rec["sleutel"], rec)
            files.append(p)
        except (OSError, ValueError, KeyError, TypeError) as e:
            try:
                os.replace(p, p.with_name(p.name + ".onleesbaar"))
            except OSError:
                pass
            _audit({"kind": "wachtend_onleesbaar", "bestand": p.name, "fout": type(e).__name__})
    return list(out.values()), files


def _release_parked(files: list) -> None:
    for p in files:
        p.unlink(missing_ok=True)
    for d in {p.parent for p in files}:
        store.fsync_dir(d)


def notify_event(cfg: dict | None, soort: str, ref: str, **kw) -> dict:
    """Aanroep vanuit de afrondingspaden (jobs/check, integratiebewijs, nieuw besluit, CLI). Draait alleen op de primaire control plane;
    krijgt het de lock niet, dan wordt het event vastgelegd en verwerkt de volgende ronde het alsnog. Mag het afrondingspad nooit laten mislukken."""
    from . import cplane
    event = {"soort": str(soort or "").strip(), "ref": str(ref or "").strip()}
    if not event["soort"] or not event["ref"]:
        raise ReassessError("bad_event", "een gebeurtenis heeft een soort en een ref nodig")
    if not cplane.is_primary():
        return {"overgeslagen": "niet_primair", "sleutel": event_key(event)}
    try:
        return on_event(cfg, event, **kw)
    except ReassessError as e:
        key = event_key(event)
        if e.code in ("archief_beschadigd", "verwerkingsgeschiedenis_onvolledig", "grootboek_ontbreekt", "grootboek_verouderd", "grootboek_onvolledig", "unreadable", "audit_beschadigd", "audit_onvolledig", "grootboek_commit_onbetrouwbaar"):
            # geschiedenis niet bewezen: het event wordt niet verwerkt maar ook niet kwijt; parkeren, auditregel, en toch fail-closed melden
            try:
                _park_event(event, key, e.code)
                if e.code not in ("audit_beschadigd", "audit_onvolledig"):
                    _audit({"kind": "gebeurtenis_geparkeerd_archief", "sleutel": key, "soort": event["soort"], "ref": event["ref"], "code": e.code})
            except OSError as oe:
                _audit({"kind": "gebeurtenis_parkeren_mislukt", "sleutel": key, "code": e.code, "fout": type(oe).__name__})
            raise
        if e.code != "busy":
            raise
        _park_event(event, key, "lock niet beschikbaar")
        return {"uitgesteld": True, "sleutel": key}


_event_threads: list = []


def hook_event(soort: str, ref, cfg: dict | None = None, wait: bool = False):
    """Haak voor de afrondingspaden: nooit blokkerend en nooit een fout naar de aanroeper. Doet niets zolang
    `action_reassess_on_events` uit staat (standaard) of er geen ref is; de ronde zelf draait in een eigen thread (netwerk)."""
    try:
        cfg = cfg or config.load_config()
        if not cfg.get("action_reassess_on_events") or not str(ref or "").strip() or float(cfg.get("action_reassess_interval_h") or 0) <= 0:
            return None
    except Exception:   # noqa: BLE001 -- afronding gaat voor
        return None

    def work():
        try:
            notify_event(cfg, soort, str(ref))
        except Exception as e:   # noqa: BLE001
            from .log import log
            log(f"actieherbeoordeling-event {soort}: {type(e).__name__}: {e}")
    t = threading.Thread(target=work, name="actieherbeoordeling-event", daemon=True)
    _event_threads[:] = [x for x in _event_threads if x.is_alive()] + [t]
    t.start()
    if wait:
        t.join()
    return t


def _round(cfg, persist, call, nodes, register, now, local, event=None) -> dict:
    from . import actionevidence
    if persist:
        initial_ledger = load_ledger()   # fail-closed vóór enige bijwerking: een verdwenen of onbetrouwbaar grootboek stopt de ronde meteen
        if not (home() / "ledger.json").exists():   # werkelijk verse installatie: eerst een leeg grootboek (ronde 0), vóór enige bijwerking
            home().mkdir(parents=True, exist_ok=True)   # zo is een ontbrekend grootboek vanaf hier altijd een verdwenen grootboek
            _save("ledger.json", {"version": VERSION, "rondes": 0, "acties": {}})
        _ensure_commit_anchor(initial_ledger)
    transfer = actionevidence.transfer(cfg, apply=persist, register=register)
    plans = collect(cfg, call, nodes)
    closure, cnote = _closure_entries(cfg, register)
    ledger, props = (load_ledger(), load_proposals()) if persist else (_peek("ledger.json", {"acties": {}}), {"voorstellen": {}})
    rep = evaluate(cfg, plans, closure, register, ledger, props, now, local, persist)
    rep["claim_transfer"] = transfer
    if cnote:
        rep["opmerkingen"] = [cnote]
    rep["headline"] = headline(rep)
    rep["planning"] = schedule(cfg)
    if not persist:
        return rep
    prev = _peek("latest.json", {})
    previous_digest = _sha(ledger)
    _update_ledger(ledger, rep, now)
    ledger["commit_protocol"] = 2
    transaction = uuid.uuid4().hex
    deferred_audit = []
    rep["ronde"] = ledger["rondes"]
    parked, parked_files = _take_parked()
    if parked and not event:
        try:
            _archived_keys(ledger)
        except ReassessError as e:   # geschiedenis niet bewezen: wachtende events blijven staan (niet verwerkt, niet weggegooid)
            parked, parked_files = [], []
            rep["geblokkeerd"] = e.code
    if event or parked:
        ev = ledger.setdefault("gebeurtenissen", {})
        archived = _archived_keys(ledger) if parked else set()
        for pk in parked:
            if pk["sleutel"] not in ev and pk["sleutel"] not in archived:
                ev[pk["sleutel"]] = {"soort": pk["soort"], "ref": pk["ref"], "ronde": rep["ronde"], "at": now, "uitgesteld_sinds": pk.get("at"),
                                     "beslissingen": _sha([b["id"] for b in rep["beslissingen"]])[:16]}
                deferred_audit.append({"kind": "gebeurtenis_uitgesteld_verwerkt", "commit_protocol": 2, "transaction": transaction, "sleutel": pk["sleutel"], "soort": pk["soort"], "ref": pk["ref"],
                        "uitgesteld_sinds": pk.get("at"), "ronde": rep["ronde"]})
        rep["uitgestelde_gebeurtenissen_verwerkt"] = [pk["sleutel"] for pk in parked]
    if event:
        ev[event["sleutel"]] = {"soort": event["soort"], "ref": event["ref"], "ronde": rep["ronde"], "at": now,
                                "beslissingen": _sha([b["id"] for b in rep["beslissingen"]])[:16]}
    old = {k: ev[k] for k in sorted(ev, key=lambda k: str(ev[k].get("at") or ""))[:-EVENT_KEEP]} if (event or parked) else {}
    archived_all = _archive_events(old, ledger)
    for k in old:
        del ev[k]
    if (event or parked) and archived_all is None and "archief" not in ledger:
        archived_all = _archived_keys(ledger)   # oud grootboek zonder manifest: een intact archief wordt nu bewezen vastgelegd
    if archived_all is not None:
        ledger["archief"] = _manifest(archived_all - set(ev))
    if event:
        rep["gebeurtenis"] = {"sleutel": event["sleutel"], "soort": event["soort"], "ref": event["ref"]}
    rep["transaction"] = transaction
    round_record = {"at": now, "dag": now[:10], "ronde": rep["ronde"], "transaction": transaction, "headline": rep["headline"], "counts": rep["counts"],
                    "planning": {k: rep["planning"].get(k) for k in ("modus", "dagelijks_om", "interval_h")}}
    record = {"version": 2, "transaction": transaction, "previous_digest": previous_digest, "ledger": ledger,
              "latest": rep, "voorstellen": props, "round_record": round_record,
              "sleutels": sorted(({event["sleutel"]} if event else set()) | {pk["sleutel"] for pk in parked})}
    record["digest"] = _sha(_commit_payload(record))
    for rec in deferred_audit:
        _audit(rec)
    _audit({"kind": "ronde", "commit_protocol": 2, "transaction": transaction, "content_digest": record["digest"],
            "ledger_digest": _sha(ledger), "previous_digest": previous_digest, **({"gebeurtenis": event["sleutel"], "soort": event["soort"], "ref": event["ref"]} if event else {}), "ronde": rep["ronde"], "counts": rep["counts"], "nodes": {n: v["status"] for n, v in rep["nodes"].items()},
            "nieuw_voor_beheerder": [x["id"] for x in rep["nieuw_voor_beheerder"]], "voorbereid": [x["id"] for x in rep["voorbereid"]],
            "gewijzigd": rep["gewijzigd_sinds_vorige_ronde"], "vorige": prev.get("generated_at")})
    # Volgorde (crashbestendig): eerst voorstellen, rapport en rondelog; het grootboek is het commitpunt (daarin staat het event als
    # verwerkt) en pas daarna, met fsync van de map, worden de wachtbestanden verwijderd. Crasht het vóór het grootboek, dan is het
    # event nog niet verwerkt en staat het wachtbestand er nog: de herstart doet de ronde opnieuw met hetzelfde rondenummer
    # (liever dubbel dan ontbrekend). Crasht het na het grootboek, dan staat het event als verwerkt en is het rapport al actueel.
    _save("voorstellen.json", props)
    _save("latest.json", rep)
    _keep_local_dry_run(plans, local)
    with open(home() / "rondes.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(round_record, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    store.fsync_dir(home())  # ook een nieuw rondelog moet vindbaar zijn vóór het herstelbeeld commit
    _save("ledger-commit.json", record)  # duurzaam volledig herstelbeeld: commitpunt vóór de export ledger.json
    _save("ledger.json", ledger)   # atomisch + fsync van de bovenliggende map (store.atomic_write)
    # Commitpunt duurzaam vastgelegd in het hash-geketende auditspoor, vóór het vrijgeven van de wachtbestanden: een later teruggezet,
    # één ronde ouder grootboek is daarmee aantoonbaar verouderd (`load_ledger`). Crasht het vóór dit record, dan is het grootboek
    # juist nieuwer dan het spoor en klopt alles; crasht het ervoor in `_save`, dan is er geen record en blijft de redo toegestaan.
    _audit({"kind": "ronde_vastgelegd", "ronde": rep["ronde"], "commit_protocol": 2, "transaction": transaction, "ledger_digest": _sha(ledger),
            "sleutels": sorted(({event["sleutel"]} if event else set()) | {pk["sleutel"] for pk in parked})})
    _release_parked(parked_files)   # pas nu: het grootboek met deze events staat duurzaam op schijf
    return rep


def _keep_local_dry_run(plans: dict, local: str) -> None:
    """De dagelijkse herbeoordeling vervangt de aparte dagelijkse dry run van het actiebeheer (`actionreview.tick` slaat dan over):
    het lokale plan van deze ronde wordt als dezelfde dry-run-rapportage bewaard, zodat er één dagelijkse scheduler is."""
    try:
        for p in plans["plans"]:
            if p["node"] == local:
                actionreview.save_report(p)
    except Exception:   # noqa: BLE001 -- een rapportagebestand mag de ronde nooit laten mislukken
        pass


def _peek(name: str, default: dict) -> dict:
    try:
        return _load(name, default)
    except ReassessError:
        return default


def latest() -> dict | None:
    """Het laatst bewaarde rapport (alleen lezen), of None."""
    d = _peek("latest.json", {})
    return d or None


def summary(rep: dict) -> dict:
    return {"generated_at": rep["generated_at"], "ronde": rep.get("ronde"), "headline": rep.get("headline"), "counts": rep["counts"],
            "nodes": rep["nodes"], "wachtrij": rep["wachtrij"], "sluit_niets": True, "claim_transfer": rep.get("claim_transfer"), "planning": rep.get("planning"), "nieuw_voor_beheerder": rep["nieuw_voor_beheerder"][:SUMMARY_ITEMS],
            "voorbereid": rep["voorbereid"][:SUMMARY_ITEMS], "gewijzigd_sinds_vorige_ronde": rep["gewijzigd_sinds_vorige_ronde"][:SUMMARY_ITEMS]}


def render(rep: dict) -> str:
    lines = ["Herbeoordeling van open acties (besluit 46/47; deze ronde sluit niets en zet niets in de wachtrij)", "", rep.get("headline", "")]
    if (pl := rep.get("planning")) and pl.get("actief"):
        lines.append(f"Cadence: {pl['modus']}" + (f" om {pl['dagelijks_om']} ({pl['tijdzone_machine']})" if pl.get("dagelijks_om") else f" elke {pl['interval_h']:g} uur")
                     + f"; vroegste volgende start {pl['vroegste_volgende_start']}")
    for n, v in sorted(rep["nodes"].items()):
        lines.append(f"  {n}: " + (f"{v['open']} open" if v["status"] == "ok" else f"niet beoordeeld ({v.get('fout')})"))
    lines += ["", "Per klasse:"] + [f"  {k}: {v}" for k, v in rep["counts"].items() if v]
    for title, key in (("Werkelijk nieuw voor Beheerder", "nieuw_voor_beheerder"), ("Voorbereid technisch vervolgwerk", "voorbereid")):
        if rep[key]:
            lines += ["", title + ":"] + [f"  - {x.get('id') or x.get('action_id')} ({x['node']}) {x.get('text') or x.get('title') or ''}" for x in rep[key]]
    lines += ["", "Beslissingenlijst (alleen positief herkende menselijke punten):"]
    lines += [f"  - {b['id']} ({b['node']}) [{b['bewijs']}]{' NIEUW' if b['nieuw'] else ''} {b['text']}" for b in rep.get("beslissingen") or []] or ["  (geen)"]
    for o in rep.get("opmerkingen") or []:
        lines.append(f"Opmerking: {o}")
    return "\n".join(lines) + "\n"


# --- watch-haak ---

_thread = [None]


def _daily_at(cfg: dict) -> tuple | None:
    """(uur, minuut) van het vaste dagelijkse moment, of None (leeg of ongeldig: dan geldt alleen het interval)."""
    m = re.fullmatch(r"\s*([01]?\d|2[0-3]):([0-5]\d)\s*", str(cfg.get("action_reassess_daily_at") or ""))
    return (int(m.group(1)), int(m.group(2))) if m else None


def _last_run() -> float | None:
    try:
        return (home() / "last-run").stat().st_mtime
    except OSError:
        return None


def current_slot(cfg: dict, now: float) -> float | None:
    """Het laatste dagelijkse moment dat al verstreken is (epoch), of None zonder vast dagmoment."""
    at = _daily_at(cfg)
    if at is None:
        return None
    dt = datetime.fromtimestamp(now)
    slot = dt.replace(hour=at[0], minute=at[1], second=0, microsecond=0)
    return (slot - timedelta(days=1) if slot > dt else slot).timestamp()


def due(cfg: dict, now: float | None = None) -> bool:
    """Is er een ronde aan de beurt? `action_reassess_interval_h = 0` = nooit. Met een vast dagmoment: precies één ronde per dag,
    als er sinds (kort vóór) het laatste verstreken dagmoment nog geen ronde is gestart (ook na een herstart of gemist moment: dan één inhaalronde)."""
    hours = float(cfg.get("action_reassess_interval_h") or 0)
    if hours <= 0:
        return False
    now = now or time.time()
    last = _last_run()
    if last is None:
        return True
    slot = current_slot(cfg, now)
    if slot is None:
        return now - last >= hours * 3600
    return last < slot - MIN_GAP_S


def schedule(cfg: dict, now: float | None = None) -> dict:
    """De werkelijke dagelijkse cadence als bewijs: modus, dagmoment, laatste start en vroegste volgende start."""
    now = now or time.time()
    hours = float(cfg.get("action_reassess_interval_h") or 0)
    at = _daily_at(cfg)
    last = _last_run()
    def iso(t):
        return None if t is None else datetime.fromtimestamp(t).astimezone().isoformat(timespec="seconds")
    if hours <= 0:
        return {"actief": False, "modus": "uit", "interval_h": hours}
    slot = current_slot(cfg, now)
    if slot is None:
        nxt = now if last is None else last + hours * 3600
        modus = "interval"
    else:
        nxt = now if last is None or last < slot - MIN_GAP_S else slot + 86400
        modus = "dagelijks"
    return {"actief": True, "modus": modus, "interval_h": hours, "dagelijks_om": f"{at[0]:02d}:{at[1]:02d}" if at else None,
            "tijdzone_machine": time.strftime("%Z") or "lokaal", "laatste_start": iso(last), "vroegste_volgende_start": iso(max(nxt, now)),
            "ronde_aan_de_beurt": due(cfg, now)}


def tick(cfg: dict, wait: bool = False) -> threading.Thread | bool | None:
    """Watch-haak: hoogstens één ronde per dag op `action_reassess_daily_at` (zonder dagmoment: elke `action_reassess_interval_h` uur; 0 = uit),
    alleen op de primaire control plane. Dit is de enige scheduler van de volledige herbeoordeling.

    De ronde draait in een eigen thread (ssh naar andere nodes kan traag zijn en mag de watch niet ophouden). De tijdstempel wordt vooraf
    gezet: een mislukte ronde (node onbereikbaar) herhaalt dus niet continu maar wacht het volgende interval af."""
    from . import cplane
    if not due(cfg) or not cplane.is_primary():
        return None
    if _thread[0] is not None and _thread[0].is_alive():
        return None
    home().mkdir(parents=True, exist_ok=True)
    (home() / "last-run").write_text(store.now())

    def work():
        try:
            run(cfg)
        except ReassessError:
            pass
        except Exception as e:   # noqa: BLE001 -- mag de watch nooit stilleggen
            from .log import log
            log(f"actieherbeoordeling: {type(e).__name__}: {e}")
    t = threading.Thread(target=work, name="actieherbeoordeling", daemon=True)
    _thread[0] = t
    t.start()
    if wait:
        t.join()
    return t
