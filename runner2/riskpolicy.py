"""Verplichte menselijke goedkeuringspoort voor opdrachten die ChatGPT indient (besluit Beheerder).

Opdrachten van ChatGPT mogen zelfstandig doorlopen zolang ze gewone code en veilige tests betreffen. Alles wat naar
productie/live/publicatie gaat, gegevens verwijdert, beveiliging of toegangsrechten wijzigt of anderszins risicovol is,
start pas nadat een mens (jobs_approvers, niet via ChatGPT) heeft goedgekeurd.

Afdwinging staat server-side en vertrouwt niet op wat de indiener zelf kiest:
  * het KANAAL bepaalt of de poort geldt (alles behalve de menselijke kanalen "lokaal" en "rpc" -- dus ook een
    onbekend of ontbrekend kanaal -- valt eronder; fail-closed);
  * `approval='geen'` van de indiener wordt genegeerd zodra de classificatie risico ziet;
  * de classificatie zelf is fail-closed: een fout of onleesbare invoer telt als risicovol;
  * de uitslag wordt in de opdracht vastgelegd (`policy`, met hash van de prompt); bij het starten wordt dat opnieuw
    gecontroleerd, ook voor oude opdrachten zonder `policy` (legacy).
Naast de tekstclassificatie zijn er structurele regels: `risk_protected_repos` (altijd goedkeuring) en
`risk_force_all_chatgpt` (alles van ChatGPT vraagt goedkeuring).
"""
from __future__ import annotations

import hashlib
import re

HUMAN_CHANNELS = ("lokaal", "rpc")   # Monitor 2 / CLI op de control plane, en node-transport van Monitor 2
APPROVAL_FOR = {"publicatie": "publicatie", "verwijderen": "risico", "beveiliging": "risico", "overig": "risico"}

_NEG = re.compile(r"\b(geen|zonder|nooit|verboden|no|never|without|don'?t|do not|must not|mag niet)\b", re.I)
_FORGET = re.compile(r"vergeten|forget", re.I)
_SENT_BREAK = re.compile(r"[.\n;:!?]")

_PUBLISH_VERB = (r"(?:deploy\w*|uitrol\w*|uitgerold|release\w*|publiceer\w*|publiceren|publish\w*|go[- ]?live|live\s*(?:zetten|gaan|brengen)"
                 r"|livezetten|ship\w*)")
_PUBLISH_TARGET = (r"(?:productie\w*|production|prod|live|publiek|openbaar|internet|npm|pypi|app\s?store|registry|klanten|customers?)")
_DELETE_VERB = r"(?:verwijder\w*|wis|wissen|wist|delete\w*|remove\w*|purge\w*|destroy\w*|opruimen|leegmaken|empty)"
_DELETE_OBJ = (r"(?:database|databank|productiedata|productiegegevens|klantgegevens|backups?|back-ups?|snapshots?|volumes?|bucket"
               r"|schijf|disk|partitie|dataset|(?:alle|all)\s+(?:data|gegevens|bestanden|files|records|rijen|rows)|logboeken)")
_SEC_VERB = (r"(?:wijzig\w*|verander\w*|geef|verleen\w*|grant|revoke|intrek\w*|reset\w*|roteer\w*|rotate\w*|disable\w*|uitschakel\w*"
             r"|schakel\w*|verruim\w*|verhoog\w*|escalat\w*|open\w*|change|modify|update)")
_SEC_OBJ = (r"(?:rechten|toegangsrechten|permissies|permissions|wachtwoord\w*|passwords?|credentials|inloggegevens|api[- ]?keys?"
            r"|secrets?|toegang|access\s+(?:rights|control)|firewall|sudoers|ssh[- ]?sleutel\w*|ssh[- ]?keys?|2fa|mfa|root[- ]?(?:toegang|access|rechten))")

# (categorie, regex op een enkele zin) -- direct herkenbare handelingen
_DIRECT = [
    ("publicatie", r"\bgit\s+push\b|\bnpm\s+publish\b|\btwine\s+upload\b|\bdocker\s+push\b|\bgh\s+release\s+create\b"
                   r"|\bkubectl\s+(?:apply|delete|rollout|scale)\b|\bterraform\s+(?:apply|destroy)\b|\bsystemctl\s+(?:restart|stop|disable|mask)\b"),
    ("verwijderen", r"\brm\s+-\w*[rf]|\bdrop\s+(?:table|database|schema)\b|\btruncate\s+table\b|\bdelete\s+from\b|\bmkfs\b"
                    r"|\bdd\s+if=|\bshred\b|\bgit\s+clean\s+-\w*f|\bgit\s+reset\s+--hard\s+origin|\bgit\s+push\s+(?:-f\b|--force)"),
    ("beveiliging", r"\bsudo\b|\bvisudo\b|/etc/sudoers|authorized_keys|\bchmod\s+(?:-\w+\s+)?(?:0?777|a\+w|o\+w)\b|\bchown\b"
                    r"|\b(?:usermod|useradd|adduser|userdel|passwd|chpasswd)\b|/etc/(?:shadow|passwd|ssh)\b|\b(?:ufw|iptables|nftables)\b"),
]
_COMBOS = [
    ("publicatie", re.compile(rf"\b{_PUBLISH_VERB}\b", re.I), re.compile(rf"\b{_PUBLISH_TARGET}\b", re.I)),
    ("verwijderen", re.compile(rf"\b{_DELETE_VERB}\b", re.I), re.compile(rf"\b{_DELETE_OBJ}\b", re.I)),
    ("beveiliging", re.compile(rf"\b{_SEC_VERB}\b", re.I), re.compile(rf"\b{_SEC_OBJ}\b", re.I)),
]
_DIRECT_RX = [(c, re.compile(p, re.I)) for c, p in _DIRECT]


def _negated(sentence: str, start: int) -> bool:
    before = sentence[max(0, start - 80):start]
    return bool(_NEG.search(before)) and not _FORGET.search(sentence)


def _sentences(text: str):
    pos = 0
    for m in list(_SENT_BREAK.finditer(text)) + [None]:
        end = m.start() if m else len(text)
        yield text[pos:end]
        pos = end + 1


def classify(*texts) -> dict:
    """{'risky': bool, 'categories': [...], 'reasons': [...]}. Fail-closed: niet-tekst of een fout geeft risky=True."""
    try:
        cats: list[str] = []
        reasons: list[str] = []
        for t in texts:
            if t is None:
                continue
            if not isinstance(t, str):
                raise TypeError("invoer is geen tekst")
            for s in _sentences(t):
                for cat, rx in _DIRECT_RX:
                    for m in rx.finditer(s):
                        if not _negated(s, m.start()):
                            cats.append(cat)
                            reasons.append(f"{cat}: '{m.group(0).strip()}'")
                            break
                for cat, a, b in _COMBOS:
                    ma, mb = a.search(s), b.search(s)
                    if ma and mb and not _negated(s, min(ma.start(), mb.start())):
                        cats.append(cat)
                        reasons.append(f"{cat}: '{ma.group(0)}' + '{mb.group(0)}'")
        return {"risky": bool(cats), "categories": sorted(set(cats)), "reasons": reasons[:6]}
    except Exception as exc:   # noqa: BLE001 -- fail-closed
        return {"risky": True, "categories": ["overig"], "reasons": [f"classificatie mislukt ({type(exc).__name__}): als risicovol behandeld"]}


def is_human_channel(channel) -> bool:
    return channel in HUMAN_CHANNELS


def prompt_hash(prompt) -> str:
    return hashlib.sha256(str(prompt).encode("utf-8", "replace")).hexdigest()[:16]


def approval_kind(categories) -> str:
    return "publicatie" if "publicatie" in (categories or []) else "risico"


def assess(channel, texts, repo: str | None = None, cfg: dict | None = None) -> dict:
    """Beoordeling voor een nieuwe opdracht. {'gated': bool, 'risky': bool, 'kind': str|None, 'categories', 'reasons'}.
    Menselijke kanalen worden niet gepoort; elk ander kanaal wel (fail-closed)."""
    if is_human_channel(channel):
        return {"gated": False, "risky": False, "kind": None, "categories": [], "reasons": []}
    cfg = cfg or {}
    res = classify(*texts)
    reasons = list(res["reasons"])
    cats = list(res["categories"])
    risky = res["risky"]
    protected = [str(p).rstrip("/") for p in (cfg.get("risk_protected_repos") or [])]
    if repo and str(repo).rstrip("/") in protected:
        risky = True
        cats.append("overig")
        reasons.append("beschermde repository: altijd goedkeuring")
    if cfg.get("risk_force_all_chatgpt"):
        risky = True
        cats.append("overig")
        reasons.append("risk_force_all_chatgpt staat aan")
    return {"gated": True, "risky": risky, "kind": approval_kind(cats) if risky else None,
            "categories": sorted(set(cats)), "reasons": reasons[:6]}


def policy_record(channel, prompt, verdict: dict) -> dict:
    return {"channel": str(channel), "gated": verdict["gated"], "risky": verdict["risky"], "categories": verdict["categories"],
            "reasons": verdict["reasons"], "prompt_sha": prompt_hash(prompt)}


def enabled(cfg):
    value = cfg.get("risk_gate_enabled", False)
    if type(value) is not bool:
        raise ValueError("risk_gate_enabled must be a boolean")
    return value
