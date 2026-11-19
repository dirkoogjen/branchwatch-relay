"""De inhoud van een melding. Alleen een gesloten set velden; nooit prompts of geheimen."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .detect import ACTION_REQUIRED, COMPLETED, FAILED, Item

_SECRET_PATTERNS = (
    re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"(?i)\b(password|passwd|secret|token|api[_-]?key|authorization)\b\s*[:=]\s*\S+"),
    re.compile(r"\b(sk|pk|rk|ghp|gho|ghs|github_pat|xox[abprs]|AKIA|ASIA)[-_A-Za-z0-9]{12,}"),
    re.compile(r"\b[A-Fa-f0-9]{40,}\b"),
    re.compile(r"\b[A-Za-z0-9+/_-]{40,}={0,2}"),
    re.compile(r"(?i)-----BEGIN [A-Z ]*PRIVATE KEY-----"),
)

#: Nederlandse uitleg per menselijke actie. Onbekende acties krijgen de tekst van de
#: controller zelf (ingekort en gezuiverd) -- de tabel is een verduidelijking, geen filter.
ACTION_HINTS = {
    "controller_accept_continue": "Beoordeel de voorgestelde vervolgstap en kies doorgaan, aanpassen of stoppen.",
    "controller_approve_apply": "Beoordeel het voorstel en keur het toepassen goed of stop de taak.",
    "controller_resume": "Neem de genoemde blokkade weg en hervat de taak.",
    "controller_stop": "Beoordeel het resultaat en sluit de taak af.",
    "controller_retry_evaluation": "Laat de beoordeling opnieuw uitvoeren of stop de taak.",
    "node_projects_continue": "Kies hoe de projectsessie verder moet.",
    "node_project_continue": "Kies hoe de projectsessie verder moet.",
    "supervisor_inactive": "De supervisor van de sessie draait niet: hervat of stop de sessie.",
    "claude_login_expiring": "Meld Claude op de Mac opnieuw aan met `claude auth login` vóór de vervaldatum; "
                             "tot dan loopt alles gewoon door.",
    "node_none": "Er is geen knop voor: bekijk de taak op de node (node_controller_status) en beslis via ChatGPT.",
}

KIND_LABEL = {ACTION_REQUIRED: "actie vereist", COMPLETED: "klaar", FAILED: "mislukt"}
FOOTER = "Open ChatGPT en vraag om de status van deze taak te controleren."


def redact(text: str, limit: int = 300) -> str:
    """Haalt alles weg dat op een geheim lijkt en kapt af. Regeleinden worden spaties."""
    clean = " ".join(str(text).split())
    for pattern in _SECRET_PATTERNS:
        clean = pattern.sub("[verborgen]", clean)
    return clean[:limit]


@dataclass(frozen=True)
class Message:
    subject: str
    body: str


def _line(label: str, value: str) -> str:
    return f"{label}: {value}"


def build_message(kind: str, item: Item) -> Message:
    title = redact(item.title, 80) or item.item_id
    lines = [f"Studio OBF — {KIND_LABEL[kind]}", "",
             _line("Project/sessie", title),
             _line("ID", redact(item.item_id, 60)),
             _line("Node", redact(item.node, 30) or "onbekend"),
             _line("Status", redact(item.status, 60))]
    if kind == ACTION_REQUIRED:
        hint = ACTION_HINTS.get(item.human_tool)
        why = redact(item.human_why)
        lines.append(_line("Reden", why or "de controller vraagt een menselijke beslissing"))
        action = hint or (redact(item.human_tool, 80) or "bekijk de status")
        if item.human_choices:
            action += " Opties: " + ", ".join(redact(c, 40) for c in item.human_choices) + "."
        lines.append(_line("Benodigde actie", action))
        subject = f"[Studio OBF] ACTIE VEREIST — {title}"
    elif kind == COMPLETED:
        lines.append(_line("Uitkomst", "afgerond en bewezen"))
        subject = f"[Studio OBF] KLAAR — {title}"
    else:
        lines.append(_line("Reden", redact(item.outcome_reason) or "definitief gestopt zonder resultaat"))
        subject = f"[Studio OBF] MISLUKT — {title}"
    lines += ["", FOOTER]
    return Message(subject=redact(subject, 140), body="\n".join(lines) + "\n")


def build_test_message(*, switch_on: bool) -> Message:
    stand = "AAN" if switch_on else "UIT"
    body = ("Studio OBF — TEST\n\n"
            "Dit is een TESTMELDING. Er is geen taak of project bij betrokken en er is niets gewijzigd.\n"
            f"De schakelaar Meldingen staat nu {stand}; deze test negeert die schakelaar bewust.\n\n"
            "Ontvang je dit op je telefoon, dan werkt de e-mailroute.\n")
    return Message(subject="[Studio OBF] TEST — testmelding", body=body)
