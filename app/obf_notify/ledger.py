"""Het duurzame notificatiegeheugen: welke grens is gemeld, wat wacht op (her)verzending.

Eén JSON-bestand, atomair geschreven. Er staat geen berichttekst in en geen enkel
geheim: alleen identiteiten (fingerprints), tellers en tijdstippen. De inhoud van een
melding wordt telkens opnieuw opgebouwd uit de actuele, geredigeerde status.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .settings import atomic_write_json, read_json

SCHEMA = "obf-notify-ledger-v1"
PENDING, SENT, SUPPRESSED, OBSOLETE, ABANDONED = "pending", "sent", "suppressed", "obsolete", "abandoned"
MAX_ITEMS = 500
KEEP_SECONDS = 90 * 24 * 3600


def fingerprint(*parts: Any) -> str:
    raw = json.dumps([str(part) for part in parts], ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


class Ledger:
    def __init__(self, path: Path):
        self.path = path
        data = read_json(path)
        ok = isinstance(data, dict) and data.get("schema") == SCHEMA
        self.items: dict[str, dict] = dict(data.get("items") or {}) if ok else {}
        self.notifications: dict[str, dict] = dict(data.get("notifications") or {}) if ok else {}
        self._saved = self._digest()

    def _digest(self) -> str:
        return hashlib.sha256(json.dumps([self.items, self.notifications], sort_keys=True).encode()).hexdigest()

    def save(self, now: float) -> None:
        self._prune(now)
        digest = self._digest()
        if digest == self._saved:
            return
        atomic_write_json(self.path, {"schema": SCHEMA, "items": self.items, "notifications": self.notifications})
        self._saved = digest

    def _prune(self, now: float) -> None:
        old = [fp for fp, n in self.notifications.items()
               if n.get("state") != PENDING and now - float(n.get("created_at") or 0) > KEEP_SECONDS]
        for fp in old:
            del self.notifications[fp]
        if len(self.items) > MAX_ITEMS:
            ordered = sorted(self.items, key=lambda k: float(self.items[k].get("seen_at") or 0))
            for key in ordered[: len(self.items) - MAX_ITEMS]:
                del self.items[key]

    def pending(self) -> list[tuple[str, dict]]:
        return [(fp, n) for fp, n in self.notifications.items() if n.get("state") == PENDING]

    def last_sent(self) -> dict | None:
        sent = [n for n in self.notifications.values() if n.get("state") == SENT]
        return max(sent, key=lambda n: float(n.get("sent_at") or 0)) if sent else None
