"""Welke eigen apparaten Monitor 2 op het tailnetadres zonder sleutel mogen openen (``monitor2-ctl tailnet-devices|-allow|-disallow``).

Schrijft alleen het blok ``identity`` in ``tailnet.json`` (zie ``lan_route``, "Tailscale-identiteit"); de sleutel, de bootstrap-link
en de rest van het bestand blijven ongemoeid. Monitor 2 leest het bestand bij de start, dus na een wijziging: ``monitor2-ctl restart``.

Een apparaat wordt aangewezen met zijn naam (``ipad-air-10-5``), MagicDNS-naam, tailnetadres of Tailscale-ID. Geweigerd worden:
GERARD zelf (dan zou elk proces op GERARD binnenkomen), getagde apparaten (geen persoon als eigenaar) en apparaten van een
andere login dan die al in de lijst staat.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


class AccessError(ValueError):
    pass


def tailscale_status() -> dict:
    result = subprocess.run(["tailscale", "status", "--json"], capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        raise AccessError("tailscale status mislukt: " + (result.stderr.strip() or f"code {result.returncode}"))
    return json.loads(result.stdout)


def devices(status: dict) -> list[dict]:
    """Alle apparaten (eigen apparaat eerst) als eenvoudige rijen; geen sleutels of eindpunten."""
    users = status.get("User") or {}
    rows = []
    for own, peer in [(True, status.get("Self") or {})] + [(False, p) for p in (status.get("Peer") or {}).values()]:
        dns = str(peer.get("DNSName") or "").rstrip(".").lower()
        rows.append({
            "id": str(peer.get("ID") or ""),
            "name": dns.split(".", 1)[0] if dns else str(peer.get("HostName") or ""),
            "dns": dns,
            "host": str(peer.get("HostName") or ""),
            "ips": [str(ip) for ip in peer.get("TailscaleIPs") or []],
            "login": str((users.get(str(peer.get("UserID"))) or {}).get("LoginName") or "").lower(),
            "os": str(peer.get("OS") or ""),
            "online": bool(peer.get("Online")) or own,
            "tags": list(peer.get("Tags") or []),
            "self": own,
        })
    return rows


def find(rows: list[dict], wanted: str) -> dict:
    needle = wanted.strip().rstrip(".").lower()
    hits = [r for r in rows if needle and (needle in (r["id"].lower(), r["name"], r["dns"], r["host"].lower()) or needle in r["ips"])]
    if len(hits) != 1:
        raise AccessError(f"apparaat {wanted!r}: {'niet gevonden' if not hits else 'niet eenduidig'} (zie monitor2-ctl tailnet-devices)")
    return hits[0]


def allow(doc: dict, rows: list[dict], names: list[str]) -> dict:
    identity = dict(doc.get("identity") or {})
    logins = list(identity.get("allowed_logins") or [])
    nodes = list(identity.get("allowed_nodes") or [])
    for name in names:
        row = find(rows, name)
        if row["self"]:
            raise AccessError(f"{row['name']} is deze machine zelf: dan zou elk proces hier binnenkomen; geweigerd")
        if row["tags"] or not row["login"]:
            raise AccessError(f"{row['name']} is een getagd apparaat zonder persoonlijke eigenaar; geweigerd")
        if logins and row["login"] not in logins:
            raise AccessError(f"{row['name']} hoort bij een andere login ({row['login']}) dan {', '.join(logins)}; geweigerd")
        if row["login"] not in logins:
            logins.append(row["login"])
        if row["id"] not in nodes:
            nodes.append(row["id"])
    return {**doc, "identity": {"allowed_logins": logins, "allowed_nodes": nodes}}


def disallow(doc: dict, rows: list[dict], names: list[str]) -> dict:
    identity = dict(doc.get("identity") or {})
    nodes = list(identity.get("allowed_nodes") or [])
    for name in names:
        try:
            gone = find(rows, name)["id"]
        except AccessError:
            gone = name.strip()          # een apparaat dat al uit het tailnet is verdwenen: op ID weghalen
        nodes = [n for n in nodes if n != gone]
    out = {k: v for k, v in doc.items() if k != "identity"}
    if nodes:
        out["identity"] = {"allowed_logins": list(identity.get("allowed_logins") or []), "allowed_nodes": nodes}
    return out


def write(path: Path, doc: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(doc) + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def main(argv: list[str], status_source=tailscale_status) -> int:
    if len(argv) < 2 or argv[0] not in ("list", "allow", "disallow"):
        print("gebruik: tailnet_access list|allow|disallow <tailnet.json> [apparaat ...]", file=sys.stderr)
        return 2
    command, path = argv[0], Path(argv[1]).expanduser()
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        rows = devices(status_source())
        if command == "list":
            allowed = set((doc.get("identity") or {}).get("allowed_nodes") or [])
            for r in rows:
                mark = "deze machine" if r["self"] else ("TOEGESTAAN" if r["id"] in allowed else "-")
                print(f"{r['name']:<24} {r['id']:<20} {r['login'] or '(getagd)':<28} {r['os']:<8} "
                      f"{'online' if r['online'] else 'offline':<8} {mark}")
            if not allowed:
                print("Nog geen apparaat zonder sleutel toegestaan (alleen de toegangssleutel geldt).")
            return 0
        if len(argv) < 3:
            raise AccessError("noem minstens één apparaat")
        new = allow(doc, rows, argv[2:]) if command == "allow" else disallow(doc, rows, argv[2:])
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"tailnet_access: {exc}", file=sys.stderr)
        return 1
    write(path, new)
    nodes = (new.get("identity") or {}).get("allowed_nodes") or []
    print(f"tailnet.json bijgewerkt: {len(nodes)} apparaat/apparaten zonder sleutel toegestaan; "
          "wordt actief na: monitor2-ctl restart")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
