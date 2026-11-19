"""De enige route naar Monitor 2 van buiten deze Mac: Tailscale Serve, en alleen vanaf de beheerder zijn eigen apparaten.

Hoe Tailscale Serve een aanvraag aanbiedt (broncode v1.102.4, ``ipn/ipnlocal/serve.go``):

* de TCP-verbinding naar Monitor 2 komt van ``127.0.0.1`` (de ``tailscaled``-/
  netwerkextensieproxy; Monitor 2 blijft op 127.0.0.1 luisteren);
* ``Host`` is de oorspronkelijke naam (``<machine>.<tailnet>.ts.net``);
* ``X-Forwarded-For`` wordt door Serve *gezet* (niet aangevuld) op het
  tailnetadres van de afzender, ``X-Forwarded-Proto: https`` bij TLS;
* ``Tailscale-User-Login`` e.d. worden eerst *gewist* en daarna uit Tailscales
  eigen WhoIs gezet; een Funnel-aanvraag krijgt ``Tailscale-Funnel-Request`` en
  geen identiteit.

Waarom dat alleen niet genoeg is: elk proces op deze Mac kan
``https://<machine>.ts.net`` openen, en Serve stuurt het dan door naar
127.0.0.1:8772 -- ook een proces in de modelsandbox, waarvan het profiel alleen
``*:8765``/``*:8772`` weigert (SEC-06). Zo'n aanvraag van de Mac zelf draagt als
``X-Forwarded-For`` het *eigen* tailnetadres van de Mac. De grens is daarom:

    alleen een aanvraag waarvan Serve zelf zegt dat hij van een ander, uitdrukkelijk
    toegestaan tailnetapparaat komt (``allowed_peers``), met een toegestane login.

Het eigen adres van de monitorhost hoort buiten die lijst te blijven. Een proces dat 127.0.0.1:8772
rechtstreeks bereikt en deze kopjes zelf zet, kon dat zonder deze route al met
``Host: localhost``: dat is de bestaande lokale vertrouwensgrens, en de sandbox
bereikt 8772 rechtstreeks niet (Seatbelt-deny). LAN en internet bereiken 8772 niet
(bind 127.0.0.1) en Serve niet (alleen het tailnetadres, geen Funnel).

Instellingen: ``~/obf-runner-bridge/monitor2-tailscale.json`` (of
``OBF_MONITOR2_TAILSCALE_FILE``), ingelezen bij de start. Ontbreekt of klopt het
bestand niet, dan bestaat de route niet (fail-closed)::

    {"schema": "obf-monitor2-tailscale-v1",
     "host": "mijn-mac.tail0000.ts.net",
     "allowed_logins": ["beheerder@example.invalid"],
     "allowed_peers": ["<tailnetadres-van-toegestaan-apparaat>"]}
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

SCHEMA = "obf-monitor2-tailscale-v1"
DEFAULT_FILE = Path.home() / "obf-runner-bridge" / "monitor2-tailscale.json"
ENV_FILE = "OBF_MONITOR2_TAILSCALE_FILE"

#: De adresruimten die Tailscale aan apparaten geeft (CGNAT-blok en het ULA-voorvoegsel).
TAILNET_NETWORKS = (ipaddress.ip_network("100.64.0.0/10"), ipaddress.ip_network("fd7a:115c:a1e0::/48"))
LOOPBACK_PEERS = {"127.0.0.1", "::1", "::ffff:127.0.0.1"}
_HOSTNAME = re.compile(r"^[a-z0-9-]+(\.[a-z0-9-]+)*\.ts\.net$")
_LOGIN = re.compile(r"^[^\s@]+@[^\s@]+$")


#: Kopjes die een doorsturende proxy zet -- Tailscale Serve zet de eerste drie en de
#: Tailscale-kopjes altijd zelf. Een gewone lokale browser stuurt geen van alle. Een
#: aanvraag met één ervan kwam dus via een proxy (Serve), ook als zijn ``Host``
#: localhost zegt, en krijgt nooit de lokale vertrouwensgrens (SEC-06).
PROXY_HEADERS = (
    "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "Forwarded", "X-Real-IP",
    "Tailscale-User-Login", "Tailscale-User-Name", "Tailscale-User-Profile-Pic",
    "Tailscale-Headers-Info", "Tailscale-Funnel-Request",
)


def forwarded(headers) -> bool:
    """Draagt deze aanvraag een kopje dat alleen een doorsturende proxy zet?"""
    return any(headers.get_all(name) for name in PROXY_HEADERS)


class TailscaleConfigError(ValueError):
    pass


def _host_only(value: str) -> str:
    """``naam`` of ``naam:443`` -> ``naam``; een andere poort blijft staan (en past dan nergens bij)."""
    value = value.strip().lower()
    return value[:-4] if value.endswith(":443") else value


@dataclass(frozen=True)
class TailscaleServeRoute:
    host: str
    allowed_logins: frozenset[str]
    allowed_peers: frozenset[ipaddress.IPv4Address | ipaddress.IPv6Address]

    @property
    def origin(self) -> str:
        return f"https://{self.host}"

    def matches_host(self, host_header: str) -> bool:
        return _host_only(host_header) == self.host

    def refusal(self, client_ip: str, headers) -> str | None:
        """Waarom dit géén aanvraag via Serve van een toegestaan apparaat is, of ``None``.

        ``headers`` is een ``email.message.Message`` (``BaseHTTPRequestHandler.headers``).
        Elk kopje dat Serve zelf zet, moet precies één keer voorkomen.
        """
        if client_ip not in LOOPBACK_PEERS:
            return "not_via_local_proxy"
        if headers.get_all("Tailscale-Funnel-Request"):
            return "funnel_refused"

        def single(name: str) -> str | None:
            values = headers.get_all(name) or []
            return values[0].strip() if len(values) == 1 else None

        if (single("X-Forwarded-Proto") or "").lower() != "https":
            return "not_https"
        if _host_only(single("X-Forwarded-Host") or "") != self.host:
            return "forwarded_host_mismatch"
        peer_raw = single("X-Forwarded-For")
        try:
            peer = ipaddress.ip_address(peer_raw or "")
        except ValueError:
            return "peer_missing"
        if getattr(peer, "ipv4_mapped", None):
            peer = peer.ipv4_mapped
        if peer not in self.allowed_peers:
            return "peer_not_allowed"
        login = (single("Tailscale-User-Login") or "").lower()
        if not login or login not in self.allowed_logins:
            return "login_not_allowed"
        return None


def load(path: Path | str | None = None) -> TailscaleServeRoute | None:
    """De route uit het instellingenbestand, of ``None`` als er geen (geldig) bestand is."""
    target = Path(path) if path is not None else Path(os.environ.get(ENV_FILE) or DEFAULT_FILE)
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        return None
    try:
        return parse(raw)
    except TailscaleConfigError:
        return None


def parse(raw: object) -> TailscaleServeRoute:
    if not isinstance(raw, dict) or raw.get("schema") != SCHEMA:
        raise TailscaleConfigError("schema")
    host = raw.get("host")
    if not isinstance(host, str) or not _HOSTNAME.match(host.strip().lower()):
        raise TailscaleConfigError("host")
    logins = raw.get("allowed_logins")
    if not isinstance(logins, list) or not logins \
            or not all(isinstance(item, str) and _LOGIN.match(item.strip()) for item in logins):
        raise TailscaleConfigError("allowed_logins")
    peers_raw = raw.get("allowed_peers")
    if not isinstance(peers_raw, list) or not peers_raw or not all(isinstance(item, str) for item in peers_raw):
        raise TailscaleConfigError("allowed_peers")
    peers = set()
    for item in peers_raw:
        try:
            peer = ipaddress.ip_address(item.strip())
        except ValueError as exc:
            raise TailscaleConfigError("allowed_peers") from exc
        if not any(peer in network for network in TAILNET_NETWORKS):
            raise TailscaleConfigError("allowed_peers")
        peers.add(peer)
    return TailscaleServeRoute(host=host.strip().lower(),
                               allowed_logins=frozenset(item.strip().lower() for item in logins),
                               allowed_peers=frozenset(peers))
