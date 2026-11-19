"""Optionele LAN/VPN-route voor Monitor 2 op één privéadres, met toegangssleutel.

De monitor luistert standaard op 127.0.0.1 en kan daarnaast op één privéadres luisteren.
De localhost-regels (SEC-06) blijven op 127.0.0.1 ongewijzigd;
op het LAN-adres geldt daarbovenop, fail-closed:

* ``Host`` moet precies een toegestane naam of dit adres met deze poort zijn (geen DNS-rebinding);
* de verbinding moet van een toegestaan privénetwerk komen (``allowed_networks``); een doorgestuurde aanvraag
  (``X-Forwarded-For`` e.d.) wordt geweigerd;
* een toegangssleutel: pas na ``/?key=<sleutel>`` zet de server een ``HttpOnly; SameSite=Strict``-cookie en stuurt
  hij door naar ``/``; zonder dat cookie krijgt elk adres (ook ``/healthz``) een 401. Daarmee kan een willekeurig
  apparaat op het thuisnetwerk de Stop/Pauze/Beantwoorden-knoppen niet bedienen;
* het actiecookie, de exacte ``Origin`` en ``Sec-Fetch-Site: same-origin`` gelden voor elke POST, net als lokaal.

Instellingen: ``~/.config/monitor2/lan.json`` (of ``OBF_MONITOR2_LAN_FILE``), ingelezen bij de start. Ontbreekt of klopt het
bestand niet, dan bestaat de route niet (alleen 127.0.0.1)::

    {"schema": "obf-monitor2-lan-v1", "bind": "<privéadres-monitor>", "port": 8772,
     "hosts": ["<privéadres-monitor>"], "allowed_networks": ["<toegestaan-privésubnet-in-CIDR>"],
     "key_file": "~/.config/monitor2/lan-access.key"}

De sleutel staat alleen in ``key_file`` (modus 0600, minstens 32 tekens), nooit in het instellingenbestand, in de repository of in een log.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import secrets
import socket
import stat
import struct
import threading
import time
import hashlib
from dataclasses import dataclass, replace
from pathlib import Path

SCHEMA = "obf-monitor2-lan-v1"
DEFAULT_FILE = Path.home() / ".config" / "monitor2" / "lan.json"
ENV_FILE = "OBF_MONITOR2_LAN_FILE"
KEY_COOKIE = "obf_monitor2_lan"
MIN_KEY_CHARS = 32
LOOPBACK_PEERS = {"127.0.0.1", "::1", "::ffff:127.0.0.1"}

_HOSTNAME = re.compile(r"^(?:[a-z0-9]([a-z0-9-]*[a-z0-9])?)(?:\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$")
_HOST_PORT = re.compile(r"^(\[[0-9a-fA-F:]+\]|[^:\[\]]+)(?::(\d{1,5}))?$")


@dataclass(frozen=True)
class LanRoute:
    bind: str
    port: int
    hosts: frozenset
    networks: tuple
    key: str
    label: str = "lan"          # alleen voor logregels
    interface: str = ""         # alleen tailnet-route met bind "auto": het adres komt van deze interface (tailscale0)
    identity: "TailnetIdentity | None" = None   # alleen tailnet-route: toegang op Tailscale-identiteit (zie onder)

    def resolved(self) -> "LanRoute | None":
        """De route met een echt bindadres, of ``None`` zolang dat nog niet bestaat (Tailscale nog niet aangemeld/gestart)."""
        if self.bind:
            return self
        address = interface_ipv4(self.interface) if self.interface else None
        if address is None or not _in_tailnet(ipaddress.ip_address(address)):
            return None
        return replace(self, bind=address, hosts=self.hosts | {address})

    def matches_host(self, host_header: str) -> bool:
        match = _HOST_PORT.match((host_header or "").strip())
        if match is None:
            return False
        port = int(match.group(2)) if match.group(2) else 80
        return port == self.port and match.group(1).lower() in self.hosts

    def peer_allowed(self, address: str) -> bool:
        try:
            peer = ipaddress.ip_address(address)
        except ValueError:
            return False
        if isinstance(peer, ipaddress.IPv6Address) and peer.ipv4_mapped is not None:
            peer = peer.ipv4_mapped
        return any(peer in network for network in self.networks)

    def key_ok(self, value: str) -> bool:
        return bool(value) and secrets.compare_digest(value.encode("utf-8"), self.key.encode("utf-8"))


def _read_key(path: Path) -> str | None:
    """De sleutel, alleen als het bestand van de eigenaar is, niet voor anderen leesbaar, en lang genoeg."""
    try:
        info = path.stat()
        if info.st_uid != os.getuid() or info.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            return None
        key = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return key if len(key) >= MIN_KEY_CHARS and key.isprintable() and " " not in key else None


def parse(doc: object) -> LanRoute | None:
    if not isinstance(doc, dict) or doc.get("schema") != SCHEMA:
        return None
    try:
        bind = ipaddress.ip_address(str(doc.get("bind")))
        port = int(doc.get("port", 8772))
        networks = tuple(ipaddress.ip_network(str(n), strict=False) for n in doc.get("allowed_networks") or [])
    except ValueError:
        return None
    # Alleen een privéadres, nooit loopback, nooit 0.0.0.0/::: geen publieke bind en geen "alle interfaces".
    if bind.is_unspecified or bind.is_loopback or not bind.is_private or bind.is_global or not 1 <= port <= 65535:
        return None
    if not networks or any(n.is_global or n.prefixlen < 8 for n in networks):
        return None
    hosts = {str(bind)}
    for name in doc.get("hosts") or []:
        if not isinstance(name, str) or not name.strip() or not _HOSTNAME.match(name.strip().lower().rstrip(".")):
            return None
        hosts.add(name.strip().lower().rstrip("."))
    key_file = doc.get("key_file")
    key = _read_key(Path(str(key_file)).expanduser()) if isinstance(key_file, str) and key_file else None
    if key is None:
        return None
    return LanRoute(bind=str(bind), port=port, hosts=frozenset(hosts), networks=networks, key=key)


def load() -> LanRoute | None:
    path = Path(os.environ.get(ENV_FILE) or DEFAULT_FILE).expanduser()
    try:
        return parse(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        return None


# ----------------------------------------------------------------------------- tailnet-route (Tailscale op GERARD)
#
# Zelfde regels als de LAN-route (Host-check, peer-netwerk, toegangssleutel, geen doorgestuurde aanvragen), maar op het
# Tailscale-adres van GERARD zelf. Er is geen Serve/Funnel nodig: de socket bindt alleen op het tailnetadres (nooit
# 0.0.0.0, nooit het LAN- of een publiek adres) en alleen tailnetpeers (100.64.0.0/10) komen erdoor. Het adres bestaat pas
# na ``tailscale up``; met ``"bind": "auto"`` leest Monitor 2 het van de interface en probeert het elke 10 s opnieuw.
#
#     {"schema": "obf-monitor2-tailnet-v1", "bind": "auto", "interface": "tailscale0", "port": 8772,
#      "hosts": ["gerard", "gerard.tailnet-naam.ts.net"], "key_file": "~/.config/monitor2/tailnet-access.key",
#      "identity": {"allowed_logins": ["eigenaar@example.invalid"], "allowed_nodes": ["nAbCdEf12311CNTRL"]}}
#
# ``identity`` is optioneel (zie "Tailscale-identiteit" hieronder): ontbreekt het, dan geldt alleen de sleutel.

TAILNET_SCHEMA = "obf-monitor2-tailnet-v1"
TAILNET_DEFAULT_FILE = Path.home() / ".config" / "monitor2" / "tailnet.json"
TAILNET_ENV_FILE = "OBF_MONITOR2_TAILNET_FILE"
TAILNET_NETWORKS = (ipaddress.ip_network("100.64.0.0/10"), ipaddress.ip_network("fd7a:115c:a1e0::/48"))
_SIOCGIFADDR = 0x8915


def _in_tailnet(address) -> bool:
    return any(address in network for network in TAILNET_NETWORKS if network.version == address.version)


def interface_ipv4(name: str) -> str | None:
    """Het IPv4-adres van een interface (ioctl SIOCGIFADDR), of ``None`` als de interface of het adres er niet is."""
    try:
        import fcntl
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            packed = fcntl.ioctl(sock.fileno(), _SIOCGIFADDR, struct.pack("256s", name.encode()[:15]))
        return socket.inet_ntoa(packed[20:24])
    except (OSError, ImportError):
        return None


def parse_tailnet(doc: object) -> LanRoute | None:
    if not isinstance(doc, dict) or doc.get("schema") != TAILNET_SCHEMA:
        return None
    try:
        port = int(doc.get("port", 8772))
        raw_networks = doc.get("allowed_networks") or [str(TAILNET_NETWORKS[0])]
        networks = tuple(ipaddress.ip_network(str(n), strict=False) for n in raw_networks)
        bind = "" if doc.get("bind", "auto") == "auto" else str(ipaddress.ip_address(str(doc.get("bind"))))
    except ValueError:
        return None
    interface = str(doc.get("interface", "tailscale0"))
    if not 1 <= port <= 65535 or not re.match(r"^[A-Za-z0-9_.-]{1,15}$", interface):
        return None
    if bind and not (ipaddress.ip_address(bind).version == 4 and _in_tailnet(ipaddress.ip_address(bind))):
        return None
    # Alleen tailnetadressen als peer: dit pad accepteert nooit het LAN of een publiek netwerk.
    if not networks or not all(any(n.version == t.version and n.subnet_of(t) for t in TAILNET_NETWORKS) for n in networks):
        return None
    hosts = {bind} if bind else set()
    for name in doc.get("hosts") or []:
        if not isinstance(name, str) or not name.strip() or not _HOSTNAME.match(name.strip().lower().rstrip(".")):
            return None
        hosts.add(name.strip().lower().rstrip("."))
    key_file = doc.get("key_file")
    key = _read_key(Path(str(key_file)).expanduser()) if isinstance(key_file, str) and key_file else None
    if key is None:
        return None
    identity = None
    if "identity" in doc:
        identity = parse_identity(doc["identity"])
        if identity is None:
            return None
    return LanRoute(bind=bind, port=port, hosts=frozenset(hosts), networks=networks, key=key, label="tailnet", interface=interface,
                    identity=identity)


def load_tailnet() -> LanRoute | None:
    path = Path(os.environ.get(TAILNET_ENV_FILE) or TAILNET_DEFAULT_FILE).expanduser()
    try:
        return parse_tailnet(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        return None


# ----------------------------------------------------------------------------- Tailscale-identiteit (tailnet-route)
#
# Zonder sleutelcookie toch binnen, maar alleen vanaf een uitdrukkelijk genoemd eigen apparaat. Tailscale weet van elke
# verbinding op het tailnetadres welk apparaat en welke login erachter zit: de afzender is een WireGuard-peer, en tailscaled
# (NetfilterMode "on") laat een 100.64.0.0/10-afzender alleen via tailscale0 binnen. Monitor 2 vraagt dat per verbinding
# lokaal na (LocalAPI ``whois``, alleen lezen, geen root nodig) en laat de aanvraag door als:
#
# * het apparaat (``Node.StableID``) in ``allowed_nodes`` staat; dezelfde login alléén is niet genoeg, want ook de
#   servers met autonome taken (OBF, Windows) zitten onder die login in het tailnet;
# * de login (``UserProfile.LoginName``) in ``allowed_logins`` staat, het apparaat geen tags heeft (een getagd apparaat
#   heeft geen persoon als eigenaar) en het gevraagde adres echt bij dat apparaat hoort.
#
# Lukt de vraag niet (tailscaled weg, time-out, onbekend adres), dan valt de route terug op de sleutel: fail-closed. Het
# actiecookie, de exacte Origin, Sec-Fetch-Site en het eenmalige kenmerk blijven voor elke knopdruk gelden; identiteit
# vervangt alleen het sleutelcookie. Zo blijft het werken na een herstart, een nieuwe sleutel of gewiste cookies.

TAILSCALED_SOCKET = "/var/run/tailscale/tailscaled.sock"
TAILSCALED_SOCKET_ENV = "OBF_MONITOR2_TAILSCALED_SOCKET"
WHOIS_TIMEOUT_SECONDS = 2.0
IDENTITY_CACHE_SECONDS = 30.0
_STABLE_ID = re.compile(r"^[A-Za-z0-9]{6,64}$")
_LOGIN = re.compile(r"^[^\s@]+@[^\s@]+$")


@dataclass(frozen=True)
class TailnetIdentity:
    allowed_logins: frozenset
    allowed_nodes: frozenset

    def refusal(self, whois: dict | None, peer: str) -> str | None:
        """Waarom dit antwoord van ``whois`` géén toegestaan eigen apparaat is, of ``None``."""
        if not isinstance(whois, dict):
            return "whois_unavailable"
        node, user = whois.get("Node"), whois.get("UserProfile")
        if not isinstance(node, dict) or not isinstance(user, dict):
            return "whois_incomplete"
        if node.get("Tags"):
            return "tagged_node"
        if str(node.get("StableID") or "") not in self.allowed_nodes:
            return "node_not_allowed"
        if str(user.get("LoginName") or "").strip().lower() not in self.allowed_logins:
            return "login_not_allowed"
        try:
            address = ipaddress.ip_address(peer)
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
                address = address.ipv4_mapped
            owned = {ipaddress.ip_interface(str(a)).ip for a in node.get("Addresses") or []}
        except ValueError:
            return "address_mismatch"
        return None if address in owned else "address_mismatch"


def parse_identity(doc: object) -> TailnetIdentity | None:
    if not isinstance(doc, dict):
        return None
    logins, nodes = doc.get("allowed_logins"), doc.get("allowed_nodes")
    if not isinstance(logins, list) or not logins or not all(isinstance(x, str) and _LOGIN.match(x.strip()) for x in logins):
        return None
    if not isinstance(nodes, list) or not nodes or not all(isinstance(x, str) and _STABLE_ID.match(x.strip()) for x in nodes):
        return None
    return TailnetIdentity(allowed_logins=frozenset(x.strip().lower() for x in logins),
                           allowed_nodes=frozenset(x.strip() for x in nodes))


def tailscale_whois(address: str, port: int) -> dict | None:
    """``whois`` van tailscaled voor ``address:port`` (LocalAPI over de lokale socket), of ``None`` bij elke fout."""
    import http.client

    path = os.environ.get(TAILSCALED_SOCKET_ENV) or TAILSCALED_SOCKET

    class _Conn(http.client.HTTPConnection):
        def connect(self):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(WHOIS_TIMEOUT_SECONDS)
            try:
                sock.connect(path)
            except OSError:
                sock.close()
                raise
            self.sock = sock

    host = f"[{address}]" if ":" in address else address
    conn = _Conn("local-tailscaled.sock", timeout=WHOIS_TIMEOUT_SECONDS)
    try:
        conn.request("GET", f"/localapi/v0/whois?addr={host}:{int(port)}", headers={"Host": "local-tailscaled.sock"})
        response = conn.getresponse()
        body = response.read(256 * 1024)
        if response.status != 200:
            return None
        doc = json.loads(body)
        return doc if isinstance(doc, dict) else None
    except (OSError, ValueError, http.client.HTTPException):
        return None
    finally:
        conn.close()


class IdentityCheck:
    """Per afzenderadres het oordeel over ``TailnetIdentity`` even onthouden (de pagina vraagt elke paar seconden gegevens).

    Alleen een echt antwoord van tailscaled wordt onthouden; een fout (tailscaled tijdelijk weg) niet. ``on_refusal`` krijgt
    elke nieuwe weigering één keer per bewaartermijn (voor het logboek), zonder geheimen.
    """

    def __init__(self, identity: TailnetIdentity, whois=None, clock=time.monotonic, ttl: float = IDENTITY_CACHE_SECONDS,
                 own_addresses=()):
        self.identity = identity
        # Het eigen tailnetadres: een proces op GERARD zelf (ook een Runner-2-taak) krijgt nooit toegang op identiteit.
        self._own = frozenset(own_addresses)
        self._whois = whois or tailscale_whois
        self._clock = clock
        self._ttl = ttl
        self._cache: dict[str, tuple[float, str | None]] = {}
        self._lock = threading.Lock()

    def check(self, address: str, port: int, on_refusal=None) -> bool:
        if address in self._own:
            if on_refusal is not None:
                on_refusal("own_machine")
            return False
        now = self._clock()
        with self._lock:
            hit = self._cache.get(address)
        if hit is not None and hit[0] > now:
            return hit[1] is None
        answer = self._whois(address, port)
        refusal = self.identity.refusal(answer, address)
        if answer is not None:
            with self._lock:
                if len(self._cache) > 256:
                    self._cache.clear()
                self._cache[address] = (now + self._ttl, refusal)
        if refusal is not None and on_refusal is not None:
            on_refusal(refusal)
        return refusal is None


# ----------------------------------------------------------------------------- bootstrap-link (kopieer-veilig, eenmalig)
#
# De permanente toegangssleutel bevat ``_`` en ``-``; dat gaat bij kopiëren/plakken (markdown, chat, automatisch
# afbreken) makkelijk mis en geeft dan ``bad_key``. ``monitor2-ctl tailnet-url`` geeft daarom een kortlevende, eenmalige
# link met alleen hexadecimale tekens. Alleen de sha256 van de token staat op schijf (map 0700, bestanden 0600); de server
# controleert bij elke aanvraag, dus er is geen herstart nodig. Na inwisselen zet de server het gewone sleutelcookie.

BOOTSTRAP_ENV_DIR = "OBF_MONITOR2_BOOTSTRAP_DIR"
BOOTSTRAP_TTL_SECONDS = 600
_BOOTSTRAP_TOKEN = re.compile(r"^[0-9a-f]{32}$")


def _bootstrap_dir() -> Path:
    return Path(os.environ.get(BOOTSTRAP_ENV_DIR) or Path.home() / ".config" / "monitor2" / "bootstrap").expanduser()


def _bootstrap_file(token: str) -> Path:
    return _bootstrap_dir() / hashlib.sha256(token.encode("ascii")).hexdigest()


def mint_bootstrap(label: str, ttl: int = BOOTSTRAP_TTL_SECONDS) -> str:
    """Een nieuwe eenmalige token voor route ``label`` ("tailnet" of "lan"); verlopen/gebruikte bestanden worden opgeruimd."""
    directory = _bootstrap_dir()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    now = time.time()
    for old in directory.iterdir():
        try:
            if now - old.stat().st_mtime > BOOTSTRAP_TTL_SECONDS * 6:
                old.unlink()
        except OSError:
            pass
    token = secrets.token_hex(16)
    path = _bootstrap_file(token)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"route": label, "expires": now + ttl}) + "\n")
    return token


def redeem_bootstrap(token: str, label: str) -> bool:
    """Wisselt een token eenmalig in: het bestand verdwijnt altijd (ook als het verlopen is of voor een andere route was)."""
    if not _BOOTSTRAP_TOKEN.match(token or ""):
        return False
    path = _bootstrap_file(token)
    claimed = path.with_name(path.name + f".{secrets.token_hex(4)}.used")
    try:
        os.rename(path, claimed)                  # atomisch: twee gelijktijdige aanvragen kunnen niet allebei winnen
    except OSError:
        return False
    try:
        doc = json.loads(claimed.read_text(encoding="utf-8"))
        return doc.get("route") == label and float(doc.get("expires", 0)) >= time.time()
    except (OSError, ValueError, AttributeError):
        return False
    finally:
        try:
            claimed.unlink()
        except OSError:
            pass
