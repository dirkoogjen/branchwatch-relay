"""Externe controller-MCP over HTTP (Streamable HTTP, stateless JSON): de ChatGPT-kant van de control plane op GERARD.

Dunne HTTP-schil om `controller_mcp.handle`: geen eigen tools, geen eigen state. Luistert alleen op localhost; de
Cloudflare-tunnel (dienst runner2-tunnel) brengt het naar buiten. Toegang vereist het geheim uit R2_MCP_TOKEN
(of R2_MCP_PREVIOUS_TOKEN tijdens een rotatie), op twee manieren:

    POST /mcp                     met  Authorization: Bearer <geheim>
    POST /s/<geheim>/mcp          voor clients zonder headers (de URL is dan zelf het geheim)

GET /readyz en /healthz geven zonder geheim 200 (alleen "het proces leeft"). Al het andere zonder geheim is 404.
Het geheim komt nooit in een log. Een notificatie (geen id) krijgt 202; een batch wordt per bericht beantwoord.

Diagnose (mcp-http.log): per tools/call de toolnaam, fout=, de foutcode (code=), het JSON-RPC-id, een eigen verzoek-id (req=, ook
als antwoordheader X-Request-Id), de Cloudflare-Ray-ID (ray=, als die meekomt) en de duur. Geweigerde verzoeken (400/401/404/413/500)
krijgen een regel met status, methode en een gemaskeerd pad (nooit /s/<geheim>), begrensd per minuut. Nooit argumenten, berichtteksten,
headers met het geheim, IP-adressen of het geheim zelf.
"""
import hmac
import json
import os
import re
import secrets
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import controller_mcp
from .log import log

MAX_BODY = 2_000_000
DEFAULT_HOST, DEFAULT_PORT = "127.0.0.1", 8766   # poort die controller.health() als Runner-2-tunnel verwacht
LOGNAME = "mcp-http.log"
_slots = threading.BoundedSemaphore(8)
ACCESS_PER_MIN = 30   # plafond voor regels over geweigerde verzoeken; de rest wordt alleen geteld
_SAFE = re.compile(r"[A-Za-z0-9_.:-]{1,64}")
_CODE = re.compile(r"[a-z][a-z0-9_]{0,47}")
_access_lock = threading.Lock()
_access = {"window": 0, "count": 0, "dropped": 0}


def tokens() -> list:
    return [t for t in (os.environ.get("R2_MCP_TOKEN", "").strip(), os.environ.get("R2_MCP_PREVIOUS_TOKEN", "").strip()) if len(t) >= 32]


def _same(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


def authorize(path: str, auth_header: str) -> tuple:
    """(toegestaan, schoon pad). Zonder geldig geheim is het pad nooit toegestaan."""
    toks = tokens()
    if path.startswith("/s/"):
        _, _, rest = path[3:].partition("/")
        given = path[3:].split("/", 1)[0]
        ok = any(_same(given, t) for t in toks)
        return ok, "/" + rest
    scheme, _, given = (auth_header or "").partition(" ")
    ok = scheme.lower() == "bearer" and any(_same(given.strip(), t) for t in toks)
    return ok, path


def safe(value) -> str:
    """Logbare vorm van een waarde van de client (id, toolnaam, Ray-ID): kort en onschuldig, anders '?'. Nooit een geheim."""
    if value is None:
        return "-"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if not isinstance(value, str) or not _SAFE.fullmatch(value) or any(t in value for t in tokens()):
        return "?"
    return value


def error_code(reply: dict | None) -> str | None:
    """De vaste Runner-2-foutcode uit een tools/call-antwoord (alleen de code, nooit de reden), of None zonder fout."""
    res = (reply or {}).get("result") or {}
    if not res.get("isError"):
        return None
    try:
        code = json.loads(res["content"][0]["text"]).get("error")
    except (KeyError, IndexError, TypeError, ValueError, AttributeError):
        return "onbekend"
    return code if isinstance(code, str) and _CODE.fullmatch(code) else "onbekend"


def mask_path(path: str) -> str:
    """Pad voor het log: het geheim-pad wordt /s/***/..., een onbekend pad alleen 'anders' (kan willekeurige tekst bevatten)."""
    if path.startswith("/s/"):
        rest = path[3:].partition("/")[2]
        return "/s/***/" + ("mcp" if rest.rstrip("/") == "mcp" else "anders")
    if path.rstrip("/") == "/mcp":
        return "/mcp"
    return path if path in ("/readyz", "/healthz") else "anders"


def access_log(msg: str) -> None:
    """Regel over een geweigerd verzoek; hooguit ACCESS_PER_MIN per minuut, zodat ruis van buitenaf het log niet vult."""
    now = int(time.time() // 60)
    with _access_lock:
        if now != _access["window"]:
            dropped = _access["dropped"]
            _access.update(window=now, count=0, dropped=0)
            if dropped:
                log(f"http: {dropped} regels over geweigerde verzoeken overgeslagen (meer dan {ACCESS_PER_MIN} per minuut)", LOGNAME)
        if _access["count"] >= ACCESS_PER_MIN:
            _access["dropped"] += 1
            return
        _access["count"] += 1
    log(msg, LOGNAME)


def _ctx_text(ctx: dict | None) -> str:
    ctx = ctx or {}
    return f"req={ctx.get('req') or '-'} ray={safe(ctx.get('ray'))}"


def handle_body(raw: bytes, ctx: dict | None = None) -> tuple:
    """(status, antwoordtekst of None). Geen netwerk; dit is wat de tests aanroepen. ctx: {req, ray} voor het log."""
    try:
        msg = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return 400, json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}})
    batch = isinstance(msg, list)
    out = []
    for m in (msg if batch else [msg]):
        if not isinstance(m, dict):
            out.append({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "invalid request"}})
            continue
        if m.get("method") == "tools/call":
            t0 = time.monotonic()
            with _slots:
                r = controller_mcp.handle(m)
            params = m.get("params")
            code = error_code(r)
            log(f"tools/call {safe((params if isinstance(params, dict) else {}).get('name'))} fout={code is not None}"
                + (f" code={code}" if code else "")
                + f" id={safe(m.get('id'))} {_ctx_text(ctx)} ms={int((time.monotonic() - t0) * 1000)}", LOGNAME)
        else:
            r = controller_mcp.handle(m)
            if m.get("method") == "initialize":   # alleen naam/versie van de client; nooit headers of paden (geheim)
                info = (m.get("params") or {}).get("clientInfo") or {}
                log(f"initialize client={str(info.get('name', '?'))[:60]} versie={str(info.get('version', '?'))[:30]}", LOGNAME)
            elif m.get("method") == "tools/list" and r:
                names = [t["name"] for t in (r.get("result") or {}).get("tools", [])]
                log(f"tools/list {len(names)} tools" + (" (met runner2_jobs)" if "runner2_jobs" in names else " (ZONDER runner2_jobs)"), LOGNAME)
        if r:
            out.append(r)
    if not out:
        return 202, None
    return 200, json.dumps(out if batch else out[0], ensure_ascii=False)


class Handler(BaseHTTPRequestHandler):
    server_version = "runner2-control"
    protocol_version = "HTTP/1.1"
    timeout = 120

    def log_message(self, fmt, *args):   # geen toegangslog met paden: die kunnen het geheim bevatten
        pass

    def parse_request(self):   # één verbinding kan meerdere verzoeken dragen: per verzoek een nieuw verzoek-id
        self._req_ctx = None
        return super().parse_request()

    def _ctx(self) -> dict:
        """Verzoek-id en Ray-ID van het huidige verzoek."""
        if getattr(self, "_req_ctx", None) is None:
            self._req_ctx = {"req": secrets.token_hex(6), "ray": self.headers.get("Cf-Ray")}
        return self._req_ctx

    def _send(self, code: int, body: str | None = None, ctype: str = "application/json", extra: dict | None = None, why: str = ""):
        data = (body or "").encode()
        path = self.path.split("?", 1)[0]
        if code >= 400 and code != 405 and path not in ("/readyz", "/healthz"):
            if code == 400 and not why:
                why = "parse_error"
            if code == 401:
                why = "auth=" + ("pad_onjuist" if path.startswith("/s/") else "onjuist" if self.headers.get("Authorization") else "ontbreekt")
            access_log(f"http {code} {self.command} {mask_path(path)}" + (f" {why}" if why else "") + f" {_ctx_text(self._ctx())}")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Request-Id", self._ctx()["req"])
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if data and self.command != "HEAD":
            self.wfile.write(data)

    def _route(self) -> tuple:
        path = self.path.split("?", 1)[0]
        if path in ("/readyz", "/healthz") and self.command in ("GET", "HEAD"):
            return "ready", path
        ok, clean = authorize(path, self.headers.get("Authorization", ""))
        if path.startswith("/s/") or path.rstrip("/") == "/mcp":
            if not ok:
                return "deny", path
            return ("mcp" if clean.rstrip("/") == "/mcp" else "none"), clean
        return "none", path

    def do_GET(self):
        kind, _ = self._route()
        if kind == "ready":
            return self._send(200, json.dumps({"ok": True, "tokens": bool(tokens())}))
        if kind == "mcp":   # geen server-initiated stream: toegestaan volgens de specificatie
            return self._send(405, extra={"Allow": "POST"})
        return self._send(401 if kind == "deny" else 404, extra={"WWW-Authenticate": "Bearer"} if kind == "deny" else None)

    do_HEAD = do_GET

    def do_DELETE(self):   # sessie beëindigen: er zijn geen sessies
        kind, _ = self._route()
        return self._send(405 if kind == "mcp" else 401 if kind == "deny" else 404, extra={"Allow": "POST"} if kind == "mcp" else None)

    def do_POST(self):
        kind, _ = self._route()
        if kind == "deny":
            return self._send(401, json.dumps({"error": "unauthorized"}), extra={"WWW-Authenticate": "Bearer"})
        if kind != "mcp":
            return self._send(404)
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            n = -1
        if n < 0 or n > MAX_BODY:
            return self._send(413, json.dumps({"error": "payload_too_large"}))
        try:
            status, text = handle_body(self.rfile.read(n), self._ctx())
        except Exception as e:   # nooit een verbroken verbinding zonder antwoord of logregel; alleen het fouttype, geen inhoud
            return self._send(500, json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": "internal error"}}),
                              why=f"fout={type(e).__name__}")
        self._send(status, text)


def make_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit("mcp_http luistert alleen op localhost; publiek bereik loopt via de tunnel")
    srv = ThreadingHTTPServer((host, port), Handler)
    srv.daemon_threads = True
    return srv


def main() -> int:
    if not tokens():
        print("R2_MCP_TOKEN ontbreekt of is korter dan 32 tekens; start geweigerd", file=sys.stderr)
        return 2
    host, port = os.environ.get("R2_MCP_HOST", DEFAULT_HOST), int(os.environ.get("R2_MCP_PORT", DEFAULT_PORT))
    srv = make_server(host, port)
    log(f"mcp-http gestart op {host}:{port} (pid {os.getpid()})", LOGNAME)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    log("mcp-http gestopt", LOGNAME)
    return 0


if __name__ == "__main__":
    sys.exit(main())
