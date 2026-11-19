"""Cloudflare Tunnel voor de controller-MCP van de control plane (aparte tunnel, los van die van de publieke site).

`python3 -m runner2.cptunnel create HOSTNAME | status HOSTNAME | delete HOSTNAME`

Gebruikt het bestaande token uit ~/.config/gerard/cloudflare.env (rechten: Tunnel Edit, DNS Edit op de zone van de site; leest
alleen, toont het nooit). Maakt de tunnel `gerard-runner2-control` (remote-managed), zet de ingress naar de lokale controller-MCP
(127.0.0.1:8766) en een geproxied CNAME. Bestaat de hostname al met een ander doel, dan weigert het (nooit overschrijven).
De connector-token komt in ~/.config/runner2/cloudflared.env (0600) voor dienst runner2-tunnel. De tunnel van de site wordt niet aangeraakt.
"""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API = "https://api.cloudflare.com/client/v4"
TUNNEL_NAME = "gerard-runner2-control"
SERVICE = "http://127.0.0.1:8766"
CF_ENV = Path.home() / ".config" / "gerard" / "cloudflare.env"
OUT_ENV = Path.home() / ".config" / "runner2" / "cloudflared.env"


class CFError(RuntimeError):
    pass


def creds() -> tuple:
    env = {}
    for line in CF_ENV.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env["GERARD_CF_ACCOUNT_ID"], env["GERARD_CF_API_TOKEN"]


def call(token: str, method: str, path: str, body: dict | None = None, query: dict | None = None):
    url = API + path + (("?" + urllib.parse.urlencode(query)) if query else "")
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:   # noqa: S310
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            msg = "; ".join(x.get("message", "") for x in json.loads(e.read()).get("errors", []))
        except ValueError:
            msg = ""
        raise CFError(f"{method} {path}: HTTP {e.code} {msg}") from None
    if not data.get("success"):
        raise CFError(f"{method} {path}: " + "; ".join(x.get("message", "") for x in data.get("errors", [])))
    return data["result"]


def zone_for(token: str, hostname: str) -> dict:
    labels = hostname.split(".")
    for i in range(len(labels) - 1):
        apex = ".".join(labels[i:])
        res = call(token, "GET", "/zones", query={"name": apex})
        if res:
            return res[0]
    raise CFError(f"geen zone gevonden voor {hostname}")


def find_tunnel(token: str, account: str) -> dict | None:
    res = call(token, "GET", f"/accounts/{account}/cfd_tunnel", query={"name": TUNNEL_NAME, "is_deleted": "false"})
    return res[0] if res else None


def create(hostname: str) -> dict:
    account, token = creds()
    zone = zone_for(token, hostname)
    out = {"hostname": hostname, "zone": zone["name"]}
    existing = call(token, "GET", f"/zones/{zone['id']}/dns_records", query={"name": hostname})
    tun = find_tunnel(token, account)
    if existing and not (tun and any(r.get("content") == f"{tun['id']}.cfargotunnel.com" for r in existing)):
        raise CFError(f"{hostname} bestaat al in DNS met een ander doel; niets gewijzigd")
    if not tun:
        tun = call(token, "POST", f"/accounts/{account}/cfd_tunnel", {"name": TUNNEL_NAME, "config_src": "cloudflare"})
        out["tunnel"] = "aangemaakt"
    else:
        out["tunnel"] = "bestond al"
    call(token, "PUT", f"/accounts/{account}/cfd_tunnel/{tun['id']}/configurations",
         {"config": {"ingress": [{"hostname": hostname, "service": SERVICE}, {"service": "http_status:404"}]}})
    if not existing:
        call(token, "POST", f"/zones/{zone['id']}/dns_records",
             {"type": "CNAME", "name": hostname, "content": f"{tun['id']}.cfargotunnel.com", "proxied": True, "comment": "Runner 2 control plane (cptunnel)"})
        out["dns"] = "aangemaakt"
    else:
        out["dns"] = "bestond al (zelfde tunnel)"
    tok = call(token, "GET", f"/accounts/{account}/cfd_tunnel/{tun['id']}/token")
    OUT_ENV.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(OUT_ENV, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"TUNNEL_TOKEN={tok}\n")
    out["connector_env"] = str(OUT_ENV)
    out["tunnel_id"] = tun["id"]
    return out


def status(hostname: str) -> dict:
    account, token = creds()
    tun = find_tunnel(token, account)
    out = {"tunnel": None}
    if tun:
        conns = call(token, "GET", f"/accounts/{account}/cfd_tunnel/{tun['id']}/connections")
        out["tunnel"] = {"id": tun["id"], "status": tun.get("status"), "connections": len(conns or [])}
    zone = zone_for(token, hostname)
    out["dns"] = [{"type": r["type"], "content": r["content"], "proxied": r.get("proxied")}
                  for r in call(token, "GET", f"/zones/{zone['id']}/dns_records", query={"name": hostname})]
    return out


def delete(hostname: str) -> dict:
    account, token = creds()
    tun = find_tunnel(token, account)
    zone = zone_for(token, hostname)
    out = {}
    for r in call(token, "GET", f"/zones/{zone['id']}/dns_records", query={"name": hostname}):
        if tun and r.get("content") == f"{tun['id']}.cfargotunnel.com":   # alleen ons eigen record
            call(token, "DELETE", f"/zones/{zone['id']}/dns_records/{r['id']}")
            out["dns"] = "verwijderd"
    if tun:
        call(token, "DELETE", f"/accounts/{account}/cfd_tunnel/{tun['id']}/connections")
        call(token, "DELETE", f"/accounts/{account}/cfd_tunnel/{tun['id']}")
        out["tunnel"] = "verwijderd"
    OUT_ENV.unlink(missing_ok=True)
    return out


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 2 or argv[0] not in ("create", "status", "delete"):
        print(__doc__)
        return 2
    try:
        print(json.dumps({"create": create, "status": status, "delete": delete}[argv[0]](argv[1]), indent=1))
    except CFError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
