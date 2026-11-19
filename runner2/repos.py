"""Repositorykeuze: welke repositories een node heeft, bij welk project ze horen, en welke repo een opdracht nodig heeft.

De gebruiker zegt "verander project.example.invalid en de managementconsole" en weet niet welke repository daarbij hoort. Deze module
bepaalt dat zonder te gokken:

* **Ontdekken per node** (`discover`): alleen repositories uit vier bekende bronnen, elk daarna read-only geïnspecteerd:
  1. de projectafspraken in `~/.runner2/config/projects.toml` (plus de ingebouwde afspraken, `BUILTIN_PROJECTS`);
  2. de repositories die Runner 2 al kent: opdrachtwachtrij en supervisieprojecten (dossier-targets);
  3. een begrensde inspectie van de `roots` uit `[discovery]`: alleen de directe submappen (diepte 1), geen
     verborgen mappen, geen symlinks, geen worktrees of back-ups, en alleen mappen met een echte `.git`-map;
  4. niets anders. Er is geen recursieve scan en geen vrij pad: een repository komt alleen in de lijst als ze uit 1-3 komt
     én de inspectie slaagt (map bestaat, leesbaar, echte git-repository).
* **Inspectie** (`inspect_repo`) leest alleen: `.git/HEAD`, de remote-url uit `.git/config` (zonder inloggegevens), een
  eventuele `.obf-durable-project.json` en de aanwezigheid van de `hints` van een project. Er wordt geen git of ander
  programma gestart en niets geschreven.
* **Beslissen** (`resolve`): de tekst van de opdracht noemt projecten (via de `aliases`); per project moet er op de gekozen
  node precies één repository zijn. Meerdere kandidaten of geen kandidaat = één gerichte vraag, nooit stilzwijgend een
  gok. Noemt de tekst projecten in verschillende repositories, dan is het resultaat een gesplitst voorstel (één groep
  per repository) dat de aanroeper moet bevestigen. Een node wordt nooit stilzwijgend gewisseld.

Een node die we niet live kunnen inspecteren (geen route, oudere Runner 2) levert alleen *bekende* repositories op, met
`verified: false`; die tellen mee, maar de plek waar ze staan is dan niet gecontroleerd.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import config

NODES = ("MAC", "GERARD", "LINUX", "WINDOWS")
AUTO = "auto"
MAX_SCAN_ENTRIES = 300
MAX_FILE = 65536
CACHE_S = 60.0

# Ingebouwde projectafspraken. `hints` = paden binnen een repository die het project daar herkenbaar maken (alleen
# bestaan wordt gecontroleerd); `repos` = bekende mapnamen. Wat generiek is (een map `website`) staat hier bewust niet
# in: een verkeerde enkele kandidaat zou dan stilzwijgend gekozen worden. Uitbreiden of overschrijven: projects.toml.
BUILTIN_PROJECTS = {}
DEFAULT_ROOTS = ("~/repos",)

_NOT_A_SOURCE = re.compile(r"(^|[\\/])\.runner2[\\/]worktrees([\\/]|$)|(^|[\\/])\.[^\\/]+-worktrees([\\/]|$)")
_BACKUP_NAME = re.compile(r"\.(bak|backup|prev|previous|old|tmp|orig)([.-]|$)|[-.]backup[-.]|\.bak-", re.I)

_cache: dict = {}
_cache_lock = threading.Lock()


class ReposError(Exception):
    def __init__(self, code: str, message: str, data: dict | None = None):
        super().__init__(message)
        self.code, self.message, self.data = code, message, data


# --- projectafspraken ---

def projects_file() -> Path:
    return config.r2_home() / "config" / "projects.toml"


def _clean_list(v, limit=40, maxlen=120) -> list[str]:
    if not isinstance(v, list):
        return []
    return [x.strip() for x in v if isinstance(x, str) and x.strip() and len(x) <= maxlen][:limit]


def _safe_rel(rel) -> str | None:
    """Een relatief pad binnen een repository ('' = de hele repository), of None als het niet veilig is."""
    if not isinstance(rel, str) or len(rel) > 200:
        return None
    rel = rel.strip().replace("\\", "/")
    if rel.startswith("/") or re.match(r"^[A-Za-z]:", rel):
        return None
    parts = [p for p in rel.split("/") if p not in ("", ".")]
    if any(p == ".." or ":" in p or p.startswith("~") for p in parts):
        return None
    return "/".join(parts)


def load_projects() -> dict:
    """{'projects': {id: {...}}, 'roots': [...], 'scan': bool, 'error': str|None}; een kapot bestand valt terug op de ingebouwde lijst."""
    projects = {}
    for pid, p in BUILTIN_PROJECTS.items():
        projects[pid] = {"id": pid, "name": p["name"], "aliases": list(p["aliases"]), "hints": list(p["hints"]),
                         "repos": list(p["repos"]), "nodes": {}, "marker_ids": [pid]}
    out = {"projects": projects, "roots": list(DEFAULT_ROOTS), "scan": True, "error": None}
    path = projects_file()
    if not path.exists():
        return out
    try:
        if path.stat().st_size > 256 * 1024:
            raise ValueError("bestand te groot")
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, tomllib.TOMLDecodeError) as e:
        out["error"] = f"projects.toml onleesbaar ({type(e).__name__}); alleen de ingebouwde projecten gelden"
        return out
    disc = data.get("discovery") if isinstance(data.get("discovery"), dict) else {}
    if isinstance(disc.get("scan"), bool):
        out["scan"] = disc["scan"]
    if "roots" in disc:
        out["roots"] = _clean_list(disc["roots"], limit=12, maxlen=300)
    for pid, p in (data.get("project") or {}).items():
        if not isinstance(p, dict) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,40}", str(pid)):
            continue
        cur = projects.setdefault(pid, {"id": pid, "name": pid, "aliases": [], "hints": [], "repos": [], "nodes": {},
                                        "marker_ids": [pid]})
        if isinstance(p.get("name"), str) and p["name"].strip():
            cur["name"] = p["name"].strip()[:80]
        for key in ("aliases", "repos", "marker_ids"):
            if key in p:
                cur[key] = _clean_list(p[key])
        if "hints" in p:
            cur["hints"] = [h for h in (_safe_rel(x) for x in _clean_list(p["hints"])) if h]
        for node, spec in (p.get("nodes") or {}).items():
            if str(node).upper() in NODES and isinstance(spec, dict) and isinstance(spec.get("repo"), str):
                rel = _safe_rel(spec.get("path", ""))
                if rel is not None and spec["repo"].strip() and len(spec["repo"]) <= 300:
                    cur["nodes"][str(node).upper()] = {"repo": spec["repo"].strip(), "path": rel}
    return out


# --- inspectie (alleen lezen) ---

def _read_small(p: Path, limit: int = MAX_FILE) -> str | None:
    try:
        if p.is_symlink() or not p.is_file() or p.stat().st_size > limit:
            return None
        return p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _strip_credentials(url: str) -> str:
    return re.sub(r"^([a-z][a-z0-9+.-]*://)[^/@\s]*@", r"\1", url.strip(), flags=re.I)


def _within(root: Path, rel: str) -> Path | None:
    """`root/rel` als dat bestaat en na het oplossen van symlinks binnen `root` blijft."""
    try:
        target = (root / rel).resolve()
        target.relative_to(root.resolve())
        return target if target.exists() else None
    except (OSError, ValueError, RuntimeError):
        return None


def inspect_repo(path: str | Path) -> dict:
    """Read-only inspectie van één map. {'ok': True, 'path', 'name', 'branch', 'remote', 'marker'} of {'ok': False, 'why'}."""
    try:
        p = Path(str(path)).expanduser()
        if not p.is_dir():
            return {"ok": False, "why": "bestaat niet"}
        p = p.resolve()
        git = p / ".git"
        if _NOT_A_SOURCE.search(str(p)) or _under(p, config.worktrees_dir()):
            return {"ok": False, "why": "is een worktree van Runner 2 of een andere tool"}
        if _BACKUP_NAME.search(p.name):
            return {"ok": False, "why": "lijkt een back-up of tijdelijke kopie"}
        if not git.exists():
            return {"ok": False, "why": "is geen git-repository"}
        if git.is_file():
            return {"ok": False, "why": "is een worktree of submodule, geen bronrepository"}
        if not os.access(p, os.R_OK | os.X_OK):
            return {"ok": False, "why": "is niet toegankelijk"}
    except (OSError, RuntimeError):
        return {"ok": False, "why": "is niet toegankelijk"}
    head = (_read_small(git / "HEAD", 512) or "").strip()
    branch = head.split("refs/heads/", 1)[1] if "refs/heads/" in head else ("(losse commit)" if head else None)
    remote = None
    m = re.search(r"^\s*url\s*=\s*(\S+)", _read_small(git / "config") or "", re.M)
    if m:
        remote = _strip_credentials(m.group(1))[:200]
    marker = None
    raw = _read_small(p / ".obf-durable-project.json", 4096)
    if raw:
        try:
            v = json.loads(raw).get("project_id")
            marker = v if isinstance(v, str) and len(v) <= 80 else None
        except (ValueError, AttributeError):
            pass
    return {"ok": True, "path": str(p), "name": p.name, "branch": branch, "remote": remote, "marker": marker}


def _under(p: Path, base: Path) -> bool:
    try:
        p.relative_to(base.resolve())
        return True
    except (ValueError, OSError):
        return False


def _associate(info: dict, defs: dict, node: str, mapped: dict) -> list[dict]:
    """De projecten die bij een geïnspecteerde repository horen, met het onderdeel (subpad) binnen de repository."""
    root = Path(info["path"])
    out = []
    for pid, p in defs.items():
        comp, reason = None, None
        spec = mapped.get(pid)
        if spec is not None and spec["real"] == info["path"]:
            comp, reason = spec["path"], "afspraak"
            if comp and _within(root, comp) is None:
                comp, reason = None, None
        if comp is None or comp == "":
            for hint in p["hints"]:   # een bestaand onderdeel is specifieker dan "de hele repository"
                if _within(root, hint) is not None:
                    comp, reason = hint, reason or "herkend"
                    break
        if comp is None and (info["name"] in p["repos"] or (info.get("marker") and info["marker"] in p["marker_ids"])):
            comp, reason = "", "herkend"
        if comp is not None:
            out.append({"id": pid, "name": p["name"], "component": comp, "reason": reason})
    return out


# --- bronnen ---

def _norm(path: str, windows: bool = False) -> str:
    return path.lower() if windows else path


def _known_sources(node: str) -> dict[str, set]:
    """Repositories die Runner 2 voor `node` kent uit eigen bestanden: pad -> bronnen. Geen netwerk, geen scan."""
    out: dict[str, set] = {}

    def add(path, source):
        if isinstance(path, str) and path.strip() and len(path) <= 300 and "\n" not in path:
            out.setdefault(path.strip(), set()).add(source)

    try:
        from . import jobs
        q, _p, _a, _l = jobs._paths()
        if q.exists():
            d = jobs._read(q)
            for j in d["jobs"].values():
                if j.get("node") == node and (j.get("last_error") or {}).get("code") != "repo_invalid":
                    add(j.get("repo"), "opdracht")
    except Exception:   # noqa: BLE001 -- een onleesbare wachtrij mag het ontdekken niet breken
        pass
    try:
        for f in sorted(config.supervision_dir().glob("P-*/dossier.json"))[:50]:
            raw = _read_small(f, 4 * 1024 * 1024)
            for t in (json.loads(raw).get("targets") or []) if raw else []:
                if isinstance(t, dict) and str(t.get("node", "")).upper() == node:
                    add(t.get("repository"), "supervisie")
    except (OSError, ValueError, AttributeError):
        pass
    return out


def _scan_roots(roots: list[str]) -> list[str]:
    """Directe submappen (diepte 1) van de toegestane roots die een `.git`-map hebben. Geen recursie, geen symlinks."""
    found, seen_roots = [], set()
    for root in roots:
        try:
            r = Path(root).expanduser().resolve()
            if r in seen_roots or not r.is_dir():
                continue
            seen_roots.add(r)
            with os.scandir(r) as it:
                for n, e in enumerate(it):
                    if n >= MAX_SCAN_ENTRIES:
                        break
                    if e.name.startswith(".") or e.is_symlink() or not e.is_dir(follow_symlinks=False):
                        continue
                    if os.path.isdir(os.path.join(e.path, ".git")):
                        found.append(e.path)
        except OSError:
            continue
    return found


def _finish(records: dict, hidden: list) -> dict:
    repos = sorted(records.values(), key=lambda r: (not r["projects"], r["name"].lower()))
    for r in repos:
        r["sources"] = sorted(r["sources"])
        names = [p["name"] for p in r["projects"]]
        r["label"] = (" + ".join(names) + " — " if names else "") + r["name"]
    return {"repos": repos, "hidden": hidden}


def discover_local(scan: bool | None = None) -> dict:
    """Live ontdekking op deze machine (zie de modulekop). {'node','checked':'live','repos':[...],'hidden':[...]}"""
    node = config.local_node()
    cfg = load_projects()
    defs = cfg["projects"]
    windows = os.name == "nt"
    mapped: dict = {}
    sources: dict[str, set] = {}
    for pid, p in defs.items():
        spec = p["nodes"].get(node)
        if spec:
            sources.setdefault(spec["repo"], set()).add("afspraak")
            mapped[pid] = {"repo": spec["repo"], "path": spec["path"], "real": None}
    for path, src in _known_sources(node).items():
        sources.setdefault(path, set()).update(src)
    if cfg["scan"] if scan is None else scan:
        for path in _scan_roots(cfg["roots"]):
            sources.setdefault(path, set()).add("gevonden")
    records: dict[str, dict] = {}
    hidden: list = []
    for path, src in sources.items():
        info = inspect_repo(path)
        if not info["ok"]:
            if src & {"afspraak", "opdracht", "supervisie"}:
                hidden.append({"path": path, "why": info["why"] + f" op {node}", "sources": sorted(src)})
            continue
        key = _norm(info["path"], windows)
        rec = records.setdefault(key, {"path": info["path"], "name": info["name"], "branch": info["branch"],
                                       "remote": info["remote"], "marker": info["marker"], "sources": set(),
                                       "verified": True, "node": node})
        rec["sources"] |= src
        for pid, spec in mapped.items():
            if spec["repo"] == path:
                spec["real"] = info["path"]
    for key, rec in records.items():
        rec["projects"] = _associate({"path": rec["path"], "name": rec["name"], "marker": rec["marker"]}, defs, node, mapped)
    out = _finish(records, hidden)
    return {"node": node, "checked": "live", "scan": cfg["scan"] if scan is None else scan, "roots": cfg["roots"],
            "config_error": cfg["error"], **out}


def known_for_node(node: str) -> dict:
    """Wat we van een andere node weten zonder hem te bevragen: afspraken en eerder gebruikte repositories (niet gecontroleerd)."""
    node = node.upper()
    cfg = load_projects()
    sources = {}
    mapped = {}
    for pid, p in cfg["projects"].items():
        spec = p["nodes"].get(node)
        if spec:
            sources.setdefault(spec["repo"], set()).add("afspraak")
            mapped[pid] = spec
    for path, src in _known_sources(node).items():
        sources.setdefault(path, set()).update(src)
    records = {}
    merged: dict[str, set] = {}
    for path, src in sorted(sources.items(), key=lambda kv: kv[0].startswith("~")):   # absolute paden eerst
        target = next((k for k in merged if same_path(k, path, node == "WINDOWS")), None)
        merged.setdefault(target or path, set()).update(src)
    sources = merged
    for path, src in sources.items():
        name = re.split(r"[\\/]", path.rstrip("\\/"))[-1] or path
        if _NOT_A_SOURCE.search(path) or _BACKUP_NAME.search(name):
            continue
        projects = []
        for pid, spec in mapped.items():
            if same_path(path, spec["repo"], node == "WINDOWS"):
                projects.append({"id": pid, "name": cfg["projects"][pid]["name"], "component": spec["path"], "reason": "afspraak"})
        for pid, p in cfg["projects"].items():
            if name in p["repos"] and all(x["id"] != pid for x in projects):
                projects.append({"id": pid, "name": p["name"], "component": "", "reason": "herkend"})
        records[path] = {"path": path, "name": name, "branch": None, "remote": None, "marker": None, "sources": set(src),
                         "verified": False, "node": node, "projects": projects}
    return {"node": node, "checked": "known", **_finish(records, [])}


# --- paden van een andere node vergelijken ---

_HOME_PREFIX = re.compile(r"^(?:/Users/[^/]+|/home/[^/]+|/root|[A-Za-z]:[\\/]Users[\\/][^\\/]+)[\\/](.+)$")


def same_path(path: str, other: str, windows: bool = False) -> bool:
    """Is `path` dezelfde map als `other`? `~/X` wordt herkend in `/Users/<naam>/X`, `/home/<naam>/X` en `C:\\Users\\<naam>\\X`."""
    fold = (lambda x: x.lower()) if windows else (lambda x: x)
    norm = lambda x: fold(x.replace("\\", "/").rstrip("/"))   # noqa: E731
    if norm(path) == norm(other):
        return True
    for a, b in ((path, other), (other, path)):
        if a.startswith(("~/", "~\\")):
            m = _HOME_PREFIX.match(b)
            if m and norm(m.group(1)) == norm(a[2:]):
                return True
    return False


def _relabel(rec: dict) -> None:
    names = [p["name"] for p in rec["projects"]]
    rec["label"] = (" + ".join(names) + " — " if names else "") + rec["name"]


def _overlay_mappings(res: dict, node: str) -> None:
    """De afspraken van deze (control-plane-)machine voor een andere node gelden ook voor wat die node zelf meldde."""
    windows = node == "WINDOWS"
    for pid, p in load_projects()["projects"].items():
        spec = p["nodes"].get(node)
        if not spec:
            continue
        for rec in res.get("repos", []):
            if same_path(rec.get("path", ""), spec["repo"], windows) and not any(x.get("id") == pid for x in rec.get("projects", [])):
                rec.setdefault("projects", []).append({"id": pid, "name": p["name"], "component": spec["path"], "reason": "afspraak"})
                _relabel(rec)


# --- per node, met cache en terugval ---

def local_name() -> str:
    return config.local_node()


def discover(node: str | None = None, refresh: bool = False, live_remote: bool = True) -> dict:
    """De repositories van `node` (lokaal live; een andere node via het node-transport, anders alleen bekend)."""
    local = local_name()
    node = (node or local).upper()
    if node not in NODES:
        raise ReposError("invalid_input", f"onbekende node {node!r}")
    key = (node, live_remote)
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and not refresh and now < hit[0]:
            return hit[1]
    if node == local:
        res = discover_local()
    else:
        res = None
        note = None
        if live_remote:
            from . import remote
            from .controller import ControllerError
            try:
                res = remote.call(node, "runner2_repos", {"action": "discover"})
                if not isinstance(res, dict) or not isinstance(res.get("repos"), list):
                    res, note = None, "onverwacht antwoord"
            except ControllerError as e:
                note = ("deze Runner 2 kent de repositorykeuze nog niet" if e.code == "invalid_input" and "onbekende tool" in e.message.lower()
                        else e.message[:160] or e.code)
            except Exception as e:   # noqa: BLE001
                note = f"{type(e).__name__}"
        if res is None:
            res = known_for_node(node)
            res["note"] = note or "niet live gecontroleerd"
        else:
            _overlay_mappings(res, node)
        res["node"] = node
    with _cache_lock:
        _cache[key] = (now + CACHE_S, res)
    return res


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()


def catalog(refresh: bool = False) -> dict:
    """Alle nodes (parallel) plus de projectenlijst, voor de keuzelijst van Monitor 2."""
    from . import remote
    names = [local_name(), *[n for n in remote.nodes() if n in NODES and n != local_name()]]
    names = [n for n in NODES if n in names]

    def one(n):
        try:
            return n, discover(n, refresh=refresh)
        except Exception as e:   # noqa: BLE001
            return n, {"node": n, "checked": "error", "repos": [], "hidden": [], "note": f"{type(e).__name__}: {e}"[:200]}

    with ThreadPoolExecutor(max_workers=max(1, len(names))) as pool:
        nodes = dict(pool.map(one, names))
    cfg = load_projects()
    return {"nodes": nodes, "local": local_name(), "config_error": cfg["error"],
            "projects": [{"id": p["id"], "name": p["name"], "aliases": p["aliases"]} for p in cfg["projects"].values()]}


# --- beslissen ---

def _alias_re(alias: str) -> re.Pattern:
    words = [re.escape(w) for w in re.split(r"[\s_-]+", alias.strip().lower()) if w]
    return re.compile(r"(?<![a-z0-9])" + r"[\s_-]*".join(words) + r"(?![a-z0-9])")


def mentions(text: str, defs: dict | None = None) -> list[dict]:
    """De projecten die de tekst noemt, in volgorde van eerste vermelding: [{id, name, matched}]."""
    defs = defs if defs is not None else load_projects()["projects"]
    low = (text or "").lower()
    found = []
    for pid, p in defs.items():
        best = None
        for alias in p["aliases"]:
            if not alias.strip():
                continue
            m = _alias_re(alias).search(low)
            if m and (best is None or m.start() < best[0]):
                best = (m.start(), alias)
        if best:
            found.append((best[0], {"id": pid, "name": p["name"], "matched": best[1]}))
    return [f for _pos, f in sorted(found, key=lambda x: x[0])]


def _option(rec: dict, project_id: str | None = None) -> dict:
    comp = next((p["component"] for p in rec["projects"] if p["id"] == project_id), "")
    return {"node": rec["node"], "repository": rec["path"], "label": rec["label"], "name": rec["name"],
            "component": comp, "verified": rec["verified"]}


def resolve(text: str, node: str, choices: dict | None = None, live_remote: bool = True) -> dict:
    """Bepaalt de repository (of repositories) voor een opdracht op `node`. Gooit nooit; kijk naar `decision`:

    - `resolved`: `groups` = één of meer {node, repository, projects:[{id,name,component}], verified}; meer dan één groep = gesplitst.
    - `choice_needed`: `question` = {project, text, options:[{node, repository, label, ...}]} (één vraag tegelijk).
    - `unresolved`: er is geen herkenbaar project of geen kandidaat op deze node; `question.options` toont wat er op de node is.
    `choices` = {project_id: repository} uit een eerder beantwoorde vraag; alleen kandidaten van dat project zijn geldig.
    """
    node = (node or "").upper()
    cfg = load_projects()
    defs = cfg["projects"]
    found = mentions(text, defs)
    try:
        inv = discover(node, live_remote=live_remote)
    except ReposError as e:
        return {"decision": "unresolved", "node": node, "mentions": found, "message": e.message, "groups": []}
    recs = inv["repos"]
    base = {"node": node, "mentions": found, "checked": inv.get("checked"), "note": inv.get("note"), "groups": [],
            "warnings": []}
    if inv.get("checked") != "live":
        base["warnings"].append(f"{node} is niet live gecontroleerd ({inv.get('note') or 'alleen bekende repositories'}).")
    everything = [_option(r) for r in recs]
    if not found:
        return {**base, "decision": "unresolved",
                "message": "In de tekst staat geen herkenbaar project; kies een repository.",
                "question": {"project": None, "text": f"Welke repository op {node} hoort bij deze opdracht?", "options": everything}}
    choices = choices or {}
    picked: list[tuple[dict, dict]] = []
    for m in found:
        cands = [r for r in recs if any(p["id"] == m["id"] for p in r["projects"])]
        want = choices.get(m["id"])
        if want is not None:
            cands = [r for r in cands if r["path"] == want]
            if not cands:
                return {**base, "decision": "unresolved", "message": f"De gekozen repository is geen geldige kandidaat voor {m['name']} op {node}.",
                        "question": {"project": m["id"], "text": f"Welke repository hoort bij {m['name']}?",
                                     "options": [_option(r, m["id"]) for r in recs if any(p["id"] == m["id"] for p in r["projects"])]}}
        if len(cands) == 1:
            picked.append((m, cands[0]))
            continue
        if not cands:
            elsewhere = []
            for other in NODES:
                if other == node:
                    continue
                try:
                    o = discover(other, live_remote=False)
                except Exception:   # noqa: BLE001
                    continue
                elsewhere += [_option(r, m["id"]) for r in o["repos"] if any(p["id"] == m["id"] for p in r["projects"])]
            msg = f"{m['name']} is niet gevonden op {node}."
            if elsewhere:
                msg += " Wel op: " + ", ".join(sorted({e["node"] for e in elsewhere})) + " (kies die Runner)."
            return {**base, "decision": "unresolved", "message": msg, "elsewhere": elsewhere,
                    "question": {"project": m["id"], "text": f"Welke repository op {node} hoort bij {m['name']}?", "options": everything}}
        return {**base, "decision": "choice_needed",
                "message": f"Voor {m['name']} zijn meerdere repositories mogelijk op {node}.",
                "question": {"project": m["id"], "text": f"Welke repository hoort bij {m['name']} ({m['matched']})?",
                             "options": [_option(r, m["id"]) for r in cands]}}
    groups: dict[str, dict] = {}
    for m, rec in picked:
        g = groups.setdefault(rec["path"], {"node": node, "repository": rec["path"], "name": rec["name"], "label": rec["label"],
                                            "verified": rec["verified"], "projects": []})
        comp = next(p["component"] for p in rec["projects"] if p["id"] == m["id"])
        g["projects"].append({"id": m["id"], "name": m["name"], "component": comp, "matched": m["matched"]})
    out = list(groups.values())
    msg = (f"Alles hoort bij {out[0]['name']} op {node}." if len(out) == 1 else
           f"Gesplitst: {len(out)} repositories op {node} ({', '.join(g['name'] for g in out)}).")
    return {**base, "decision": "resolved", "groups": out, "split": len(out) > 1, "message": msg}


def explicit_warnings(text: str, node: str, repository: str) -> list[str]:
    """Bij een expliciet gekozen repository: noemt de tekst projecten die elders (zeker) in een andere repository staan?
    Alleen lokaal en uit eigen bestanden; nooit een verbinding naar een andere node."""
    if not mentions(text):
        return []
    try:
        res = resolve(text, node, live_remote=False)
    except Exception:   # noqa: BLE001
        return []
    if res.get("decision") != "resolved":
        return []
    chosen = repository.rstrip("/\\")
    out = []
    for g in res["groups"]:
        if g["repository"].rstrip("/\\") != chosen and not chosen.endswith(g["name"]):
            out.append("De tekst noemt " + ", ".join(p["name"] for p in g["projects"]) + f", dat in {g['name']} staat; "
                       "die repository is niet gekozen en wordt niet aangepast.")
    return out


def scope_header(group: dict, others: list[dict]) -> str:
    """Kop vóór de prompt van een automatisch bepaalde opdracht: wat dit stuk mag aanraken en wat apart gebeurt."""
    parts = []
    for p in group["projects"]:
        parts.append(p["name"] + (f" (map {p['component']})" if p["component"] else " (hele repository)"))
    lines = ["[Runner 2 · automatisch bepaalde repository]",
             f"Repository: {group['repository']} op {group['node']}.",
             "Onderdeel van deze opdracht: " + "; ".join(parts) + ".",
             "Werk uitsluitend aan dit onderdeel. Wijzig niets aan andere onderdelen, projecten of repositories."]
    if others:
        lines.append("Een ander deel van dezelfde vraag is een aparte opdracht in een andere repository ("
                     + "; ".join(f"{', '.join(p['name'] for p in g['projects'])} in {g['name']}" for g in others)
                     + "): raak dat niet aan.")
    lines += ["[Einde kop; hieronder de opdracht]", ""]
    return "\n".join(lines)
