"""Welke repositories het opdrachtformulier per Runner aanbiedt, en de controle vóór een opdracht naar Runner 2 gaat.

Runner 2 kent geen lijst met toegestane repositories: ``runner2_start`` controleert pas bij het starten op de doelnode
of de map bestaat (``repo_invalid``). Vroeger bood het formulier één vaste voorinstelling voor alle Runners aan
(``OBF_JOBS_REPOS``, standaard ``~/StudioOBF``) in een vrij tekstveld; die map bestaat niet op elke machine, en een
tikfout kwam pas uit als de opdracht mislukte. Deze module maakt per Runner een lijst die alleen bevat wat

* **toegestaan** is: een vermelding in ``OBF_JOBS_REPOS`` (``pad`` = elke Runner, ``RUNNER=pad`` = alleen die), of
  een repository waarin Runner 2 op die Runner al taken draaide (``OBF_JOBS_REPOS_HISTORY=0`` zet dat laatste uit);
* **beschikbaar** is: op de machine waar Monitor 2 zelf draait wordt de map live gecontroleerd (bestaat, is een
  git-repository, leesbaar); voor een andere Runner geldt alleen wat Runner 2 daar zelf bevestigde, namelijk de
  ``repository`` van een taak in zijn takenlijst (een taak bestaat pas als de worktree gemaakt kon worden). Meldt
  een latere opdracht ``repo_invalid`` voor die map, dan valt hij weer weg.

Wat gezien is, wordt bewaard in een klein JSON-bestand (``OBF_MONITOR2_REPOS_FILE``, anders naast het
meldingsjournaal), zodat de lijst een herstart van Monitor 2 en een tijdelijk onbereikbare Runner overleeft. Daarin
staat ook per Runner de laatst verzonden repository. Een opdracht (``add``) gaat alleen door als de repository
letterlijk in de lijst van de gekozen Runner staat.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

log = logging.getLogger("obf_monitor2")

REPOS_ENV = "OBF_JOBS_REPOS"
HISTORY_ENV = "OBF_JOBS_REPOS_HISTORY"
FILE_ENV = "OBF_MONITOR2_REPOS_FILE"
JOURNAL_ENV = "OBF_MONITOR2_NOTIFY_JOURNAL"
DEFAULT_REPOS = "~/StudioOBF"
FILE_NAME = "jobs-repos.json"
NODES = ("MAC", "GERARD", "LINUX", "WINDOWS")
# Alleen voor de meldteksten: de NUC heet op de pagina "STUDIO"; de node-ID blijft LINUX (opslag, API, Runner 2).
SHOWN_NAME = {"LINUX": "STUDIO"}

MAX_REPO = 300                      # zelfde grens als Runner 2 (``jobs.MAX_REPO``)
MAX_SEEN_PER_NODE = 40
#: Zo lang blijft een door Runner 2 bevestigde repository in de lijst zonder nieuwe bevestiging.
MAX_AGE_S = 90 * 86400
#: Een al bekende bevestiging wordt hoogstens zo vaak opnieuw weggeschreven (de pagina pollt elke 4 s).
REFRESH_WRITE_S = 3600

#: Een worktree van Runner 2 is geen bronrepository voor een opdracht.
_NOT_A_SOURCE = re.compile(r"[\\/]\.runner2[\\/]worktrees([\\/]|$)")
#: Een thuismap aan het begin van een absoluut pad: zo wordt ``~/StudioOBF`` herkend in ``/Users/naam/StudioOBF``.
_HOME_PREFIX = re.compile(r"^(?:/Users/[^/]+|/home/[^/]+|/root|[A-Za-z]:[\\/]Users[\\/][^\\/]+)[\\/](.+)$")


class DiscoveryFeed:
    """De ontdekte repositories van Runner 2 (``runner2_repos`` catalog), 60 s gecachet en op de achtergrond ververst.

    ``fetch()`` geeft de catalogus of ``None`` (Runner 2 kent het niet, of is niet bereikbaar). Een lezing kost ssh naar
    de andere nodes; daarom wacht een pagina-aanvraag er nooit op: ``current()`` geeft de laatste stand en start zo nodig
    een verversing. Een mislukte lezing wordt na ``retry`` seconden opnieuw geprobeerd en wist de laatste goede stand niet.
    """

    def __init__(self, fetch: Callable[[], dict | None], *, ttl: float = 60.0, retry: float = 15.0,
                 clock: Callable[[], float] = time.monotonic, background: bool = True):
        self._fetch, self._ttl, self._retry, self._clock, self._background = fetch, ttl, retry, clock, background
        self._lock = threading.Lock()
        self._data: dict | None = None
        self._due = 0.0
        self._running = False
        self._started = False

    def current(self) -> dict | None:
        with self._lock:
            stale = not self._running and (not self._started or self._clock() >= self._due)
            if stale:
                self._running = True
        if stale:
            if self._background:
                threading.Thread(target=self._refresh, name="repo-discovery", daemon=True).start()
            else:
                self._refresh()
        with self._lock:
            return self._data

    def _refresh(self) -> None:
        try:
            data = self._fetch()
        except Exception as exc:  # noqa: BLE001 -- ontdekken mag de pagina nooit breken
            log.info("repositories ontdekken mislukt: %s", exc.__class__.__name__)
            data = None
        with self._lock:
            self._started, self._running = True, False
            if isinstance(data, dict) and isinstance(data.get("nodes"), dict):
                self._data, self._due = data, self._clock() + self._ttl
            else:
                self._due = self._clock() + self._retry


def parse_entries(raw: str | None) -> list[tuple[str | None, str]]:
    """``OBF_JOBS_REPOS`` naar ``[(RUNNER of None, pad)]``. ``RUNNER=pad`` geldt alleen voor die Runner."""
    out: list[tuple[str | None, str]] = []
    for item in (raw if raw is not None else DEFAULT_REPOS).split(","):
        item = item.strip()
        node, sep, rest = item.partition("=")
        if sep and node.strip().upper() in NODES:
            entry = (node.strip().upper(), rest.strip())
        else:
            entry = (None, item)
        if entry[1] and len(entry[1]) <= MAX_REPO and entry not in out:
            out.append(entry)
    return out


def local_repo_ok(path: str) -> str | None:
    """``None`` als ``path`` op deze machine een bruikbare git-repository is, anders de reden in gewone taal."""
    try:
        p = Path(path).expanduser()
        if not p.is_dir():
            return "bestaat niet"
        if not (p / ".git").exists():
            return "is geen git-repository"
        if not os.access(p, os.R_OK | os.X_OK):
            return "is niet toegankelijk"
    except (OSError, RuntimeError):
        return "is niet toegankelijk"
    return None


def _source_path(path: object) -> str | None:
    """Een repository uit de takenlijst van Runner 2, als die als keuze in aanmerking komt."""
    if not isinstance(path, str):
        return None
    path = path.strip()
    absolute = path.startswith("/") or re.match(r"^[A-Za-z]:[\\/]", path)
    if not path or len(path) > MAX_REPO or not absolute or _NOT_A_SOURCE.search(path) or "\n" in path:
        return None
    return path


def _epoch(text: object) -> float | None:
    """Runner-2-tijd (``2026-10-14T06:33:03+0000``) naar seconden; onleesbaar = ``None``."""
    if not isinstance(text, str):
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S%z").timestamp()
    except ValueError:
        return None


def default_path(env: dict | None = None) -> Path | None:
    env = env if env is not None else os.environ
    if env.get(FILE_ENV):
        return Path(env[FILE_ENV]).expanduser()
    journal = env.get(JOURNAL_ENV)
    if journal:
        return Path(journal).expanduser().parent / FILE_NAME
    return None


class RepoCatalog:
    """De repositorykeuze per Runner (zie de modulekop). Draadveilig; alle tijden in seconden sinds 1970."""

    def __init__(self, entries: Iterable[tuple[str | None, str]], *, local_node: str | None, path: Path | None = None,
                 history: bool = True, clock: Callable[[], float] = time.time,
                 check_local: Callable[[str], str | None] = local_repo_ok):
        self.entries = list(entries)
        self.local_node = (local_node or "").upper() or None
        self.history = history
        self._path, self._clock, self._check_local = path, clock, check_local
        self._lock = threading.Lock()
        self._data = self._load()
        self._found: dict | None = None      # ``runner2_repos`` catalog (alleen in het geheugen; Runner 2 is de bron)

    @classmethod
    def from_env(cls, *, local_node: str | None, env: dict | None = None, **kwargs) -> "RepoCatalog":
        env = env if env is not None else os.environ
        path = kwargs.pop("path", None) or default_path(env)
        if path is None:
            log.warning("%s staat niet gezet: de bevestigde repositories blijven alleen in het geheugen", FILE_ENV)
        history = (env.get(HISTORY_ENV) or "1").strip().lower() not in ("0", "nee", "uit", "false", "no", "off")
        return cls(parse_entries(env.get(REPOS_ENV)), local_node=local_node, path=path, history=history, **kwargs)

    def configured(self) -> list[str]:
        """De voorinstellingen zoals ingesteld (alle Runners samen), voor ``meta.repos``."""
        out: list[str] = []
        for _node, path in self.entries:
            if path not in out:
                out.append(path)
        return out

    # -- opslag

    def _blank(self) -> dict:
        return {"version": 1, "nodes": {}}

    def _load(self) -> dict:
        if self._path is None:
            return self._blank()
        try:
            data = json.loads(self._path.read_text("utf-8"))
        except FileNotFoundError:
            return self._blank()
        except (OSError, ValueError) as exc:
            log.warning("repositorybestand %s onleesbaar (%s); begint leeg", self._path, exc.__class__.__name__)
            return self._blank()
        out = self._blank()
        nodes = data.get("nodes") if isinstance(data, dict) else None
        for node, info in (nodes or {}).items():
            if node not in NODES or not isinstance(info, dict):
                continue
            entry = self._node(out, node)
            for key in ("seen", "invalid"):
                for repo, at in (info.get(key) or {}).items():
                    if isinstance(repo, str) and isinstance(at, (int, float)) and len(repo) <= MAX_REPO:
                        entry[key][repo] = float(at)
            if isinstance(info.get("last"), str) and len(info["last"]) <= MAX_REPO:
                entry["last"] = info["last"]
        return out

    def _save(self) -> None:
        if self._path is None:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=".jobs-repos-", dir=str(self._path.parent))
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self._data, handle, ensure_ascii=False, indent=1, sort_keys=True)
            os.replace(tmp, self._path)
        except OSError as exc:
            log.warning("repositorybestand %s niet bijgewerkt: %s", self._path, exc)

    @staticmethod
    def _node(data: dict, node: str) -> dict:
        return data["nodes"].setdefault(node, {"seen": {}, "invalid": {}, "last": None})

    # -- wat Runner 2 en de wachtrij laten zien

    def observe(self, node: str, repos: Iterable[object]) -> None:
        """De repositories van taken die Runner 2 op ``node`` heeft (dus bestonden toen de taak begon)."""
        node = (node or "").upper()
        if node not in NODES:
            return
        now = self._clock()
        changed = False
        with self._lock:
            entry = self._node(self._data, node)
            for repo in repos:
                path = _source_path(repo)
                if path is None:
                    continue
                before = entry["seen"].get(path)
                if before is None or now - before >= REFRESH_WRITE_S:
                    entry["seen"][path] = now
                    changed = True
            changed |= self._prune(entry, now)
            if changed:
                self._save()

    def note_failures(self, overview: dict | None) -> None:
        """Opdrachten die met ``repo_invalid`` mislukten: die map is op die Runner (nu) niet bruikbaar."""
        nodes = overview.get("nodes") if isinstance(overview, dict) else None
        if not isinstance(nodes, dict):
            return
        changed = False
        with self._lock:
            for node, column in nodes.items():
                if node not in NODES or not isinstance(column, dict):
                    continue
                for job in [*(column.get("active") or []), *(column.get("queue") or []), *(column.get("finished") or [])]:
                    err = job.get("last_error") if isinstance(job, dict) else None
                    if not isinstance(err, dict) or err.get("code") != "repo_invalid" or not isinstance(job.get("repo"), str):
                        continue
                    at = _epoch(err.get("at")) or self._clock()
                    entry = self._node(self._data, node)
                    if entry["invalid"].get(job["repo"], 0) < at:
                        entry["invalid"][job["repo"]] = at
                        changed = True
            if changed:
                self._save()

    def note_used(self, node: str, repo: str) -> None:
        """Na een gelukte opdracht: de laatst gekozen repository van die Runner (ook voor andere browsers)."""
        node = (node or "").upper()
        if node not in NODES:
            return
        with self._lock:
            entry = self._node(self._data, node)
            if entry["last"] != repo:
                entry["last"] = repo
                self._save()

    def _prune(self, entry: dict, now: float) -> bool:
        changed = False
        for key in ("seen", "invalid"):
            for repo, at in list(entry[key].items()):
                if now - at > MAX_AGE_S:
                    del entry[key][repo]
                    changed = True
        if len(entry["seen"]) > MAX_SEEN_PER_NODE:
            for repo, _at in sorted(entry["seen"].items(), key=lambda kv: kv[1])[:len(entry["seen"]) - MAX_SEEN_PER_NODE]:
                del entry["seen"][repo]
            changed = True
        return changed

    def set_discovered(self, catalog: dict | None) -> None:
        """De laatste catalogus van Runner 2 (of ``None`` = onbekend). Zet "Automatisch bepalen" aan zodra er een is."""
        with self._lock:
            self._found = catalog if isinstance(catalog, dict) and isinstance(catalog.get("nodes"), dict) else None

    def auto_available(self) -> bool:
        with self._lock:
            return self._found is not None

    def projects(self) -> list[dict]:
        with self._lock:
            found = self._found
        out = []
        for item in (found or {}).get("projects") or []:
            if isinstance(item, dict) and isinstance(item.get("id"), str) and isinstance(item.get("name"), str):
                out.append({"id": item["id"], "name": item["name"][:80]})
        return out

    def _discovered(self, node: str) -> tuple[list[dict], str | None]:
        """De door Runner 2 gevonden repositories van ``node`` (genormaliseerd) en hoe ze gecontroleerd zijn."""
        with self._lock:
            found = self._found
        info = ((found or {}).get("nodes") or {}).get(node)
        if not isinstance(info, dict):
            return [], None
        out = []
        for item in info.get("repos") or []:
            path = _source_path(item.get("path")) if isinstance(item, dict) else None
            if path is None:
                continue
            projects = [{"id": str(p.get("id"))[:41], "name": str(p.get("name"))[:80], "component": str(p.get("component") or "")[:200]}
                        for p in (item.get("projects") or []) if isinstance(p, dict) and p.get("id") and p.get("name")][:6]
            out.append({"path": path, "label": str(item.get("label") or "")[:200], "projects": projects,
                        "verified": item.get("verified") is True, "sources": [str(x) for x in (item.get("sources") or [])][:6]})
        return out, str(info.get("checked") or "")

    # -- de keuze

    def choices(self, node: str) -> dict:
        """``{"repos": [{path, source[, label, projects]}], "hidden": [{path, why}], "last": pad of None,
        "checked": local|runner2|none, "auto": bool}``; ``auto`` = Runner 2 kan de repository uit de tekst bepalen."""
        node = (node or "").upper()
        with self._lock:
            entry = self._node(self._data, node) if node in NODES else {"seen": {}, "invalid": {}, "last": None}
            seen, invalid, last = dict(entry["seen"]), dict(entry["invalid"]), entry["last"]
        now = self._clock()
        seen = {repo: at for repo, at in seen.items() if now - at <= MAX_AGE_S}
        local = node == self.local_node
        windows = node == "WINDOWS"
        name = SHOWN_NAME.get(node, node)
        candidates: list[tuple[str, str]] = [(path, "instelling") for want, path in self.entries if want in (None, node)]
        if self.history:
            candidates += [(path, "eerder gebruikt") for path, _at in sorted(seen.items(), key=lambda kv: -kv[1])]
        found, how = self._discovered(node) if node in NODES else ([], None)
        meta: dict[str, dict] = {}
        for item in found:
            if item["verified"]:
                candidates.append((item["path"], "gevonden"))
                meta[item["path"]] = item
        repos: list[dict] = []
        hidden: list[dict] = []
        taken: set[str] = set()
        for path, source in candidates:
            if local:
                why = self._check_local(path)
                why = f"{why} op {name}" if why else None
                key = str(Path(path).expanduser().resolve()) if why is None else path
            elif source == "gevonden":   # door Runner 2 zelf live geïnspecteerd op die node
                why, key = None, path
                if invalid.get(path, 0) > now - 3600:
                    why = f"Runner 2 op {name} meldde dat deze map niet bestaat"
            else:
                confirmed = self._confirmed(path, seen, windows)
                why = None if confirmed else f"nog niet bevestigd door Runner 2 op {name}"
                key = confirmed[0] if confirmed else path
                if confirmed and invalid.get(path, 0) > confirmed[1]:
                    why = f"Runner 2 op {name} meldde dat deze map niet bestaat"
            key = key.lower() if windows else key
            if key in taken:
                continue
            taken.add(key)
            if why is None:
                shown = {"path": path, "source": source}
                if path in meta:
                    shown.update({"label": meta[path]["label"], "projects": meta[path]["projects"]})
                repos.append(shown)
            elif source in ("instelling", "gevonden"):
                hidden.append({"path": path, "why": why})
        for item in found:   # door Runner 2 niet live te controleren (geen route, oudere versie): niet aanbieden, wel uitleggen
            if not item["verified"] and item["path"] not in taken and "afspraak" in item["sources"]:
                hidden.append({"path": item["path"], "why": f"nog niet gecontroleerd door Runner 2 op {name}"})
        paths = [item["path"] for item in repos]
        return {"repos": repos, "hidden": hidden, "last": last if last in paths else None,
                "checked": "local" if local else "runner2" if (seen or found) else "none",
                "auto": self.auto_available()}

    @staticmethod
    def _confirmed(path: str, seen: dict[str, float], windows: bool) -> tuple[str, float] | None:
        """De door Runner 2 geziene map die ``path`` aanwijst (met moment), of ``None``."""
        fold = (lambda s: s.lower()) if windows else (lambda s: s)
        for other, at in seen.items():
            if fold(other) == fold(path):
                return other, at
        if path.startswith("~/") or path.startswith("~\\"):
            want = fold(path[2:].replace("\\", "/").strip("/"))
            for other, at in seen.items():
                match = _HOME_PREFIX.match(other)
                if match and fold(match.group(1).replace("\\", "/").strip("/")) == want:
                    return other, at
        return None

    def all_choices(self) -> dict[str, dict]:
        return {node: self.choices(node) for node in NODES}

    def check(self, node: str, repo: str) -> str | None:
        """``None`` als ``repo`` voor ``node`` gekozen mag worden, anders de melding voor de pagina."""
        node = (node or "").upper()
        if node not in NODES:
            return "Kies een Runner."
        if not isinstance(repo, str) or not repo.strip():
            return "Kies een repository."
        info = self.choices(node)
        if repo in [item["path"] for item in info["repos"]]:
            return None
        hidden = next((item for item in info["hidden"] if item["path"] == repo), None)
        if hidden is not None:
            return f"Repository {repo} is niet bruikbaar voor {SHOWN_NAME.get(node, node)}: {hidden['why']}. Er is niets verstuurd."
        return f"Repository {repo} staat niet in de lijst van {SHOWN_NAME.get(node, node)}. Kies een repository uit de lijst; er is niets verstuurd."
