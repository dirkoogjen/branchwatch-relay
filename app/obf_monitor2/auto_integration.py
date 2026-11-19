"""Automatische integratie naar lokale ``main``: een eenmalige instelling per repository in plaats van een menselijke goedkeuring per integratie.

Besluit van de gebruiker: voor *lokale* integraties geeft hij altijd toestemming; Claude werkt, test en integreert autonoom.
Deze module is die instelling plus de controller die er uitvoering aan geeft. Wat dat **niet** doet: pushen, deployen,
publiceren, productiedata wijzigen of iets anders dan de lokale ``main`` van een uitdrukkelijk genoemde repository
verplaatsen. Dat blijft afzonderlijk verboden (``integration.py`` kent geen push; dit bestand ook niet).

Hoe het werkt
-------------
1. **Instelling** (mens, één keer per repository, aparte stap): een beleidsbestand ``auto-policy.json`` plus de
   sha256 daarvan in de configuratie van het Monitor 2-proces (``OBF_INTEGRATION_AUTO_POLICY`` en
   ``OBF_INTEGRATION_AUTO_POLICY_SHA256``). Wijkt het bestand af van de vastgezette hash, of is het door anderen
   schrijfbaar, dan is automatische integratie uit (fail-closed). Een repository moet ook in ``OBF_INTEGRATION_REPOS`` staan.
2. **Aanvraag** (Claude, in de sandbox): hetzelfde ``R-<12 hex>.json`` als bij de handmatige route. Een aanvraag is hier een
   *trigger* en geeft geen enkel recht; de controller gelooft er niets van en leidt alles zelf af.
3. **Validatie** (controller): het Runner 2-record van de genoemde taak (en keten) moet *expliciet afgerond* zijn
   (``runner_finish`` ontvangen, taak ``completed``, proces weg, rustperiode verstreken, branch en repository kloppen,
   doelcommit is de eindcommit van de schakel en niet later gemaakt dan de afronding). Daarna de git-controles: hoofdcheckout
   op ``main``, schone getrackte werkboom, geen git-handeling bezig, fast-forward, geen untracked botsingen, geen
   beschermde paden, geen symlinks of submodules, omvang binnen de grens.
4. **Uitvoering** (controller, ``integration.execute``): toestemming ``auto`` ondertekend met de sleutel die alleen in het geheugen
   van dit proces leeft, de gerichte tests op een export van exact de doelcommit **vóór** main verandert, opnieuw de
   git-controles, herstelbranch, ``git merge --ff-only`` van die sha, nacontrole.

Bij conflicten, afwijkende hoofdbranch, beschermde bestanden, falende tests of enige twijfel wordt **niets geforceerd**: de aanvraag
blijft liggen (of wordt als geweigerd gemarkeerd) met een duidelijke open actie in het paneel, de audittrail en het
ontvangstbewijs; de handmatige route (aanvraag -> goedkeuring door een mens) blijft voor die aanvraag beschikbaar.

Wat Claude vanuit de sandbox niet kan: de sleutel lezen (alleen geheugen), de toestemmingenmap schrijven (staat dicht), het beleid
wijzigen (hashvastzetting in de configuratie), de merge zelf uitvoeren (alleen het Monitor 2-proces; er is geen ``run`` in de
opdrachtregel). De eerlijke rest staat in ``docs/monitor2-integratie.md`` (hoofdstuk Automatische integratie).
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import hashlib
import json
import os
import re
import stat
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from . import human_approval as ha
from . import integration as ig
from . import test_budget as tb

POLICY_ENV = "OBF_INTEGRATION_AUTO_POLICY"
POLICY_SHA_ENV = "OBF_INTEGRATION_AUTO_POLICY_SHA256"
R2_HOME_ENV = "OBF_INTEGRATION_R2_HOME"
AUTO_ACTOR = "auto-integratie"
SCHEMA = 1
SCOPES = ("chain", "task")
POLL_S = 10.0
GRANT_TTL_S = 30 * 60
MAX_POLICY_BYTES = 16 * 1024
MAX_RECORD_BYTES = 4 * 1024 * 1024
TIP_SLACK_S = 120
ATTENTION_KEEP = 20
PAUSE_FILE = "auto.paused"
MAX_ATTEMPTS = 3
ACTION_GRACE_S = 15 * 60   # een wachtreden die een mens kan oplossen wordt pas daarna een Runner 2-actiepunt (vaak lost hij zich op)

TASK_ID = re.compile(r"^R2-[0-9a-f]{12}$")
CHAIN_ID = re.compile(r"^C-[0-9a-f]{12}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
#: Paden die een automatische integratie nooit zelf mag binnenbrengen (de mens doet dat handmatig, zie ``allow_protected``).
#: Een beleid kan hier patronen aan toevoegen (``extra_protected``), niets van afhalen. Vergelijking op kleine letters.
DEFAULT_PROTECTED = (".gitmodules", ".gitattributes", ".github/*", ".githooks/*", ".husky/*", "*.plist", "*.plist.*", "*launchagent*",
                     "*.service", "scripts/monitor2-ctl", ".env", ".env.*", "*.pem", "*.key", "*secret*", "*credential*",
                     "runner2/*", "deploy/*")

_POLICY_KEYS = {"scope", "tests", "max_commits", "max_files", "extra_protected", "settle_s", "request_ttl_s", "require_test_wrapper"}


# ----------------------------------------------------------------------
# De instelling per repository
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RepoPolicy:
    repo: str
    scope: str = "chain"            # chain: bij een keten pas als de keten klaar is en alleen de laatste schakel; task: elke afgeronde taak
    tests: tuple = ()               # vaste gerichte tests (door de mens gekozen); de tests uit de aanvraag komen erbij
    max_commits: int = 50
    max_files: int = 300
    extra_protected: tuple = ()
    settle_s: int = 30              # rustperiode na de afronding van de taak
    request_ttl_s: int = 6 * 3600   # hoe lang een aanvraag mag wachten op een voorwaarde (schone werkboom, einde keten, ...)
    require_test_wrapper: bool = False   # alleen aan als ook OBF_INTEGRATION_TEST_WRAPPER (OS-sandbox voor de tests) is ingesteld

    def public(self) -> dict:
        return {"repo": self.repo, "scope": self.scope, "tests": list(self.tests), "max_commits": self.max_commits,
                "max_files": self.max_files, "extra_protected": list(self.extra_protected), "settle_s": self.settle_s,
                "request_ttl_s": self.request_ttl_s, "require_test_wrapper": self.require_test_wrapper}


@dataclass
class AutoPolicy:
    configured: bool = False        # er is een beleid opgegeven
    enabled: bool = False           # en het is geldig, vastgezet en niet te ruim schrijfbaar
    problem: str = ""
    sha256: str = ""
    path: str = ""
    repos: dict = field(default_factory=dict)   # echt pad -> RepoPolicy

    def for_repo(self, repo: str) -> RepoPolicy | None:
        return self.repos.get(os.path.realpath(os.path.expanduser(repo))) if self.enabled else None

    def public(self) -> dict:
        return {"configured": self.configured, "enabled": self.enabled, "problem": self.problem, "policy_sha256": self.sha256,
                "repos": [p.public() for p in self.repos.values()] if self.enabled else []}


def _int(value, low: int, high: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f"{name} moet een geheel getal van {low} tot {high} zijn")
    return value


def _repo_policy(repo: str, raw: dict) -> RepoPolicy:
    if not isinstance(raw, dict) or set(raw) - _POLICY_KEYS:
        raise ValueError(f"onbekende of ongeldige velden voor {repo}")
    scope = raw.get("scope", "chain")
    if scope not in SCOPES:
        raise ValueError("scope moet chain of task zijn")
    tests = raw.get("tests", [])
    if not isinstance(tests, list) or len(tests) > ig.MAX_TESTS \
            or any(not isinstance(t, str) or not ig.TEST_PATH.match(t) or ".." in t.split("/") for t in tests):
        raise ValueError("tests moet een lijst van tests/....py|.mjs zijn")
    extra = raw.get("extra_protected", [])
    if not isinstance(extra, list) or len(extra) > 30 or any(not isinstance(e, str) or not e or len(e) > 100 for e in extra):
        raise ValueError("extra_protected moet een lijst korte patronen zijn")
    wrapper = raw.get("require_test_wrapper", False)
    if not isinstance(wrapper, bool):
        raise ValueError("require_test_wrapper moet true of false zijn")
    return RepoPolicy(repo=repo, scope=scope, require_test_wrapper=wrapper, tests=tuple(dict.fromkeys(tests)),
                      max_commits=_int(raw.get("max_commits", 50), 1, 500, "max_commits"),
                      max_files=_int(raw.get("max_files", 300), 1, 5000, "max_files"),
                      extra_protected=tuple(e.lower() for e in extra),
                      settle_s=_int(raw.get("settle_s", 30), 0, 3600, "settle_s"),
                      request_ttl_s=_int(raw.get("request_ttl_s", 6 * 3600), 300, 7 * 24 * 3600, "request_ttl_s"))


def parse_policy(raw: bytes) -> dict[str, RepoPolicy]:
    """Strikt: onbekende velden of typen maken het hele beleid ongeldig (liever uit dan half aan)."""
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict) or set(data) != {"schema", "repos"} or data["schema"] != SCHEMA \
            or not isinstance(data["repos"], dict) or not data["repos"]:
        raise ValueError("beleidsbestand heeft niet de verwachte vorm (schema, repos)")
    out = {}
    for path, item in data["repos"].items():
        real = os.path.realpath(os.path.expanduser(str(path)))
        out[real] = _repo_policy(real, item)
    return out


def load_policy(env: dict | None = None, allowed: list[str] | None = None) -> AutoPolicy:
    """Leest het beleid en controleert het tegen de vastgezette hash. Nooit een uitzondering: een fout is ``enabled=False``."""
    env = env if env is not None else os.environ
    path = (env.get(POLICY_ENV) or "").strip()
    pinned = (env.get(POLICY_SHA_ENV) or "").strip().lower()
    if not path and not pinned:
        return AutoPolicy()
    out = AutoPolicy(configured=True, path=path, sha256=pinned)
    if not path or not SHA256.match(pinned):
        out.problem = f"{POLICY_ENV} en {POLICY_SHA_ENV} (64 hex tekens) horen samen ingesteld te zijn"
        return out
    full = Path(path).expanduser()
    try:
        info = os.lstat(full)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_POLICY_BYTES:
            out.problem = "het beleidsbestand is geen gewoon, klein bestand"
            return out
        if info.st_mode & 0o022:
            out.problem = "het beleidsbestand is door anderen schrijfbaar; chmod 600"
            return out
        raw = full.read_bytes()
    except OSError:
        out.problem = "het beleidsbestand is niet te lezen"
        return out
    if hashlib.sha256(raw).hexdigest() != pinned:
        out.problem = "het beleidsbestand wijkt af van de vastgezette hash; automatische integratie staat uit tot een mens het opnieuw vastzet"
        return out
    try:
        repos = parse_policy(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        out.problem = f"ongeldig beleidsbestand: {exc}"
        return out
    if allowed is not None:
        missing = [r for r in repos if r not in allowed]
        if missing:
            out.problem = f"{missing[0]} staat niet in OBF_INTEGRATION_REPOS"
            return out
    real_file = os.path.realpath(full)
    inbox = os.path.realpath(ig.inbox_dir(env))
    for guarded in [*repos, inbox]:
        if real_file == guarded or real_file.startswith(guarded + os.sep):
            out.problem = "het beleidsbestand ligt in een repository of in de inbox; zet het in de configuratiemap van Monitor 2"
            return out
    if any(r.require_test_wrapper for r in repos.values()) and not ig.test_wrapper(env):
        out.problem = f"het beleid eist een testomhulling ({ig.TEST_WRAPPER_ENV}) maar die is niet ingesteld"
        return out
    out.repos, out.enabled = repos, True
    return out


def policy_file_text(repos: dict[str, dict]) -> str:
    """Canonieke tekst voor het beleidsbestand (voor de opdrachtregel ``auto-policy``)."""
    parse_policy(json.dumps({"schema": SCHEMA, "repos": repos}).encode("utf-8"))   # valideert
    return json.dumps({"schema": SCHEMA, "repos": repos}, indent=1, sort_keys=True, ensure_ascii=False) + "\n"


# ----------------------------------------------------------------------
# De Runner 2-administratie (alleen lezen)
# ----------------------------------------------------------------------


def r2_home(env: dict | None = None) -> Path:
    env = env if env is not None else os.environ
    return Path(env.get(R2_HOME_ENV) or env.get("R2_HOME") or Path.home() / ".runner2").expanduser()


class R2Records:
    """Taak- en ketenrecords van Runner 2 (``tasks/<id>/state.json``, ``chains/<id>/chain.json``). Alleen lezen, geen symlinks."""

    def __init__(self, home: Path):
        self.home = Path(home)

    def _load(self, path: Path) -> dict | None:
        try:
            info = os.lstat(path)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_RECORD_BYTES:
                return None
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def task(self, task_id: str) -> dict | None:
        return self._load(self.home / "tasks" / task_id / "state.json") if TASK_ID.match(task_id or "") else None

    def chain(self, chain_id: str) -> dict | None:
        return self._load(self.home / "chains" / chain_id / "chain.json") if CHAIN_ID.match(chain_id or "") else None


def _epoch(text) -> float | None:
    if not isinstance(text, str):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            value = dt.datetime.strptime(text, fmt)
            return (value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)).timestamp()
        except ValueError:
            continue
    return None


def _alive(pid) -> bool:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


# ----------------------------------------------------------------------
# Beoordeling van één aanvraag
# ----------------------------------------------------------------------


class Refusal(Exception):
    """Een reden waarom een aanvraag (nog) niet automatisch geïntegreerd wordt.

    ``retry``: de voorwaarde kan vanzelf of door een mens veranderen; de aanvraag blijft liggen en wordt opnieuw bekeken.
    ``attention``: een mens moet iets doen (open actie in het paneel). ``info``: geen alarm (bv. al geïntegreerd)."""

    def __init__(self, code: str, text: str, *, retry: bool = False, attention: bool = True, info: bool = False):
        super().__init__(text)
        self.code, self.text, self.retry, self.attention, self.info = code, text, retry, attention, info


def _later(code: str, text: str, attention: bool = False) -> Refusal:
    return Refusal(code, text, retry=True, attention=attention)


def _protected_hits(files: list[str], policy: RepoPolicy) -> list[str]:
    patterns = tuple(ig.PROTECTED_PATHS) + DEFAULT_PROTECTED + policy.extra_protected
    hits = []
    for path in files:
        low = path.lower()
        if path in ig.PROTECTED_PATHS or any(fnmatch.fnmatchcase(low, pattern.lower()) for pattern in patterns):
            hits.append(path)
    return hits


def _special_entries(repo: str, base: str, tip: str) -> list[str]:
    """Paden die de integratie als symlink (120000) of submodule (160000) toevoegt of wijzigt."""
    out = ig._git(repo, "diff", "--raw", "-z", "--no-renames", base, tip).stdout.split("\0")
    hits = []
    for i in range(0, len(out) - 1, 2):
        meta = out[i].split()
        if len(meta) >= 2 and (meta[1] in ("120000", "160000") or meta[0] in (":120000", ":160000")):
            hits.append(out[i + 1])
    return hits


def validate_finished(request: dict, policy: RepoPolicy, r2: R2Records, now: float) -> dict:
    """Controleert dat de taak (en de keten) *expliciet afgerond* is en bij deze aanvraag hoort. Raised ``Refusal``.

    Geeft de feiten (``task``, ``chain``, ``finished_at``) voor de audittrail."""
    task_id = request["task_id"]
    if not TASK_ID.match(task_id or ""):
        raise Refusal("no_task", "de aanvraag noemt geen Runner 2-taak (R2_TASK_ID ontbreekt); deze aanvraag kan niet automatisch "
                      "worden geïntegreerd en wacht op handmatige goedkeuring in Monitor 2")
    rec = r2.task(task_id)
    if rec is None or rec.get("task_id") != task_id:
        raise Refusal("task_unknown", f"taak {task_id} is niet te vinden in de administratie van Runner 2")
    if os.path.realpath(os.path.expanduser(str(rec.get("repo") or ""))) != policy.repo:
        raise Refusal("repo_mismatch", "de taak hoort bij een andere repository dan de aanvraag")
    if rec.get("branch") != request["source_branch"] or request["source_branch"] != "r2/" + task_id:
        raise Refusal("branch_mismatch", f"de aanvraag gaat niet over de taakbranch r2/{task_id}")
    status = rec.get("status")
    if status in ("stopped", "failed", "cancelled", "canceled", "aborted"):
        raise Refusal("task_not_completed", f"de taak is niet afgerond (status {status}); er wordt niets geïntegreerd")
    if status != "completed":
        raise _later("task_not_finished", f"de taak is nog bezig (status {status})")
    finish = rec.get("finish")
    if not isinstance(finish, dict) or not str(finish.get("summary") or "").strip():
        raise Refusal("no_finish", "de taak is niet met runner_finish afgesloten; zonder uitdrukkelijke afronding wordt niets geïntegreerd")
    if request["id"] not in json.dumps(finish, ensure_ascii=False):
        raise Refusal("request_not_in_finish", f"de afronding van de taak noemt aanvraag {request['id']} niet; zonder die uitdrukkelijke "
                      "verwijzing wordt niet automatisch geïntegreerd (een mens kan het handmatig goedkeuren)")
    if rec.get("failure"):
        raise Refusal("task_failed", "de taak meldt een fout; er wordt niets geïntegreerd")
    if rec.get("question") or any(_alive(rec.get(key)) for key in ("pid", "claude_pid")) or rec.get("in_flight"):
        raise _later("task_not_finished", "de taak heeft nog een lopend proces of een openstaande vraag")
    finished_at = _epoch(rec.get("updated"))
    if finished_at is None:
        raise Refusal("no_finish_time", "het afrondingstijdstip van de taak is niet te lezen")
    if now - finished_at < policy.settle_s:
        raise _later("settling", f"de taak is net afgerond; het wacht nog {int(policy.settle_s - (now - finished_at)) + 1} s rust af")
    facts = {"task": task_id, "finished_at": rec.get("updated"), "chain": None, "chain_status": finish.get("chain_status"),
             "open_actions": len(finish.get("open_actions") or [])}
    chain_id = rec.get("chain_id")
    if chain_id:
        chain = r2.chain(chain_id)
        if chain is None or chain.get("chain_id") != chain_id:
            raise Refusal("chain_unknown", f"keten {chain_id} is niet te vinden in de administratie van Runner 2")
        if os.path.realpath(os.path.expanduser(str(chain.get("repo") or ""))) != policy.repo:
            raise Refusal("repo_mismatch", "de keten hoort bij een andere repository dan de aanvraag")
        links = [link for link in chain.get("links") or [] if isinstance(link, dict)]
        link = next((x for x in links if x.get("task_id") == task_id), None)
        if link is None:
            raise Refusal("chain_mismatch", "de taak komt niet voor in de genoemde keten")
        state, stop = chain.get("state"), (chain.get("stop") or {}).get("kind")
        facts["chain"] = {"id": chain_id, "state": state, "stop": stop, "link": link.get("link"), "links": len(links)}
        verdict = link.get("chain_status") or finish.get("chain_status")
        needs_complete = policy.scope == "chain" or verdict == "goal_reached"
        if needs_complete:
            if state == "active":
                raise _later("chain_not_complete", "de keten is nog niet afgerond (voltooiingscontrole of vervolgschakel loopt nog)")
            if state != "completed" or stop != "goal_reached":
                raise Refusal("chain_stopped", f"de keten is niet met doel bereikt geëindigd (status {state}, reden {stop}); "
                              "er wordt niets geïntegreerd")
        elif state not in ("active", "completed"):
            raise Refusal("chain_stopped", f"de keten is gestopt (status {state}); er wordt niets geïntegreerd")
        if policy.scope == "chain" and links and links[-1].get("task_id") != task_id:
            raise Refusal("superseded", "dit is niet de laatste schakel van de keten; de laatste schakel brengt dit werk mee",
                          attention=False, info=True)
        if not link.get("evaluated") and needs_complete is False:
            raise _later("chain_pending", "Runner 2 heeft deze schakel nog niet verwerkt")
        end_commit = link.get("end_commit")
        if end_commit and end_commit != request["target_commit"]:
            raise Refusal("end_commit_mismatch", "de aangevraagde commit is niet de eindcommit die Runner 2 voor deze schakel noteerde")
        if link.get("clean") is False:
            raise Refusal("link_not_clean", "Runner 2 meldt dat de schakel niet schoon is afgerond")
    return facts


def _task_worktree_ok(rec_worktree, target: str) -> None:
    if not rec_worktree or not os.path.isdir(rec_worktree):
        return   # opgeruimd: dan is er niets meer te vergelijken
    head = ig._rev(rec_worktree, "HEAD")
    if head != target:
        raise Refusal("worktree_mismatch", "de taakworktree staat niet op de aangevraagde commit")
    if ig.tracked_changes(rec_worktree):
        raise Refusal("worktree_dirty", "de taakworktree heeft niet-gecommitte wijzigingen; er wordt niets geïntegreerd")


# ----------------------------------------------------------------------
# De controller-kant: wachtrij afwerken
# ----------------------------------------------------------------------

#: Redenen waarbij de aanvraag wacht en een mens iets kan doen (komt in het paneel als open actie).
HUMAN_WAIT = ("tracked_dirty", "main_not_checked_out", "operation_in_progress", "untracked_collision")


class AutoIntegrator:
    """Werkt de inbox af voor repositories met een automatisch beleid. Eén aanvraag tegelijk, alleen in dit proces.

    Alle toestand die ertoe doet staat in de ``Store`` (markeringen met ``O_EXCL``, audittrail); het geheugen houdt alleen
    de laatste wachtreden bij om de audittrail niet te vullen."""

    def __init__(self, store: ig.Store, inbox: ig.Inbox, policy: AutoPolicy, r2: R2Records, *, exec_lock: threading.Lock | None = None,
                 run_test_suite: Callable[[str, list[str]], dict] | None = None, clock: Callable[[], float] = time.time,
                 linker=None, action_grace_s: float = ACTION_GRACE_S):
        self.store, self.inbox, self.policy, self.r2 = store, inbox, policy, r2
        #: Optioneel: legt wat een mens moet beslissen ook vast als open actiepunt van Runner 2 (``action_link.ActionLinker``).
        self.linker = linker
        self.action_grace_s = action_grace_s
        self._since: dict[str, tuple[str, float]] = {}
        self.exec_lock = exec_lock or threading.Lock()
        self.run_test_suite = run_test_suite
        self.clock = clock
        self.waiting: dict[str, dict] = {}
        self.attempts: dict[str, int] = {}
        self.running: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- aan/uit

    @property
    def _pause_path(self) -> Path:
        return self.store.dir / PAUSE_FILE

    def paused(self) -> dict | None:
        try:
            return json.loads(self._pause_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            return {"by": "?", "at": "?"}

    def pause(self, by: str) -> dict:
        """Zet automatische integratie stil (veilige richting: mag door elke goedkeurder, zonder code)."""
        self.store._ensure()
        data = {"by": by, "at": ig._iso(self.clock())}
        try:
            fd = os.open(self._pause_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return self.paused() or data
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(data))
            handle.flush()
            os.fsync(handle.fileno())
        self.store.audit("auto_pause", f"automatische integratie stilgezet door {by}", by=by)
        return data

    def resume(self, by: str) -> None:
        try:
            self._pause_path.unlink()
        except FileNotFoundError:
            return
        self.store.audit("auto_resume", f"automatische integratie hervat door {by}", by=by)

    # -- achtergronddraad

    def start(self, poll_s: float = POLL_S) -> None:
        if self._thread is not None or not self.policy.enabled:
            return
        self.store.audit("auto_policy", "automatische integratie actief voor " + ", ".join(sorted(self.policy.repos)),
                         policy_sha256=self.policy.sha256, repos=[p.public() for p in self.policy.repos.values()])

        def loop() -> None:
            while not self._stop.wait(poll_s):
                try:
                    self.tick()
                except Exception:  # noqa: BLE001 -- de lus mag nooit stoppen op één fout; de volgende ronde probeert opnieuw
                    pass

        self._thread = threading.Thread(target=loop, name="integration-auto", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()

    # -- een ronde

    def tick(self) -> list[dict]:
        if not self.policy.enabled or self.paused() is not None:
            return []
        out = []
        entries = []
        for name in self.inbox._names():
            try:
                entries.append((os.lstat(self.inbox.dir / name).st_mtime, name[:-5]))
            except OSError:
                continue
        for _mtime, request_id in sorted(entries):
            if self._stop.is_set() or self.paused() is not None:
                break
            refusal = self.store.marker(request_id, ".autorefused")
            human_retry = (refusal and refusal.get("code") == "protected_paths"
                           and ha.approval_path(self.store.dir, request_id).exists())
            if (any(self.store.marker(request_id, suffix) is not None for suffix in (".granted", ".rejected"))
                    or (refusal and not human_retry)):
                self.waiting.pop(request_id, None)
                self._since.pop(request_id, None)
                continue
            result = self.process(request_id)
            if result is not None:
                out.append(result)
        self.link_actions()
        return out

    def link_actions(self) -> list[dict]:
        """Legt wat een mens moet beslissen vast als Runner 2-actiepunt (idempotent, sluit niets). Raised nooit."""
        if self.linker is None:
            return []
        try:
            return self.linker.sync(self.action_candidates())
        except Exception:  # noqa: BLE001 -- de koppeling mag de integratie nooit hinderen
            return []

    def action_candidates(self) -> list[dict]:
        """Uitkomsten waar een mens iets mee moet: definitief geweigerd (met aandacht), uitgevoerd maar mislukt, en een wachtreden die
        een mens kan oplossen en al langer dan ``action_grace_s`` duurt. Zuiver informatieve uitkomsten (al geïntegreerd,
        opgevolgd door een latere schakel) en gewoon wachten (taak nog bezig, rustperiode) zijn nooit een punt."""
        now = self.clock()
        out: list[dict] = []

        def task_of(request_id: str, fallback: str = "") -> tuple[str, str]:
            try:
                request, _sha = self.inbox.read(request_id)
            except ig.IntegrationError:
                return fallback, ""
            return request.get("task_id") or fallback, request.get("source_branch") or ""

        for rid, info in list(self.waiting.items()):
            if info.get("info") or not info.get("attention") or info.get("code") not in HUMAN_WAIT:
                continue
            seen = self._since.get(rid)
            if seen is None or seen[0] != info.get("code") or now - seen[1] < self.action_grace_s:
                continue
            task, _branch = task_of(rid)
            out.append({"request_id": rid, "code": info["code"], "kind": "waiting", "task_id": task, "text": info.get("text"),
                        "at": info.get("at")})
        try:
            refused = sorted(self.store.dir.glob("R-*.autorefused"), key=lambda p: p.stat().st_mtime)
        except OSError:
            refused = []
        for path in refused:
            if self._outcome(path.stem)["state"] == "merged":
                continue  # retain historical refusal, but no active human-action signal
            marker = self.store.marker(path.stem, ".autorefused") or {}
            if marker.get("info") or not marker.get("attention") or not marker.get("code"):
                continue
            task, _branch = task_of(path.stem)
            out.append({"request_id": path.stem, "code": marker["code"], "kind": "refused", "task_id": task,
                        "text": marker.get("text"), "at": marker.get("at")})
        for grant in self.store.list_grants(limit=50):
            result = grant.get("result") or {}
            if not grant.get("auto") or not result or result.get("ok") or not grant.get("request_id"):
                continue
            task, _branch = task_of(grant["request_id"], (grant.get("auto") or {}).get("task") or "")
            code = str(result.get("result") or "controller_error")
            out.append({"request_id": grant["request_id"], "code": code, "kind": "failed", "task_id": task, "grant": grant["id"],
                        "text": "; ".join(result.get("problems") or []) or code, "recovery_branch": result.get("recovery_branch"),
                        "at": grant.get("approved_at")})
        return out

    # -- één aanvraag

    def process(self, request_id: str) -> dict | None:
        now = self.clock()
        try:
            request, sha = self.inbox.read(request_id)
        except ig.IntegrationError:
            return None   # ongeldige aanvraag: de handmatige lijst toont hem als ongeldig; hier niets mee
        try:
            repo = os.path.realpath(os.path.expanduser(request["repo"]))
        except (OSError, ValueError):
            return None
        policy = self.policy.for_repo(repo)
        if policy is None:
            return None   # niet onze repository: de handmatige route blijft zoals ze was
        try:
            self._evaluate_and_run(request, sha, policy, now)
        except Refusal as refusal:
            return self._refused(request, refusal, now)
        except ig.IntegrationError as exc:   # git of bestandssysteem gaf een fout: niets is veranderd, later opnieuw
            return self._refused(request, _later("git_failed", exc.message, attention=False), now)
        self.waiting.pop(request_id, None)
        self._since.pop(request_id, None)
        self.attempts.pop(request_id, None)
        return self._outcome(request_id)

    def _refused(self, request: dict, refusal: Refusal, now: float) -> dict:
        rid = request["id"]
        policy = self.policy.for_repo(request["repo"])
        if refusal.retry and policy is not None and now >= request["created_epoch"] + policy.request_ttl_s:
            refusal = Refusal("expired", f"de aanvraag kon binnen {policy.request_ttl_s // 3600} uur niet worden geïntegreerd "
                              f"(laatste reden: {refusal.text}); vraag opnieuw aan of integreer handmatig", retry=False)
        info = {"id": rid, "state": "waiting" if refusal.retry else "refused", "code": refusal.code, "text": refusal.text,
                "attention": refusal.attention and not refusal.info, "info": refusal.info, "at": ig._iso(now),
                "attention_kind": "human" if refusal.code in HUMAN_WAIT or not refusal.retry else "wait"}
        previous = self.waiting.get(rid)
        changed = previous is None or previous.get("code") != refusal.code
        if refusal.retry:
            self.waiting[rid] = info
            seen = self._since.get(rid)
            if seen is None or seen[0] != refusal.code:
                self._since[rid] = (refusal.code, now)
        else:
            self.waiting.pop(rid, None)
            self._since.pop(rid, None)
            if not self.store.set_marker(rid, ".autorefused", {k: info[k] for k in ("code", "text", "attention", "info", "at")}):
                return info
            changed = True
        if changed:
            self.store.audit("auto_wait" if refusal.retry else "auto_refused", f"{rid}: {refusal.code}: {refusal.text}", request=rid,
                             code=refusal.code, task=request.get("task_id") or None, retry=refusal.retry)
            self.inbox.write_receipt(rid, {"state": info["state"], "auto": True, "code": refusal.code, "message": refusal.text,
                                           "next": "wacht (Monitor 2 probeert het opnieuw)" if refusal.retry else
                                           "niet automatisch geïntegreerd; een mens kan dit handmatig goedkeuren in Monitor 2"})
        return info

    def _outcome(self, request_id: str) -> dict:
        marker = self.store.marker(request_id, ".granted") or {}
        grant_id = marker.get("grant")
        result = self.store.marker(grant_id, ".result") if grant_id else None
        return {"id": request_id, "state": "merged" if result and result.get("ok") else "failed", "grant": grant_id, "result": result}

    def _evaluate_and_run(self, request: dict, sha: str, policy: RepoPolicy, now: float) -> None:
        """Alle controles; raised ``Refusal`` bij het eerste wat niet klopt. Bij groen: toestemming, tests, merge."""
        rid, repo, branch, target = request["id"], policy.repo, request["source_branch"], request["target_commit"]
        if request["mode"] != "ff-only":
            raise Refusal("mode_not_allowed", "automatische integratie is alleen ff-only; no-ff vraagt een mens")
        if not os.path.isdir(repo):
            raise Refusal("not_found", "de repository bestaat niet op deze machine")
        top = ig._git(repo, "rev-parse", "--show-toplevel", check=False).stdout.strip()
        if not top or os.path.realpath(top) != repo or "/.runner2/worktrees/" in repo:
            raise Refusal("wrong_repo", "dit pad is niet de hoofdcheckout van een git-repository")
        task_facts = validate_finished(request, policy, self.r2, now)
        rec = self.r2.task(request["task_id"]) or {}
        # -- de branch en main, uit git zelf
        try:
            facts = ig.inspect(repo, branch)
        except ig.IntegrationError as exc:
            raise Refusal(exc.code, exc.message) from None
        if facts["source_tip"] != target:
            raise Refusal("source_moved", "de taakbranch staat niet meer op de aangevraagde commit; vraag opnieuw aan")
        if facts["already_merged"] or facts["commits_ahead"] == 0:
            raise Refusal("nothing_to_do", "main bevat deze commit al; er is niets te integreren", attention=False, info=True)
        _task_worktree_ok(rec.get("worktree"), target)
        tip_time = ig._git(repo, "log", "-1", "--format=%ct", target).stdout.strip()
        finished_at = _epoch(rec.get("updated")) or 0
        if tip_time.isdigit() and int(tip_time) > finished_at + TIP_SLACK_S:
            raise Refusal("tip_after_finish", "de doelcommit is gemaakt na de afronding van de taak; er wordt niets geïntegreerd")
        if not facts["fast_forward"]:
            raise Refusal("not_fast_forward", "main is verder gegaan dan de basis van deze branch (geen fast-forward); werk de branch "
                          "bij op main en vraag opnieuw aan, of kies handmatig no-ff")
        if facts["conflicts"]:
            raise Refusal("conflicts", "samenvoegen geeft conflicten; los die eerst op")
        if facts["commits_ahead"] > policy.max_commits or facts["files_total"] > policy.max_files:
            raise Refusal("too_large", f"de wijziging is groter dan het beleid toestaat ({facts['commits_ahead']} commits, "
                          f"{facts['files_total']} bestanden; grens {policy.max_commits}/{policy.max_files})")
        every_file = ig._git(repo, "diff", "--name-only", facts["main_head"], target).stdout.split("\n")
        hits = _protected_hits([f for f in every_file if f], policy)
        human = None
        if hits:
            if not ha.approval_path(self.store.dir, rid).exists():
                raise Refusal("protected_paths", "de wijziging raakt beschermde bestanden (" + ", ".join(hits[:4]) +
                              "); automatisch integreren is daarvoor uitgesloten, een mens bekijkt dit")
            try:
                human = ha.validate(self.store.dir, request, sha, facts["main_head"], self.policy.sha256,
                                    list(dict.fromkeys([*policy.tests, *request["tests"]])), hits, at=now)
            except (OSError, ValueError, KeyError, TypeError):
                raise Refusal("human_approval_invalid", "de menselijke toestemming ontbreekt, is verlopen of past niet") from None
        special = _special_entries(repo, facts["main_head"], target)
        if special:
            raise Refusal("special_entries", "de wijziging bevat symlinks of submodules (" + ", ".join(special[:3]) + "); een mens bekijkt dit")
        # -- de hoofdcheckout (wachten tot het weer kan; een mens kan dit oplossen)
        head_ref = ig._git(repo, "symbolic-ref", "-q", "HEAD", check=False).stdout.strip()
        if head_ref != "refs/heads/" + ig.MAIN_BRANCH:
            raise _later("main_not_checked_out", f"de hoofdcheckout staat niet op main (nu: {head_ref or 'losse HEAD'}); zet hem terug op main",
                         attention=True)
        busy = ig.in_progress(repo)
        if busy:
            raise _later("operation_in_progress", "in de hoofdcheckout is een andere git-handeling bezig: " + ", ".join(busy), attention=True)
        dirty = ig.tracked_changes(repo)
        if dirty:
            raise _later("tracked_dirty", f"de hoofdcheckout heeft {len(dirty)} niet-gecommitte wijziging(en) aan getrackte bestanden; "
                         "commit of stash ze, dan gaat de integratie vanzelf door", attention=True)
        collisions = ig.untracked_collisions(repo, facts["main_head"], target)
        if collisions:
            raise _later("untracked_collision", "de integratie zou untracked bestanden overschrijven (" + ", ".join(collisions[:3]) +
                         "); verplaats ze, dan gaat de integratie vanzelf door", attention=True)
        tests = list(dict.fromkeys([*policy.tests, *request["tests"]]))
        if not tests:
            raise Refusal("no_tests", "er zijn geen gerichte tests (niet in het beleid, niet in de aanvraag)")
        if len(tests) > ig.MAX_TESTS:
            # Anders weigert create_grant met "geef 1 tot 20", terwijl de aanvraag zelf binnen de grens bleef.
            asked = len(dict.fromkeys(request["tests"]))
            raise Refusal("too_many_tests", f"de aanvraag noemt {asked} testbestanden en het beleid voegt er "
                          f"{len(tests) - asked} vaste aan toe ({len(tests)} samen); per integratie passen er maximaal "
                          f"{ig.MAX_TESTS}. Vraag opnieuw aan met hooguit {max(ig.MAX_TESTS - len(policy.tests), 0)} gerichte tests")
        if not self.exec_lock.acquire(blocking=False):
            raise _later("busy", "er loopt al een integratie")
        try:
            self._grant_and_execute(request, sha, policy, facts, tests, task_facts, now, human=human, protected=hits)
        finally:
            self.exec_lock.release()

    def _grant_and_execute(self, request: dict, sha: str, policy: RepoPolicy, facts: dict, tests: list[str], task_facts: dict,
                           now: float, *, human: dict | None = None, protected: list[str] | None = None) -> None:
        rid = request["id"]
        auto = {"policy_sha256": self.policy.sha256, "scope": policy.scope, "task": task_facts["task"], "chain": task_facts["chain"],
                "finished_at": task_facts["finished_at"], "requested_tests": request["tests"], "policy_tests": list(policy.tests)}
        if human:
            auto.update(human_approval=human, protected_paths=protected)
        actor = human["approved_by"] if human else AUTO_ACTOR
        try:
            grant = ig.create_grant(
                self.store, repo=policy.repo, source_branch=request["source_branch"], expected_main_head=facts["main_head"],
                target_commit=request["target_commit"], tests=tests, by=actor, mode="ff-only",
                note=f"{'menselijk goedgekeurd' if human else 'automatisch'}; taak {task_facts['task']}", ttl_s=GRANT_TTL_S, allowed=[policy.repo],
                approvers_list=[actor], inbox=self.inbox, request_id=rid, request_sha256=sha, allow_protected=bool(human),
                auto=auto, now=now)
        except ig.IntegrationError as exc:
            retry = exc.code in ("request_changed", "wrong_state")
            raise Refusal(exc.code, exc.message, retry=retry, attention=not retry) from None
        self.running = grant["id"]
        try:
            ig.publish_receipt(self.store, self.inbox, grant["id"])
            try:
                ig.execute(self.store, grant["id"], run_test_suite=self.run_test_suite)
            except ig.IntegrationError as exc:
                # Geweigerd vóór de toestemming verbruikt werd (bv. main net verplaatst): niets is veranderd. De toestemming
                # wordt ingetrokken en de aanvraag vrijgegeven, zodat de volgende ronde alles opnieuw beoordeelt.
                self.store.set_marker(grant["id"], ".revoked", {"by": AUTO_ACTOR, "at": ig._iso(self.clock()), "reason": exc.code})
                self.store.audit("revoke", f"{grant['id']}: ingetrokken door {AUTO_ACTOR} ({exc.code})", grant=grant["id"], by=AUTO_ACTOR)
                try:
                    self.store._path(rid, ".granted").unlink()
                except OSError:
                    pass
                self.attempts[rid] = self.attempts.get(rid, 0) + 1
                final = self.attempts[rid] >= MAX_ATTEMPTS
                raise Refusal(exc.code, exc.message, retry=not final, attention=exc.code in HUMAN_WAIT or final) from None
            except Exception as exc:  # noqa: BLE001
                if self.store.marker(grant["id"], ".result") is None:
                    self.store.set_marker(grant["id"], ".result", {"ok": False, "result": "controller_error", "grant": grant["id"],
                                                                   "problems": [exc.__class__.__name__]})
                    self.store.audit("result", f"{grant['id']}: controller_error", grant=grant["id"], ok=False, result="controller_error")
            ig.publish_receipt(self.store, self.inbox, grant["id"])
        finally:
            self.running = None

    # -- voor het paneel

    def status(self) -> dict:
        out = self.policy.public()
        paused = self.paused()
        out.update(paused=paused, running=self.running, actor=AUTO_ACTOR, poll_s=POLL_S,
                   waiting=sorted(self.waiting.values(), key=lambda x: x["at"], reverse=True)[:ATTENTION_KEEP])
        attention, recent = [], []
        try:
            names = sorted(self.store.dir.glob("R-*.autorefused"), key=lambda p: p.stat().st_mtime, reverse=True)[:ATTENTION_KEEP]
        except OSError:
            names = []
        for path in names:
            if self._outcome(path.stem)["state"] == "merged":
                continue
            marker = self.store.marker(path.stem, ".autorefused") or {}
            item = {"id": path.stem, "state": "refused", **{k: marker.get(k) for k in ("code", "text", "at", "attention", "info")}}
            (attention if marker.get("attention") and not marker.get("info") else recent).append(item)
        for grant in self.store.list_grants(limit=ATTENTION_KEEP):
            if not grant.get("auto"):
                continue
            result = grant.get("result") or {}
            if result.get("ok"):
                recent.append({"id": grant.get("request_id"), "grant": grant["id"], "state": "merged", "code": "merged",
                               "text": f"{grant['source_branch']} -> main ({str(result.get('head_after') or '')[:10]})",
                               "at": grant.get("approved_at")})
            elif result:
                attention.append({"id": grant.get("request_id"), "grant": grant["id"], "state": "failed", "code": result.get("result"),
                                  "text": "; ".join(result.get("problems") or []) or str(result.get("result")),
                                  "recovery_branch": result.get("recovery_branch"), "at": grant.get("approved_at")})
        out["attention"] = attention[:ATTENTION_KEEP]
        out["recent"] = recent[:ATTENTION_KEEP]
        out["r2_actions"] = self.linker.public() if self.linker is not None else {"enabled": False, "problem": None, "linked": []}
        # Verstreken versus geschat en toegestaan testtijd-budget (alleen tonen; test_budget.summary raised niet).
        out["test_run"] = tb.summary(self.store.dir, active=self.running is not None, now=self.clock())
        return out


# ----------------------------------------------------------------------
# Opdrachtregel voor de mens (de instelling klaarzetten)
# ----------------------------------------------------------------------


def cli_policy(args, emit: Callable[[dict], None]) -> int:
    """``auto-policy``: schrijft het beleidsbestand (0600) en toont de twee regels voor de configuratie. Geeft zelf geen recht:
    pas door de hash in de configuratie van Monitor 2 te zetten en die te herstarten wordt het beleid actief."""
    repo = os.path.realpath(os.path.expanduser(args.repo))
    raw = {"scope": args.scope, "tests": list(args.test or []), "max_commits": args.max_commits, "max_files": args.max_files,
           "extra_protected": list(args.protect or []), "settle_s": args.settle, "request_ttl_s": args.ttl_hours * 3600}
    if getattr(args, "require_wrapper", False):
        raw["require_test_wrapper"] = True
    text = policy_file_text({repo: raw})
    out = Path(args.out).expanduser()
    real_out = os.path.realpath(out)
    for guarded in (repo, os.path.realpath(ig.inbox_dir(os.environ))):
        if real_out == guarded or real_out.startswith(guarded + os.sep):
            raise ValueError("zet het beleidsbestand buiten de repository en buiten de inbox (bv. ~/.config/monitor2/)")
    out.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.chmod(out, 0o600)
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    emit({"ok": True, "policy_file": str(out), "policy_sha256": digest, "repo": repo,
          "config": [f"{POLICY_ENV}={out}", f"{POLICY_SHA_ENV}={digest}", f"OBF_INTEGRATION_REPOS moet {repo} bevatten"],
          "next": "Zet deze regels in de configuratie van Monitor 2 (plist) en herstart; dat is de activering. "
                  "Zonder die stap is er niets aan."})
    return 0


def cli_verify(env: dict, emit: Callable[[dict], None]) -> int:
    policy = load_policy(env, ig.allowed_repos(env))
    emit({"ok": policy.enabled, **policy.public(), "path": policy.path})
    return 0 if policy.enabled or not policy.configured else 1
