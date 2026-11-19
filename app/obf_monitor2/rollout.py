"""Uitrolcontroles voor de automatische integratie (voor een mens of voor Claude, nooit zelf activerend).

Drie opdrachten, alle read-only ten opzichte van de echte repository, ``~/.runner2`` en de configuratie van Monitor 2:

``rehearse``       Repetitie in een **wegwerprepository** en een wegwerp-Runner 2-administratie in een tijdelijke map: dezelfde
                   controller-code als in productie (beleid, validatie, tests op export, ff-only, herstelbranch, koppeling met de
                   open actiepunten), daarna nog eens via de losse integrator op de Mac (§13: stilzetten/hervatten, statusbestand,
                   nagebootst versturen). Bewijst ook dat er niets gepusht wordt. Geen enkel echt pad wordt aangeraakt.
``sandbox-check``  Draait *binnen* de Runner-taak (de sandbox) en bewijst dat de sandbox de toestemmingenmap, het controllerpoort,
                   de Runner 2-administratie en de rest van de Monitor 2-configuratie niet kan lezen of schrijven, en wel de
                   inbox kan schrijven. Dit is voorwaarde 5 van het uitrolplan.
``policy-check``   Controleert het beleidsdiff voor ``runner2-src`` (sha256, ``git apply --check``, de beleidstests) in een
                   tijdelijke kopie; de echte ``runner2-src`` blijft onaangeroerd.

Elke opdracht geeft JSON met per controle ``ok`` en een totaaloordeel; exit 0 alleen als alles klopt.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from . import action_link as al
from . import auto_integration as au
from . import integration as ig

TASK = "R2-0a0b0c0d0e0f"
PORT = 8772
DEFAULT_DIFF = "docs/runner2-beleid-integratie.diff"
GIT_ENV = {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_TERMINAL_PROMPT": "0",
           "GIT_AUTHOR_NAME": "Uitrolrepetitie", "GIT_AUTHOR_EMAIL": "repetitie@example.invalid",
           "GIT_COMMITTER_NAME": "Uitrolrepetitie", "GIT_COMMITTER_EMAIL": "repetitie@example.invalid"}


def _git(repo, *args: str, check: bool = True) -> str:
    env = {**os.environ, **GIT_ENV}
    proc = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, env=env, timeout=120)
    if check and proc.returncode:
        raise RuntimeError(f"git {' '.join(args)}: {proc.stderr.strip()[:300]}")
    return proc.stdout.strip()


def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+0000", time.gmtime())


class Report:
    def __init__(self, name: str):
        self.name, self.checks = name, []

    def add(self, check: str, ok: bool, detail: str = "", skipped: bool = False) -> bool:
        self.checks.append({"check": check, "ok": bool(ok), "skipped": skipped, "detail": detail})
        return bool(ok)

    @property
    def ok(self) -> bool:
        return all(c["ok"] or c["skipped"] for c in self.checks) and any(not c["skipped"] for c in self.checks)

    def public(self) -> dict:
        return {"command": self.name, "ok": self.ok, "checks": self.checks,
                "failed": [c["check"] for c in self.checks if not c["ok"] and not c["skipped"]]}


# ----------------------------------------------------------------------
# rehearse
# ----------------------------------------------------------------------


class _Lab:
    """Eén wegwerpwereld: repository met main en een taakbranch, Runner 2-administratie, beleid, controller."""

    def __init__(self, base: Path, actions_backend, *, tests: tuple[str, ...] = ("tests/test_rehearsal.py",), sub: str = "",
                 task_offset: int = 0):
        self.base = base
        self.repo = base / f"{sub}proef-repo"
        self.repo.mkdir()
        _git(self.repo, "init", "-q", "-b", "main")
        (self.repo / "README.md").write_text("proefrepository\n")
        _git(self.repo, "add", "-A")
        _git(self.repo, "commit", "-q", "-m", "start")
        self.main0 = _git(self.repo, "rev-parse", "HEAD")
        self.real = str(self.repo.resolve())
        self.r2 = base / "r2-home"
        (self.r2 / "tasks").mkdir(parents=True, exist_ok=True)
        (self.r2 / "chains").mkdir(exist_ok=True)
        self.store = ig.Store(base / f"{sub}toestemmingen", key=os.urandom(32))
        self.inbox = ig.Inbox(base / f"{sub}inbox")
        raw = {"scope": "task", "tests": list(tests), "settle_s": 0}
        text = au.policy_file_text({self.real: raw})
        self.policy_path = base / f"{sub}auto-policy.json"
        self.policy_path.write_text(text)
        self.policy_path.chmod(0o600)
        self.policy_sha256 = hashlib.sha256(text.encode()).hexdigest()
        self.policy = au.load_policy({au.POLICY_ENV: str(self.policy_path), au.POLICY_SHA_ENV: self.policy_sha256}, [self.real])
        self.records = au.R2Records(self.r2)
        linker = al.ActionLinker(self.store, actions_backend, self.records) if actions_backend is not None else None
        self.auto = au.AutoIntegrator(self.store, self.inbox, self.policy, self.records, linker=linker, action_grace_s=0)
        self.n = task_offset

    def branch(self, files: dict[str, str], test_ok: bool = True) -> tuple[str, str, str]:
        """Maakt taak + branch vanaf de huidige main met `files`; geeft (taak-id, branch, tip)."""
        self.n += 1
        task = f"R2-{self.n:012x}"
        branch = "r2/" + task
        _git(self.repo, "checkout", "-q", "-b", branch, "main")
        body = "def test_ok():\n    assert " + ("True" if test_ok else "False") + "\n"
        files = {"tests/test_rehearsal.py": body, f"app/werk_{self.n}.py": f"x = {self.n}\n", **files}
        for name, text in files.items():
            path = self.repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        _git(self.repo, "add", "-A")
        _git(self.repo, "commit", "-q", "-m", f"werk {self.n}")
        tip = _git(self.repo, "rev-parse", "HEAD")
        _git(self.repo, "checkout", "-q", "main")
        return task, branch, tip

    def finish_task(self, task: str, branch: str, request_id: str | None, title: str = "Proefwerk") -> None:
        summary = "klaar" + (f"; aanvraag {request_id}" if request_id else "")
        state = {"task_id": task, "repo": self.real, "branch": branch, "worktree": None, "status": "completed", "title": title,
                 "created": _iso_now(), "updated": _iso_now(), "finish": {"summary": summary, "chain_status": "goal_reached",
                                                                          "open_actions": []},
                 "failure": None, "question": None, "in_flight": [], "pid": None, "claude_pid": None, "chain_id": None,
                 "human_actions": []}
        path = self.r2 / "tasks" / task / "state.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state))

    def submit(self, task: str, branch: str, tip: str, *, mention: bool = True) -> str:
        saved = self.inbox.submit(repo=self.real, source_branch=branch, target_commit=tip, tests=["tests/test_rehearsal.py"],
                                  task_id=task)
        self.finish_task(task, branch, saved["id"] if mention else None)
        return saved["id"]

    def state(self, task: str) -> dict:
        return json.loads((self.r2 / "tasks" / task / "state.json").read_text())

    def head(self) -> str:
        return _git(self.repo, "rev-parse", "main")


def rehearse(runner2_src: str | None = None, keep: bool = False) -> dict:
    report = Report("rehearse")
    base = Path(tempfile.mkdtemp(prefix="monitor2-repetitie-"))
    old_home = os.environ.get("R2_HOME")
    try:
        try:
            import pytest  # noqa: F401
        except ImportError:
            report.add("pytest beschikbaar in deze Python (de gerichte tests draaien via sys.executable -m pytest)", False,
                       f"{sys.executable} heeft geen pytest; draai dit met de .venv van de repository")
            return report.public()
        report.add("pytest beschikbaar in deze Python", True, sys.executable)
        # De echte Runner 2-functie ``actions.record`` gebruiken als die er is, in een tijdelijke R2_HOME.
        backend = None
        os.environ["R2_HOME"] = str(base / "r2-home")
        src = runner2_src or os.environ.get(al.SRC_ENV)
        for candidate in [src] if src else ["~/StudioOBF-wt-runner2", "~/runner2-src"]:
            if candidate and (Path(candidate).expanduser() / "runner2" / "actions.py").is_file():
                if str(Path(candidate).expanduser()) not in sys.path:
                    sys.path.insert(0, str(Path(candidate).expanduser()))
                break
        try:
            import importlib

            importlib.import_module("runner2.actions")
            have_r2 = True
        except ImportError:
            have_r2 = False
        lab = _Lab(base, None)
        if have_r2:
            backend = al.Runner2Actions(lab.r2)
            lab.auto.linker = al.ActionLinker(lab.store, backend, lab.records)
            lab.auto.action_grace_s = 0
        report.add("Runner 2-actiepuntfunctie gevonden (koppeling echt gecontroleerd)", have_r2,
                   "runner2.actions" if have_r2 else "niet gevonden: de koppeling is niet gerepeteerd (zet OBF_RUNNER2_SRC)",
                   skipped=not have_r2)
        # remote die nooit gepusht mag worden
        remote = base / "remote.git"
        _git(base, "init", "-q", "--bare", str(remote))
        _git(lab.repo, "remote", "add", "origin", str(remote))
        remote_before = _git(remote, "for-each-ref")

        report.add("beleid geldig en vastgezet (hash)", lab.policy.enabled, lab.policy.problem or lab.policy.sha256[:12])

        # a. goed pad
        task, branch, tip = lab.branch({})
        rid = lab.submit(task, branch, tip)
        results = lab.auto.tick()
        merged = [r for r in results if r["state"] == "merged"]
        report.add("a. afgeronde taak die haar aanvraag noemt wordt geïntegreerd (main == doelcommit)",
                   bool(merged) and lab.head() == tip, f"main={lab.head()[:10]} doel={tip[:10]}")
        report.add("a. herstelbranch staat op de oude main", _git(lab.repo, "rev-parse", "--verify", "-q",
                   (merged[0]["result"] or {}).get("recovery_branch") or "HEAD^{none}", check=False) == lab.main0 if merged else False)
        receipt = lab.inbox.dir / f"{rid}.status.json"
        report.add("a. ontvangstbewijs zegt merged", receipt.exists() and json.loads(receipt.read_text()).get("state") == "merged")
        report.add("a. geen Runner 2-actiepunt voor een geslaagde integratie", not lab.state(task).get("human_actions"))
        main_after_a = lab.head()

        # b. taak noemt de aanvraag niet
        task_b, branch_b, tip_b = lab.branch({})
        # (main is verder gegaan door a; branch b is gemaakt na a, dus fast-forward)
        lab.submit(task_b, branch_b, tip_b, mention=False)
        lab.auto.tick()
        report.add("b. niet-genoemde aanvraag: main ongewijzigd", lab.head() == main_after_a)
        # c. beschermd bestand
        task_c, branch_c, tip_c = lab.branch({".github/workflows/ci.yml": "on: push\n"})
        rid_c = lab.submit(task_c, branch_c, tip_c)
        lab.auto.tick()
        report.add("c. beschermd bestand wordt niet geïntegreerd", lab.head() == main_after_a)
        # d. falende test
        task_d, branch_d, tip_d = lab.branch({}, test_ok=False)
        lab.submit(task_d, branch_d, tip_d)
        lab.auto.tick()
        report.add("d. falende test: main onaangeroerd", lab.head() == main_after_a)
        # e. vuile werkboom (wachten, geen merge)
        task_e, branch_e, tip_e = lab.branch({})
        lab.submit(task_e, branch_e, tip_e)
        (lab.repo / "README.md").write_text("handmatig\n")
        lab.auto.tick()
        report.add("e. vuile werkboom: wacht, geen merge, werkboom ongemoeid",
                   lab.head() == main_after_a and (lab.repo / "README.md").read_text() == "handmatig\n")
        _git(lab.repo, "checkout", "--", "README.md")

        if have_r2:
            for label, t in (("b", task_b), ("c", task_c), ("d", task_d)):
                items = [i for i in lab.state(t).get("human_actions") or [] if i.get("status") == "open"]
                report.add(f"{label}. open actiepunt bij de taak in Runner 2 (bron en aanvraag traceerbaar)",
                           len(items) == 1 and "Monitor 2 automatische integratie" in (items[0].get("details") or "")
                           and items[0].get("source", {}).get("task_id") == t, f"{len(items)} open")
            before = {t: lab.state(t).get("human_actions") for t in (task_b, task_c, task_d)}
            for _ in range(2):
                lab.auto.tick()
            after = {t: lab.state(t).get("human_actions") for t in (task_b, task_c, task_d)}
            report.add("idempotent: herhaalde rondes maken geen dubbele punten en wijzigen niets", before == after)
            for t in (task_b, task_c, task_d):
                for it in lab.state(t).get("human_actions") or []:
                    report.add(f"punt {it['id']} blijft open (niets automatisch gesloten)", it.get("status") == "open")
            # de vuile werkboom (e) is weer schoon: de aanvraag gaat nu vanzelf door; het punt (indien al gemaakt) blijft staan
            lab.auto.tick()
            report.add("e. na schoonmaken gaat de aanvraag vanzelf door", lab.head() == tip_e, f"main={lab.head()[:10]}")
        else:
            lab.auto.tick()
            report.add("e. na schoonmaken gaat de aanvraag vanzelf door", lab.head() == tip_e, f"main={lab.head()[:10]}")

        ok, why = lab.store.verify_audit()
        report.add("hashketen van de audittrail klopt", ok, why)
        report.add("er is niets gepusht (de remote is ongewijzigd)", _git(remote, "for-each-ref") == remote_before == "")
        report.add("er is geen echt pad aangeraakt (alleen " + str(base) + ")", str(lab.repo).startswith(str(base)))
        _rehearse_mac(report, base, have_r2)
        return report.public() | {"workdir": str(base) if keep else None}
    except Exception as exc:  # noqa: BLE001
        report.add("repetitie zonder uitzondering", False, f"{exc.__class__.__name__}: {exc}"[:400])
        return report.public()
    finally:
        if old_home is None:
            os.environ.pop("R2_HOME", None)
        else:
            os.environ["R2_HOME"] = old_home
        if not keep:
            shutil.rmtree(base, ignore_errors=True)


def _rehearse_mac(report: Report, base: Path, have_r2: bool) -> None:
    """Dezelfde repetitie via de losse integrator op de Mac (``mac_integrator``, §13): eigen proces-object met sleutel in het
    geheugen, opdrachtregel voor stilzetten/hervatten, statusbestand en (nagebootst) versturen naar GERARD. ``scp`` wordt hier
    vervangen door een kopie naar een map in de wegwerpwereld: er wordt geen netwerk gebruikt."""
    import contextlib
    import io
    import secrets as _secrets

    from . import mac_integrator as mi

    lab = _Lab(base, None, sub="mac-", task_offset=0x100)
    remote = base / "mac-remote.git"
    _git(base, "init", "-q", "--bare", str(remote))
    _git(lab.repo, "remote", "add", "origin", str(remote))
    remote_before = _git(remote, "for-each-ref")
    code = "repetitie-" + _secrets.token_hex(8)
    secret_hash = ig.hash_secret(code, n=2 ** 4)
    status_file = base / "mac-status" / "mac-integrator-status.json"
    gerard = base / "gerard-kopie"
    gerard.mkdir()
    env = {**{k: v for k, v in os.environ.items() if not k.startswith("OBF_INTEGRATION")},
           ig.ENABLE_ENV: "1", ig.REPOS_ENV: lab.real, au.POLICY_ENV: str(lab.policy_path), au.POLICY_SHA_ENV: lab.policy_sha256,
           ig.DIR_ENV: str(lab.store.dir), ig.INBOX_ENV: str(lab.inbox.dir), au.R2_HOME_ENV: str(lab.r2),
           ig.SECRET_HASH_ENV: secret_hash, mi.STATUS_FILE_ENV: str(status_file),
           mi.SCP_TARGET_ENV: "operator@worker.example.invalid:/home/operator/.config/monitor2/mac-integrator-status.json",
           al.ENABLE_ENV: "1" if have_r2 else "0"}
    scp = {"fail": False, "calls": 0}

    def fake_scp(cmd, **_kw):
        scp["calls"] += 1
        if scp["fail"]:
            return subprocess.CompletedProcess(cmd, 255, "", "ssh: connect to host gerard.invalid port 22: Connection refused")
        shutil.copyfile(cmd[-2], gerard / "mac-integrator-status.json")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    uploader = mi.StatusUploader(env, runner=fake_scp)
    uploader.interval = 0
    integ = mi.MacIntegrator(env, uploader=uploader, action_backend=al.Runner2Actions(lab.r2) if have_r2 else None,
                             action_grace_s=0)

    def step() -> None:
        integ.step()
        if uploader._thread is not None:
            uploader._thread.join(10)

    def cli(*argv: str) -> int:
        with contextlib.redirect_stdout(io.StringIO()):
            return mi.cli_main(["--plist", str(base / "geen.plist"), *argv], env=env, ask_code=lambda _p: answer["code"],
                               is_terminal=lambda: True)

    def status() -> dict:
        return json.loads(status_file.read_text())

    answer = {"code": ""}
    # m1. een aanvraag van vóór de start van de integrator (bestaande, openstaande tak) wordt niet automatisch geïntegreerd
    task0, branch0, tip0 = lab.branch({})
    rid0 = lab.submit(task0, branch0, tip0)
    time.sleep(1.1)
    integ.acquire()
    try:
        second = mi.MacIntegrator(env, uploader=mi.StatusUploader({}))
        try:
            second.acquire()
            report.add("m. één integrator tegelijk (tweede proces weigert)", False)
            second.release()
        except ig.IntegrationError as exc:
            report.add("m. één integrator tegelijk (tweede proces weigert)", exc.code == "busy")
        step()
        report.add("m1. bestaande aanvraag (van vóór de start) wordt niet automatisch geïntegreerd",
                   lab.head() == lab.main0 and any(x["id"] == rid0 for x in status()["auto"]["ignored_preexisting"]))
        # m2. nieuwe aanvraag: geïntegreerd door de losse integrator
        task, branch, tip = lab.branch({})
        lab.submit(task, branch, tip)
        step()
        report.add("m2. losse integrator integreert een afgeronde taak (main == doelcommit)", lab.head() == tip, f"main={lab.head()[:10]}")
        # m3. stilzetten zonder code; hervatten alleen met de juiste code
        report.add("m3. stilzetten via de opdrachtregel, zonder code", cli("pause", "--by", "repetitie") == 0)
        task_p, branch_p, tip_p = lab.branch({})
        lab.submit(task_p, branch_p, tip_p)
        step()
        report.add("m3. stilgezet: geen integratie, status meldt gepauzeerd", lab.head() == tip and bool(status()["auto"]["paused"]))
        answer["code"] = "verkeerde-code-1234"
        report.add("m3. hervatten met een foute code wordt geweigerd", cli("resume", "--by", "repetitie") == 2
                   and integ.auto.paused() is not None)
        answer["code"] = code
        report.add("m3. hervatten met de goedkeuringscode", cli("resume", "--by", "repetitie") == 0 and integ.auto.paused() is None)
        scp["fail"] = True   # m5 meteen mee: GERARD is onbereikbaar terwijl de wachtende aanvraag doorgaat
        step()
        report.add("m3. na hervatten gaat de wachtende aanvraag door", lab.head() == tip_p, f"main={lab.head()[:10]}")
        report.add("m5. versturen naar GERARD mislukt: integratie gaat gewoon door", lab.head() == tip_p)
        step()
        report.add("m5. status meldt het mislukte versturen", status()["upload"]["problem"] == "GERARD is niet bereikbaar",
                   str(status()["upload"]["problem"]))
        scp["fail"] = False
        # m4. weigering met open actiepunt (beschermd bestand), main onaangeroerd
        task_c, branch_c, tip_c = lab.branch({".github/workflows/ci.yml": "on: push\n"})
        lab.submit(task_c, branch_c, tip_c)
        step()
        report.add("m4. beschermd bestand: niet geïntegreerd, zichtbaar onder aandacht",
                   lab.head() == tip_p and any(x.get("code") == "protected_paths" for x in status()["auto"]["attention"]))
        if have_r2:
            items = [i for i in lab.state(task_c).get("human_actions") or [] if i.get("status") == "open"]
            report.add("m4. open actiepunt bij de taak in Runner 2, niets gesloten", len(items) == 1)
        step()
        # m6. statusbestand: alleen-lezen, vorm van het auto-deel, nooit sleutel, code of hash; kopie op 'GERARD' gelijk
        text = status_file.read_text()
        data = json.loads(text)
        auto = data.get("auto") or {}
        report.add("m6. statusbestand is alleen-lezen en heeft de vorm van het auto-deel van /api/integration",
                   (status_file.stat().st_mode & 0o777) == 0o444 and data.get("read_only") is True and {
                       "enabled", "paused", "waiting", "attention", "recent", "r2_actions", "audit_ok"} <= set(auto))
        forbidden = [code, secret_hash, lab.policy_sha256, integ.store.key().hex()]
        copy = (gerard / "mac-integrator-status.json").read_text() if (gerard / "mac-integrator-status.json").exists() else ""
        report.add("m6. geen sleutel, goedkeuringscode of hash in het statusbestand of de kopie op GERARD",
                   not any(f in text or f in copy for f in forbidden) and "scrypt$" not in text + copy and scp["calls"] > 0)
        report.add("m6. kopie op GERARD gelijk aan het lokale statusbestand", copy == text)
        # Monitor 2 op GERARD (zelf geen integratie) leest de kopie alleen om te tonen (§13.2, ``remote_auto``)
        from . import remote_status as rs
        shown = rs.read(gerard / "mac-integrator-status.json")
        report.add("m6. Monitor 2 op GERARD toont de kopie (alleen lezen), zonder sleutel, code of hash",
                   shown is not None and shown["state"] == "ok"
                   and any(x.get("code") == "protected_paths" for x in shown["status"]["auto"]["attention"])
                   and not any(f in json.dumps(shown) for f in forbidden))
        # m7. beleid gewijzigd: de integrator doet niets meer (fail-closed)
        lab.policy_path.chmod(0o600)
        lab.policy_path.write_text(lab.policy_path.read_text() + " ")
        task_t, branch_t, tip_t = lab.branch({})
        lab.submit(task_t, branch_t, tip_t)
        step()
        report.add("m7. gewijzigd beleidsbestand: niets geïntegreerd, status meldt uit",
                   lab.head() == tip_p and status()["auto"]["enabled"] is False and "hash" in (status()["auto"]["problem"] or ""))
        ok, why = integ.store.verify_audit()
        report.add("m8. hashketen van de audittrail van de integrator klopt", ok, why)
        report.add("m8. er is niets gepusht (de remote van de proefrepository is ongewijzigd)", _git(remote, "for-each-ref") == remote_before == "")
        source = Path(mi.__file__).read_text(encoding="utf-8")
        report.add("m8. geen luisterende poort of webserver in de integrator",
                   not any(x in source for x in (".listen(", ".bind(", "http.server", "socketserver")))
    finally:
        integ.release()


# ----------------------------------------------------------------------
# sandbox-check
# ----------------------------------------------------------------------


def _try_write(directory: Path, name: str) -> tuple[bool, str]:
    """Probeert een nieuw bestand te maken; ruimt het op als dat lukte. ``(gelukt, detail)``."""
    path = directory / name
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as exc:
        return False, exc.__class__.__name__ + (f" ({exc.strerror})" if exc.strerror else "")
    os.close(fd)
    try:
        os.unlink(path)
    except OSError:
        return True, f"aangemaakt maar niet te verwijderen: {path}"
    return True, "aangemaakt en verwijderd"


def _try_list(directory: Path) -> tuple[bool, str]:
    try:
        names = os.listdir(directory)
    except OSError as exc:
        return False, exc.__class__.__name__
    return True, f"{len(names)} namen leesbaar"


def sandbox_check(env: dict | None = None, port: int = PORT, home: Path | None = None) -> dict:
    """Moet *in de sandbox van een Runner-taak* draaien. ``ok`` per controle = het gewenste (geweigerde of toegestane) resultaat."""
    env = env if env is not None else dict(os.environ)
    report = Report("sandbox-check")
    grants = ig.grants_dir(env)
    inbox = ig.inbox_dir(env)
    conf = Path(env.get("OBF_MONITOR2_CONF_DIR") or Path.home() / ".config" / "monitor2").expanduser()
    r2 = au.r2_home(env)
    pid = os.getpid()
    probe = f".sandbox-probe-{pid}"

    listed, detail = _try_list(grants)
    report.add("a1. toestemmingenmap lezen moet WEIGEREN", not listed, f"{grants}: {detail}")
    made, detail = _try_write(grants, probe)
    report.add("a2. in de toestemmingenmap schrijven moet WEIGEREN", not made, f"{grants}: {detail}")
    key_files = []
    for p in (grants / "hmac.key", grants / "audit.jsonl"):
        try:
            if p.exists():
                key_files.append(p)
        except PermissionError:
            pass                                   # dichte map: de bestanden zijn niet eens te zien, dat is het gewenste
    opened = []
    for p in key_files:
        try:
            with open(p, "rb") as fh:
                fh.read(1)
            opened.append(p.name)
        except OSError:
            pass
    report.add("a3. sleutel- en auditbestanden lezen moet WEIGEREN", not opened, ", ".join(opened) or "niet leesbaar of afwezig")

    why = ""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/integration", timeout=5) as response:
            reached = f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        reached = f"HTTP {exc.code}"   # er antwoordde iets: de poort is bereikbaar
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        reached = ""
        why = exc.__class__.__name__
    report.add("b. controllerpoort 127.0.0.1:" + str(port) + " moet GEWEIGERD of onbereikbaar zijn", reached == "",
               reached or f"geen verbinding ({why})")

    made, detail = _try_write(inbox, probe)
    report.add("c. aanvraag-inbox schrijven moet LUKKEN", made, f"{inbox}: {detail}")

    made, detail = _try_write(conf, probe)
    report.add("d. elders in de Monitor 2-configuratiemap schrijven moet WEIGEREN", not made, f"{conf}: {detail}")

    for sub in ("tasks", "chains"):
        target = r2 / sub
        if target.is_dir():
            made, detail = _try_write(target, probe)
            report.add(f"e. Runner 2-administratie ({sub}/) schrijven moet WEIGEREN", not made, f"{target}: {detail}")
        else:
            report.add(f"e. Runner 2-administratie ({sub}/) bestaat niet op dit pad", True, str(target), skipped=True)
    return report.public() | {"next": "Alles ok: dan mag het beleid uit §6 worden uitgerold. Faalt a/b/d/e: STOP; sandbox eerst dichtzetten."}


# ----------------------------------------------------------------------
# policy-check
# ----------------------------------------------------------------------


def policy_check(runner2_src: str, diff_path: str, expect_sha256: str | None = None) -> dict:
    report = Report("policy-check")
    src = Path(runner2_src).expanduser()
    diff = Path(diff_path).expanduser()
    if not (src / "runner2" / "session.py").is_file() or not diff.is_file():
        report.add("runner2-bron en beleidsdiff gevonden", False, f"{src} / {diff}")
        return report.public()
    digest = hashlib.sha256(diff.read_bytes()).hexdigest()
    report.add("sha256 van het beleidsdiff" + (" komt overeen" if expect_sha256 else " (geen verwachte waarde opgegeven)"),
               digest == expect_sha256 if expect_sha256 else True, digest, skipped=not expect_sha256)
    head = _git(src, "rev-parse", "HEAD", check=False)
    dirty = _git(src, "status", "--porcelain", "--untracked-files=no", check=False)
    report.add("runner2-bron: HEAD bekend en getrackte werkboom schoon", bool(head) and not dirty, f"HEAD {head[:10]}; {'schoon' if not dirty else 'vuil'}")
    tmp = Path(tempfile.mkdtemp(prefix="monitor2-beleidscheck-"))
    try:
        archive = subprocess.run(["git", "-C", str(src), "archive", "--format=tar", "HEAD"], capture_output=True, timeout=120)
        if archive.returncode:
            report.add("kopie van HEAD maken", False, archive.stderr.decode()[:200])
            return report.public()
        subprocess.run(["tar", "-x", "-C", str(tmp)], input=archive.stdout, check=True, timeout=120)
        _git(tmp, "init", "-q")
        check = subprocess.run(["git", "-C", str(tmp), "apply", "--check", "-p1", str(diff)], capture_output=True, text=True)
        already = False
        if check.returncode:
            reverse = subprocess.run(["git", "-C", str(tmp), "apply", "--reverse", "--check", "-p1", str(diff)], capture_output=True, text=True, timeout=60)
            already = reverse.returncode == 0
        report.add("beleidsdiff toepasbaar of aantoonbaar al volledig aanwezig op tijdelijke HEAD-kopie", check.returncode == 0 or already,
                   "reeds aanwezig (reverse --check); bestaande tekst en beleidstests worden gecontroleerd" if already else check.stderr.strip()[:300])
        if check.returncode and not already:
            return report.public()
        if not already:
            subprocess.run(["git", "-C", str(tmp), "apply", "-p1", str(diff)], check=True, timeout=60)
        text = (tmp / "runner2" / "session.py").read_text(encoding="utf-8")
        report.add("push blijft verboden in de nieuwe tekst", "Push nooit" in text and "integratie doet een mens later" not in text)
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "R2_HOME": str(tmp / "r2home")}
        proc = subprocess.run([sys.executable, "-m", "unittest", "runner2.tests.test_policy_integratie", "runner2.tests.test_actions",
                               "runner2.tests.test_actions_dedupe"], cwd=tmp, env=env, capture_output=True, text=True, timeout=300)
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-3:]
        report.add("beleidstests en actietests groen op de gecontroleerde kopie", proc.returncode == 0, " | ".join(tail))
    except (OSError, subprocess.SubprocessError) as exc:
        report.add("beleidscontrole zonder uitzondering", False, f"{exc.__class__.__name__}: {exc}"[:300])
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return report.public()


# ----------------------------------------------------------------------
# opdrachtregel
# ----------------------------------------------------------------------


def cli_main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="obf_monitor2_rollout", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    rep = sub.add_parser("rehearse", help="repetitie in een wegwerprepository (raakt niets echts aan)")
    rep.add_argument("--runner2-src", help="map met het pakket runner2 (standaard $OBF_RUNNER2_SRC of de bekende plekken)")
    rep.add_argument("--keep", action="store_true", help="laat de tijdelijke map staan (voor inspectie)")
    sub.add_parser("sandbox-check", help="bewijs binnen de Runner-sandbox dat de sandbox dicht staat waar het moet")
    pol = sub.add_parser("policy-check", help="controleer het beleidsdiff voor runner2-src in een tijdelijke kopie")
    pol.add_argument("--runner2-src", required=True)
    pol.add_argument("--diff", default=str(Path(__file__).resolve().parents[2] / DEFAULT_DIFF))
    pol.add_argument("--sha256", help="verwachte sha256 van het diff (uit de documentatie)")
    args = parser.parse_args(argv)
    if args.cmd == "rehearse":
        out = rehearse(args.runner2_src, args.keep)
    elif args.cmd == "sandbox-check":
        out = sandbox_check()
    else:
        out = policy_check(args.runner2_src, args.diff, args.sha256)
    sys.stdout.write(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    return 0 if out["ok"] else 1
