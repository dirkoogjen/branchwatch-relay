"""De gezondheid van de NUC (LINUX, de productiehost van Studio OBF) in Monitor 2 tonen, alleen lezen (contract §10).

Op de NUC schrijft ``studioobf-health.timer`` elke 5 min ``/var/lib/studioobf-health/status.json`` (0644). Monitor 2
op GERARD leest dat bestand met één achtergrondronde per ``POLL_S`` -- nooit per browserverzoek -- over de bestaande
ssh-route van Runner 2 (``nodes.toml``, blok LINUX: dezelfde gebruiker, sleutel en tailnetnaam als de machinekaart).
De opdracht op de NUC is vast (``head -c`` op dat ene bestand); er wordt op de NUC niets gestart, gewijzigd of geschreven.

Naar de pagina gaat alleen een vaste vorm: oordeel, per app het oordeel, de dienst, de ``/healthz``-code, de release en
óf er een externe luisteraar is (nooit welk adres), vrije ruimte, journalgrootte en leeftijden. Nooit de hostnaam, een
adres of een ssh-melding: een mislukte ronde wordt alleen een vaste toestand (``onbereikbaar``, ``ontbreekt`` …).

Een ``tijd`` ouder dan ``STALE_AFTER_S`` (15 min = drie gemiste timerrondes) betekent dat de timer stilstaat:
toestand ``verouderd``, met de laatste stand eronder. Na een mislukte ronde blijft de laatste geldige stand staan.

Aan/uit: ``OBF_MONITOR2_NUC_HEALTH`` = ``1`` of ``0``; zonder waarde alleen aan als dit proces op GERARD of op de NUC zelf
draait (``OBF_MONITOR2_LOCAL_NODE=gerard`` of ``linux``, zie ``scripts/monitor2.service``).

Draait Monitor 2 zelf op de NUC (lokale node ``linux``, na de verhuizing van de control plane), dan is er geen ssh-route naar
de eigen machine en wordt hetzelfde bestand rechtstreeks gelezen (``local_fetch``, zelfde grootte- en vormcontrole). Een
ander pad voor een proefinstallatie: ``OBF_MONITOR2_NUC_HEALTH_FILE``.
"""

from __future__ import annotations

import calendar
import json
import os
import re
import stat
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable

from . import r2_adapter

ENV = "OBF_MONITOR2_NUC_HEALTH"
FILE_ENV = "OBF_MONITOR2_NUC_HEALTH_FILE"
NODE = "LINUX"
STATUS_PATH = "/var/lib/studioobf-health/status.json"
POLL_S = 60.0
STALE_AFTER_S = 15 * 60
FETCH_TIMEOUT_S = 20
CONNECT_TIMEOUT_S = 5
MAX_BYTES = 64 * 1024
MAX_APPS = 8

_APP = re.compile(r"^[a-z][a-z0-9-]{1,30}$")
_RELEASE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_WORD = re.compile(r"^[a-z-]{1,16}$")
_JOURNAL = re.compile(r"^[0-9]{1,6}(\.[0-9]{1,2})?[KMGT]?$")
_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

#: Vaste teksten per toestand; nooit tekst van ssh of van de NUC zelf.
STATES = {
    "wacht": ("off", "WORDT GELEZEN", "De NUC-status wordt gelezen."),
    "geen_route": ("off", "GEEN ROUTE", "Er is vanaf hier geen ssh-route naar de NUC ingesteld (Runner 2, blok LINUX)."),
    "onbereikbaar": ("warn", "ONBEREIKBAAR", "De NUC antwoordt nu niet over ssh."),
    "ontbreekt": ("warn", "GEEN STATUS", "Het statusbestand op de NUC ontbreekt of is niet leesbaar; draait de gezondheidstimer?"),
    "ongeldig": ("warn", "ONLEESBAAR", "Het statusbestand op de NUC heeft niet de verwachte vorm."),
    "verouderd": ("warn", "VEROUDERD", "De laatste stand is ouder dan 15 minuten: de gezondheidstimer op de NUC lijkt stil te staan."),
    "ongezond": ("alert", "ONGEZOND", "De NUC meldt een probleem."),
    "gezond": ("ok", "GEZOND", None),
}


class FetchError(Exception):
    """Een mislukte ronde, alleen met een soort (``geen_route``, ``onbereikbaar``, ``ontbreekt``) en geen tekst."""

    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


def ssh_command(spec: dict, control_dir: Path | None = None, path: str = STATUS_PATH) -> list[str]:
    """De ssh-aanroep: zelfde gebruiker, adres en sleutel als Runner 2, alleen lezen, geen nieuwe hostsleutels.

    Staat er al een ssh-verbinding van Runner 2 open (``ControlPath``), dan gaat deze aanroep daarover; anders een
    eigen verbinding (``ControlMaster=no``: dit proces start zelf geen blijvende verbinding)."""
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={CONNECT_TIMEOUT_S}", "-o", "StrictHostKeyChecking=yes",
           "-o", "ControlMaster=no", "-p", str(int(spec.get("port", 22)))]
    if control_dir is not None:
        cmd += ["-o", f"ControlPath={control_dir}/cm-%C"]
    if spec.get("identity_file"):
        cmd += ["-i", str(Path(str(spec["identity_file"])).expanduser()), "-o", "IdentitiesOnly=yes"]
    return cmd + [f"{spec['user']}@{spec['host']}", f"head -c {MAX_BYTES + 1} {path}"]


def ssh_fetch(*, loader: Callable[[], Any] = r2_adapter.load_runner2_remote,
              run: Callable[..., Any] = subprocess.run) -> bytes:
    """Eén keer het statusbestand van de NUC lezen; ``FetchError`` bij elk probleem (zonder de ssh-melding)."""
    try:
        remote = loader()
        spec = remote.nodes().get(NODE)
    except Exception:  # noqa: BLE001 -- geen Runner 2 of geen nodes.toml: dan is er geen route
        raise FetchError("geen_route") from None
    if not spec or not spec.get("host") or not spec.get("user") or str(spec.get("os", "")).lower() == "windows":
        raise FetchError("geen_route")
    try:
        control_dir = Path(remote.config.r2_home())
    except Exception:  # noqa: BLE001
        control_dir = None
    try:
        result = run(ssh_command(spec, control_dir), stdin=subprocess.DEVNULL, capture_output=True, timeout=FETCH_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError):
        raise FetchError("onbereikbaar") from None
    if result.returncode == 255:
        raise FetchError("onbereikbaar")
    if result.returncode != 0:
        raise FetchError("ontbreekt")
    return result.stdout


def local_fetch(path: str | None = None) -> bytes:
    """Het statusbestand op deze machine lezen (Monitor 2 draait zelf op de NUC); ``FetchError("ontbreekt")`` bij elk probleem.

    Alleen een gewoon bestand, nooit via een symlink, nooit meer dan ``MAX_BYTES`` + 1 byte; het bestand wordt niet gewijzigd."""
    target = path or os.environ.get(FILE_ENV) or STATUS_PATH
    try:
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise FetchError("ontbreekt") from None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise FetchError("ontbreekt")
        return os.read(fd, MAX_BYTES + 1)
    except OSError:
        raise FetchError("ontbreekt") from None
    finally:
        os.close(fd)


def _epoch(value: str) -> float:
    if not _TIME.match(value):
        raise ValueError(value)
    return float(calendar.timegm(time.strptime(value, "%Y-%m-%dT%H:%M:%SZ")))


def _int(value: Any, low: int, high: int) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and low <= value <= high else None


def _app(item: Any) -> dict | None:
    if not isinstance(item, dict) or not isinstance(item.get("app"), str) or not _APP.match(item["app"]):
        return None
    release = item.get("release")
    dienst = item.get("dienst")
    return {"app": item["app"],
            "oordeel": "ok" if item.get("oordeel") == "ok" else "fout",
            "dienst": dienst if isinstance(dienst, str) and _WORD.match(dienst) else "onbekend",
            "healthz": _int(item.get("healthz"), 0, 599),
            "release": release if isinstance(release, str) and _RELEASE.match(release) else None,
            # Alleen óf er iets op een extern adres luistert; het adres zelf gaat niet naar de pagina.
            "extern": bool(isinstance(item.get("extern"), str) and item["extern"].strip())}


def parse(raw: bytes) -> dict:
    """Het statusbestand → vaste vorm; ``ValueError`` als het niet de vorm van ``studioobf-health --status`` heeft."""
    if len(raw) > MAX_BYTES:
        raise ValueError("te groot")
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("gezond"), bool) or not isinstance(data.get("apps"), list) \
            or not isinstance(data.get("tijd"), str):
        raise ValueError("geen gezondheidsstatus")
    written = _epoch(data["tijd"])
    apps = [shaped for shaped in (_app(item) for item in data["apps"][:MAX_APPS]) if shaped is not None]
    journal = data.get("journal")
    return {"tijd": data["tijd"], "written_at": written, "gezond": data["gezond"] and all(a["oordeel"] == "ok" for a in apps),
            "vrij_procent": _int(data.get("vrij_procent"), 0, 100),
            "journal": journal if isinstance(journal, str) and _JOURNAL.match(journal) else None,
            "apps": apps, "apps_total": len(data["apps"])}


class NucHealthFeed:
    """De laatste NUC-status, ververst door één achtergronddraad; ``current()`` doet zelf nooit ssh."""

    def __init__(self, fetch: Callable[[], bytes] = ssh_fetch, *, clock: Callable[[], float] = time.time,
                 poll_s: float = POLL_S, stale_after_s: int = STALE_AFTER_S):
        self._fetch, self._clock, self.poll_s, self.stale_after_s = fetch, clock, poll_s, stale_after_s
        self._lock = threading.Lock()
        self._status: dict | None = None       # de laatste geldige stand (vaste vorm)
        self._problem: str | None = None       # soort van de laatste mislukte ronde, None na een geslaagde
        self._checked_at: float | None = None  # wanneer de laatste ronde (geslaagd of niet) klaar was
        self._failures = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @classmethod
    def from_env(cls, env: dict | None = None) -> "NucHealthFeed | None":
        value = ((env if env is not None else os.environ).get(ENV) or "").strip().lower()
        if value in ("0", "uit", "off", "false", "no"):
            return None
        local = r2_adapter.local_node_id()
        if value in ("1", "aan", "on", "true", "yes") or (not value and local in ("gerard", "linux")):
            # Op de NUC zelf geen ssh naar jezelf: het bestand staat hier.
            return cls(local_fetch) if local == "linux" else cls()
        return None

    def refresh(self) -> None:
        """Eén ronde. Faalt die, dan blijft de laatste geldige stand staan, met de soort van het probleem ernaast."""
        problem, status = None, None
        try:
            status = parse(self._fetch())
        except FetchError as exc:
            problem = exc.kind if exc.kind in STATES else "onbereikbaar"
        except (ValueError, UnicodeDecodeError):
            problem = "ongeldig"
        except Exception:  # noqa: BLE001 -- een onverwachte fout mag de achtergronddraad niet stoppen
            problem = "onbereikbaar"
        with self._lock:
            self._checked_at = self._clock()
            self._problem = problem
            if status is not None:
                self._status, self._failures = status, 0
            else:
                self._failures += 1

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="monitor2-nuc-health", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.refresh()
            self._stop.wait(self.poll_s)

    def close(self) -> None:
        self._stop.set()

    def current(self) -> dict:
        with self._lock:
            status, problem, checked_at, failures = self._status, self._problem, self._checked_at, self._failures
        now = self._clock()
        age = max(0, int(now - status["written_at"])) if status else None
        stale = age is not None and age > self.stale_after_s
        if checked_at is None:
            state = "wacht"
        elif problem is not None:
            state = problem
        elif stale:
            state = "verouderd"
        elif not status["gezond"]:
            state = "ongezond"
        else:
            state = "gezond"
        tone, headline, message = STATES[state]
        if state == "verouderd":
            message = f"De laatste stand is {age // 60} min oud: de gezondheidstimer op de NUC lijkt stil te staan."
        elif problem is not None and status is not None:
            message = f"{message} Hieronder de laatste stand{' (verouderd)' if stale else ''}."
        view = {k: v for k, v in (status or {}).items() if k != "written_at"} if status else None
        return {"state": state, "tone": tone, "headline": headline, "message": message,
                "stale": stale, "status_age_s": age, "stale_after_s": self.stale_after_s,
                "checked_age_s": None if checked_at is None else max(0, int(now - checked_at)),
                "failures": failures, "poll_s": self.poll_s, "status": view}
