"""De koppeling van een nieuwe run/taak naar de zichtbare monitor.

Er is precies één normale zichtbare monitor: Monitor 2 (``app/obf_monitor2``,
http://127.0.0.1:8772, LaunchAgent com.studioobf.obf-monitor2) voor Mac, Linux en
Gerard. De losse webmonitors op 8770/8771 zijn op 2026-09-26 uitgefaseerd. Monitor 2
kiest zelf de actuele run of controllertaak per runner; dit bestand hoeft
hem dus niets te *vertellen* -- hoogstens hem in beeld te brengen wanneer Beheerder
nog geen tabblad openstaan heeft.

**Standaard gebeurt hier niets.** De monitor volgt de actuele run uit zichzelf, en
een venster dat ongevraagd naar voren springt is een onderbreking -- bij een reeks
runs worden het er bovendien ongemerkt veel. ``OBF_MONITOR_WEB_OPEN=1`` zet het aan;
handmatig openen blijft ``scripts/open-monitor``.

Drie eigenschappen waar dit op staat of valt:

* **Nooit vanuit een test.** Draait er een testrunner, dan wordt er niets geopend,
  hoe de instellingen ook staan. Tests draaien op wegwerpmappen en dus met een
  verse markering; zonder deze grendel opende een testrun een tabblad per
  gespawnd werkproces. Dat is precies één keer echt gebeurd.
* **Nooit een tabblad per statuspoll.** Er wordt hoogstens één keer per run/taak
  geopend, en alleen als de vorige opening lang genoeg geleden is. De teller
  staat in een klein markeerbestand naast de andere MCP-administratie.
* **Nooit een reden om een run te raken.** Elke fout hier (geen monitorserver,
  geen Chrome, geen schrijfrecht op het markeerbestand) is stil en zonder
  gevolgen: :func:`ensure_web_monitor_visible` geeft dan gewoon ``False``.

Het oude PySide6/Qt-venster (``obf_claude_monitor host``) blijft bestaan voor
ontwikkeling en debugging, maar gaat nooit meer vanzelf open. Zie
``BridgeConfig.monitor_qt_autostart`` / ``OBF_MCP_CLAUDE_MONITOR_QT`` en
``docs/obf_monitor2.md``.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

#: Zo lang na een vorige opening wordt er geen tweede tabblad meer geopend.
#: Ruim boven de pollcyclus van elke statusaanvraag, en ruim onder de duur van
#: een normale run: één run levert dus één tabblad op, een reeks polls geen enkel.
REOPEN_AFTER_SECONDS = 600.0

#: Kort, want dit zit op het pad van ``claude_start``/``controller_start``: die
#: aanroep moet snel antwoorden. De monitorserver draait op dezelfde machine en
#: antwoordt in milliseconden; draait hij niet, dan weigert de verbinding meteen.
HEALTH_TIMEOUT_SECONDS = 0.5


def monitor_url() -> str:
    """Het adres van Monitor 2.

    De poort komt uit ``OBF_MONITOR2_PORT`` -- dezelfde variabele waarmee de LaunchAgent
    van Monitor 2 zijn poort kiest -- en anders uit de ene constante in
    ``obf_monitor2.server.DEFAULT_PORT``. De oude ``OBF_MONITOR_WEB_PORT`` telt
    bewust niet meer mee: die wees naar de uitgefaseerde 8770/8771.
    """
    from obf_monitor2.server import DEFAULT_PORT as MONITOR_PORT  # noqa: PLC0415

    MONITOR_HOST = "127.0.0.1"

    port = os.environ.get("OBF_MONITOR2_PORT") or ""
    if not (port.isdigit() and 1 <= int(port) <= 65535):
        port = str(MONITOR_PORT)
    return f"http://{MONITOR_HOST}:{port}"  # de monitor bindt sowieso alleen lokaal


def monitor_healthy(url: str | None = None) -> bool:
    """Antwoordt de lokale monitorserver? Nooit een fout, alleen ja of nee.

    Bewust álles opvangen: ``urlopen`` kan naast de verwachte netwerkfouten ook
    ``HTTPException`` of ``OverflowError`` opleveren (een onmogelijke poort), en
    dit zit op het pad van een echte Claude-run. Een monitor die niet antwoordt
    is hier gewoon "nee".
    """
    base = url or monitor_url()
    try:
        with urllib.request.urlopen(base + "/healthz", timeout=HEALTH_TIMEOUT_SECONDS) as response:
            return response.status == 200
    except Exception:
        return False


def _recently_opened(marker: Path, ident: str) -> bool:
    try:
        raw = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if not isinstance(raw, dict):
        return False
    if raw.get("ident") == ident:
        return True  # voor deze run/taak is al een venster geopend
    try:
        return time.time() - float(raw.get("at") or 0) < REOPEN_AFTER_SECONDS
    except (TypeError, ValueError):
        return False


def _note_opened(marker: Path, ident: str) -> None:
    try:
        marker.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        marker.write_text(json.dumps({"ident": ident, "at": time.time()}), encoding="utf-8")
    except OSError:
        pass  # geen markering kunnen schrijven is geen reden om iets te laten mislukken


def running_under_test() -> bool:
    """Draait dit binnen een testrun? Dan blijft het scherm van Beheerder met rust."""
    return "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules


def ensure_web_monitor_visible(marker: Path, ident: str, *, enabled: bool = False) -> bool:
    """Breng Monitor 2 in beeld voor ``ident`` (een run_id of task_id).

    Geeft ``True`` wanneer er daadwerkelijk een venster is geopend. ``False`` bij
    alles wat daar tegen pleit: uitgezet (de standaard), een testrun, geen
    draaiende monitorserver, of kort geleden al een venster geopend. Doet nooit
    iets anders dan één ``open``.
    """
    if not enabled or running_under_test():
        return False
    if _recently_opened(marker, ident):
        return False
    url = monitor_url()
    if not monitor_healthy(url):
        return False
    _note_opened(marker, ident)
    return _open_in_browser(url)


def _open_in_browser(url: str) -> bool:
    """Open ``url``, het liefst in Chrome. OS-specifieke stappen via de platformlaag.

    Chrome is de monitorbrowser (zie ``docs/obf_monitor2.md``), dus op macOS
    wordt die met name genoemd. Lukt dat niet, of draait dit op Linux, dan opent
    ``Platform.open_browser`` de standaardbrowser -- dezelfde terugval als
    ``scripts/open-monitor``.
    """
    opener = shutil.which("open")
    if sys.platform == "darwin" and opener:
        try:
            subprocess.Popen([opener, "-a", "Google Chrome", url],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
            return True
        except OSError:
            pass
    try:
        from core.platform_service import Platform  # noqa: PLC0415

        return Platform.open_browser(url)
    except Exception:
        return False
