"""OS-specifieke laag: taakproces starten/stoppen, slaapbescherming, nodenaam. De rest van Runner 2 is platformneutraal.

Mac: taakproces in eigen sessie, caffeinate. Linux: elke beurt een eigen `systemd-run --user`-eenheid, zodat
Claude en alle kindprocessen in één cgroup zitten en een herstart van de watch geen taak raakt.
Windows: taakproces losgekoppeld van de aanroeper (eigen procesgroep, geen console); stoppen via `taskkill /T`.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

LINUX = sys.platform.startswith("linux")
WINDOWS = sys.platform == "win32"
DEFAULT_SHELL = "/bin/bash" if LINUX else "/bin/zsh"

# CreateProcess-vlaggen (constanten bestaan in subprocess alleen op Windows).
DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200
CREATE_NO_WINDOW = 0x08000000
CREATE_BREAKAWAY_FROM_JOB = 0x01000000


def node_name() -> str:
    return "WINDOWS" if WINDOWS else "LINUX" if LINUX else "MAC"


def windows_taskproc_flags(breakaway: bool = True) -> int:
    """Vlaggen voor een taakproces dat de ssh-sessie of de watch moet overleven (job-breakaway kan geweigerd worden)."""
    flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
    return flags | CREATE_BREAKAWAY_FROM_JOB if breakaway else flags


def child_process_flags() -> int:
    """Vlaggen voor het Claude-proces: eigen procesgroep, zonder zichtbare console (POSIX: geen; daar geldt start_new_session)."""
    return CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW if WINDOWS else 0


def needs_session_handoff(environ=None) -> bool:
    """Mac: een vanuit ssh gestart proces zit buiten de inlogsessie en kan de inlogsleutelhanger (waar Claude Code zijn
    login bewaart) niet lezen; zo'n Claude-beurt eindigt direct met 'Not logged in'. Dan start de watch het taakproces."""
    env = os.environ if environ is None else environ
    return not (LINUX or WINDOWS) and bool(env.get("SSH_CONNECTION") or env.get("SSH_CLIENT"))


def _bus_env() -> dict:
    """Omgeving waarin `systemctl --user` werkt, ook vanuit een kale ssh-/service-omgeving."""
    env = dict(os.environ)
    rt = env.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path={rt}/bus")
    return env


def spawn_taskproc(tid: str, logfile: Path, code_dir: Path, r2_home: Path) -> str | None:
    """Start `python -m runner2.taskproc <tid>` los van de aanroeper; geeft de systemd-eenheid (Linux) of None."""
    env = dict(os.environ, PYTHONPATH=str(code_dir))
    if WINDOWS:
        env["PYTHONUTF8"] = "1"
        for breakaway in (True, False):   # buiten een job die breakaway verbiedt: zonder de vlag opnieuw
            try:
                with open(logfile, "ab") as out:
                    subprocess.Popen([sys.executable, "-m", "runner2.taskproc", tid], env=env, stdin=subprocess.DEVNULL,
                                     stdout=out, stderr=out, cwd=str(r2_home),
                                     creationflags=windows_taskproc_flags(breakaway), close_fds=True)
                return None
            except OSError:
                if not breakaway:
                    raise
    if not LINUX:
        with open(logfile, "ab") as out:
            subprocess.Popen([sys.executable, "-m", "runner2.taskproc", tid], env=env, stdin=subprocess.DEVNULL,
                             stdout=out, stderr=out, start_new_session=True)
        return None
    unit = f"r2-{tid.lower()}-{int(time.time())}"
    cmd = ["systemd-run", "--user", "--collect", "--quiet", f"--unit={unit}", f"--working-directory={r2_home}",
           f"--setenv=PYTHONPATH={code_dir}", f"--property=StandardOutput=append:{logfile}",
           "--property=KillMode=control-group", "--property=TimeoutStopSec=5"]
    if os.environ.get("R2_HOME"):
        cmd.append(f"--setenv=R2_HOME={os.environ['R2_HOME']}")
    r = subprocess.run(cmd + [sys.executable, "-m", "runner2.taskproc", tid], capture_output=True, text=True,
                       env=_bus_env(), timeout=20)
    if r.returncode:
        raise OSError(f"systemd-run mislukt: {r.stderr.strip()[:300]}")
    return unit


def stop_unit(unit: str | None) -> None:
    """Stopt de hele cgroup van een taakeenheid (taakproces, Claude, alle kinderen, ook losgemaakte)."""
    if LINUX and unit:
        subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True, env=_bus_env(), timeout=30)


def keep_awake(cfg: dict):
    """Slaapbescherming zolang dit proces leeft; alleen op de Mac nodig (de Linux- en Windows-node slapen niet)."""
    if LINUX or WINDOWS:
        return None
    try:
        return subprocess.Popen([cfg["caffeinate_path"], "-i", "-s", "-w", str(os.getpid())],
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError:
        return None
