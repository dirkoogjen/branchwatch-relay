"""Dienst voor de watch: LaunchAgent (Mac), systemd --user-eenheid (Linux) of geplande taak bij aanmelden (Windows)."""
import json
import os
import plistlib
import subprocess
import sys
import time
from pathlib import Path

from . import config, osadapter

LABEL = "com.studioobf.runner2.watch"
PLIST = Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"


def make_plist(code_dir: Path, python: str = sys.executable) -> dict:
    home = config.r2_home()
    return {
        "Label": LABEL,
        "ProgramArguments": [python, "-m", "runner2.watch"],
        "WorkingDirectory": str(home),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        # Taakprocessen (eigen sessie) mogen een stop of herstart van de watch overleven.
        "AbandonProcessGroup": True,
        "StandardOutPath": str(home / "logs" / "watch.out"),
        "StandardErrorPath": str(home / "logs" / "watch.err"),
        "EnvironmentVariables": {
            "PYTHONPATH": str(code_dir), "HOME": str(Path.home()), "USER": os.environ.get("USER", ""),
            "SHELL": os.environ.get("SHELL", "/bin/zsh"), "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            **({"R2_HOME": os.environ["R2_HOME"]} if os.environ.get("R2_HOME") else {}),
        },
    }


def _domain() -> str:
    return f"gui/{os.getuid()}"


def _ctl(*args) -> subprocess.CompletedProcess:
    return subprocess.run(["launchctl", *args], capture_output=True, text=True)


UNIT_NAME = "runner2-watch.service"
UNIT_PATH = Path.home() / ".config" / "systemd" / "user" / UNIT_NAME


def make_unit(code_dir: Path, python: str = sys.executable) -> str:
    """systemd-eenheid van de watch. Taakbeurten zijn eigen transient units, dus een herstart raakt geen taak."""
    home = config.r2_home()
    env = f"Environment=PYTHONPATH={code_dir}\n" + (f"Environment=R2_HOME={os.environ['R2_HOME']}\n" if os.environ.get("R2_HOME") else "")
    return (f"[Unit]\nDescription=Studio OBF Runner 2 watch\n\n[Service]\nType=simple\n"
            f"ExecStart={python} -m runner2.watch\nWorkingDirectory={home}\n{env}"
            f"Restart=always\nRestartSec=10\nStandardOutput=append:{home}/logs/watch.out\n"
            f"StandardError=append:{home}/logs/watch.err\n\n[Install]\nWantedBy=default.target\n")


def _sctl(*args) -> subprocess.CompletedProcess:
    return subprocess.run(["systemctl", "--user", *args], capture_output=True, text=True, env=osadapter._bus_env())


WINDOWS_TASK = "StudioOBF-Runner2-Watch"


def windows_launcher_path() -> Path:
    return config.r2_home() / "bin" / "watch.cmd"


def _windows_no_console_python(python: str) -> str:
    """pythonw.exe (geen consolevenster) naast python.exe als die er is."""
    w = Path(python).with_name("pythonw.exe")
    return str(w) if w.is_file() else python


def make_windows_launcher(code_dir: Path, python: str = sys.executable) -> str:
    """Startscript van de watch (.cmd): milieu zetten en de watch losgekoppeld starten. Regeleindes zijn CRLF."""
    home = config.r2_home()
    lines = ["@echo off", f'set "PYTHONPATH={code_dir}"', "set PYTHONUTF8=1"]
    if os.environ.get("R2_HOME"):
        lines.append(f'set "R2_HOME={os.environ["R2_HOME"]}"')
    lines += [f'cd /d "{home}"', f'start "" /b "{_windows_no_console_python(python)}" -m runner2.watch']
    return "\r\n".join(lines) + "\r\n"


def windows_create_command(launcher: Path) -> list:
    """schtasks-opdracht: bij aanmelden van deze gebruiker starten, zonder verhoogde rechten."""
    return ["schtasks", "/Create", "/TN", WINDOWS_TASK, "/SC", "ONLOGON", "/RL", "LIMITED", "/F",
            "/TR", f'"{launcher}"']


def _schtasks(*args) -> subprocess.CompletedProcess:
    return subprocess.run(["schtasks", *args], capture_output=True, text=True)


def _windows_watch_pid() -> int | None:
    try:
        return int(json.loads((config.r2_home() / "watch.json").read_text())["pid"])
    except (OSError, ValueError, KeyError):
        return None


def install(code_dir: Path, plist_path: Path = PLIST) -> str:
    if osadapter.WINDOWS:
        (config.r2_home() / "logs").mkdir(parents=True, exist_ok=True)
        launcher = windows_launcher_path()
        launcher.parent.mkdir(parents=True, exist_ok=True)
        launcher.write_bytes(make_windows_launcher(code_dir).encode("utf-8"))
        r = subprocess.run(windows_create_command(launcher), capture_output=True, text=True)
        if r.returncode:
            return f"schtasks /Create mislukt: {(r.stderr or r.stdout).strip()}"
        _schtasks("/Run", "/TN", WINDOWS_TASK)
        return "geïnstalleerd"
    if osadapter.LINUX:
        (config.r2_home() / "logs").mkdir(parents=True, exist_ok=True)
        UNIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        UNIT_PATH.write_text(make_unit(code_dir))
        _sctl("daemon-reload")
        r = _sctl("enable", "--now", UNIT_NAME)
        return "geïnstalleerd" if r.returncode == 0 else f"enable mislukt: {r.stderr.strip()}"
    (config.r2_home() / "logs").mkdir(parents=True, exist_ok=True)
    plist_path.parent.mkdir(parents=True, exist_ok=True)
    plist_path.write_bytes(plistlib.dumps(make_plist(code_dir)))
    _ctl("bootout", f"{_domain()}/{LABEL}")   # herinstallatie: eerst de oude watch (taakprocessen blijven leven)
    r = _ctl("bootstrap", _domain(), str(plist_path))
    return "geïnstalleerd" if r.returncode == 0 else f"bootstrap mislukt: {r.stderr.strip()}"


def uninstall(plist_path: Path = PLIST) -> str:
    if osadapter.WINDOWS:
        _schtasks("/End", "/TN", WINDOWS_TASK)
        _schtasks("/Delete", "/TN", WINDOWS_TASK, "/F")
        pid = _windows_watch_pid()
        if pid:
            from . import procs
            row = next((r for r in procs.snapshot() if r["pid"] == pid), None)
            if row and "runner2.watch" in row["cmd"]:
                procs.terminate(pid)
        windows_launcher_path().unlink(missing_ok=True)
        return "verwijderd (lopende taakprocessen zijn niet aangeraakt)"
    if osadapter.LINUX:
        _sctl("disable", "--now", UNIT_NAME)
        UNIT_PATH.unlink(missing_ok=True)
        _sctl("daemon-reload")
        return "verwijderd (lopende taakeenheden zijn niet aangeraakt)"
    _ctl("bootout", f"{_domain()}/{LABEL}")   # stopt alleen de watch; taakprocessen zitten in eigen sessies
    plist_path.unlink(missing_ok=True)
    return "verwijderd (lopende taakprocessen zijn niet aangeraakt)"


def status() -> str:
    if osadapter.WINDOWS:
        r = _schtasks("/Query", "/TN", WINDOWS_TASK, "/FO", "LIST", "/V")
        if r.returncode:
            return "niet geladen"
        fields = {k.strip(): v.strip() for k, _, v in (l.partition(":") for l in r.stdout.splitlines()) if v.strip()}
        info = {"taak": fields.get("Status", "?"), "laatste resultaat": fields.get("Last Result", "?")}
    elif osadapter.LINUX:
        r = _sctl("show", UNIT_NAME, "-p", "ActiveState", "-p", "SubState", "-p", "MainPID", "-p", "NRestarts", "-p", "UnitFileState")
        info = dict(l.partition("=")[::2] for l in r.stdout.splitlines())
        if info.get("UnitFileState") in (None, ""):
            return "niet geladen"
    else:
        r = _ctl("print", f"{_domain()}/{LABEL}")
        if r.returncode:
            return "niet geladen"
        info = {k: v.strip() for k, _, v in (l.strip().partition(" = ") for l in r.stdout.splitlines())
                if k in ("state", "pid", "runs", "last exit code")}
    hb = config.r2_home() / "watch.json"
    if hb.exists():
        h = json.loads(hb.read_text())
        info["laatste scan"] = f"{time.time() - h['epoch']:.0f}s geleden, cpu {h['cpu_s']}s"
    return " ".join(f"{k}={v}" for k, v in info.items())
