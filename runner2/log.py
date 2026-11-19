"""Eenvoudig logbestand met rotatie: ~/.runner2/logs/runner2.log (max 1 MB, één oude generatie)."""
import os
import time

from . import config

MAX_BYTES = 1_000_000


def log(msg: str, name: str = "runner2.log") -> None:
    try:
        d = config.r2_home() / "logs"
        d.mkdir(parents=True, exist_ok=True)
        p = d / name
        if p.exists() and p.stat().st_size > MAX_BYTES:
            os.replace(p, p.with_name(name + ".1"))
        with open(p, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} [{os.getpid()}] {msg}\n")
    except OSError:
        pass
