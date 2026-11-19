"""Exclusieve bestandslock voor alle platformen: flock op POSIX, msvcrt-byte-lock op Windows.

De lock hoort bij het geopende bestand en vervalt zodra het bestand sluit (of het proces sterft).
"""
import os
import sys
import time

WINDOWS = sys.platform == "win32"

if WINDOWS:
    import msvcrt

    def lock_exclusive(f, blocking: bool = True) -> None:
        """Neemt de lock; niet-blokkerend gooit OSError als een ander de lock heeft."""
        f.seek(0)
        while True:
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError:
                if not blocking:
                    raise
                time.sleep(0.05)
else:
    import fcntl

    def lock_exclusive(f, blocking: bool = True) -> None:
        fcntl.flock(f, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))


def replace(src, dst, attempts: int = 40) -> None:
    """Atomair vervangen, ook als het doel al bestaat (os.rename faalt daarin op Windows).

    Op Windows mislukt vervangen kortstondig (PermissionError) zolang een lezer het doel open heeft; dan opnieuw proberen.
    """
    for n in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if not WINDOWS or n == attempts - 1:
                raise
            time.sleep(0.05)
