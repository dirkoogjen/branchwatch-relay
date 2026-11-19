"""Start the local Monitor 2 server."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for entry in (str(ROOT / "app"), str(ROOT)):
    sys.path.insert(0, entry)


def main() -> int:
    from obf_monitor2.server import main as serve

    return serve(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
