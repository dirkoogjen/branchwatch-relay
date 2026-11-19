"""Hooks van Runner 2: PreToolUse (veilige proceswacht), Stop en SessionStart.

De Stop-hook beoordeelt geen codekwaliteit. Claude Code kapt zelf een eindeloze blokkadelus af.
"""
import json
import os
import sys
from pathlib import Path

from . import store
from .waitguard import REASON, unsafe_process_wait

MAX_SIGNAL_BLOCKS = 2


def stop_decision(event: dict, tdir: Path, turn: int) -> str | None:
    """Geeft de blokkadereden, of None als de beurt mag eindigen."""
    if (tdir / "pause").exists():
        return None
    bg = event.get("background_tasks") or []
    if bg:
        names = ", ".join((t.get("description") or t.get("id") or "?")[:60] for t in bg)
        return f"Er loopt nog achtergrondwerk ({names}). Wacht op het resultaat voordat je je beurt beëindigt."
    if store.read_signals(tdir, turn):
        return None
    counts = tdir / f"hook-blocks.{turn}"
    n = int(counts.read_text()) if counts.exists() else 0
    if n >= MAX_SIGNAL_BLOCKS:
        return None
    counts.write_text(str(n + 1))
    return ("Je beurt eindigt zonder runner_finish of runner_ask. Ben je klaar: commit en roep "
            "mcp__r2__runner_finish aan. Heb je een echte beslissing nodig: mcp__r2__runner_ask. Anders: ga verder.")


def main() -> None:
    name = sys.argv[1] if len(sys.argv) > 1 else ""
    tdir, turn = os.environ.get("R2_TASK_DIR"), int(os.environ.get("R2_TURN", "0"))
    if not tdir:
        return
    tdir = Path(tdir)
    if name == "start":
        (tdir / f"session-start.{turn}").write_text(store.now())
    elif name == "pretool":
        event = json.loads(sys.stdin.read() or "{}")
        if event.get("tool_name") == "Bash" and unsafe_process_wait((event.get("tool_input") or {}).get("command")):
            print(REASON, file=sys.stderr)
            raise SystemExit(2)
    elif name == "stop":
        reason = stop_decision(json.loads(sys.stdin.read() or "{}"), tdir, turn)
        if reason:
            print(json.dumps({"decision": "block", "reason": reason}))


if __name__ == "__main__":
    main()
