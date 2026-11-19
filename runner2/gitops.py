"""Git-worktree per taak en droge integratiecontrole (verplaatst nooit een ref)."""
import subprocess
from pathlib import Path


def git(repo, *args, check=True) -> subprocess.CompletedProcess:
    r = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and r.returncode:
        raise RuntimeError(f"git {' '.join(args)}: {r.stderr.strip()}")
    return r


def create_worktree(repo: str, worktree: Path, branch: str, start: str | None = None, ref: str | None = None) -> dict:
    """Nieuwe branch op de huidige HEAD (of op `start`, bijv. de tip van een vorige ketenschakel); de hoofdcheckout
    (ook oncommitted werk) blijft onaangeroerd. `ref` overschrijft de basisbranch voor de droge integratiecontrole."""
    head = start or git(repo, "rev-parse", "HEAD").stdout.strip()
    ref = ref or git(repo, "symbolic-ref", "--short", "-q", "HEAD", check=False).stdout.strip() or head
    dirty = bool(git(repo, "status", "--porcelain").stdout.strip())
    worktree.parent.mkdir(parents=True, exist_ok=True)
    git(repo, "worktree", "add", "-b", branch, str(worktree), head)
    return {"ref": ref, "commit": head, "main_checkout_dirty": dirty}


def head_commit(worktree) -> str:
    return git(worktree, "rev-parse", "HEAD").stdout.strip()


def _is_bytecode(path: str) -> bool:
    return path.endswith((".pyc", ".pyo")) or "__pycache__" in path.rstrip("/").split("/")


def uncommitted(worktree) -> list[str]:
    """Paden met ongecommitte wijzigingen volgens git status, zonder door Python gegenereerde bytecode
    (__pycache__/, *.pyc): een testrun in een repository zonder .gitignore liet anders elke schakel 'vuil' achter en
    stopte de keten met dirty_worktree terwijl al het werk gecommit was. Hernoemingen tellen altijd mee."""
    fields, paths, i = git(worktree, "status", "--porcelain", "-z").stdout.split("\0"), [], 0
    while i < len(fields):
        entry, i = fields[i], i + 1
        if not entry:
            continue
        code, path = entry[:2], entry[3:]
        if "R" in code or "C" in code:              # ook " R": hernoeming in de worktree (na git add -N)
            i += 1                                  # -z: het oorspronkelijke pad volgt als apart veld
        elif _is_bytecode(path):
            continue
        elif code == "??" and path.endswith("/"):   # samengevouwen nieuwe map: alleen meetellen wat geen bytecode is
            inner = git(worktree, "ls-files", "--others", "--exclude-standard", "-z", "--", path).stdout.split("\0")
            inner = [f for f in inner if f]
            paths += [f for f in inner if not _is_bytecode(f)] if inner else [path]   # niets te zien: voorzichtig vuil
            continue
        paths.append(path)
    return paths


def is_clean(worktree) -> bool:
    return not uncommitted(worktree)


def commits_between(worktree, base: str) -> int:
    return int(git(worktree, "rev-list", "--count", f"{base}..HEAD").stdout.strip() or 0)


def dry_run_integration(state: dict) -> dict:
    """Rapporteert fast-forward/conflicten t.o.v. de basisbranch, zonder iets te verplaatsen."""
    wt, base_ref = state["worktree"], state["base"]["ref"]
    tip = git(wt, "rev-parse", base_ref).stdout.strip()
    behind, ahead = git(wt, "rev-list", "--left-right", "--count", f"{base_ref}...HEAD").stdout.split()
    merge = git(wt, "merge-tree", "--write-tree", base_ref, "HEAD", check=False)
    return {
        "base_ref": base_ref, "base_tip": tip, "commits_ahead": int(ahead), "base_moved_by": int(behind),
        "fast_forward": int(behind) == 0, "conflicts": merge.returncode == 1,
        "worktree_clean": is_clean(wt),
    }


def discard_worktree(repo: str, worktree: Path, branch: str) -> None:
    """Ruimt een half aangemaakte taakworktree op (best effort); alleen voor een eigen, nog ongebruikte taakbranch."""
    git(repo, "worktree", "remove", "--force", str(worktree), check=False)
    if Path(worktree).exists():
        import shutil
        shutil.rmtree(worktree, ignore_errors=True)
    git(repo, "worktree", "prune", check=False)
    git(repo, "branch", "-D", branch, check=False)


def is_ancestor(repo, ancestor: str, descendant: str = "HEAD") -> bool:
    return git(repo, "merge-base", "--is-ancestor", ancestor, descendant, check=False).returncode == 0
