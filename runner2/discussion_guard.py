"""Discussion tools are read-only and confined to one real project tree."""
from pathlib import Path

TOOLS = frozenset(('Read', 'Glob', 'Grep'))


def check(tool, arguments, project):
    if tool not in TOOLS or not isinstance(arguments, dict):
        return {'allowed': False, 'reason': 'Dit gereedschap is niet beschikbaar tijdens overleg.'}
    try:
        base = Path(project).resolve()
        value = arguments.get('file_path') if tool == 'Read' else arguments.get('path', str(base))
        if not isinstance(value, str) or '\x00' in value:
            raise ValueError()
        path = Path(value)
        path = (path if path.is_absolute() else base / path).resolve()
        if not path.is_relative_to(base):
            raise ValueError()
        if any(p in ('.git', '.claude', '.mcp.json') for p in path.relative_to(base).parts):
            raise ValueError()
        if tool == 'Glob':
            pattern = arguments.get('pattern', '')
            if not isinstance(pattern, str) or pattern.startswith('/') or '..' in Path(pattern).parts:
                raise ValueError()
        return {'allowed': True}
    except (OSError, ValueError, RuntimeError):
        return {'allowed': False, 'reason': 'Alleen bestanden binnen het gekozen project zijn beschikbaar.'}
