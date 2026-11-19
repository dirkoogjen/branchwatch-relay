"""Idempotent Ubuntu read-only model worker. Never falls back to programming mode."""
import copy
import json
import os
import re
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from . import config, discussions as d, store, procs
from .filelock import lock_exclusive
from .discussion_guard import TOOLS, check

PUBLIC = ('conversation_id', 'turn_id', 'execution_id', 'node', 'repository', 'model', 'status', 'answer',
          'reason', 'code', 'heartbeat', 'started', 'finished', 'pid', 'pid_start', 'starts')
REQUIRED_FLAGS = ('--restricted', '--safe-mode', '--setting-sources', '--strict-mcp-config', '--no-session-persistence', '--tools')


def execution_dir(eid):
    return config.r2_home() / 'discussion-executions' / d._id(eid, d.TID)


@contextmanager
def _lock(eid):
    p = execution_dir(eid);p.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (p / 'state.lock').open('a+') as f:
        lock_exclusive(f)
        yield p


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_suffix('.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(value, f);f.flush();os.fsync(f.fileno())
    os.replace(tmp, path)


def write_state(state):
    with _lock(state['execution_id']) as path:
        _write(path / 'state.json', state)


def _read(eid):
    try:
        v = json.loads((execution_dir(eid) / 'state.json').read_text())
        if v['execution_id'] != eid or v['turn_id'] != eid or v['status'] not in d.STATES:
            raise ValueError()
        return v
    except FileNotFoundError:
        raise ValueError('Overleguitvoering niet gevonden.') from None
    except (ValueError, KeyError, TypeError):
        raise ValueError('Overleguitvoering is beschadigd; niet opnieuw gestart.') from None


def status(execution_id):
    s = _read(execution_id)
    if s.get('pid'):
        try:os.waitpid(s['pid'], os.WNOHANG)
        except ChildProcessError:pass
    try:old_claim = time.time() - datetime.fromisoformat(s.get('started', '')).timestamp() > 30
    except ValueError:old_claim = True
    lost = (procs.proc_start(s['pid']) != s.get('pid_start')) if s.get('pid') else old_claim
    if s['status'] in ('running', 'starting') and lost:
        with _lock(execution_id) as path:
            s = _read(execution_id)
            if s['status'] in ('running', 'starting'):
                s.update(status='failed', code='worker_lost', reason='De overlegworker is gestopt; de vraag en ontvangen tekst zijn bewaard.', finished=store.now())
                _write(path / 'state.json', s)
    return {k: s.get(k) for k in PUBLIC}


def active_local():
    directory = config.r2_home() / 'discussion-executions'
    for p in directory.glob('DT-*/state.json'):
        s = status(p.parent.name)
        if s['status'] in d.PENDING:
            return s
    return None


def validate(q):
    allowed = {'conversation_id', 'turn_id', 'execution_id', 'node', 'repository', 'model', 'context'}
    if not isinstance(q, dict) or set(q) != allowed:
        raise ValueError('Ongeldig overlegverzoek.')
    d._id(q['conversation_id'], d.CID);d._id(q['turn_id'], d.TID)
    if q['execution_id'] != q['turn_id'] or q['node'] not in ('GERARD', 'LINUX'):
        raise ValueError('Overleguitvoering hoort niet bij deze beurt.')
    if not isinstance(q['repository'], str) or (q['repository'] != '' and not Path(q['repository']).is_absolute()):
        raise ValueError('Ongeldig project.')
    if not isinstance(q['context'], str) or not q['context'].strip() or len(q['context']) > d.MAX_CONTEXT:
        raise ValueError('Ongeldige of te lange gesprekscontext.')
    if not isinstance(q['model'], str) or not q['model'].startswith('claude-') or len(q['model']) > 80:
        raise ValueError('Ongeldig model.')
    return q


def claude_command(binary, model, sources=None):
    directories = ["/context/" + name for name in (sources or {})]
    return [binary, '-p', '--output-format', 'stream-json', '--verbose', '--model', model,
            '--restricted', '--safe-mode', '--tools', 'Read,Glob,Grep', '--allowedTools', 'Read', 'Glob', 'Grep',
            '--permission-mode', 'dontAsk', '--permission-prompts', 'none', '--setting-sources', '',
            '--strict-mcp-config', '--mcp-config', '{"mcpServers":{}}', '--no-session-persistence',
            '--append-system-prompt', 'Je voert uitsluitend overleg. Lees alleen /project en de expliciet beschikbare codebronnen onder /context. Geen uitvoering, wijzigingen of integratie. Antwoord in het Nederlands. De actuele omgevingsinformatie bij deze beurt gaat voor aannames uit oudere antwoorden. Zonder projectcode kun je nog steeds inhoudelijk overleggen en gericht vragen naar ontbrekende informatie. Toestemming in de chat verandert de beschikbare paden niet; vraag nooit om code naar /project te kopiëren of om sandboxpaden op de host aan te maken.'] + (['--add-dir', *directories] if directories else [])


def read_sources(repository, cfg=None):
    """Local administrator mappings, separate from executable job repositories."""
    cfg = cfg if cfg is not None else config.load_config()
    if not repository:return {}
    d.project(cfg.get('node', '').upper(), repository, cfg)
    sources = (cfg.get('discussion_sources') or {}).get(repository, {})
    if not isinstance(sources, dict) or len(sources) > 8:raise ValueError('Ongeldige overlegbronnen.')
    for name, value in sources.items():
        if not isinstance(name, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,39}', name):
            raise ValueError('Ongeldige naam voor overlegbron.')
        if not isinstance(value, str):raise ValueError('Ongeldige overlegbron.')
        path = Path(value)
        if not path.is_absolute() or str(path) != value or path.resolve() != path or not path.is_dir():
            raise ValueError('Overlegbron ontbreekt of is niet canoniek.')
    return dict(sources)


def execution_context(q, sources):
    lines = ['ACTUELE OVERLEGOMGEVING (gaat voor aannames uit oudere antwoorden):']
    if q['repository']:
        lines.append('Gekozen bronproject: ' + q['repository'] + '; hier alleen-lezen beschikbaar als /project. Dit is broncode, niet automatisch de actuele productieversie.')
    else:
        lines.append('Algemeen overleg: er is geen project gekozen; /project is leeg. Geef waar mogelijk een inhoudelijk voorstel en stel alleen gerichte vragen naar ontbrekende informatie. Verzin geen projectmap.')
    for name, host in sources.items():
        lines.append('Actuele aanvullende codebron: /context/' + name + ' (host: ' + host + '). Gebruik dit exacte sandboxpad rechtstreeks, niet de hostlocatie of de bovenliggende map /context. Lees deze bij vragen over de huidige werking; deze kan nieuwer zijn dan /project. Dit pad is al beschikbaar, extra toestemming is niet nodig.')
    lines.append('Andere hostpaden zijn niet beschikbaar. Een URL bewijst geen lokale projectmap. Houd Monitor/Runner en het websiteproject uit elkaar. Vraag bij ontbrekende code om in een nieuw gesprek het bedoelde project te kiezen of om een relevant fragment; een extra toestemming kan geen map in deze omgeving ontsluiten. Historische antwoorden kunnen verouderde conclusies bevatten; controleer die aan de beschikbare code.')
    return '\n'.join(lines) + '\n\nGESPREKSGESCHIEDENIS:\n' + q['context']


def sandbox_command(binary, repo, scratch, credentials=None, sources=None):
    if sys.platform != 'linux' or not shutil.which('bwrap'):
        raise ValueError('De verplichte Ubuntu-leesbeveiliging is niet beschikbaar.')
    repo = Path(repo)
    if not repo.is_dir() or repo.resolve() != repo:
        raise ValueError('Het projectpad is niet canoniek.')
    scratch = Path(scratch);home = scratch / 'home';home.mkdir(parents=True, exist_ok=True, mode=0o700)
    (home / '.claude').mkdir(exist_ok=True, mode=0o700)
    (home / '.claude.json').write_text('{"hasCompletedOnboarding":true}')
    cmd = [shutil.which('bwrap'), '--die-with-parent', '--new-session', '--unshare-pid', '--unshare-ipc', '--unshare-uts']
    for p in ('/usr', '/bin', '/lib', '/lib64'):
        if Path(p).exists():cmd += ['--ro-bind', p, p]
    for p in ('/etc/resolv.conf', '/etc/hosts', '/etc/nsswitch.conf', '/etc/ssl/certs', '/etc/ca-certificates', '/etc/passwd'):
        if Path(p).exists():cmd += ['--ro-bind', p, p]
    cmd += ['--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp', '--bind', str(home), '/home/discussion',
            '--ro-bind', str(repo), '/project', '--ro-bind', str(Path(binary).resolve()), '/cli/claude']
    cmd += ['--tmpfs', '/context']
    for name, source in (sources or {}).items():
        cmd += ['--ro-bind', source, '/context/' + name]
    cmd += ['--remount-ro', '/context']
    for name in ('.git', '.claude', '.mcp.json'):
        path = repo / name
        if path.is_dir():cmd += ['--tmpfs', '/project/' + name, '--remount-ro', '/project/' + name]
        elif path.exists():cmd += ['--ro-bind', '/dev/null', '/project/' + name]
    if credentials and Path(credentials).is_file():
        (home / '.claude/.credentials.json').touch(mode=0o600, exist_ok=True)
        cmd += ['--ro-bind', str(credentials), '/home/discussion/.claude/.credentials.json']
    cmd += ['--setenv', 'HOME', '/home/discussion', '--setenv', 'CLAUDE_CONFIG_DIR', '/home/discussion/.claude',
            '--setenv', 'PATH', '/usr/bin:/bin', '--chdir', '/project', '--remount-ro', '/', '/cli/claude']
    return cmd


class Events:
    def __init__(self, version, project='/project', sources=None):
        self.version, self.project = version, project
        self.sources = sources or {}
        self.refusal = None
        self.initialized, self.finished, self.answer = False, False, ''
        self.partial, self.code, self.reason = '', '', ''

    def fail(self, code, reason):
        self.code, self.reason = code, reason

    def feed(self, line):
        if len(line) > 1_000_000:
            self.fail('answer_limit', 'Claude gaf te grote uitvoer.');return
        try:v = json.loads(line)
        except (ValueError, TypeError):
            self.fail('invalid_output', 'Claude gaf onleesbare uitvoer.');return
        if not isinstance(v, dict):
            self.fail('invalid_output', 'Claude gaf onleesbare uitvoer.');return
        kind = v.get('type')
        if kind == 'system' and v.get('subtype') == 'init':
            tools = v.get('tools')
            if self.initialized or not isinstance(tools, list) or set(tools) != TOOLS or v.get('permissionMode') != 'dontAsk' or v.get('claude_code_version') != self.version or v.get('mcp_servers'):
                self.fail('unsafe_profile', 'Het vereiste leesprofiel is niet bevestigd.');return
            self.initialized = True
        elif kind == 'assistant':
            if not self.initialized:
                self.fail('unsafe_profile', 'Leesprofiel ontbreekt.');return
            for item in (v.get('message') or {}).get('content') or []:
                if item.get('type') == 'text':self.partial += str(item.get('text', ''))
                elif item.get('type') == 'tool_use':
                    args = copy.deepcopy(item.get('input') or {})
                    base = self.project
                    for field in ('file_path', 'path'):
                        value = args.get(field)
                        if not isinstance(value, str):continue
                        for visible, host in [('/project', self.project)] + [('/context/' + k, v) for k, v in self.sources.items()]:
                            if value == visible or value.startswith(visible + '/'):
                                base = host
                                args[field] = str(Path(host) / value[len(visible):].lstrip('/'))
                                break
                    if not check(item.get('name'), args, base)['allowed']:
                        original = item.get('input') or {}
                        self.refusal = {'tool': item.get('name'), 'path': original.get('path'), 'file_path': original.get('file_path')}
                        self.fail('unsafe_tool', 'Claude vroeg om toegang buiten het leesprofiel.');return
            if len(self.partial) > 200_000:self.fail('answer_limit', 'Het antwoord is langer dan 200.000 tekens.')
        elif kind == 'result':
            text = v.get('result', '')
            if v.get('is_error') or v.get('subtype') != 'success':
                from .taskproc import auth_problem
                auth = bool(auth_problem(str(text)))
                self.fail('auth_error' if auth else 'model_error', 'Claude moet opnieuw worden aangemeld.' if auth else 'Claude kon deze vraag niet beantwoorden.');return
            if not self.initialized or not isinstance(text, str) or not text.strip() or len(text) > 200_000:
                self.fail('invalid_answer', 'Er is geen volledig geldig antwoord ontvangen.');return
            self.answer, self.finished = text, True

    def result(self):
        if self.code or not self.finished:
            return {'status': 'failed', 'code': self.code or 'incomplete_answer', 'reason': self.reason or 'Het antwoord is afgebroken.', 'answer': self.partial[:200_000]}
        return {'status': 'completed', 'code': '', 'reason': 'Antwoord klaar.', 'answer': self.answer}


def parse_events(lines, expected_version, project='/project'):
    e = Events(expected_version, project)
    for line in lines:
        e.feed(line)
        if e.code:break
    return e.result()


def start(request):
    q = validate(copy.deepcopy(request));cfg = config.load_config()
    if q['node'] != config.local_node():raise ValueError('Overleg is aan een andere machine gericht.')
    d.project(q['node'], q['repository'], cfg)
    sources = read_sources(q['repository'], cfg)
    if q['repository'] and Path(q['repository']).resolve() != Path(q['repository']):raise ValueError('Kies het canonieke projectpad.')
    if q['model'] != cfg.get('model_implement', 'claude-sonnet-5-5'):raise ValueError('Onbekend overlegmodel.')
    from . import controller
    with controller._start_lock():
        path = execution_dir(q['execution_id'])
        if (path / 'state.json').exists():
            old = _read(q['execution_id'])
            if any(old.get(k) != q[k] for k in q):raise ValueError('Uitvoeringsnummer is al voor andere inhoud gebruikt.')
            return status(q['execution_id'])
        controller._require_free()
        if active_local():raise ValueError('Er loopt al een overleg.')
        binary = cfg.get('claude_path') or shutil.which('claude')
        if not binary:raise ValueError('Claude is niet geïnstalleerd.')
        version = cfg.get('expected_claude_version')
        actual = subprocess.run([binary, '--version'], capture_output=True, text=True, timeout=20)
        helptext = subprocess.run([binary, '--help'], capture_output=True, text=True, timeout=20)
        if actual.returncode or not actual.stdout.startswith(version or '\x00') or helptext.returncode or any(f not in helptext.stdout for f in REQUIRED_FLAGS):
            raise ValueError('Deze Claude-versie ondersteunt het vereiste leesprofiel niet.')
        if sys.platform != 'linux' or not shutil.which('bwrap'):raise ValueError('De verplichte Ubuntu-leesbeveiliging ontbreekt.')
        s = {**q, 'status': 'starting', 'answer': '', 'reason': 'Overleg wordt gestart.', 'starts': 1, 'started': store.now(), 'heartbeat': store.now()}
        with _lock(q['execution_id']) as path:
            _write(path / 'request.json', {**q, 'binary': str(Path(binary).resolve()), 'expected_version': version, 'sources': sources})
            _write(path / 'state.json', s)
            env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parent.parent), 'R2_HOME': str(config.r2_home())}
            actions = [(os.POSIX_SPAWN_OPEN, fd, os.devnull, os.O_RDONLY if fd == 0 else os.O_WRONLY, 0o600) for fd in (0, 1, 2)]
            pid = os.posix_spawn(sys.executable, [sys.executable, '-m', 'runner2.discussion_worker', str(path / 'request.json')], env, setsid=True, file_actions=actions)
            s.update(pid=pid, pid_start=procs.proc_start(pid));_write(path / 'state.json', s)
        return status(q['execution_id'])


def stop(execution_id, reason='Stoppen aangevraagd.'):
    with _lock(execution_id) as path:
        s = _read(execution_id)
        if s['status'] in d.TERMINAL:return {k: s.get(k) for k in PUBLIC}
        pid, identity = s.get('pid'), s.get('pid_start')
        match = bool(pid and identity and identity != procs.UNKNOWN and procs.proc_start(pid) == identity)
        s.update(status='stopped' if match else 'failed', code='' if match else 'worker_lost',
                 reason='Overleg gestopt.' if match else 'De eerdere overlegworker bestaat niet meer.', finished=store.now())
        _write(path / 'state.json', s)
        if match:
            try:os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:pass
    return status(execution_id)


def stop_request(request):
    """A stop arriving before a lost/delayed start writes a permanent, bound tombstone."""
    q = validate(copy.deepcopy(request))
    if q['node'] != config.local_node():raise ValueError('Verkeerde overlegnode.')
    from . import controller
    with controller._start_lock():
        path = execution_dir(q['execution_id'])
        if not (path / 'state.json').exists():
            write_state({**q, 'status': 'stopped', 'answer': '', 'reason': 'Vraag geannuleerd vóór starten.', 'finished': store.now(), 'starts': 0})
        else:
            old = _read(q['execution_id'])
            if any(old.get(k) != v for k, v in q.items()):raise ValueError('Stop hoort niet bij deze overleguitvoering.')
        return stop(q['execution_id'])


def run(path):
    q = json.loads(Path(path).read_text());eid = q['execution_id'];child = None;machine = store.MachineLock(eid);acquired = False
    try:
        with _lock(eid) as directory:
            s = _read(eid)
            if s['status'] in d.TERMINAL:return
            s.update(pid=os.getpid(), pid_start=procs.proc_start(os.getpid()));_write(directory / 'state.json', s)
        machine.acquire();acquired = True
        sources = q.get('sources', {})
        repo = Path(q['repository']) if q['repository'] else execution_dir(eid) / 'empty-project'
        if not q['repository']:repo.mkdir(exist_ok=True, mode=0o700)
        cmd = sandbox_command(q['binary'], repo, execution_dir(eid) / 'scratch', Path.home() / '.claude/.credentials.json', sources=sources)
        cmd += claude_command('/cli/claude', q['model'], sources=sources)[1:]
        env = {'LANG': 'C.UTF-8', 'PATH': '/usr/bin:/bin', 'HOME': '/home/discussion'}
        with tempfile.TemporaryFile(dir=execution_dir(eid)) as prompt_input:
            prompt_input.write(execution_context(q, sources).encode());prompt_input.seek(0)
            child = subprocess.Popen(cmd, stdin=prompt_input, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, start_new_session=True)
        sel = selectors.DefaultSelector();sel.register(child.stdout, selectors.EVENT_READ)
        events = Events(q['expected_version'], str(repo), sources=sources);buffer = b'';start_time = time.monotonic();last_output = start_time
        while True:
            if time.monotonic() - start_time > 1800 or time.monotonic() - last_output > 180:
                events.fail('timeout', 'Claude reageert niet meer; de vraag is bewaard.');break
            with _lock(eid) as directory:
                s = _read(eid)
                if s['status'] in d.TERMINAL:break
                s.update(status='running', heartbeat=store.now(), answer=events.partial[:200_000], reason='Claude antwoordt.')
                _write(directory / 'state.json', s)
            ready = sel.select(2)
            if not ready:
                if child.poll() is not None:break
                continue
            chunk = os.read(child.stdout.fileno(), 65536)
            if not chunk:break
            last_output = time.monotonic();buffer += chunk
            if len(buffer) > 1_000_000:
                events.fail('answer_limit', 'Claude gaf te grote uitvoer.');break
            while b'\n' in buffer:
                line, buffer = buffer.split(b'\n', 1)
                if line.strip():events.feed(line.decode('utf-8'))
                if events.code:break
            if events.code:break
        if buffer.strip() and not events.code:events.feed(buffer.decode('utf-8'))
        with _lock(eid) as directory:
            s = _read(eid)
            if s['status'] not in d.TERMINAL:
                if events.refusal:_write(directory / 'tool-refusal.json', events.refusal)
                s.update(events.result(), finished=store.now(), heartbeat=store.now());_write(directory / 'state.json', s)
    except Exception:
        with _lock(eid) as directory:
            s = _read(eid)
            if s['status'] not in d.TERMINAL:
                s.update(status='failed', code='worker_error', reason='De beveiligde overleguitvoering kon niet worden voltooid.', finished=store.now());_write(directory / 'state.json', s)
    finally:
        if child and child.poll() is None:
            try:os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:pass
            child.wait(timeout=10)
        if child:
            if child.stdout:child.stdout.close()
            if child.stdin and not child.stdin.closed:child.stdin.close()
        if 'sel' in locals():sel.close()
        if acquired:machine.release()


if __name__ == '__main__':
    run(sys.argv[1])
