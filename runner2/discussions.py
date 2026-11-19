"""Saved read-only design conversations, separate from the programming queue."""
import copy
import hashlib
import json
import os
import re
import uuid
from contextlib import contextmanager
from pathlib import Path

from . import config, cplane, store
from .filelock import lock_exclusive

CID = re.compile(r'D-[0-9a-f]{12}\Z')
TID = re.compile(r'DT-[0-9a-f]{12}\Z')
PENDING = ('queued', 'starting', 'running')
TERMINAL = ('completed', 'failed', 'stopped')
STATES = PENDING + TERMINAL
MAX_CONTEXT = 240_000


def root():
    return config.r2_home() / 'discussions'


def _id(value, pattern):
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError('Ongeldig gespreks- of beurtnummer.')
    return value


def _path(cid):
    return root() / 'conversations' / (_id(cid, CID) + '.json')


def load(conversation_id):
    try:
        c = json.loads(_path(conversation_id).read_text())
        if not isinstance(c, dict) or c['conversation_id'] != conversation_id or not isinstance(c['turns'], list):
            raise ValueError()
        for t in c['turns']:
            _id(t['turn_id'], TID)
            if t['status'] not in STATES:
                raise ValueError()
        return c
    except (OSError, ValueError, KeyError, TypeError):
        raise ValueError('Gesprek ontbreekt of de opgeslagen gegevens zijn beschadigd; niets overschreven.') from None


def all_conversations():
    directory = root() / 'conversations'
    return sorted((load(p.stem) for p in directory.glob('D-*.json')), key=lambda c: c['created'], reverse=True)


@contextmanager
def _locked():
    root().mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root() / 'storage.lock').open('a+') as f:
        lock_exclusive(f)
        yield


def _save(c):
    p = _path(c['conversation_id'])
    p.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Private even during atomic replacement, independent of the service umask.
    tmp = p.with_name(p.name + '.private-' + uuid.uuid4().hex)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(c, f, ensure_ascii=False);f.flush();os.fsync(f.fileno())
        os.replace(tmp, p)
    finally:
        tmp.unlink(missing_ok=True)


def _text(value, limit, name):
    if not isinstance(value, str) or not value.strip() or len(value) > limit or '\x00' in value:
        raise ValueError(f'{name} ontbreekt of is te lang (maximaal {limit} tekens).')
    return value.strip()


def _version(c, version):
    if type(version) is not int or version != c['version']:
        raise ValueError('Het gesprek is ondertussen gewijzigd; ververs en probeer opnieuw.')


def project(node, repository, cfg=None):
    cfg = cfg or config.load_config()
    if node not in ('GERARD', 'LINUX'):
        raise ValueError('Overleg is alleen beschikbaar op GERARD en STUDIO.')
    if repository == '':return ''  # General discussion has no filesystem context.
    entries = (cfg.get('jobs_integration_workflows') or {}).get(node, {})
    if not isinstance(repository, str) or repository not in entries:
        raise ValueError('Kies een bekend, toegestaan project op deze machine.')
    if not repository.startswith('/') or str(Path(repository)) != repository or '..' in Path(repository).parts:
        raise ValueError('Kies het exacte projectpad uit de projectlijst.')
    return repository


def context(c, include_rules=True):
    parts = ['Bespreek het ontwerp met de gebruiker. Lees uitsluitend de projectcode. Voer niets uit en wijzig niets.']
    if not include_rules:parts = []
    for t in c['turns']:
        parts.append('Gebruiker:\n' + t['question'])
        if t.get('answer'):
            parts.append(('Claude (onvolledig):\n' if t['status'] != 'completed' else 'Claude:\n') + t['answer'])
    text = '\n\n'.join(parts)
    if len(text) > MAX_CONTEXT:
        raise ValueError('Het gesprek is te lang. Begin een nieuw gesprek en neem zelf een zichtbare samenvatting mee.')
    return text


def update_turn(conversation_id, turn_id, expected_version, patch):
    allowed = {'status', 'answer', 'reason', 'heartbeat', 'started', 'finished', 'execution_id', 'stop_requested'}
    if not isinstance(patch, dict) or set(patch) - allowed:
        raise ValueError('Ongeldige beurtwijziging.')
    with _locked():
        c = load(conversation_id);_version(c, expected_version)
        t = next((t for t in c['turns'] if t['turn_id'] == turn_id), None)
        if t is None or t['status'] in TERMINAL:
            raise ValueError('Deze beurt kan niet meer worden gewijzigd.')
        if patch.get('status', t['status']) not in STATES:
            raise ValueError('Ongeldige beurtstatus.')
        if 'answer' in patch and (not isinstance(patch['answer'], str) or len(patch['answer']) > 200_000):
            raise ValueError('Antwoord is ongeldig of te lang.')
        if patch.get('status') == 'completed' and not str(patch.get('answer', t.get('answer', ''))).strip():
            raise ValueError('Claude heeft geen antwoord gegeven.')
        t.update(copy.deepcopy(patch));c['version'] += 1;c['updated'] = store.now();_save(c)
        return copy.deepcopy(c)


def handle(args, by):
    if not isinstance(args, dict):
        raise ValueError('Ongeldig overlegverzoek.')
    action = args.get('action', 'list')
    fields = {'action', 'conversation_id', 'turn_id', 'version', 'node', 'repository', 'model', 'question', 'title', 'nonce'}
    if set(args) - fields:
        raise ValueError('Onbekend overlegveld.')
    if action == 'list':
        rows = []
        for c in all_conversations():
            row = {k: c[k] for k in ('conversation_id', 'version', 'node', 'repository', 'model', 'title', 'created', 'updated')}
            row.update(status=c['turns'][-1]['status'], reason=c['turns'][-1].get('reason', ''), turn_count=len(c['turns']))
            rows.append(row)
        cfg = config.load_config()
        projects = {node: list(entries) for node, entries in (cfg.get('jobs_integration_workflows') or {}).items() if node in ('GERARD', 'LINUX') and isinstance(entries, dict)}
        return {'conversations': rows, 'projects': projects, 'model': cfg.get('model_implement', 'claude-sonnet-5-5')}
    if action in ('get', 'draft'):
        c = load(args.get('conversation_id'))
        if action == 'get':
            return {'conversation': c}
        _version(c, args.get('version'))
        if not c['repository']:raise ValueError('Kies eerst een project voor een uitvoeropdracht; kopieer je ontwerp naar een nieuw projectgesprek.')
        if c['turns'][-1]['status'] != 'completed':
            raise ValueError('Wacht eerst op een volledig antwoord.')
        return {'draft': {'prompt': 'Maak de besproken functionaliteit volgens de hieronder gekozen ontwerpafspraken. Het overleg staat hieronder als context; dit is nu een uitvoeropdracht.\n\n' + f"Bron: gesprek {c['conversation_id']}, versie {c['version']}.\n\n" + context(c, include_rules=False),
                          'node': c['node'], 'repository': c['repository'],
                          'source': {'conversation_id': c['conversation_id'], 'turn_id': c['turns'][-1]['turn_id'], 'version': c['version']}}}
    if action not in ('ask', 'cancel'):
        raise ValueError('Onbekende overlegactie.')
    cplane.require_primary()
    from . import jobs
    jobs._check_fence()
    by = _text(by, 100, 'Gebruiker')
    nonce = _text(args.get('nonce'), 120, 'nonce')
    fingerprint = hashlib.sha256(json.dumps({k: v for k, v in args.items() if k != 'nonce'}, sort_keys=True).encode()).hexdigest()
    with _locked():
        conversations = all_conversations()
        for existing in conversations:
            for receipt in existing.get('receipts', []):
                if receipt['by'] == by and receipt['nonce'] == nonce:
                    if receipt['fingerprint'] != fingerprint:
                        raise ValueError('Deze nonce is al gebruikt voor andere inhoud.')
                    return {'conversation': existing}
        if action == 'cancel':
            c = load(args.get('conversation_id'));_version(c, args.get('version'))
            t = c['turns'][-1]
            if t['turn_id'] != args.get('turn_id') or t['status'] not in PENDING:
                raise ValueError('Deze beurt kan niet worden gestopt.')
            if t['status'] == 'queued':
                t.update(status='stopped', reason='Vraag geannuleerd.', finished=store.now())
            else:
                t.update(stop_requested=True, reason='Stoppen aangevraagd; bestaande uitvoering wordt gecontroleerd.')
            c['version'] += 1
        else:
            question = _text(args.get('question'), 20_000, 'Vraag')
            cid = args.get('conversation_id')
            if cid:
                c = load(cid);_version(c, args.get('version'))
                if any(t['status'] in PENDING for t in c['turns']):
                    raise ValueError('Wacht eerst op het antwoord op de vorige vraag.')
                if len(c['turns']) >= 50:
                    raise ValueError('Dit gesprek bevat al 50 beurten; begin een nieuw gesprek.')
                for field in ('node', 'repository', 'model'):
                    if field in args and args[field] != c[field]:
                        raise ValueError('Project, machine en model staan vast voor dit gesprek.')
            else:
                cfg = config.load_config();node = args.get('node');repo = project(node, args.get('repository'), cfg)
                model = cfg.get('model_implement', 'claude-sonnet-5-5')
                if args.get('model', model) != model:
                    raise ValueError('Kies het ingestelde overlegmodel.')
                c = {'conversation_id': 'D-' + uuid.uuid4().hex[:12], 'version': 0, 'node': node,
                     'repository': repo, 'model': model, 'title': _text(args.get('title') or question[:80], 100, 'Titel'),
                     'by': by, 'created': store.now(), 'turns': [], 'receipts': []}
            if sum(t['status'] in PENDING for x in conversations if x['node'] == c['node'] for t in x['turns']) >= 10:
                raise ValueError('Er wachten al 10 vragen op deze machine.')
            tid = 'DT-' + uuid.uuid4().hex[:12]
            c['turns'].append({'turn_id': tid, 'execution_id': tid, 'question': question, 'answer': '',
                               'status': 'queued', 'reason': 'Wacht op een vrije, veilige machine.', 'by': by, 'created': store.now()})
            context(c)
            c['version'] += 1
        c['updated'] = store.now();c['receipts'].append({'by': by, 'nonce': nonce, 'fingerprint': fingerprint})
        _save(c)
        return {'conversation': copy.deepcopy(c)}
