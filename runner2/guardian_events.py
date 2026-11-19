"""Durable technical hints over the existing restricted NUC RPC; never execution authority."""
import hashlib
import hmac
import json
import math
import os
import re
import shlex
import threading
import time
import subprocess
import urllib.request
from pathlib import Path

from . import config, cplane, jobs, remote, store
from . import guardian_node as node
from .filelock import lock_exclusive

REPO = '/home/operator/StudioOBF'
TECHNICAL = {'tests_failed', 'interrupted', 'api_error', 'stalled', 'retry_exhausted', 'config_error', 'request_missing'}
_thread = None
_last_scan = 0
_seen = {}


def digest(event):
    body = {k: v for k, v in event.items() if k != 'id'}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def validate(event):
    fields = {'schema', 'node', 'repo', 'task_id', 'session_id', 'turn', 'kind', 'request', 'failure_code', 'id'}
    if not isinstance(event, dict) or set(event) != fields:
        raise ValueError('invalid event fields')
    if (event['schema'] != 1 or event['node'] != 'LINUX' or event['repo'] != REPO
            or not isinstance(event['turn'], int) or isinstance(event['turn'], bool) or event['turn'] < 0
            or not isinstance(event['task_id'], str) or not re.fullmatch(r'R2-[0-9a-f]{12}', event['task_id'])
            or not isinstance(event['session_id'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', event['session_id'])
            or event['kind'] not in ('integration_failed', 'task_failed', 'integration_missing') or event['failure_code'] not in TECHNICAL):
        raise ValueError('event outside technical recovery scope')
    if event['kind'] == 'integration_failed':
        if event['failure_code'] != 'tests_failed' or not re.fullmatch(r'R-[0-9a-f]{12}', str(event['request'])):
            raise ValueError('invalid failed integration event')
    elif event['kind'] == 'integration_missing':
        if event['request'] != '' or event['failure_code'] != 'request_missing':
            raise ValueError('invalid missing integration event')
    elif event['failure_code'] == 'request_missing' or event['request'] != '':
        raise ValueError('task failure cannot name integration request')
    if event['id'] != digest(event):
        raise ValueError('event identity hash mismatch')
    return event


def eligible(event, queue):
    from .guardian import candidates
    return next((j for j in candidates(queue) if j.get('task_id') == event['task_id']
                 and j.get('node') == event['node'] and j.get('repo') == event['repo']), None)


def wake_notifier():
    # The existing local wake endpoint is signed with the existing service secret.
    secret = os.environ.get('R2_WEBHOOK_SECRET', '')
    if not secret:
        path = Path.home() / '.config/runner2/control.env'
        if path.exists():
            for line in path.read_text().splitlines():
                if line.startswith('R2_WEBHOOK_SECRET='):
                    values = shlex.split(line.split('=', 1)[1]);secret = values[0] if values else ''
    body = b'{}';headers = {}
    if secret:
        headers['X-R2-Signature'] = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    try:
        from .notifier import DEFAULT_PORT
        port = int(os.environ.get('R2_NOTIFY_PORT', DEFAULT_PORT))
        request = urllib.request.Request(f'http://127.0.0.1:{port}/wake', data=body, headers=headers)
        with urllib.request.urlopen(request, timeout=2) as response:
            return response.status == 202
    except (OSError, ValueError):
        return False  # durable request is still picked up by the existing notifier loop


def receive(event):
    validate(event)
    cfg = node.data(node.root() / 'config.json', {})
    if not cplane.is_primary() or not cfg.get('enabled') or cfg.get('paused') or not cfg.get('ownership_enrolled'):
        return {'id': event['id'], 'accepted': False, 'state': 'retry', 'reason': 'recovery temporarily unavailable'}
    queue = jobs.load()
    paused = queue.get('paused', {})
    if paused.get('all') or paused.get('nodes', {}).get(event['node']):
        return {'id': event['id'], 'accepted': False, 'state': 'retry', 'reason': 'queue temporarily paused'}
    if not eligible(event, queue):
        return {'id': event['id'], 'accepted': False, 'state': 'ignored', 'reason': 'not an admitted active recovery candidate'}
    node.root().mkdir(parents=True, exist_ok=True)
    directory = node.root() / 'events';directory.mkdir(exist_ok=True)
    with (node.root() / 'events.lock').open('a+') as handle:
        lock_exclusive(handle)
        path = directory / (event['id'] + '.json')
        if not path.exists():
            store.atomic_write(path, json.dumps({'event': event, 'state': 'pending', 'received_at': time.time(), 'next_at': 0}))
        record = node.data(path, {})
    try:
        woken = wake_notifier() if record.get('state') == 'pending' else False
    except (OSError, ValueError):
        woken = False
    return {'id': event['id'], 'accepted': True, 'state': record['state'], 'woken': woken}


def pending(now=None):
    now = time.time() if now is None else now
    result, errors = [], []
    for path in sorted((node.root() / 'events').glob('*.json')):
        try:
            record = json.loads(path.read_text())
            if not isinstance(record, dict):
                raise ValueError('invalid durable event envelope')
            if record.get('state') != 'pending':
                continue
            due = record.get('next_at', 0)
            if not isinstance(due, (int, float)) or isinstance(due, bool) or not math.isfinite(due):
                raise ValueError('invalid retry timestamp')
            validate(record['event'])
            if path.stem != record['event']['id']:
                raise ValueError('event filename mismatch')
            if due <= now:
                result.append(record)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            errors.append(path.name + ': ' + str(exc)[:200])
    diagnostics = node.root() / 'events-diagnostics.json'
    if errors or diagnostics.exists():
        if node.data(diagnostics, {}).get('errors') != errors:
            store.atomic_write(diagnostics, json.dumps({'at': now, 'errors': errors}))
    return result[:100]


def finish(batch, result, now=None):
    now = time.time() if now is None else now
    with (node.root() / 'events.lock').open('a+') as handle:
        lock_exclusive(handle)
        queue = jobs.load()
        for record in batch:
            event = record['event'];path = node.root() / 'events' / (event['id'] + '.json')
            current = node.data(path, {})
            if current.get('state') != 'pending':
                continue
            paused = queue.get('paused', {})
            if paused.get('all') or paused.get('nodes', {}).get(event['node']):
                continue  # preserve the pending event until the queue can be assessed
            job = eligible(event, queue)
            proof = result.get('event_probes', {}).get((job or {}).get('id'), {})
            identity = proof.get('identity', {})
            changed = identity and any(identity.get(k) != event.get(k) for k in ('task_id', 'session_id', 'turn'))
            verified = any(x.get('job') == (job or {}).get('id') and x.get('verified') for x in result.get('actions', []))
            retry = bool(job and not changed and not verified and (
                result.get('errors') or not proof.get('integrator', {}).get('idle', True)
                or proof.get('failure') or identity.get('status') == 'failed'))
            count = current.get('checks', 0) + 1
            current.update(state='pending' if retry else 'processed', checks=count,
                           processed_at=now, last_round=result.get('started_at'),
                           next_at=now + (15 if not result.get('errors') else min(60 * count, 900)),
                           outcome='retry_after_fresh_recheck' if retry else ('effect_observed' if verified else ('superseded_by_current_state' if changed else 'no_admitted_technical_action')),
                           errors=result.get('errors', []), observed=proof)
            store.atomic_write(path, json.dumps(current))


def _packet(task, kind, request, code):
    value = {'schema': 1, 'node': 'LINUX', 'repo': REPO, 'task_id': task['task_id'],
             'session_id': task['session_id'], 'turn': task['turn'], 'kind': kind, 'request': request, 'failure_code': code}
    value['id'] = digest(value)
    return validate(value)


def detect(task, settings):
    if task.get('repo') != REPO or task.get('question') or task.get('status') in ('paused', 'waiting_for_human', 'stopped'):
        return None
    if task.get('status') == 'failed':
        code = (task.get('failure') or {}).get('kind')
        return _packet(task, 'task_failed', '', code) if code in TECHNICAL else None
    if task.get('status') != 'completed':
        return None
    try:
        failure = node.failed_request(task)
    except (ValueError, OSError, KeyError, TypeError, subprocess.CalledProcessError):
        failure = None  # Unbound/historical requests convey no recovery authority.
    if failure and failure.get('kind') == 'submission_missing':
        return _packet(task, 'integration_missing', '', 'request_missing')
    inbox = Path(settings['inbox'])
    for rid in node.ic.RID.findall((task.get('finish') or {}).get('summary', '')):
        status = node.data(inbox / (rid + '.status.json'), {})
        if status.get('state') != 'failed' or status.get('result') != 'tests_failed':
            continue
        request = node.data(inbox / (rid + '.json'), {})
        if (request.get('task_id') == task['task_id'] and request.get('repo') == REPO
                and request.get('source_branch') == task.get('branch')
                and request.get('target_commit') == node.git(task['worktree'], 'rev-parse', 'HEAD')):
            return _packet(task, 'integration_failed', rid, 'tests_failed')
    return None


def emit_round():
    cfg = node.data(node.root() / 'events-source.json', {})
    if not cfg.get('enabled') or config.local_node() != 'LINUX':
        return {'state': 'disabled'}
    directory = node.root() / 'outbound';directory.mkdir(parents=True, exist_ok=True)
    errors = []
    settings = node.data(config.r2_home() / 'config/integration-check.json', {})
    if 'repositories' in settings:
        settings = settings['repositories'].get(REPO, {})
    for path in config.tasks_dir().glob('R2-*/state.json'):
        try:
            task = node.data(path, {})
            if task.get('repo') != REPO:
                continue
            rids = node.ic.RID.findall((task.get('finish') or {}).get('summary', ''))
            statuses = [(rid, (Path(settings['inbox']) / (rid + '.status.json')).stat().st_mtime_ns
                         if (Path(settings['inbox']) / (rid + '.status.json')).exists() else 0) for rid in rids] if settings.get('inbox') else []
            failed_status = any(node.data(Path(settings['inbox']) / (rid + '.status.json'), {}).get('result') == 'tests_failed' for rid in rids) if settings.get('inbox') else False
            tip = node.git(task['worktree'], 'rev-parse', 'HEAD') if task.get('status') == 'completed' and failed_status else ''
            signature = (path.stat().st_mtime_ns, tuple(statuses), tip)
            if _seen.get(path.parent.name) == signature:
                continue
            event = detect(task, settings)
            if event:
                dest = directory / (event['id'] + '.json')
                if not dest.exists():
                    store.atomic_write(dest, json.dumps({'event': event, 'state': 'pending', 'attempts': 0, 'next_at': 0}))
            _seen[path.parent.name] = signature
        except Exception as exc:
            errors.append(path.parent.name + ': ' + str(exc)[:200])
    # Single watch worker owns this outbox; receipts are persisted only after matching durable acceptance.
    for path in sorted(directory.glob('*.json')):
        record = node.data(path, {})
        if record.get('state') != 'pending' or record.get('next_at', 0) > time.time():
            continue
        record['attempts'] += 1
        try:
            reply = (receive(record['event']) if cplane.is_primary() else remote.call('GERARD', 'guardian_event', record['event']))
            if (reply.get('id') != record['event']['id'] or reply.get('state') not in ('pending', 'processed', 'ignored')
                    or reply.get('accepted') is not (reply.get('state') != 'ignored')):
                raise ValueError('invalid event receipt')
            record.update(state='delivered', receipt=reply, delivered_at=time.time())
        except Exception as exc:
            record.update(error=str(exc)[:300], next_at=time.time() + min(5 * 2 ** min(record['attempts'], 4), 60))
        store.atomic_write(path, json.dumps(record))
    summary = {'at': time.time(), 'errors': errors, 'pending': sum(node.data(p, {}).get('state') == 'pending' for p in directory.glob('*.json'))}
    store.atomic_write(node.root() / 'events-source-status.json', json.dumps(summary))
    return summary


def emit_tick():
    global _thread, _last_scan
    if time.time() - _last_scan < 5 or (_thread is not None and _thread.is_alive()):
        return
    if not node.data(node.root() / 'events-source.json', {}).get('enabled') or config.local_node() != 'LINUX':
        return
    _last_scan = time.time()
    def worker():
        try:
            emit_round()
        except Exception as exc:
            store.atomic_write(node.root() / 'events-source-status.json', json.dumps({'at': time.time(), 'errors': [str(exc)[:400]]}))
    _thread = threading.Thread(target=worker, name='guardian-technical-signals', daemon=True)
    _thread.start()


