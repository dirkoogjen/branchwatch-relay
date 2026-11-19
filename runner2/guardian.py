"""The existing GERARD notifier reconciles bounded recovery every fifteen minutes."""
import copy
import datetime
import hashlib
import json
import math
import shlex
import subprocess
import threading
import time
from pathlib import Path

from . import config, cplane, jobs, remote, store
from . import guardian_node as node
from .filelock import lock_exclusive

INTERVAL = 900
STALLED = 3600
_thread = None
_watchdog_at = 0.0


def timestamp(value, fallback=0):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else fallback


def valid_heartbeat(beat, now):
    return (isinstance(beat, dict) and beat.get('schema') == 1
            and beat.get('automation_id') == 'studio-wachtrij-tot-integratie-controleren'
            and timestamp(beat.get('at'), -1) > 0 and beat['at'] <= now + 120
            and beat.get('phase') in ('running', 'completed', 'failed'))


def candidates(queue):
    if queue.get('paused', {}).get('all') or queue.get('paused', {}).get('nodes', {}).get('LINUX'):
        return []
    records = queue.get('jobs', {})

    def permitted(job):
        return (job.get('node') == 'LINUX' and job.get('repo') == '/home/operator/StudioOBF'
                and not any(job.get(k) for k in ('held', 'approval', 'archived', 'cancel_requested'))
                and not ((job.get('check') or {}).get('source') in ('mens', 'chatgpt')))

    needed = {dep for j in records.values() if permitted(j) and j.get('status') == 'queued'
              for dep in j.get('depends_on', [])}
    authorized = node.data(node.root() / 'config.json', {}) or {}
    needed.update(jid for jid in authorized.get('recoverable_job_ids', []) if jid in records)
    # All future Studio series enroll on actual execution, including a final task
    # without a successor. The persisted cutoff leaves unrelated old history alone.
    now = time.time()
    cutoff = timestamp(authorized.get('recover_started_since'))
    if 0 < cutoff <= now:
        for jid, job in records.items():
            try:
                started = datetime.datetime.fromisoformat(job.get('started_at', ''))
                if started.tzinfo is not None and cutoff <= started.timestamp() <= now + 120:
                    needed.add(jid)
            except (TypeError, ValueError, OverflowError):
                continue
    result = []
    for jid in queue.get('order', {}).get('LINUX', []):
        job = records[jid]
        if permitted(job) and job.get('status') == 'running':
            result.append(job)
    for jid in sorted(needed):
        job = records.get(jid, {})
        check = job.get('check') or {}
        if (permitted(job) and job.get('status') in ('done', 'failed') and job.get('require_check') is True
                and (job.get('status') != 'failed' or (job.get('started_at') and not job.get('continuation') and not job.get('supervisor')))
                and not (check.get('source') == 'integrator' and check.get('state') == 'ok')):
            result.append(job)
    return result


def observe(snapshot, previous, now):
    act = snapshot['activity']
    previous = previous or {}
    same = (previous.get('identity') == snapshot['identity'] and previous.get('signature') == act['signature']
            and act['cpu_s'] >= previous.get('cpu_s', 0) and act['cpu_s'] - previous.get('cpu_s', 0) <= 1
            and act.get('known'))
    obs = {'identity': copy.deepcopy(snapshot['identity']), **act,
           'quiet_since': previous.get('quiet_since', now) if same else now, 'at': now}
    if not snapshot['integrator'].get('idle'):
        return 'wait', obs
    task = snapshot['task']
    if task['status'] == 'failed' and snapshot.get('technical_recoverable'):
        return 'resume_technical', obs
    if task['status'] == 'completed' and (snapshot.get('failure') or {}).get('validated'):
        return 'resume_failed', obs
    if task['status'] == 'paused' and snapshot.get('guardian_pause'):
        return 'resume_paused', obs
    if task['status'] == 'running' and same and now - obs['quiet_since'] > STALLED:
        return 'pause_stalled', obs
    return 'wait', obs


def confirmed(action, before, after):
    old, new = before['identity'], after['identity']
    if any(old.get(k) != new.get(k) for k in ('task_id', 'session_id')):
        return False
    if action == 'resume_technical' and new.get('turn', 0) > old.get('turn', 0) and after['task']['status'] == 'completed':
        return True  # A genuine later finished turn also proves continuation; integration remains separately gated.
    if action.startswith('resume_'):
        return (new.get('turn', 0) > old.get('turn', 0) and after['task']['status'] == 'running'
                and after.get('process_alive') is True)
    return action == 'pause_stalled' and after['task']['status'] == 'paused' and after.get('guardian_pause') is True


def admission(job):
    return {k: job.get(k) for k in ('id', 'node', 'repo', 'task_id', 'run', 'status', 'held', 'approval',
                                   'archived', 'cancel_requested', 'require_check', 'check', 'prompt', 'depends_on')}


def ssh(spec, program, payload):
    command = shlex.join([spec.get('python', 'python3'), '-c', program, json.dumps(payload)])
    # _ssh_cmd already includes an RPC command. Replace it; appending would silently run RPC instead.
    result = subprocess.run(remote._ssh_cmd(spec)[:-1] + [command], capture_output=True, text=True, timeout=55)
    if result.returncode:
        raise RuntimeError(f'remote execution failed ({result.returncode}): {result.stderr[:300]}')
    return json.loads(result.stdout)


NODE_PROGRAM = '''import sys,json
payload=json.loads(sys.argv[1])
sys.path.insert(0,payload['code'])
from runner2.guardian_node import dispatch
request=json.loads(sys.argv[1])['request']
print(json.dumps(dispatch(request)))
'''

MAC_PROGRAM = '''import sys,json,sqlite3,time,subprocess
from pathlib import Path
p=json.loads(sys.argv[1]);h=Path.home();db=h/'.codex/sqlite/codex-dev.db'
c=sqlite3.connect('file:'+str(db)+'?mode=ro',uri=True)
r=c.execute('select status,kind,target_thread_id,rrule,last_run_at from automations where id=?',(p['automation_id'],)).fetchone()
if not r or r[1]!='heartbeat' or r[2]!=p['thread_id'] or r[3]!='FREQ=MINUTELY;INTERVAL=15':raise RuntimeError('automation identity/schedule mismatch')
if r[0]!='ACTIVE':
 print(json.dumps({'state':'intentional_pause','changed':False}));raise SystemExit(0)
s=sqlite3.connect('file:'+str(h/'.codex/state_5.sqlite')+'?mode=ro',uri=True)
t=s.execute('select archived,rollout_path from threads where id=?',(p['thread_id'],)).fetchone()
if not t or t[0]:raise RuntimeError('thread missing or archived')
path=Path(t[1]);last_started=last_completed=''
with path.open('rb') as f:
 f.seek(max(0,path.stat().st_size-1500000))
 for line in f:
  try:d=json.loads(line)
  except ValueError:continue
  if d.get('type')=='event_msg':
   typ=d.get('payload',{}).get('type')
   if typ=='task_started':last_started=d.get('timestamp','')
   if typ in ('task_complete','turn_aborted'):last_completed=d.get('timestamp','')
if last_started and last_started>last_completed:
 print(json.dumps({'state':'active_turn_preserved','changed':False,'last_run_at':r[4]}));raise SystemExit(0)
rc=subprocess.run(['/usr/bin/open','-g','-a','/Applications/ChatGPT.app','codex://threads/'+p['thread_id']],timeout=15).returncode
if rc:raise RuntimeError('open failed '+str(rc))
print(json.dumps({'state':'reactivation_requested','changed':True,'last_run_at':r[4]}))
'''


def call_node(name, request):
    if name == config.local_node():
        return node.dispatch(request)
    if config.local_node() == "LINUX" and name == "GERARD" and request.get("action") in ("services", "restart_service"):
        reply = remote.call(name, "guardian_node_services", request)
        if not isinstance(reply, dict) or reply.get("error"):
            raise RuntimeError("remote service request failed")
        return reply
    spec = remote.nodes()[name]
    return ssh(spec, NODE_PROGRAM, {'code': spec['code'], 'request': request})


def service_info(name):
    if name == config.local_node():
        return {u: node.unit_info(u) for u in node.UNITS}
    return call_node(name, {"action": "services"})

def service_restart(name, unit, info):
    if name == config.local_node():
        return node.restart_service(unit, info)
    return call_node(name, {"action": "restart_service", "unit": unit, "expected": info})

def _enabled():
    cfg = node.data(node.root() / 'config.json', {})
    return cfg if cfg.get('enabled') and not cfg.get('paused') and cplane.is_primary() else None


def _write(value):
    value['updated_at'] = time.time()
    store.atomic_write(node.root() / 'status.json', json.dumps(value, indent=2))


def queue_failed_resume(job, snapshot, backup):
    """Use the existing queue resume transition: same job/run/task and original worktree."""
    if job.get('status') != 'failed' or job.get('resume_requested'):
        return {'state': 'await_existing_dispatcher', 'current': snapshot}
    fresh = call_node(job['node'], {'action': 'probe', 'job': job})
    if (fresh['identity'] != snapshot['identity'] or not fresh['integrator']['idle']
            or not fresh.get('technical_recoverable') or fresh['task'].get('failure') != snapshot['task'].get('failure')):
        raise ValueError('failed task changed before queue resume')
    journal = node.Journal(node.root() / 'technical-resumes')
    key = job['id'] + ':' + str(job.get('run')) + ':' + json.dumps(fresh['identity'], sort_keys=True)
    previous = node.data(journal.file(key), {})
    entries = [node.data(p, {}) for p in journal.path.glob('*.json')]
    attempts = sum(x.get('attempts', 1) for x in entries if x.get('identity', {}).get('job') == job['id'] and x.get('at', 0) > time.time() - 21600)
    if attempts >= 3:
        raise ValueError('three bounded failed-task resumes in six hours; preserve task and escalate')
    if previous and time.time() - previous.get('last_attempt_at', previous['at']) < 60:
        return {'state': 'await_previous_request', 'current': fresh}
    if previous:
        store.atomic_write(journal.file(key), json.dumps({**previous, 'last_attempt_at': time.time(), 'attempts': previous.get('attempts', 1) + 1}))
    elif not journal.begin(key, {'job': job['id'], 'task_id': job['task_id'], 'identity': fresh['identity'], 'backup': backup}):
        return {'state': 'another_request_owns_intent', 'current': fresh}
    note = ('GERARD technisch herstel; behoud dezelfde taak, sessie, werkboom en commits. '
            'Herstel binnen de oorspronkelijke scope; geen poorten omzeilen. Back-up: ' + backup)
    result = jobs.act('resume', {'job_id': job['id'], 'note': note, 'guardian_expected': admission(job), 'guardian_context': fresh}, 'GERARD guardian')
    journal.finish(key, result)
    return {'state': 'queued_existing_task_resume', 'queue': result, 'current': fresh}


def run_round(force=False):
    cfg = _enabled()
    if not cfg:
        return {'state': 'disabled_or_not_primary'}
    node.root().mkdir(parents=True, exist_ok=True)
    with (node.root() / 'recovery.lock').open('a+') as handle:
        try:
            lock_exclusive(handle, blocking=False)
        except OSError:
            return {'state': 'another_recovery_owns_lock'}
        old = node.data(node.root() / 'status.json', {})
        now = time.time()
        from . import guardian_events as events
        batch = events.pending(now)
        periodic_at = timestamp(old.get('last_periodic_at'), timestamp(old.get('started_at')))
        periodic_due = now - periodic_at >= INTERVAL
        if not force and not batch and not periodic_due:
            return {'state': 'not_due', 'next_at': periodic_at + INTERVAL}
        previous_healthy = (old.get('phase') == 'completed' and not old.get('errors') and not old.get('pending'))
        out = {'started_at': now, 'last_completed_at': old.get('last_completed_at'), 'last_completed_healthy': bool(previous_healthy), 'phase': 'running',
               'observations': old.get('observations', {}), 'pending': old.get('pending', {}), 'actions': [], 'errors': []}
        out.update(trigger='event+periodic' if batch and periodic_due else ('event' if batch else 'periodic'),
                   event_ids=[r['event']['id'] for r in batch], event_probes={},
                   last_periodic_at=now if periodic_due or (force and not batch) else periodic_at)
        _write(out)
        beat = node.data(config.r2_home() / 'control/work-watch/heartbeat.json', {})
        # Enrollment is recorded only after a real Work round used the delegated-owner protocol.
        enrolled = cfg.get('ownership_enrolled') is True
        valid = valid_heartbeat(beat, now)
        stale = not valid or beat.get('phase') == 'failed' or now - beat['at'] > 2100
        prior_restore = old.get('work_restore', {})
        if prior_restore and valid and not stale and beat['phase'] != 'failed' and beat['at'] > timestamp(prior_restore.get('at'), now):
            out['work_restore'] = {**prior_restore, 'verified_by_real_heartbeat': True, 'heartbeat': beat}
        elif stale:
            try:
                answer = ssh(remote.nodes()['MAC'], MAC_PROGRAM, {
                    'automation_id': 'studio-wachtrij-tot-integratie-controleren',
                    'thread_id': cfg.get('work_thread_id') or '01a15d14-6250-72a0-8eb9-eb11871c5b0c'})
                out['work_restore'] = {'at': time.time(), 'result': answer, 'verified_by_real_heartbeat': False}
                out['actions'].append({'work_restore': answer})
            except Exception as exc:
                out['work_restore'] = {'at': time.time(), 'state': 'unreachable',
                                       'error': str(exc)[:400], 'verified_by_real_heartbeat': False}
        else:
            out['work_restore'] = prior_restore
        out['mode'] = 'GERARD_fallback' if stale else 'GERARD_reconciler_Work_observes'
        _write(out)
        # Existing control services only; never touch backup, web, data or networking services.
        # The NUC owns the control plane after handover; GERARD is optional and must not be restarted as primary.
        service_nodes = ('LINUX',) if config.local_node() == 'LINUX' else ('GERARD', 'LINUX')
        for name in service_nodes:
            try:
                services = service_info(name)
                for unit, info in services.items():
                    if node.restart_needed(unit, info):
                        answer = service_restart(name, unit, info)
                        out['actions'].append({'node': name, 'service': unit, 'result': answer})
                        if answer.get('state') in ('cooldown', 'already_requested'):
                            out['errors'].append(name + ' ' + unit + ': ' + answer['state'])
                            _write(out)
                _write(out)
            except Exception as exc:
                out['errors'].append(name + ' service check: ' + str(exc)[:400])
                _write(out)
        if enrolled:
            current_jobs = jobs.load()['jobs']
            for jid in list(out['pending']):
                checked = (current_jobs.get(jid, {}).get('check') or {})
                if checked.get('state') == 'ok' and checked.get('source') == 'integrator':
                    out['actions'].append({'job': jid, 'verified': 'authentic_integrator_completion', 'check': checked})
                    del out['pending'][jid]
            for job in candidates(jobs.load()):
                try:
                    snapshot = call_node(job['node'], {'action': 'probe', 'job': job})
                    out['event_probes'][job['id']] = {'identity': snapshot['identity'], 'integrator': snapshot['integrator'], 'failure': bool(snapshot.get('failure'))}
                    pending = out['pending'].get(job['id'])
                    if pending and confirmed(pending['action'], pending['before'], snapshot):
                        if pending['action'] == 'resume_technical' and job.get('status') == 'failed':
                            with jobs._launch_lock(job['node']):
                                jobs.act('resume', {'job_id': job['id'], 'note': 'Eerder hervatte taak opnieuw volgen; geen nieuwe uitvoering',
                                         'guardian_expected': admission(job), 'guardian_context': pending['before']}, 'GERARD guardian')
                        out['actions'].append({'job': job['id'], 'verified': pending['action'], 'actual': snapshot['identity']})
                        del out['pending'][job['id']]
                    action, observation = observe(snapshot, out['observations'].get(job['id']), time.time())
                    if (action == 'wait' and snapshot['task']['status'] == 'completed'
                            and snapshot['integrator'].get('idle')
                            and (job.get('last_error') or {}).get('code') == 'integration_check_blocked'):
                        out['errors'].append(job['id'] + ': integration blocked without admitted recovery proof: '
                                             + str(job['last_error'].get('message', ''))[:350])
                    out['observations'][job['id']] = observation
                    if action != 'wait':
                        if action == 'resume_technical' and (job.get('status') != 'failed' or job.get('resume_requested')):
                            continue
                        prepared = call_node(job['node'], {'action': 'prepare', 'job': job,
                                                          'expected': snapshot, 'mode': action})
                        snapshot = prepared['snapshot']
                        # Same primary launch lock as the dispatcher; fresh queue admission immediately before transport.
                        with jobs._launch_lock(job['node']):
                            current = {j['id']: j for j in candidates(jobs.load())}
                            if job['id'] not in current or admission(current[job['id']]) != admission(job):
                                continue
                            out['pending'][job['id']] = {'action': action, 'before': snapshot, 'at': time.time()}
                            _write(out)
                            if action == 'resume_technical':
                                result = queue_failed_resume(job, snapshot, prepared['backup'])
                            else:
                                result = call_node(job['node'], {'action': 'recover', 'job': job, 'expected': snapshot, 'mode': action})
                        verified = confirmed(action, snapshot, result.get('current', snapshot))
                        out['actions'].append({'job': job['id'], 'action': action, 'result': result, 'verified': verified})
                        if verified:
                            del out['pending'][job['id']]
                    _write(out)
                except Exception as exc:
                    out['errors'].append(job['id'] + ': ' + str(exc)[:500])
                    _write(out)
        else:
            out['errors'].append('Task recovery awaits explicit Work single-writer enrollment')
            _write(out)
        if batch:
            events.finish(batch, out)
        out.update(phase='completed', last_completed_at=time.time(), last_completed_healthy=not out.get('errors') and not out.get('pending'))
        _write(out)
        return out


def conditions():
    if not _enabled():
        return []
    state = node.data(node.root() / 'status.json', {})
    now = time.time()
    errors = state.get('errors', []) + node.data(node.root() / 'events-diagnostics.json', {}).get('errors', [])
    if state.get('phase') == 'worker_failed':
        errors = errors + [state.get('error', 'recovery worker failed')]
    stale = now - timestamp(state.get('last_completed_at'), timestamp(state.get('started_at'), now)) > INTERVAL + 300
    if not errors and not stale and not state.get('pending'):
        return []
    detail = json.dumps({'errors': errors, 'pending': list(state.get('pending', {})), 'stale': stale}, ensure_ascii=False)
    return [{'kind': 'guardian_recovery', 'key': 'gerard-autonomous-recovery',
             'title': 'ZELFSTANDIG HERSTEL NIET BEWEZEN',
             'text': 'GERARD blijft veilig controleren; deze herstelstap vraagt nader onderzoek. ' + detail[:1800]}]


def tick():
    global _thread
    if not _enabled() or (_thread is not None and _thread.is_alive()):
        return
    previous = node.data(node.root() / 'status.json', {})
    from . import guardian_events as events
    periodic_at = timestamp(previous.get('last_periodic_at'), timestamp(previous.get('started_at')))
    if time.time() - periodic_at < INTERVAL and not events.pending():
        return
    def worker():
        try:
            run_round()
        except Exception as exc:
            old = node.data(node.root() / 'status.json', {})
            _write({**old, 'phase': 'worker_failed', 'error': str(exc)[:500]})
    _thread = threading.Thread(target=worker, name='bounded-chain-recovery', daemon=True)
    _thread.start()


def watchdog_tick():
    """The existing watch can restart a hung notifier without touching task processes."""
    global _watchdog_at
    if time.time() - _watchdog_at < 30 or not _enabled():
        return
    _watchdog_at = time.time()
    info = node.unit_info('runner2-notifier.service')
    state = node.data(node.root() / 'status.json', {})
    hung_worker = (state.get('phase') == 'running' and time.time() - timestamp(state.get('updated_at'), time.time()) > 300)
    missed_round = state.get('phase') in ('completed', 'worker_failed') and time.time() - timestamp(state.get('last_completed_at')) > INTERVAL + 300
    if hung_worker or missed_round:
        info['heartbeat_age_s'] = max(info['heartbeat_age_s'], 181)
    if not node.restart_needed('runner2-notifier.service', info):
        return
    journal = node.Journal(node.root() / 'watchdog-actions')
    recent = [node.data(p, {}) for p in journal.path.glob('*.json')]
    if any(time.time() - x.get('at', 0) < 3600 for x in recent):
        return
    key = 'notifier:' + str(info['pid']) + ':' + str(info['pid_start'])
    if journal.begin(key, info):
        subprocess.run(['systemctl', '--user', 'restart', 'runner2-notifier.service'], check=True, timeout=35)
        journal.finish(key, node.unit_info('runner2-notifier.service'))


if __name__ == '__main__':
    print(json.dumps(run_round()))



