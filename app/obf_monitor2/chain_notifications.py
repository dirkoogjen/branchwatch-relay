"""Read-only whole-chain notification policy for the two Ubuntu workers.

The primary queue supplies *all* jobs and its own integration-proof predicate.
Integrator freshness/activity and a fresh runner observation veto completion.
This policy never starts work, verifies Git, merges, retries or modifies a queue.
"""
import hashlib
import json
import re
import threading

STALL_S = 300.0
RUNNER_MAX_AGE_S = 15.0


def _text(value):
    return re.sub(r'\s+', ' ', str(value or '')).strip()[:240]


def _signature(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:24]


class ChainPolicy:
    def __init__(self):
        self._waits = {}
        self._lock = threading.Lock()

    def _waiting(self, node, reason, now):
        previous = self._waits.get(node)
        if not previous or previous[0] != reason or now < previous[1]:
            self._waits[node] = (reason, now)
            return False
        return now - previous[1] >= STALL_S

    def events(self, node, label, runner, queue, integrator, *, now):
        with self._lock:
            return self._events(node, label, runner, queue, integrator, now=now)

    def _events(self, node, label, runner, queue, integrator, *, now):
        data = (queue or {}).get('nodes', {}).get(node.upper())
        rows = data.get('jobs') if isinstance(data, dict) else None
        jobs = rows if isinstance(rows, list) else []
        latest = max((j for j in jobs if j.get('started_at')), key=lambda j: (j.get('started_at') or '', j.get('id') or ''), default={})
        ident = f"{latest.get('id', 'source')}|{latest.get('run', 0)}|{latest.get('task_id', '')}"
        sample = str(runner.get('observation_id', runner.get('observation_at')))

        def error(phase, cause, action, *, key=None):
            cause, action = _text(cause), _text(action)
            return [dict(key=f'{node}|chain-error|{ident}|{key or _signature([phase,cause])}', kind='error',
                         speech=f'De keten op {label} is geblokkeerd bij {phase}. {cause} {action}',
                         phase=phase, cause=cause, action=action, at=latest.get('ended_at'),
                         task_id=latest.get('task_id') or '', title=latest.get('title'),
                         confirm_s=15.0, observation=str(now))]

        if rows is None:
            if self._waiting(node, 'queue-unavailable', now):
                return error('statuscontrole', 'De primaire wachtrij is niet leesbaar.', 'Controleer de control plane en de wachtrijstatus.')
            return []
        # A planned pause/hold is never an error and never successful completion.
        if queue.get('paused_all') or data.get('paused') or runner.get('chain_paused') or integrator.get('paused'):
            self._waits.pop(node, None)
            return []
        fresh = (runner.get('available') and not runner.get('data_delay')
                 and isinstance(runner.get('observation_at'), (float, int))
                 and 0 <= now - runner['observation_at'] <= RUNNER_MAX_AGE_S
                 and isinstance(runner.get('chain_busy'), bool))
        if not fresh or integrator.get('state') == 'onbekend':
            needs_work = runner.get('chain_busy') or any(j.get('status') in ('starting', 'running') or
                (j.get('requires_integration') and not j.get('integration_satisfied') and j.get('started_at')) or
                (j.get('status') == 'queued' and not j.get('held') and j.get('state') not in ('wacht_op_goedkeuring','vastgehouden','wacht_op_voorganger')) for j in jobs)
            if not needs_work:
                self._waits.pop(node, None)
                return []
            if self._waiting(node, ('source-unavailable', ident), now):
                return error('statuscontrole', 'Actuele runner- of integratorgegevens ontbreken.',
                             'Controleer de verbinding en de statusdiensten op deze node.')
            return []
        active = [j for j in jobs if j.get('status') in ('starting', 'running')]
        queued = [j for j in jobs if j.get('status') == 'queued']
        obligations = [j for j in jobs if j.get('requires_integration') and not j.get('integration_satisfied')
                       and (j.get('started_at') or j.get('integration_obligation'))]
        current_tasks = {j.get('task_id') for j in active + obligations}
        # An active retry takes precedence over errors left from the previous tick.
        if integrator.get('running'):
            self._waits.pop(node, None)
            return []
        if runner.get('chain_problem'):
            return error('programmeren', runner['chain_problem'], 'Bekijk de ketenbeslissing op de monitor en herstel de geblokkeerde stap.')
        if runner.get('chain_busy') or active:
            self._waits.pop(node, None)
            human = [e for e in (runner.get('events') or []) if e.get('kind') == 'human']
            if human:
                return [{**e, 'speech': f'Claude wacht op een antwoord bij programmeren op {label}. Beantwoord de vraag op de monitor.'} for e in human[:1]]
            if (runner.get('watch') or {}).get('healthy') is False:
                return error('programmeren', 'De Runner-watch staat stil.', 'Herstel de Runner-watch op deze node.')
            return []
        if any(j.get('held') for j in obligations):
            self._waits.pop(node, None)
            return []
        if integrator.get('error') and (obligations or queued):
            return error('integreren', integrator['error'], 'Controleer de integrator en herstel de gemelde oorzaak.')
        # Only request/task-bound attention for CURRENT obligations. Historical
        # refused requests are diagnostic history and cannot poison a new cycle.
        for item in integrator.get('attention', []):
            if item.get('task_id') in current_tasks and item.get('attention') is not False:
                return error('integreren', item.get('text') or item.get('code'),
                             'Herstel deze integratieaanvraag; controleer de details op de monitor.', key=item.get('id'))
        for job in obligations:
            check, err = job.get('check') or {}, job.get('last_error') or {}
            if check.get('state') == 'afgekeurd':
                return error('controleren', check.get('note') or 'De nacontrole is afgekeurd.',
                             'Herstel de bevindingen en laat de controle opnieuw uitvoeren.', key=job['id']+'|rejected')
            message = str(err.get('message') or '')
            if err.get('code') == 'integration_check_blocked' and not any(code in message for code in ('integration_evidence_incomplete', 'integration_evidence_unavailable')):
                return error('controleren', message, 'Controleer het integratiebewijs en herstel de gemelde oorzaak.', key=job['id']+'|'+_signature(message))
        if latest.get('status') in ('failed', 'incomplete'):
            return error('programmeren', (latest.get('outcome') or {}).get('text') or 'De programmeerketen kon niet afronden.',
                         'Bekijk de taak en herstel of hervat deze op de monitor.')
        blocked = next((j for j in queued if j.get('state') == 'geblokkeerd'), None)
        if blocked:
            return error('wachtrij', blocked.get('why') or 'Een voorganger is niet geslaagd.',
                         'Herstel de voorganger of pas de afhankelijkheid bewust aan.', key=blocked['id'])
        if obligations or queued:
            # Approvals and deliberate holds are expected human gates, not faults.
            if not obligations and all(j.get('held') or j.get('state') in ('vastgehouden','wacht_op_goedkeuring','wacht_op_voorganger') for j in queued):
                self._waits.pop(node, None)
                return []
            progress = [(j.get('id'), j.get('run'), j.get('status'), j.get('check'), j.get('last_error'))
                        for j in obligations + queued]
            # Integrator heartbeat/timestamps are not progress. Long-running tests
            # are handled by the producer's test budget, never this wait timer.
            if self._waiting(node, _signature(progress), now):
                return error('controleren' if obligations else 'wachtrij',
                             'De keten wacht al vijf minuten zonder voortgang.',
                             'Controleer de aanvraag, bewijscontrole en dispatcher; herstel de vastgelopen stap.', key='stalled|'+_signature(progress))
            return []
        self._waits.pop(node, None)
        if (not latest or latest.get('status') != 'done' or not latest.get('integration_satisfied')
                or not integrator.get('enabled') or integrator.get('error') or integrator.get('waiting')
                or (runner.get('watch') or {}).get('healthy') is not True):
            return []
        if latest.get('task_id') not in {j.get('task_id') for j in runner.get('tasks', [])}:
            return []  # queue is newer than the runner observation
        return [dict(key=f'{node}|chain-done|{ident}', kind='done', speech=f'Claude is klaar op {label}.',
                     at=latest.get('ended_at'), task_id=latest.get('task_id') or '', title=latest.get('title'),
                     runner_idle=True, observation=sample)]
