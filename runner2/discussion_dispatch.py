"""Primary scheduling for read-only conversations, sharing the existing launch gate."""
import copy

from . import config, cplane, discussions as d, jobs, store


def _call(tool, args):
    from . import controller_mcp
    return controller_mcp.dispatch(tool, args)


def active_local():
    from .discussion_worker import active_local as read
    return read()


def pending_on_node(node):
    if node not in ('GERARD', 'LINUX'):return None
    return next((t for c in d.all_conversations() if c['node'] == node for t in c['turns'] if t['status'] in ('starting', 'running')), None)


def request(c, turn):
    snapshot = copy.deepcopy(c)
    index = next(i for i, t in enumerate(snapshot['turns']) if t['turn_id'] == turn['turn_id'])
    snapshot['turns'] = snapshot['turns'][:index + 1]
    snapshot['turns'][-1]['answer'] = ''  # The execution input never changes when partial output arrives.
    return {'conversation_id': c['conversation_id'], 'turn_id': turn['turn_id'], 'execution_id': turn['execution_id'],
            'node': c['node'], 'repository': c['repository'], 'model': c['model'], 'context': d.context(snapshot)}


def _binding(reply, q):
    return isinstance(reply, dict) and all(reply.get(k) == q[k] for k in ('conversation_id', 'turn_id', 'execution_id', 'node', 'repository', 'model'))


def _update(c, turn, patch):
    if all(turn.get(k) == v for k, v in patch.items()):return
    try:d.update_turn(c['conversation_id'], turn['turn_id'], c['version'], patch)
    except ValueError:pass  # A human changed the version; next tick reconciles it.


def _sync(c, t):
    q = request(c, t)
    try:
        reply = _call('runner2_discussion_execution', {'action': 'stop' if t.get('stop_requested') else 'status', **q})
        if not _binding(reply, q):
            _update(c, t, {'reason': 'De identiteit van het overlegantwoord klopt niet; de veilige blokkering blijft staan.'});return
        state = reply.get('status')
        if state not in d.STATES or state == 'queued':raise ValueError()
        patch = {k: reply[k] for k in ('answer', 'reason', 'heartbeat', 'started', 'finished') if reply.get(k) is not None}
        if state == 'completed' and not str(patch.get('answer', '')).strip():raise ValueError()
        patch['status'] = state
        _update(c, t, patch)
    except Exception:
        _update(c, t, {'reason': 'De bestaande overleguitvoering is nog niet bereikbaar of bevestigd. Geen tweede vraag gestart.'})


def tick(cfg=None):
    cfg = cfg or config.load_config()
    summary = {'started': [], 'observed': 0}
    if not cplane.is_primary():return summary
    jobs._check_fence()
    for c in d.all_conversations():
        for t in c['turns']:
            if t['status'] in ('starting', 'running'):
                _sync(c, t);summary['observed'] += 1
    for candidate in reversed(d.all_conversations()):
        turn = candidate['turns'][-1]
        if turn['status'] != 'queued':continue
        node = candidate['node']
        gate = jobs.discussion_admission(node, cfg)
        if not gate['allowed']:
            _update(candidate, turn, {'reason': gate['reason']});continue
        try:
            # Slow probe is deliberately outside locks so a pause can be acknowledged immediately.
            live = _call('runner2_status', {'node': node})
            if live.get('node') != node:raise ValueError()
            busy = live.get('active_task') or live.get('discussion') or any(x.get('state') == 'active' for x in live.get('chains', [])) or any(x.get('state') == 'active' for x in live.get('supervisors', []))
            if busy:
                _update(candidate, turn, {'reason': 'De machine is nog bezig met een andere taak of keten.'});continue
        except Exception:
            _update(candidate, turn, {'reason': 'De gekozen machine is niet bereikbaar; de vraag wacht.'});continue
        with jobs._launch_lock(node):
            # Last live admission check, and no queue lock around the RPC.
            c = d.load(candidate['conversation_id']);t = c['turns'][-1]
            if t['turn_id'] != turn['turn_id'] or t['status'] != 'queued':continue
            gate = jobs.discussion_admission(node, cfg)
            if not gate['allowed']:
                _update(c, t, {'reason': gate['reason']});continue
            q = request(c, t)
            claimed = d.update_turn(c['conversation_id'], t['turn_id'], c['version'], {'status': 'starting', 'started': store.now(), 'reason': 'Overlegstart wordt bevestigd.'})
            ct = claimed['turns'][-1]
            try:
                out = _call('runner2_discussion_execution', {'action': 'start', **q})
                if not _binding(out, q):raise ValueError()
                state = out.get('status')
                if state not in d.STATES or state == 'queued':raise ValueError()
                patch = {k: out[k] for k in ('answer', 'reason', 'heartbeat', 'finished') if out.get(k) is not None}
                _update(claimed, ct, {'status': state, **patch});summary['started'].append(ct['execution_id'])
            except Exception as exc:
                code = getattr(exc, 'code', '')
                # Only explicit pre-spawn rejection certifies no model was started.
                if code in ('invalid_input', 'unsupported_node', 'busy'):
                    _update(claimed, ct, {'status': 'failed', 'reason': ('De overlegstart is geweigerd: ' + str(getattr(exc, 'message', '') or 'controleer profiel, model en machinebezetting.'))[:500], 'finished': store.now()})
                else:
                    _update(claimed, ct, {'reason': 'Start nog niet bevestigd. Eerst bestaande uitvoering controleren; geen tweede start.'})
    return summary
