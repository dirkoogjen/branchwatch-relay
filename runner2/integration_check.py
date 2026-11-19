"""Read-only standalone integration proof; transactional completion check on the primary.

Opt-in per node (jobs_integration_check_nodes). Remote verification uses the existing
SSH RPC channel and a node-local pinned config; callers cannot choose evidence paths.
No approval, merge, test execution, queue start or model invocation occurs here.
"""
import datetime as dt
import hashlib
import json
import os
import re
import stat
import subprocess
from pathlib import Path

from . import config, store

RID = re.compile(r'\bR-[0-9a-f]{12}\b')
TID = re.compile(r'R2-[0-9a-f]{12}')
GID = re.compile(r'G-[0-9a-f]{12}')
SHA = re.compile(r'[0-9a-f]{40}')


class ProofError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise ProofError(code)


def raw(path, limit=8_000_000):
    path = Path(path)
    # Evidence paths must not redirect through symlinks, including parent folders.
    require(not any(p.is_symlink() for p in [path, *path.parents]), 'symlink_evidence')
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_size <= limit, 'invalid_evidence_file')
    return path.read_bytes()


def read(path):
    value = json.loads(raw(path))
    require(isinstance(value, dict), 'invalid_evidence_object')
    return value


def git(repo, *args):
    proc = subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', '-C', repo, *args],
                          capture_output=True, text=True, timeout=15,
                          env={**os.environ, 'GIT_OPTIONAL_LOCKS': '0', 'GIT_TERMINAL_PROMPT': '0'})
    require(proc.returncode == 0, 'git_evidence_failed')
    return proc.stdout.strip()


def is_ancestor(repo, older, newer):
    proc = subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', '-C', repo, 'merge-base', '--is-ancestor', older, newer],
                          capture_output=True, timeout=15,
                          env={**os.environ, 'GIT_OPTIONAL_LOCKS': '0', 'GIT_TERMINAL_PROMPT': '0'})
    require(proc.returncode in (0, 1), 'git_evidence_failed')
    return proc.returncode == 0


def audit_records(path):
    # Shared lock prevents reading a partially appended integrator audit record.
    import fcntl
    with open(path, 'rb') as handle:
        fcntl.flock(handle, fcntl.LOCK_SH)
        content = raw(path, 64_000_000).decode('utf-8')
    records, prev = [], ''
    for line in content.splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        digest = record.pop('hash')
        require(record.get('prev') == (hashlib.sha256(prev.encode()).hexdigest() if prev else '0'*64), 'audit_chain_broken')
        require(hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode()).hexdigest() == digest,
                'audit_content_changed')
        records.append({**record, 'hash': digest})
        prev = line.strip()
    require(bool(records), 'audit_missing')
    return records


def verify_failed_history(item, status, inbox, grants, tip, settings):
    """A failed ancestor is history only with authentic evidence of no merge.

    This supplies no completion proof: the current request must still pass all
    success checks. Never discard a prior grant on the strength of prose/receipt.
    """
    code = 'obsolete_failed_request_not_proven'
    rid = item['id']
    marker = read(grants/(rid+'.granted'))
    gid = marker.get('grant')
    require(isinstance(gid, str) and GID.fullmatch(gid), code)
    grant = read(grants/(gid+'.json'))['grant']
    require(grant.get('id') == gid and grant.get('request_id') == rid
            and grant.get('request_sha256') == hashlib.sha256(raw(inbox/(rid+'.json'))).hexdigest()
            and all(grant.get(k) == item.get(k) for k in ('repo','source_branch','target_commit','mode'))
            and grant.get('main_branch') == 'main', code)
    auto = grant.get('auto') or {}
    require(auto.get('task') == item['task_id'] and not auto.get('chain')
            and auto.get('policy_sha256') == settings['policy_sha256'], code)
    human = auto.get('human_approval')
    if human is not None:
        from . import human_approval as ha
        approved = dt.datetime.fromisoformat(grant['approved_at'].replace('Z', '+00:00'))
        require(approved.tzinfo is not None, code)
        ha.validate_grant(grants, grant, at=approved.timestamp())
    else:
        require(not grant.get('allow_protected')
                and grant.get('approved_by','auto-integratie') == 'auto-integratie', code)
    used = read(grants/(gid+'.used'))
    require(bool(used) and not (grants/(gid+'.revoked')).exists(), code)
    result = read(grants/(gid+'.result'))
    require(result.get('ok') is False and result.get('result') == 'tests_failed'
            and result.get('grant') == gid and result.get('repo') == item['repo']
            and result.get('mode') == 'ff-only' and result.get('target_commit') == item['target_commit']
            and result.get('head_before') == grant.get('expected_main_head')
            and result.get('head_after') is None and result.get('recovery_branch') is None
            and bool(result.get('problems')), code)
    expected = set(grant.get('tests') or [])
    require(bool(expected) and set(item.get('tests') or []) <= expected
            and bool(auto.get('policy_tests')) and set(auto['policy_tests']) <= expected, code)
    pretest_error = ('tests' not in result and result['problems'] ==
                     ['de tests konden niet draaien (OSError); main is niet aangeraakt'])
    if not pretest_error:
        tests = result.get('tests') or {}
        runs = tests.get('results') or []
        require(tests.get('ok') is False and runs
                and any(r.get('ok') is False and isinstance(r.get('returncode'), int)
                        and r['returncode'] != 0 and r.get('outcome') == 'done' for r in runs), code)
        actual = {t for r in runs for t in (r.get('budget') or {}).get('tests', [])}
        require(expected <= actual, code)
    require(status.get('request') == rid and status.get('grant') == gid
            and status.get('state') == 'failed' and status.get('ok') is False
            and status.get('result') == 'tests_failed'
            and status.get('target_commit') == item['target_commit']
            and status.get('head_after') is None and status.get('recovery_branch') is None
            and status.get('problems') == result['problems'], code)
    records = audit_records(grants/'audit.jsonl')
    gs = [r for r in records if r.get('kind') == 'grant' and r.get('grant') == gid]
    ss = [r for r in records if r.get('kind') == 'start' and r.get('grant') == gid]
    rs = [r for r in records if r.get('kind') == 'result' and r.get('grant') == gid]
    require(len(gs) == 1 and len(ss) == 1 and len(rs) == 1
            and records.index(gs[0]) < records.index(ss[0]) < records.index(rs[0]), code)
    require(ss[0].get('repo') == item['repo']
            and ss[0].get('target_commit') == item['target_commit']
            and ss[0].get('head_before') == grant['expected_main_head']
            and ss[0].get('recovery_branch') == used.get('recovery_branch'), code)
    require(gs[0].get('request') == rid and all(gs[0].get(k) == grant.get(k) for k in
            ('repo','source_branch','target_commit','expected_main_head','mode','tests','request_sha256','auto'))
            and all(rs[0].get(k) == result.get(k) for k in ('ok','result','head_after','tests','problems')), code)
    if human is not None:
        require(gs[0].get('by') == grant.get('approved_by')
                and gs[0].get('allow_protected') == grant.get('allow_protected'), code)
    recovery = used.get('recovery_branch')
    require(isinstance(recovery,str) and recovery.startswith('recovery/'), code)
    git(item['repo'],'check-ref-format','refs/heads/'+recovery)
    if pretest_error:
        # This branch is created only after the pretest. Its presence makes the history ambiguous.
        absent = subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', '-C', item['repo'],
                                 'show-ref', '--verify', '--quiet', 'refs/heads/'+recovery],
                                capture_output=True, timeout=10,
                                env={**os.environ, 'GIT_OPTIONAL_LOCKS': '0', 'GIT_TERMINAL_PROMPT': '0'})
        require(absent.returncode == 1, code)
    git(item['repo'],'merge-base','--is-ancestor',item['target_commit'],tip)
    git(item['repo'],'merge-base','--is-ancestor',grant['expected_main_head'],tip)


def verify_no_task_history(item, inbox, grants, repo, tip):
    """Authenticate an ungranted refusal, never a replacement completion proof.

    The old audit has no request digest: it proves the refusal for this ID,
    not the original request contents. Snapshot current bytes only for TOCTOU.
    """
    code = 'no_task_history_not_proven'
    rid = item['id']
    require(item.get('task_id') == '' and item.get('repo') == repo
            and item.get('mode') == 'ff-only'
            and isinstance(item.get('target_commit'), str) and SHA.fullmatch(item['target_commit'])
            and item['target_commit'] != tip
            and isinstance(item.get('source_branch'), str) and bool(item['source_branch'])
            and not item['source_branch'].startswith('r2/'), code)
    git(repo, 'check-ref-format', 'refs/heads/'+item['source_branch'])
    paths = (inbox/(rid+'.json'), inbox/(rid+'.status.json'), grants/(rid+'.autorefused'))
    contents = tuple(raw(path) for path in paths)
    request, receipt, marker = (json.loads(value) for value in contents)
    require(request == item and isinstance(receipt, dict) and isinstance(marker, dict), code)
    require(receipt.get('request') == rid and receipt.get('state') == 'refused'
            and receipt.get('auto') is True and receipt.get('code') == 'no_task'
            and not receipt.get('ok') and receipt.get('grant') is None
            and marker.get('code') == 'no_task' and marker.get('attention') is True
            and marker.get('info') is False and isinstance(marker.get('at'), str) and bool(marker['at'])
            and marker['at'] == receipt.get('updated_at')
            and isinstance(marker.get('text'), str) and bool(marker['text'])
            and marker['text'] == receipt.get('message'), code)
    require(not any(path.exists() or path.is_symlink() for path in
                    (grants/(rid+'.granted'), grants/(rid+'.rejected'))), code)
    records = audit_records(grants/'audit.jsonl')
    refused = [r for r in records if r.get('kind') == 'auto_refused' and r.get('request') == rid]
    require(len(refused) == 1 and 'task' in refused[0] and refused[0]['task'] is None
            and refused[0].get('code') == 'no_task' and refused[0].get('retry') is False
            and refused[0].get('at') == marker['at']
            and not any(r.get('kind') in ('grant', 'reject') and r.get('request') == rid for r in records), code)
    # A removed marker, revoked grant, or unaudited grant file is never "no grant".
    for path in grants.glob('G-*.json'):
        grant = read(path).get('grant')
        require(isinstance(grant, dict) and grant.get('request_id') != rid, code)
    return {'contents': contents, 'event': refused[0]}


STANDING_REFUSAL_CODES = ('too_many_tests', 'request_not_in_finish', 'expired')


def verify_standing_refusal(item, inbox, grants, codes=STANDING_REFUSAL_CODES, *, require_content=False):
    code = 'standing_refusal_not_proven'
    rid = item['id']
    paths = (inbox/(rid+'.json'), inbox/(rid+'.status.json'), grants/(rid+'.autorefused'))
    contents = tuple(raw(path) for path in paths)
    request, receipt, marker = (json.loads(value) for value in contents)
    def when(value):
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(parsed.tzinfo is not None, code)
        return parsed
    require(request == item and receipt.get('request') == rid and receipt.get('state') == 'refused'
            and receipt.get('auto') is True and receipt.get('code') == marker.get('code')
            and marker.get('code') in codes
            and isinstance(marker.get('text'), str) and bool(marker['text'])
            and receipt.get('message') == marker['text'] and bool(marker.get('at'))
            and isinstance(receipt.get('updated_at'), str) and not receipt.get('ok')
            and receipt.get('grant') is None and isinstance(marker.get('attention'), bool)
            and isinstance(marker.get('info'), bool), code)
    require(not any((grants/(rid+x)).exists() or (grants/(rid+x)).is_symlink()
                    for x in ('.granted', '.rejected')), code)
    records = audit_records(grants/'audit.jsonl')
    events = [r for r in records if r.get('kind') == 'auto_refused' and r.get('request') == rid]
    require(len(events) == 1 and events[0].get('task') == item['task_id']
            and events[0].get('code') == marker['code'] and events[0].get('retry') is False
            and isinstance(events[0].get('at'), str)
            and events[0].get('request_sha256', hashlib.sha256(contents[0]).hexdigest()) == hashlib.sha256(contents[0]).hexdigest()
            and events[0].get('target_commit', item['target_commit']) == item['target_commit']
            and events[0].get('source_branch', item['source_branch']) == item['source_branch']
            and not any(r.get('kind') in ('grant', 'reject') and r.get('request') == rid for r in records), code)
    if require_content:
        require(events[0].get('request_sha256') == hashlib.sha256(contents[0]).hexdigest()
                and events[0].get('target_commit') == item['target_commit']
                and events[0].get('source_branch') == item['source_branch'], code)
    times = [dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
             for value in (marker['at'], events[0]['at'], receipt['updated_at'])]
    require(all(value.tzinfo is not None for value in times) and times[0] <= times[1] <= times[2], code)
    for path in grants.glob('G-*.json'):
        grant = read(path).get('grant')
        require(isinstance(grant, dict) and grant.get('request_id') != rid, code)
    return {'contents': contents, 'event': events[0]}


def verify_already_integrated(task, request, request_bytes, settings, home, node):
    """Prove a zero-delta run against an actual, fully verified earlier merge.

    The current refusal stays refused and receives no grant. Extra verification
    tests requested by the no-op task are not represented as pre-merge tests;
    only the earlier merge's exact-tree tests are carried in ``prior``.
    """
    repo, tid, rid = task['repo'], task['task_id'], request['id']
    tip = request['target_commit']
    inbox, grants = Path(settings['inbox']), Path(settings['grants'])
    require((task.get('base') or {}).get('commit') == tip
            and (task.get('integration') or {}).get('base_tip') == tip
            and (task.get('integration') or {}).get('commits_ahead') == 0,
            'already_integrated_task_has_delta')
    wt = Path(task.get('worktree', ''))
    require(wt.is_absolute() and wt.resolve() == (home/'worktrees'/tid).resolve()
            and git(wt, 'symbolic-ref', '--short', 'HEAD') == 'r2/'+tid
            and git(wt, 'rev-parse', 'HEAD') == tip
            and not git(wt, 'status', '--porcelain', '--untracked-files=all'),
            'already_integrated_worktree_changed')
    # All local requests for this task must agree with the one terminal refusal.
    own = [p.stem for p in inbox.glob('R-*.json') if '.status.' not in p.name
           and read(p).get('task_id') == tid]
    require(own == [rid], 'already_integrated_request_ambiguous')
    receipt = read(inbox/(rid+'.status.json'))
    marker = read(grants/(rid+'.autorefused'))
    require(receipt.get('request') == rid and receipt.get('state') == 'refused'
            and receipt.get('auto') is True and receipt.get('code') == 'nothing_to_do'
            and marker.get('code') == 'nothing_to_do' and marker.get('info') is True
            and marker.get('attention') is False and bool(marker.get('at'))
            and marker.get('at') == receipt.get('updated_at')
            and not any((grants/(rid+suffix)).exists() for suffix in ('.granted', '.rejected')),
            'already_integrated_refusal_not_proven')
    records = audit_records(grants/'audit.jsonl')
    refused = [r for r in records if r.get('kind') == 'auto_refused' and r.get('request') == rid]
    require(len(refused) == 1 and refused[0].get('task') == tid
            and refused[0].get('code') == 'nothing_to_do' and refused[0].get('at') == marker['at']
            and not any(r.get('kind') == 'grant' and r.get('request') == rid for r in records),
            'already_integrated_refusal_audit_mismatch')
    origins = []
    for path in grants.glob('R-*.granted'):
        item = read(inbox/(path.stem+'.json'))
        if item.get('repo') == repo and item.get('target_commit') == tip and item.get('task_id') != tid:
            status = read(inbox/(path.stem+'.status.json'))
            if status.get('state') == 'merged':
                origins.append(item)
    require(len(origins) == 1, 'already_integrated_origin_ambiguous')
    origin = origins[0]
    prior = verify({'task_id': origin['task_id'], 'repo': repo, 'node': node}, settings, home,
                   allow_already_integrated=False)
    require(valid_reply(prior, {'task_id': origin['task_id'], 'repo': repo, 'node': node})
            and prior.get('request') == origin['id'] and prior.get('commit') == tip,
            'already_integrated_origin_not_proven')
    # Prior tests, grant and recovery must predate the current audited refusal.
    prior_events = [r for r in records if r.get('hash') == prior['audit_hash']]
    require(len(prior_events) == 1 and records.index(prior_events[0]) < records.index(refused[0]),
            'already_integrated_origin_order_mismatch')
    main = git(repo, 'rev-parse', '--verify', 'refs/heads/main')
    git(repo, 'merge-base', '--is-ancestor', tip, main)
    require(prior['main_head'] == main and git(repo, 'symbolic-ref', '--short', 'HEAD') == 'main'
            and not git(repo, 'status', '--porcelain', '--untracked-files=all')
            and git(repo, 'rev-parse', 'refs/heads/r2/'+tid) == tip
            and git(wt, 'rev-parse', 'HEAD') == tip
            and not git(wt, 'status', '--porcelain', '--untracked-files=all')
            and read(home/'tasks'/tid/'state.json') == task
            and raw(inbox/(rid+'.json')) == request_bytes
            and read(inbox/(rid+'.status.json')) == receipt
            and read(grants/(rid+'.autorefused')) == marker
            and not any((grants/(rid+suffix)).exists() for suffix in ('.granted', '.rejected')),
            'already_integrated_evidence_changed')
    return {'ok': True, 'kind': 'already_integrated', 'node': node, 'task_id': tid, 'repo': repo,
            'request': rid, 'request_sha256': hashlib.sha256(request_bytes).hexdigest(),
            'commit': tip, 'main_head': main, 'audit_hash': refused[0]['hash'], 'prior': prior}


def verify_refusal_content(item, inbox, grants, snapshot):
    """Content binding for a standing too_many_tests refusal of an EARLIER commit of the task branch.

    Accepted only from the integrator's own audit chain: either the refusal record itself carries the request digest
    (recorded at refusal time: a real binding), or exactly one later ``refusal_bound`` record of kind ``anchored``. That
    record is NOT a witness of the content at refusal time. It states that the integrator checked the bytes then present
    against an anchor of proven provenance: the commit merged by the superseding request's grant, whose text (written by the
    same task, after the refusal) names the old request, its target prefix and the omitted tests; the shared test order and
    the other fields come from the superseding request, which its grant binds. Not anchored: the position of the omitted
    tests, the exact created_at (only bounded by the request's first audit record), and the byte form (key order, escapes).
    From the binding on the bytes are digest-bound. The anchor itself is checked again against the current grant in
    ``verify``."""
    code = 'refusal_content_not_bound'
    rid, event = item['id'], snapshot['event']
    sha = hashlib.sha256(snapshot['contents'][0]).hexdigest()
    asked = len(dict.fromkeys(item.get('tests') or []))
    require(event.get('code') == 'too_many_tests', code)
    if 'request_sha256' in event:
        require(event['request_sha256'] == sha and event.get('target_commit') == item['target_commit']
                and event.get('source_branch') == item['source_branch'], code)
        return {'form': 'at_refusal', 'event': event, 'request_sha256': sha}
    records = audit_records(grants/'audit.jsonl')
    bound = [r for r in records if r.get('kind') == 'refusal_bound' and r.get('request') == rid]
    require(len(bound) == 1 and event in records and records.index(event) < records.index(bound[0]), code)
    b = bound[0]
    marker = json.loads(snapshot['contents'][2])
    require(b.get('by') == 'auto-integratie' and b.get('task') == item['task_id'] and b.get('code') == 'too_many_tests'
            and b.get('request_sha256') == sha and b.get('target_commit') == item['target_commit']
            and b.get('source_branch') == item['source_branch'] and b.get('repo') == item['repo']
            and b.get('refused_hash') == event.get('hash') and b.get('refused_at') == event.get('at')
            and b.get('marker_text') == marker.get('text') and b.get('tests_asked') == asked and asked > 0
            and f'noemt {asked} testbestanden' in marker.get('text', '')
            and b.get('reflog_tip') == item['target_commit'] and b.get('binding') == 'anchored'
            and isinstance(b.get('superseded_by'), str) and isinstance(b.get('grant'), str)
            and isinstance(b.get('anchor_commit'), str) and SHA.fullmatch(b['anchor_commit'])
            and isinstance(b.get('anchor_path'), str) and isinstance(b.get('target_prefix'), str)
            and isinstance(b.get('omitted'), list) and all(isinstance(x, str) and x for x in b['omitted']), code)
    return {'form': 'anchored', 'event': b, 'request_sha256': sha}


def verify_refusal_anchor(repo, old, binding, request, rid, gid, target, marker_at, records):
    """The anchor of an ``anchored`` binding, checked independently: it is exactly the merged target of the current
    request and grant; it describes the old request; and the old request equals the current one except for the
    described target and the omitted tests."""
    code = 'refusal_anchor_mismatch'
    b = binding['event']
    prefix, omitted = b['target_prefix'], b['omitted']
    tests_old, tests_new = old.get('tests') or [], request.get('tests') or []
    require(b['superseded_by'] == rid and b['grant'] == gid and b['anchor_commit'] == target
            and len(prefix) >= 9 and old['target_commit'].startswith(prefix)
            and len(tests_old) == len(set(tests_old)) and not set(omitted) & set(tests_new)
            and set(tests_old) == set(tests_new) | set(omitted)
            and [x for x in tests_old if x in set(tests_new)] == list(tests_new)
            and all(old.get(k) == request.get(k) for k in ('schema', 'repo', 'source_branch', 'mode', 'note', 'task_id'))
            and isinstance(old.get('created_at'), str), code)
    created = dt.datetime.fromisoformat(old['created_at'].replace('Z', '+00:00'))
    refused = dt.datetime.fromisoformat(marker_at.replace('Z', '+00:00'))
    first = min((dt.datetime.fromisoformat(r['at'].replace('Z', '+00:00')) for r in records
                 if r.get('request') == old['id'] and isinstance(r.get('at'), str)), default=None)
    require(created.tzinfo is not None and refused.tzinfo is not None and created <= refused
            and first is not None and created <= first, code)
    blob = git(repo, 'show', f"{target}:{b['anchor_path']}")
    require(all(s in blob for s in (old['id'], prefix, 'too_many_tests', *(Path(o).stem for o in omitted))), code)


def verify(args, settings=None, home=None, *, allow_already_integrated=True):
    """Never infer success from the informational receipt alone. Fail closed on any gap.

    Historical grant signatures use an ephemeral process key. Durable proof is the
    trusted host's complete audit chain + exact grant/result/request binding + Git.
    This is not an adversarial security boundary against the host account itself.
    """
    try:
        home = Path(home or config.r2_home())
        settings = settings if settings is not None else read(home/'config'/'integration-check.json')
        repo, tid = args.get('repo'), args.get('task_id')
        node = config.local_node()
        require(args.get('node', node) == node, 'node_identity_mismatch')
        require(isinstance(repo, str) and Path(repo).is_absolute() and isinstance(tid, str) and TID.fullmatch(tid), 'target_not_allowed')
        if 'repositories' in settings:
            require('repo' not in settings and isinstance(settings['repositories'], dict), 'invalid_repository_config')
            selected = settings['repositories'].get(repo)
            require(isinstance(selected, dict), 'target_not_allowed')
            require('repo' not in selected, 'invalid_repository_config')
            settings = dict(selected, repo=repo)
        require(repo == settings.get('repo'), 'target_not_allowed')
        require(settings.get('approval_mode', 'legacy') in ('legacy', 'standing'), 'invalid_repository_config')
        standing = settings.get('approval_mode') == 'standing'
        require(all(isinstance(settings.get(k), str) and Path(settings[k]).is_absolute() for k in ('inbox', 'grants'))
                and bool(re.fullmatch(r'[0-9a-f]{64}', str(settings.get('policy_sha256', '')))), 'invalid_repository_config')
        task = read(home/'tasks'/tid/'state.json')
        require(task.get('task_id') == tid and task.get('repo') == repo and task.get('status') == 'completed', 'task_not_completed')
        require(not any(task.get(k) for k in ('pid', 'claude_pid', 'failure', 'chain', 'supervisor', 'chain_id', 'supervisor_id',
                                                    'question', 'in_flight', 'inbox', 'orphans')), 'task_not_standalone_or_active')
        require((task.get('integration') or {}).get('worktree_clean') is True, 'task_worktree_not_clean')
        if standing:
            finish = task.get('finish')
            require(isinstance(finish, dict) and isinstance(finish.get('summary'), str)
                    and bool(finish['summary'].strip()), 'task_finish_missing')
        ids = set(RID.findall((task.get('finish') or {}).get('summary') or ''))
        require(bool(ids) or standing, 'request_missing_or_ambiguous')
        finish_ids = set(ids)
        inbox, grants = Path(settings['inbox']), Path(settings['grants'])
        branch = 'r2/'+tid
        if standing:
            for path in inbox.glob('R-*.json'):
                if not RID.fullmatch(path.stem):
                    continue
                item = read(path)
                if item.get('repo') == repo and item.get('task_id') == tid:
                    ids.add(path.stem)
        tip = git(repo, 'rev-parse', '--verify', 'refs/heads/'+branch)
        candidates, rejected, auto_duplicates, no_task_history, standing_history = [], [], [], [], []
        earlier_refusals, divergent_refusals, refused_bases = [], [], []
        # Reports can mention a replaced request. Select by immutable identity,
        # never by prose order, timestamps, or a merely green receipt.
        for candidate in sorted(ids):
            try:
                item = read(inbox/(candidate+'.json'))
            except FileNotFoundError:
                # A report may cite a request on another node. Absent prose
                # references provide no evidence; the single local request must
                # still pass every identity, grant, audit, test and Git check.
                require(not (grants/(candidate+'.granted')).exists(), 'granted_request_missing')
                continue
            other_tid = item.get('task_id')
            if other_tid == '':
                require(item.get('id') == candidate, 'request_identity_mismatch')
                history = verify_no_task_history(item, inbox, grants, repo, tip)
                no_task_history.append((item, history))
                continue
            if other_tid != tid:
                # An audit report may cite historical requests from other tasks.
                # Exclude only a structurally bound, completed task in this repo;
                # it supplies no evidence for the task currently being verified.
                require(isinstance(other_tid, str) and TID.fullmatch(other_tid)
                        and item.get('id') == candidate and item.get('repo') == repo
                        and item.get('source_branch') == 'r2/'+other_tid
                        and item.get('mode') == 'ff-only'
                        and isinstance(item.get('target_commit'), str)
                        and SHA.fullmatch(item['target_commit'])
                        and item['target_commit'] != tip, 'request_identity_mismatch')
                other_task = read(home/'tasks'/other_tid/'state.json')
                require(other_task.get('task_id') == other_tid and other_task.get('repo') == repo
                        and other_task.get('branch') == 'r2/'+other_tid
                        and other_task.get('status') == 'completed', 'request_identity_mismatch')
                continue
            require(item.get('id') == candidate and item.get('task_id') == tid
                    and item.get('repo') == repo and item.get('source_branch') == branch
                    and item.get('mode') == 'ff-only'
                    and isinstance(item.get('target_commit'), str)
                    and SHA.fullmatch(item['target_commit']), 'request_identity_mismatch')
            # A retry may target the identical commit after a transient test failure.
            # Exclude it only with full authenticated proof that it never merged;
            # the remaining current request still needs every success check below.
            if item['target_commit'] == tip and (inbox/(candidate+'.status.json')).exists():
                status = read(inbox/(candidate+'.status.json'))
                if standing and status.get('state') == 'refused' and status.get('code') in ('too_many_tests', 'request_not_in_finish', 'expired'):
                    standing_history.append((item, verify_standing_refusal(item, inbox, grants)))
                    continue
                if status.get('state') == 'failed':
                    verify_failed_history(item, status, inbox, grants, tip, settings)
                    continue
            if item['target_commit'] == tip:
                receipt_path = inbox/(candidate+'.status.json')
                if receipt_path.exists():
                    receipt = read(receipt_path)
                    if receipt.get('state') == 'refused' and receipt.get('code') == 'nothing_to_do':
                        # A controller-refused exact duplicate is history, never proof.
                        # Keep it until one fully proven merge has been selected below.
                        marker_path = grants/(candidate+'.autorefused')
                        marker = read(marker_path)
                        require(receipt.get('request') == candidate and receipt.get('auto') is True
                                and marker.get('code') == 'nothing_to_do'
                                and marker.get('info') is True and marker.get('attention') is False
                                and bool(marker.get('at')) and marker['at'] == receipt.get('updated_at')
                                and not any((grants/(candidate+s)).exists() for s in ('.granted', '.rejected')),
                                'duplicate_auto_refusal_not_proven')
                        auto_duplicates.append((candidate, item, marker, receipt))
                        continue
                marker_path = grants/(candidate+'.rejected')
                if marker_path.exists():
                    marker = read(marker_path)
                    receipt = read(inbox/(candidate+'.status.json'))
                    require(not (grants/(candidate+'.granted')).exists()
                            and isinstance(marker.get('by'), str) and bool(marker['by'].strip())
                            and receipt.get('request') == candidate and receipt.get('state') == 'rejected'
                            and all(receipt.get(k) == marker.get(k) for k in ('by', 'reason')),
                            'duplicate_rejection_not_proven')
                    rejected.append((candidate, item, marker))
                else:
                    candidates.append(candidate)
            else:
                status = read(inbox/(candidate+'.status.json'))
                if status.get('state') == 'failed':
                    verify_failed_history(item, status, inbox, grants, tip, settings)
                    continue
                if standing and status.get('state') == 'refused' and status.get('code') == 'nothing_to_do':
                    snapshot = verify_standing_refusal(item, inbox, grants, codes=('nothing_to_do',),
                                                       require_content=True)
                    refused_bases.append((item, snapshot))
                    continue
                if standing and status.get('state') == 'refused' and status.get('code') == 'not_fast_forward':
                    snapshot = verify_standing_refusal(item, inbox, grants, codes=('not_fast_forward',),
                                                       require_content=True)
                    divergent_refusals.append((item, snapshot))
                    continue
                if standing and status.get('state') == 'refused' and status.get('code') == 'too_many_tests':
                    # Standing refusal of an earlier commit of this branch (the task then moved the branch and
                    # requested the new tip separately): full never-granted proof plus a content binding from the
                    # integrator's audit; order, ancestry and main are checked against the current grant below.
                    snapshot = verify_standing_refusal(item, inbox, grants)
                    earlier_refusals.append((item, snapshot, verify_refusal_content(item, inbox, grants, snapshot)))
                    continue
                require(status.get('request') == candidate and status.get('state') == 'refused'
                        and status.get('code') == 'source_moved'
                        and not (grants/(candidate+'.granted')).exists(),
                        'obsolete_request_not_safely_refused')
                if standing:
                    # The receipt is informational (the sandbox can write the inbox): in standing mode an obsolete request
                    # also needs the integrator's own marker and exactly one audited source_moved refusal, no grant trace.
                    try:
                        verify_standing_refusal(item, inbox, grants, codes=('source_moved',))
                    except (ProofError, OSError, ValueError, KeyError, TypeError):
                        raise ProofError('obsolete_request_not_safely_refused') from None
        if not candidates and len(auto_duplicates) == 1:
            candidates.append(auto_duplicates.pop()[0])
        require(len(candidates) == 1, 'request_missing_or_ambiguous')
        rid = candidates[0]
        request_bytes = raw(inbox/(rid+'.json'))
        request = json.loads(request_bytes)
        for duplicate, item, marker in rejected:
            # Only an exact semantic duplicate may be excluded. Its rejection
            # must be controller-owned, audited, and have no prior grant.
            semantic = lambda value: {k: v for k, v in value.items() if k not in ('id', 'created_at')}
            require(semantic(item) == semantic(request), 'rejected_request_not_exact_duplicate')
            events = [r for r in audit_records(grants/'audit.jsonl')
                      if r.get('kind') == 'reject' and r.get('request') == duplicate]
            require(len(events) == 1 and all(events[0].get(k) == marker.get(k) for k in ('by', 'reason')),
                    'duplicate_rejection_not_proven')
        for duplicate, item, marker, receipt in auto_duplicates:
            semantic = lambda value: {k: v for k, v in value.items() if k not in ('id', 'created_at')}
            require(semantic(item) == semantic(request), 'auto_refused_request_not_exact_duplicate')
            events = audit_records(grants/'audit.jsonl')
            refused = [r for r in events if r.get('kind') == 'auto_refused' and r.get('request') == duplicate]
            require(len(refused) == 1 and refused[0].get('task') == tid
                    and refused[0].get('code') == 'nothing_to_do' and refused[0].get('retry') is False
                    and refused[0].get('at') == marker['at']
                    and not any(r.get('kind') in ('grant', 'reject') and r.get('request') == duplicate for r in events),
                    'duplicate_auto_refusal_not_proven')
        target = request.get('target_commit')
        require(isinstance(target, str) and SHA.fullmatch(target), 'invalid_target_commit')
        branch = 'r2/'+tid
        require(request.get('id') == rid and request.get('task_id') == tid and request.get('repo') == repo
                and request.get('source_branch') == branch and task.get('branch') == branch
                and request.get('mode') == 'ff-only', 'request_identity_mismatch')
        if no_task_history:
            require((grants/(rid+'.granted')).exists(), 'no_task_history_current_merge_missing')
        if allow_already_integrated and not (grants/(rid+'.granted')).exists():
            receipt = read(inbox/(rid+'.status.json'))
            if receipt.get('state') == 'refused' and receipt.get('code') == 'nothing_to_do':
                return verify_already_integrated(task, request, request_bytes, settings, home, node)
        marker = read(grants/(rid+'.granted'))
        gid = marker.get('grant')
        require(isinstance(gid, str) and GID.fullmatch(gid), 'invalid_grant_id')
        envelope = read(grants/(gid+'.json'))
        grant = envelope['grant']
        require(grant.get('id') == gid and grant.get('request_id') == rid
                and grant.get('request_sha256') == hashlib.sha256(request_bytes).hexdigest(), 'grant_request_mismatch')
        auto = grant.get('auto') or {}
        human = auto.get('human_approval')
        grant_standing = auto.get('approval_mode') == 'standing'
        require(rid in finish_ids or grant_standing, 'request_not_in_finish')
        require(auto.get('approval_mode') in (None, 'standing'), 'auto_policy_mismatch')
        if grant_standing:
            require(standing and human is None and grant.get('approved_by') == 'auto-integratie'
                    and isinstance(auto.get('protected_paths'), list)
                    and all(isinstance(x, str) and x for x in auto['protected_paths'])
                    and type(grant.get('allow_protected')) is bool
                    and (not grant['allow_protected'] or bool(auto['protected_paths']))
                    and bool(re.fullmatch(r'[0-9a-f]{64}', str(envelope.get('sig', ''))))
                    and bool(re.fullmatch(r'[0-9a-f]{12}', str(grant.get('key_id', '')))), 'standing_grant_invalid')
        require(all(grant.get(k) == request.get(k) for k in ('repo', 'source_branch', 'target_commit', 'mode'))
                and grant.get('main_branch') == 'main'
                and (not grant.get('allow_protected') or human is not None or grant_standing), 'grant_identity_mismatch')
        if human is not None:
            from . import human_approval as ha
            approved = dt.datetime.fromisoformat(grant['approved_at'].replace('Z', '+00:00'))
            require(approved.tzinfo is not None, 'human_approval_invalid')
            ha.validate_grant(grants, grant, at=approved.timestamp())
        else:
            require(grant.get('approved_by', 'auto-integratie') == 'auto-integratie', 'human_approval_missing')
        require(auto.get('task') == tid and not auto.get('chain')
                and auto.get('policy_sha256') == settings['policy_sha256'], 'auto_policy_mismatch')
        used = read(grants/(gid+'.used'))
        require(bool(used), 'grant_not_consumed')
        require(not (grants/(gid+'.revoked')).exists(), 'grant_revoked')
        result = read(grants/(gid+'.result'))
        require(result.get('ok') is True and result.get('result') == 'merged' and not result.get('problems')
                and result.get('tested_before_merge') is True, 'integration_not_proven')
        require(result.get('grant') == gid and result.get('repo') == repo and result.get('mode') == 'ff-only'
                and result.get('target_commit') == target and result.get('head_after') == target
                and result.get('head_before') == grant.get('expected_main_head'), 'result_identity_mismatch')
        tests = result.get('tests') or {}
        runs = tests.get('results') or []
        require(tests.get('ok') is True and runs and all(r.get('ok') is True and r.get('returncode') == 0
                and r.get('outcome') == 'done' for r in runs), 'tests_not_proven')
        expected_tests = grant.get('tests') or []
        actual_tests = {t for r in runs for t in (r.get('budget') or {}).get('tests', [])}
        require(bool(expected_tests) and set(expected_tests) <= actual_tests
                and set(request.get('tests') or []) <= set(expected_tests)
                and bool(auto.get('policy_tests')) and set(auto['policy_tests']) <= set(expected_tests), 'test_identity_mismatch')
        receipt = read(inbox/(rid+'.status.json'))
        require(receipt.get('request') == rid and receipt.get('grant') == gid and receipt.get('ok') is True
                and receipt.get('state') == 'merged' and receipt.get('result') == 'merged'
                and receipt.get('target_commit') == target and receipt.get('head_after') == target
                and not receipt.get('problems'), 'receipt_mismatch')
        records = audit_records(grants/'audit.jsonl')
        gs = [r for r in records if r.get('kind') == 'grant' and r.get('grant') == gid]
        rs = [r for r in records if r.get('kind') == 'result' and r.get('grant') == gid]
        require(len(gs) == 1 and len(rs) == 1 and records.index(gs[0]) < records.index(rs[0]), 'audit_events_missing_or_ambiguous')
        ge, revent = gs[0], rs[0]
        require(ge.get('request') == rid and all(ge.get(k) == grant.get(k) for k in
                ('repo', 'source_branch', 'target_commit', 'expected_main_head', 'mode', 'tests', 'request_sha256', 'auto')), 'audit_grant_mismatch')
        if grant_standing:
            require(ge.get('by') == 'auto-integratie' and ge.get('allow_protected') == grant.get('allow_protected'),
                    'audit_standing_approval_mismatch')
        if human is not None:
            require(ge.get('by') == grant.get('approved_by')
                    and ge.get('allow_protected') == grant.get('allow_protected'), 'audit_human_approval_mismatch')
        require(all(revent.get(k) == result.get(k) for k in ('ok', 'result', 'head_after', 'tests', 'problems')), 'audit_result_mismatch')
        for duplicate, item, marker, duplicate_receipt in auto_duplicates:
            refused = [r for r in records if r.get('kind') == 'auto_refused' and r.get('request') == duplicate]
            require(len(refused) == 1 and records.index(revent) < records.index(refused[0])
                    and read(inbox/(duplicate+'.json')) == item
                    and read(inbox/(duplicate+'.status.json')) == duplicate_receipt
                    and read(grants/(duplicate+'.autorefused')) == marker
                    and not any((grants/(duplicate+s)).exists() for s in ('.granted', '.rejected')),
                    'duplicate_auto_refusal_changed')
        require(git(repo, 'rev-parse', '--verify', 'refs/heads/'+branch) == target, 'branch_changed_after_request')
        recovery = result.get('recovery_branch')
        require(isinstance(recovery, str) and recovery.startswith('recovery/')
                and recovery == used.get('recovery_branch'), 'recovery_not_proven')
        require(git(repo, 'rev-parse', '--verify', 'refs/heads/'+recovery) == grant['expected_main_head'],
                'recovery_identity_mismatch')
        require(git(repo, 'symbolic-ref', '--short', 'HEAD') == 'main'
                and not git(repo, 'status', '--porcelain', '--untracked-files=all'), 'main_not_clean')
        main = git(repo, 'rev-parse', '--verify', 'refs/heads/main')
        git(repo, 'merge-base', '--is-ancestor', target, main)
        git(repo, 'merge-base', '--is-ancestor', grant['expected_main_head'], target)
        for historical, snapshot in no_task_history:
            require(snapshot['event'] in records
                    and records.index(snapshot['event']) < records.index(ge),
                    'no_task_history_order_mismatch')
            require(verify_no_task_history(historical, inbox, grants, repo, tip) == snapshot,
                    'no_task_history_changed')
        for historical, snapshot in standing_history:
            require(snapshot['event'] in records and records.index(snapshot['event']) < records.index(ge)
                    and verify_standing_refusal(historical, inbox, grants) == snapshot, 'standing_refusal_changed')
        for historical, snapshot in refused_bases:
            # A refused request for the unchanged base is history only.
            require(grant_standing and snapshot['event'] in records
                    and records.index(snapshot['event']) < records.index(ge)
                    and historical['target_commit'] == grant['expected_main_head']
                    and historical['target_commit'] == (task.get('base') or {}).get('commit')
                    and historical['tests'] == request['tests']
                    and verify_standing_refusal(historical, inbox, grants, codes=('nothing_to_do',),
                                                require_content=True) == snapshot,
                    'obsolete_request_not_safely_refused')
        for historical, snapshot in divergent_refusals:
            require(snapshot['event'] in records and records.index(snapshot['event']) < records.index(ge)
                    and verify_standing_refusal(historical, inbox, grants, codes=('not_fast_forward',),
                                                require_content=True) == snapshot, 'standing_refusal_changed')
            # A rebase can replace the refused commit with a commit on a different line.
            old_target, base = historical['target_commit'], grant['expected_main_head']
            require(not is_ancestor(repo, base, old_target) and not is_ancestor(repo, old_target, target),
                    'obsolete_request_not_safely_refused')
        for historical, snapshot, binding in earlier_refusals:
            require(snapshot['event'] in records and records.index(snapshot['event']) < records.index(ge)
                    and verify_standing_refusal(historical, inbox, grants) == snapshot
                    and verify_refusal_content(historical, inbox, grants, snapshot) == binding, 'standing_refusal_changed')
            require(historical['target_commit'] != target and is_ancestor(repo, historical['target_commit'], target)
                    and not is_ancestor(repo, historical['target_commit'], grant['expected_main_head']),
                    'obsolete_request_not_safely_refused')
            if binding['form'] == 'anchored':
                verify_refusal_anchor(repo, historical, binding, request, rid, gid, target,
                                      json.loads(snapshot['contents'][2])['at'], records)
        # Task must still be the same completed run after the potentially slow reads.
        require(read(home/'tasks'/tid/'state.json') == task, 'task_changed_during_verification')
        return {'ok': True, 'node': node, 'task_id': tid, 'repo': repo, 'request': rid, 'grant': gid, 'commit': target,
                'main_head': main, 'audit_hash': revent['hash'], 'tests': expected_tests}
    except ProofError as exc:
        return {"ok": False, "code": str(exc)}
    except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError):
        # Keep paths, raw subprocess output and secrets out of the protocol.
        return {'ok': False, 'code': 'integration_evidence_incomplete',
                'message': 'Integratiebewijs ontbreekt, is gewijzigd of klopt niet; controle blijft open'}


def fetch(job):
    from . import remote
    args = {'task_id': job['task_id'], 'repo': job['repo'], 'node': job['node']}
    if job['node'] == config.load_config().get('node'):
        return verify(args)
    return remote.call(job['node'], 'integration_completion_proof', args)


def valid_reply(reply, job, *, allow_legacy=False):
    if isinstance(reply, dict) and reply.get('kind') == 'already_integrated':
        prior = reply.get('prior')
        if not isinstance(prior, dict) or prior.get('kind') is not None:
            return False
        return (reply.get('ok') is True and reply.get('node') == job['node']
                and reply.get('task_id') == job['task_id'] and reply.get('repo') == job['repo']
                and bool(RID.fullmatch(str(reply.get('request', ''))))
                and all(bool(re.fullmatch(r'[0-9a-f]{64}', str(reply.get(k, ''))))
                        for k in ('audit_hash', 'request_sha256'))
                and 'grant' not in reply and bool(TID.fullmatch(str(prior.get('task_id', ''))))
                and prior.get('task_id') != reply['task_id']
                and prior.get('request') != reply['request']
                and prior.get('commit') == reply.get('commit')
                and prior.get('main_head') == reply.get('main_head')
                and valid_reply(prior, {'task_id': prior.get('task_id'), 'repo': job['repo'], 'node': job['node']}))
    return (isinstance(reply, dict) and reply.get('ok') is True
            and reply.get('kind') is None
            and reply.get('task_id') == job['task_id'] and reply.get('repo') == job['repo']
            and (reply.get('node') == job['node'] or (allow_legacy and 'node' not in reply))
            and bool(RID.fullmatch(str(reply.get('request', ''))))
            and bool(GID.fullmatch(str(reply.get('grant', ''))))
            and bool(SHA.fullmatch(str(reply.get('commit', ''))))
            and bool(SHA.fullmatch(str(reply.get('main_head', ''))))
            and bool(re.fullmatch(r'[0-9a-f]{64}', str(reply.get('audit_hash', ''))))
            and isinstance(reply.get('tests'), list) and bool(reply['tests']))


def required(job, cfg=None):
    cfg = config.load_config() if cfg is None else cfg
    return ((bool(job.get('integration_workflow')) or job.get('node') in (cfg.get('jobs_integration_check_nodes') or []))
            and job.get('require_check') and not job.get('continuation') and not job.get('supervisor'))


def satisfied(job):
    check = job.get('check') or {}
    return (check.get('state') == 'ok' and check.get('source') == 'integrator'
            and valid_reply(check.get('evidence'), job, allow_legacy=not bool(job.get('integration_workflow'))))


def reconcile(snap, cfg):
    from . import actionreassess
    proven = []
    try:
        return _reconcile(snap, cfg, proven)
    finally:
        for ref in proven:   # buiten de wachtrijlock; nooit blokkerend
            try:
                actionreassess.hook_event('integratiebewijs', ref, cfg)
            except Exception:
                pass


def _reconcile(snap, cfg, proven):
    from . import jobs
    allowed = cfg.get('jobs_integration_check_nodes') or []
    for job in snap['jobs'].values():
        if not (required(job, cfg) and job.get('status') == jobs.DONE
                and not any(job.get(k) for k in ('archived', 'held', 'cancel_requested', 'continuation', 'supervisor'))
                and (job.get('check') or {}).get('state') != 'afgekeurd' and not satisfied(job)):
            continue
        from . import integration_no_changes
        if integration_no_changes.reconcile_empty(job, cfg):
            continue
        try:
            proof = fetch(job)  # no queue lock across the network
        except Exception:  # unreachable/unsupported transport is a visible blocker
            proof = {'ok': False, 'code': 'integration_evidence_unavailable'}
        with jobs._txn() as box:
            cur = box['d']['jobs'].get(job['id'])
            # A decision, retry, cancellation or edit made while fetching proof wins.
            if not cur or any(cur.get(k) != job.get(k) for k in
                    ('status', 'task_id', 'run', 'repo', 'node', 'check', 'require_check', 'held', 'archived', 'cancel_requested',
                     'approval', 'continuation', 'supervisor', 'integration_workflow')):
                continue
            if valid_reply(proof, job):
                if proof.get('kind') == 'already_integrated':
                    prior = proof['prior']
                    note = (f"Geen eigen wijziging bij {proof['request']}; exacte versie {proof['commit']} al bewezen "
                            f"geïntegreerd via {prior['request']} / {prior['grant']}; weigering, tests en audit gecontroleerd")
                else:
                    note = f"Bewezen integratie {proof['request']} / {proof['grant']}, commit {proof['commit']} in main; tests en audit gecontroleerd"
                cur['check'] = {'state': 'ok', 'source': 'integrator', 'by': 'standalone-integrator',
                                'at': store.now(), 'note': note, 'evidence': proof}
                cur['last_error'] = None
                jobs._log(cur, 'integrator', note)
                jobs._audit('integration_checked', note, job_id=job['id'], task_id=job['task_id'], evidence=proof)
                proven.append(f"{job['id']}:{proof.get('request')}")
                box['dirty'] = True
            else:
                code = 'integration_check_blocked'
                reason = str(proof.get('code', 'invalid_proof')) if isinstance(proof, dict) else 'invalid_proof'
                if not re.fullmatch(r'[a-z_]{1,80}', reason):
                    reason = 'invalid_proof'
                message = f'Automatische integratiecontrole geblokkeerd ({reason}); volgende opdracht wacht'
                if (cur.get('last_error') or {}).get('message') != message:
                    cur['last_error'] = {'code': code, 'message': message, 'at': store.now()}
                    jobs._log(cur, 'integrator', message)
                    box['dirty'] = True
