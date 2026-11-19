"""Independently dispose an empty local execution, never fabricate a merge/check.

Only the primary NUC's configured repository is supported. All admissions remain
fail-closed for changed work, requests, live sessions and human queue decisions.
"""
import copy
import hashlib
import json
import re
import subprocess
import uuid
from contextlib import ExitStack
from pathlib import Path

from . import config, store, guardian_node as node
from .filelock import lock_exclusive


def observe(job):
    from . import integration_check as ic
    tid=job['task_id'];home=config.r2_home();path=store.task_dir(tid)/'state.json'
    raw=ic.raw(path);task=json.loads(raw)
    settings=ic.read(home/'config/integration-check.json')
    repo=job['repo'];branch='r2/'+tid;wt=home/'worktrees'/tid
    ic.require(settings.get('repo')==repo and task.get('repo')==repo
        and task.get('task_id')==tid and task.get('branch')==branch
        and task.get('worktree')==str(wt),'empty_identity_mismatch')
    ic.require(task.get('status')=='completed' and isinstance(task.get('turn'),int)
        and task['turn']>0 and task.get('session_id') and task.get('finish'), 'empty_task_not_completed')
    ic.require(not any(task.get(k) for k in ('pid','claude_pid','inbox','in_flight','children','orphans',
        'failure','question','chain_id','supervisor_id','chain','supervisor','spawn_request',
        'resume_requested','active_tools')), 'empty_live_or_failed_work')
    mentioned_requests = set(ic.RID.findall(json.dumps(task['finish'])))
    unit=task.get('unit')
    ic.require(isinstance(unit,str) and re.fullmatch(r'r2-'+tid.lower()+r'-[0-9]+(?:\.service)?',unit),
        'empty_unit_identity_mismatch')
    try:
        info=node.unit_info(unit if unit.endswith('.service') else unit+'.service')
        closed=info.get('state') in ('inactive','failed') and info.get('pid')==0
    except subprocess.CalledProcessError as exc:
        # systemctl show of a collected transient unit returns inactive/0 with
        # an unsuccessful exit status. A bus failure has no such evidence.
        props=dict(line.split('=',1) for line in (exc.output or '').splitlines() if '=' in line)
        closed=props.get('ActiveState')=='inactive' and props.get('MainPID')=='0'
    ic.require(closed,'empty_unit_not_closed')
    integrator=node.unit_info('studioobf-integrator.service')
    ic.require(integrator.get('state')=='active' and not integrator.get('busy'),'empty_integrator_not_idle')
    ic.require(ic.git(repo,'symbolic-ref','--short','HEAD')=='main'
        and ic.git(str(wt),'symbolic-ref','--short','HEAD')==branch,'empty_branch_mismatch')
    main=ic.git(repo,'rev-parse','main');base=task.get('base') or {}
    ic.require(base.get('ref')=='main' and main==base.get('commit')
        ==ic.git(repo,'rev-parse',branch)==ic.git(str(wt),'rev-parse','HEAD'),'empty_commit_mismatch')
    ic.require(ic.git(repo,'rev-list','--count','main..'+branch)=='0'
        and not ic.git(repo,'status','--porcelain','--untracked-files=all')
        and not ic.git(str(wt),'status','--porcelain','--untracked-files=all'),'empty_dirty_or_ahead')
    external_references = {}
    for p in Path(settings['inbox']).glob('R-*.json'):
        if p.name.endswith('.status.json'):continue
        request=ic.read(p)
        ic.require(request.get('task_id')!=tid and request.get('source_branch')!=branch,
            'empty_existing_request')
        if p.stem in mentioned_requests:
            external_references[p.stem] = request
    for rid in sorted(mentioned_requests):
        item = external_references.get(rid, {})
        other_tid = item.get('task_id')
        ic.require(isinstance(other_tid, str) and ic.TID.fullmatch(other_tid)
                   and item.get('id') == rid and item.get('repo') == repo
                   and item.get('source_branch') == 'r2/'+other_tid
                   and item.get('mode') == 'ff-only'
                   and isinstance(item.get('target_commit'), str)
                   and ic.SHA.fullmatch(item['target_commit']), 'empty_request_mentioned')
        other_task = ic.read(home/'tasks'/other_tid/'state.json')
        ic.require(other_task.get('task_id') == other_tid and other_task.get('repo') == repo
                   and other_task.get('branch') == item['source_branch']
                   and other_task.get('status') == 'completed', 'empty_request_mentioned')
    return {'task_id':tid,'session_id':task['session_id'],'turn':task['turn'],
        'task_sha256':hashlib.sha256(raw).hexdigest(),'commit':main,'main_clean':True,
        'worktree_clean':True,'commits_ahead':0,'request_ids':[],
        'referenced_foreign_requests': sorted(mentioned_requests),
        'human_actions':copy.deepcopy(task.get('human_actions',[]))}


def eligible(job,cfg):
    from . import integration_check as ic
    return (isinstance(job.get('task_id'),str) and bool(ic.TID.fullmatch(job['task_id']))
        and config.local_node()=='LINUX' and job.get('node')=='LINUX'
        and job.get('status')=='done' and ic.required(job,cfg) and job.get('check') is None
        and job.get('integration_obligation')=={'node':'LINUX','task_ids':[job.get('task_id')]}
        and not any(job.get(k) for k in ('held','approval','archived','cancel_requested','continuation','supervisor')))


def reconcile_empty(job,cfg):
    from . import jobs, controller, integration_check as ic
    if not eligible(job,cfg) or not (store.task_dir(job['task_id'])/'state.json').is_file():return False
    try:
        with ExitStack() as stack:
            for p in (node.root()/'recovery.lock',jobs.jobs_dir()/'.launch-LINUX.lock',
                      config.r2_home()/'start.lock',store.task_dir(job['task_id'])/'.state.lock'):
                p.parent.mkdir(parents=True,exist_ok=True)
                handle=stack.enter_context(p.open('a+'));lock_exclusive(handle,blocking=False)
            controller._require_free()
            facts=observe(job)
            with jobs._txn() as box:
                q=box['d'];cur=q['jobs'].get(job['id'])
                if cur!=job or not eligible(cur,cfg) or jobs._paused(q,'LINUX'):return False
                if any(j.get('node')=='LINUX' and j.get('status') in jobs.ACTIVE_STATES
                       for j in q['jobs'].values()):return False
                ic.require(observe(cur)==facts,'empty_observation_changed')
                recovery=node.root()/'no-changes'/(job['task_id']+'-'+uuid.uuid4().hex[:12])
                recovery.mkdir(parents=True)
                for name,value in [('queue-before.json',q),('task-before.json',store.load(job['task_id'])),
                                   ('facts.json',facts)]:
                    store.atomic_write(recovery/name,json.dumps(value,ensure_ascii=False,indent=2)+'\n')
                # Recheck task bytes after creating durable recovery evidence.
                ic.require(hashlib.sha256(ic.raw(store.task_dir(job['task_id'])/'state.json')).hexdigest()
                    ==facts['task_sha256'],'empty_task_changed')
                cur['integration_disposition']={'kind':'not_applicable_no_changes','at':store.now(),
                    'by':'runner2-independent-empty-execution-check',
                    'product_status':'not_evaluated_no_code_changes',
                    'original_obligation':cur.pop('integration_obligation'),
                    'original_workflow':cur.pop('integration_workflow',None),
                    'original_require_check':cur['require_check'],'original_check':cur['check'],
                    'previous_error':copy.deepcopy(cur.get('last_error')),'facts':facts,
                    'recovery_directory':str(recovery)}
                cur['require_check']=False
                cur['last_error']={'code':'no_changes_integration_not_applicable','at':store.now(),
                    'message':'Nul wijzigingen onafhankelijk bewezen; geen integratie of productgoedkeuring. Open acties blijven behouden.'}
                jobs._log(cur,'runner2','Nul wijzigingen onafhankelijk bewezen; integratie niet van toepassing; geen check-ok. Herstel: '+str(recovery))
                jobs._audit('integration_not_applicable','Afgesloten lege uitvoering onafhankelijk gecontroleerd; geen grant/check/productbesluit',
                    job_id=cur['id'],task_id=cur['task_id'],facts=facts,recovery_directory=str(recovery))
                box['dirty']=True
            return True
    except (OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError,controller.ControllerError):
        # The existing integration verifier keeps the concrete blocker visible.
        return False
