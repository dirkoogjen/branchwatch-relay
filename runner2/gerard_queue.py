"""GERARD queue scoping for the existing authenticated monitor and ChatGPT gateway."""
from contextvars import ContextVar
from .gerard_queue_policy import authorize,Denied
_current=ContextVar('synthetic_gerard_request',default=None)

def active():return _current.get() is not None

def metadata():
    current=_current.get()
    if current is None:return None
    return {"subject":current[2],"role":"gerard","read_nodes":["GERARD","LINUX","MAC","WINDOWS"],"write_nodes":["GERARD"]}

def check(queue):
    current=_current.get()
    if current is not None:
        action,args,subject=current
        authorize({'subject':subject,'role':'gerard'},action,args,queue)

def act(action,args,subject,channel="lokaal"):
    from . import jobs
    if action == 'plan':
        if args.get('node') != 'GERARD':raise Denied('node')
        return jobs.plan(args)
    if action in ('list','repair'):raise Denied('unsupported in experiment')
    token=_current.set((action,dict(args),subject))
    try:return jobs.act(action,args,'Gerard',channel=channel)
    finally:_current.reset(token)

def edit_prompt(args):
    import sys
    from pathlib import Path
    from . import jobs
    app=Path(__file__).resolve().parents[2]/'monitor'/'app'
    if app.is_dir():sys.path.insert(0,str(app))
    from obf_monitor2 import queue_text
    token=_current.set(('set_prompt',dict(args),'Gerard'))
    try:return queue_text.edit_prompt(args.get('job_id'),args.get('prompt'),args.get('old_sha256'),by='Gerard',jobs=jobs)
    finally:_current.reset(token)
