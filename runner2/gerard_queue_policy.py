"""Restrict direct queue changes to GERARD; authentication remains at the gateway."""
class Denied(Exception):pass
JOB_ACTIONS={'move','set_node','set_deps','hold','release','cancel','check','retry','resume','archive','approve','reject','set_prompt'}

def authorize(principal, action, args, queue):
    if not isinstance(principal,dict) or principal.get('role')!='gerard' or principal.get('subject')!='Gerard':raise Denied('identity')
    if not isinstance(args,dict) or not isinstance(queue,dict):raise Denied('invalid')
    jobs,orders,requests=queue.get('jobs'),queue.get('order'),queue.get('requests')
    if not all(isinstance(v,dict) for v in (jobs,orders,requests)):raise Denied('queue')
    if action in ('pause_queue','resume_queue'):
        if args.get('node')!='GERARD':raise Denied('scope')
        return
    if action=='add':
        if args.get('node')!='GERARD':raise Denied('node')
        request_id=args.get('request_id')
        targets=[requests[request_id]] if request_id and request_id in requests else []
    elif action in JOB_ACTIONS:
        targets=[args.get('job_id')]
        if action=='set_node' and args.get('node')!='GERARD':raise Denied('destination')
    else:raise Denied('action')
    for jid in targets:
        if not isinstance(jid,str) or not isinstance(jobs.get(jid),dict) or jobs[jid].get('node')!='GERARD':raise Denied('ownership')
        for node,order in orders.items():
            if not isinstance(order,list):raise Denied('partition')
            if jid in order and node!='GERARD':raise Denied('partition')
