"""Read a bounded human decision from controller-owned storage, never from the inbox.

The operator records an explicit human instruction here. The Runner sandbox must
not have write access to this directory (the same trust boundary as grant/audit
storage). A record grants no merge or completion by itself.
This file is mirrored in runner2 so proof checks use exactly the same contract.
"""
import hashlib
import json
import os
import stat
import time
from pathlib import Path


def approval_path(grants, request_id):
    import re
    if not re.fullmatch(r'R-[0-9a-f]{12}', str(request_id)):
        raise ValueError('invalid approval request')
    return Path(grants)/'human-approvals'/(request_id+'.json')


def validate(grants, request, request_sha, main_head, policy_sha, tests, protected, *, at=None):
    path=approval_path(grants, request['id'])
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('symlink approval')
    parent=path.parent.stat()
    if parent.st_uid!=os.getuid() or stat.S_IMODE(parent.st_mode)!=0o700:
        raise ValueError('untrusted approval directory')
    if path.with_suffix('.revoked').exists():
        raise ValueError('revoked approval')
    fd=os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd,'rb') as f:
        info=os.fstat(f.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid()
                or stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>65536):
            raise ValueError('untrusted approval file')
        data=f.read(65537)
    record=json.loads(data)
    now=time.time() if at is None else at
    expected={'schema':1,'source':'explicit_user_instruction','request_id':request['id'],
        'request_sha256':request_sha,'task_id':request['task_id'],'repo':request['repo'],
        'source_branch':request['source_branch'],'target_commit':request['target_commit'],
        'expected_main_head':main_head,'mode':'ff-only','policy_sha256':policy_sha}
    if any(record.get(k)!=v for k,v in expected.items()):
        raise ValueError('approval identity mismatch')
    if not isinstance(record.get('approved_by'),str) or not record['approved_by'].strip():
        raise ValueError('missing approver')
    issued,expires=record.get('approved_at'),record.get('expires_at')
    if (type(issued) is not int or type(expires) is not int
            or not issued<=now<expires or expires-issued>86400):
        raise ValueError('approval outside validity')
    if (not tests or sorted(record.get('tests',[]))!=sorted(tests)
            or not protected or sorted(record.get('protected_paths',[]))!=sorted(protected)):
        raise ValueError('approval scope mismatch')
    return {'sha256':hashlib.sha256(data).hexdigest(),'approved_by':record['approved_by']}


def validate_grant(grants, grant, *, at=None):
    auto=grant.get('auto') or {}
    descriptor=auto.get('human_approval')
    if not isinstance(descriptor,dict):
        raise ValueError('missing human approval descriptor')
    request={k:grant[k] for k in ('repo','source_branch','target_commit')}
    request.update(id=grant['request_id'],task_id=auto['task'])
    actual=validate(grants,request,grant['request_sha256'],grant['expected_main_head'],
        auto['policy_sha256'],grant['tests'],auto.get('protected_paths'),at=at)
    if actual!=descriptor or actual['approved_by']!=grant.get('approved_by'):
        raise ValueError('approval digest or approver mismatch')
    return actual
