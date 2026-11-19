"""Bounded, read-only backup observations; no backup or network work in HTTP requests."""
import json
import math
import os
import time
from pathlib import Path


def number(v):
    return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)


def read_nodes(path=None, *, now=None):
    now=time.time() if now is None else now
    result={k:{'state':'unknown','percent':None,'detail':'Geen actuele back-upstatus ontvangen'} for k in ('mac','linux','windows')}
    result['gerard']={'state':'unknown','percent':None,'detail':'Geen actuele status van de ub25-backup ontvangen'}
    path=Path(path or os.environ.get('OBF_MONITOR2_BACKUP_STATUS',str(Path.home()/'.local/state/monitor2/backup-status.json')))
    for source, keys in ((path, ('mac','linux','windows')), (path.with_name('backup-status-nuc.json'), ('linux','windows')), (path.with_name('backup-status-gerard.json'), ('gerard',))):
        try:
            if source.stat().st_size>65536:continue
            data=json.loads(source.read_text())
            collected=data.get('collected')
            if not number(collected) or not -30<=now-collected<=180:
                if source != path:
                    for key in keys:result[key]={'state':'unknown','percent':None,'detail':'Statusoverdracht verouderd'}
                continue
            rows=data.get('nodes')
            if not isinstance(rows,dict):continue
            for key in keys:
                raw=rows.get(key)
                if not isinstance(raw,dict):continue
                state=raw.get('state')
                if state not in ('ok','running','failed','unknown','warning'):continue
                row={'state':state,'detail':str(raw.get('detail') or '')[:600],
                     'success':str(raw.get('success') or '')[:50], 'collected':collected,'percent':None}
                percent=raw.get('percent')
                if state=='running' and number(percent) and 0<=percent<=100:row['percent']=percent
                if key=='windows':
                    free,total,measured=(raw.get(k) for k in ('free_bytes','total_bytes','measured'))
                    if not all(number(v) for v in (free,total,measured)) or total<=0 or not 0<=free<=total or not -30<=now-measured<=5400:
                        result[key]={'state':'unknown','percent':None,'detail':row['detail'] or 'Vrije ruimte onbekend'}
                        continue
                    ratio=free/total
                    row.update(free_bytes=free,total_bytes=total,measured=measured,
                               state='failed' if ratio<.1 else 'warning' if ratio<.2 else 'ok')
                result[key]=row
        except (OSError,ValueError,TypeError,AttributeError):pass
    return result
