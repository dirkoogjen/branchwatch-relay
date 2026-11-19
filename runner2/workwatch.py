"""Availability of the existing local Work heartbeat; the notifier owns delivery.

This checker never starts tasks, edits queues, or creates another timer.
Configuration is opt-in at control/work-watch/config.json. The existing Work
round writes heartbeat.json atomically after it actually starts or progresses.
"""
import json
import math
import time

from . import config


def conditions() -> list:
    root = config.r2_home() / 'control/work-watch'
    path = root / 'config.json'
    if not path.exists():
        return []
    now = time.time()
    key = 'studio-wachtrij-tot-integratie-controleren'

    def alarm(reason):
        guardian = config.r2_home() / 'control/guardian'
        try:
            recovery = json.loads((guardian / 'status.json').read_text())
            active = json.loads((guardian / 'config.json').read_text())
            phase = recovery.get('phase')
            running_proven = (phase == 'running' and recovery.get('last_completed_healthy') is True
                              and 0 <= now - recovery.get('updated_at', 0) < 300)
            fallback = (active.get('enabled') and active.get('ownership_enrolled') and not active.get('paused')
                        and (phase == 'completed' or running_proven) and not recovery.get('errors')
                        and not recovery.get('pending') and 0 <= now - recovery.get('last_completed_at', 0) < 1200)
            if fallback:
                return []  # Work absence remains explicit in guardian status; recovery is running independently.
        except (OSError, ValueError, TypeError):
            pass
        return [{'kind': 'work_watch', 'key': key,
                 'title': 'WORK-CONTROLE NIET UITGEVOERD',
                 'text': 'De kwartiercontrole heeft geen geldig recent levensteken. '
                         'Open de controlechat en controleer de planning. ' + reason}]

    try:
        cfg = json.loads(path.read_text())
        if cfg['schema'] != 1 or not isinstance(cfg['enabled'], bool):
            raise ValueError('ongeldige configuratie')
        if not cfg['enabled']:
            return []
        key = cfg['automation_id']
        if key != 'studio-wachtrij-tot-integratie-controleren':
            raise ValueError('onbekende automatisering')
        if cfg['timezone'] != 'Europe/Amsterdam' or cfg.get('window') != 'all_day':
            raise ValueError('onverwachte tijdzone')
        max_age = cfg['max_age_s']
        if isinstance(max_age, bool) or not isinstance(max_age, (int, float)) or max_age != 2100:
            raise ValueError('onverwachte bewakingstermijn')
        beat = json.loads((root / 'heartbeat.json').read_text())
        at = beat['at']
        if (beat['schema'] != 1 or beat['automation_id'] != key
                or isinstance(at, bool) or not isinstance(at, (int, float))
                or not math.isfinite(at) or at > now + 120
                or beat['phase'] not in ('running', 'completed', 'failed')):
            raise ValueError('ongeldig levensteken')
        if beat['phase'] == 'failed':
            return alarm('De controle meldde een mislukking; herstel is nog niet bewezen.')
        if now - at > max_age:
            return alarm(f'Laatste levensteken is {int((now - at) / 60)} minuten oud.')
        return []
    except (OSError, ValueError, TypeError, KeyError, OverflowError) as exc:
        return alarm(f'Levensteken of configuratie ontbreekt of is onleesbaar ({type(exc).__name__}).')

