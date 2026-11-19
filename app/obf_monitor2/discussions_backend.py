"""Thin Monitor adapter to primary Runner discussion storage; never dispatches itself."""
from . import jobs_backend


class DiscussionsBackend:
    def __init__(self, *, call=None, env=None, role=None):
        self._call, self._env, self._role = call, env, role

    def _invoke(self, args):
        if self._call:return self._call('runner2_discussions', args)
        node = jobs_backend.control_node(self._env, self._role)
        return jobs_backend._default_call(node, 'runner2_discussions', args)

    def _read(self, args):
        try:
            data = self._invoke(args)
            if not isinstance(data, dict):raise ValueError()
            return 200, {'ok': True, **data}
        except Exception as exc:
            code = getattr(exc, 'code', '')
            if isinstance(exc, ValueError) or code == 'invalid_input':
                message = str(getattr(exc, 'message', exc))
                if 'onbekende tool' in message.lower():
                    return 501, {'ok': False, 'reason': 'not_supported', 'message': 'Deze Runner-versie kent overleg nog niet; werk beide machines bij.'}
                return 400, {'ok': False, 'reason': 'invalid_input', 'message': message[:400] or 'Ongeldig overlegverzoek.'}
            return 503, {'ok': False, 'reason': 'unavailable', 'message': 'Overleg is tijdelijk niet bereikbaar. Je vraag is mogelijk al ontvangen; herhaal dezelfde knopdruk om dat te controleren.'}

    def overview(self):return self._read({'action': 'list'})

    def get(self, conversation_id):return self._read({'action': 'get', 'conversation_id': conversation_id})

    def act(self, payload):return self._read(payload)


def clean(payload, users):
    allowed = {'action', 'by', 'nonce', 'conversation_id', 'turn_id', 'version', 'node', 'repository', 'model', 'question', 'title'}
    if not isinstance(payload, dict) or set(payload) - allowed or payload.get('action') not in ('ask', 'cancel', 'draft'):
        raise ValueError('Ongeldige overlegactie of onbekend veld.')
    if any(not isinstance(v, str) for k, v in payload.items() if k != 'version'):
        raise ValueError('Onleesbaar overlegveld.')
    if 'version' in payload and (type(payload['version']) is not int or payload['version'] < 1):
        raise ValueError('Ongeldige gespreksversie.')
    who = str(payload.get('by', '')).strip().casefold()
    user = next((u for u in users if u.casefold() == who), None)
    if not user:raise ValueError('Kies je naam uit de gebruikerslijst.')
    if payload.get('node', 'GERARD') not in ('GERARD', 'LINUX'):
        raise ValueError('Kies GERARD of STUDIO voor overleg.')
    limits = {'question': 20_000, 'title': 100, 'repository': 2000, 'nonce': 120, 'model': 80, 'conversation_id': 14, 'turn_id': 15}
    if any(len(payload.get(k, '')) > n for k, n in limits.items()):raise ValueError('De overlegtekst is te lang.')
    if payload['action'] == 'ask' and not payload.get('question', '').strip():raise ValueError('Vul je vraag in.')
    return {**payload, 'by': user}
