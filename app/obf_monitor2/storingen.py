"""Actuele open Studio-storingen; alleen een telling, nooit foutdetails of schrijfverzoeken."""
from __future__ import annotations

import json
import threading
import time
import urllib.request

from . import r2_adapter

POLL_S = 10
STALE_S = 30
MAX_BYTES = 256 * 1024


def _fetch(url):
    request = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(request, timeout=2) as response:
        raw = response.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError('Te groot storingenantwoord')
    return json.loads(raw)


class StudioStoringen:
    def __init__(self, *, fetch, clock=time.monotonic):
        self._fetch = fetch
        self._clock = clock
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._count = None
        self._checked = None

    @classmethod
    def from_env(cls):
        return cls(fetch=lambda: {})

    def refresh(self):
        count = None
        try:
            data = self._fetch()
            n, total = data.get('aantal_open'), data.get('aantal')
            if (type(n) is int and type(total) is int and 0 <= n <= total <= 100
                    and data.get('opslag') == 'ok'
                    and data.get('status') == ('open' if n else 'geen')):
                count = n
        except Exception:
            # Een leesfout is onbekend, nooit "geen storingen". Geen details doorgeven.
            pass
        with self._lock:
            self._count, self._checked = count, self._clock()

    def current(self):
        with self._lock:
            fresh = self._checked is not None and 0 <= self._clock() - self._checked <= STALE_S
            count = self._count if fresh else None
        return {'beschikbaar': count is not None, 'aantal_open': count}

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name='monitor2-storingen', daemon=True)
        self._thread.start()

    def _loop(self):
        while not self._stop.is_set():
            self.refresh()
            self._stop.wait(POLL_S)

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
