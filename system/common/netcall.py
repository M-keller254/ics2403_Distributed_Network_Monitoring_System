"""Outbound HTTP helper used by every service.

* adds X-From so the receiver's fault injector can model partitions
* counts messages and bytes per path -> message-complexity / overhead measurements
* one pooled requests.Session per thread (cheap, thread-safe)
"""
import json
import threading

import requests

from .metrics import Counters


class Net:
    def __init__(self, name, default_timeout=2.0):
        self.name = name
        self.default_timeout = default_timeout
        self.counters = Counters()
        self._local = threading.local()

    def _session(self):
        s = getattr(self._local, "s", None)
        if s is None:
            s = self._local.s = requests.Session()
            s.headers.update({"X-From": self.name})
        return s

    def request(self, method, url, payload=None, timeout=None, tag=None):
        timeout = timeout or self.default_timeout
        kind = tag or url.rsplit("/", 1)[-1].split("?")[0] or "root"
        data = json.dumps(payload) if payload is not None else None
        self.counters.inc("msgs")
        self.counters.inc("msgs:" + kind)
        if data:
            self.counters.inc("bytes", len(data))
        return self._session().request(
            method, url, data=data, headers={"Content-Type": "application/json"} if data else None, timeout=timeout
        )

    def post(self, url, payload=None, timeout=None, tag=None):
        return self.request("POST", url, payload if payload is not None else {}, timeout, tag)

    def get(self, url, timeout=None, tag=None):
        return self.request("GET", url, None, timeout, tag)

    def post_json(self, url, payload=None, timeout=None, tag=None):
        """POST and return (status, parsed_json_or_None); never raises on HTTP status, raises on network failure."""
        r = self.post(url, payload, timeout, tag)
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, None

    def get_json(self, url, timeout=None, tag=None):
        r = self.get(url, timeout, tag)
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, None
