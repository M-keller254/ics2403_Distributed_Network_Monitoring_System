"""
Name-resolution clients (Milestone 10): four strategies behind one interface.

    r = StaticResolver(table) | BroadcastResolver(nodes) | NameServerResolver(url) | PrefixResolver(root_url, ttl=...)
    r.resolve("ke.nai.NAI-01.agent") -> location (or None);   r.stats -> messages, hops, cache hits, latency

  static      a local table (like /etc/hosts): zero messages, but stale as soon as a resource moves.
  broadcast   ask every node "who has this?" in parallel; N messages per lookup, no central server, works with no setup.
  nameserver  one central server: 1 message, but a single point of failure and a hot spot.
  prefix      hierarchical (DNS-like): iterative resolution root -> zone -> authoritative server, keeping a PREFIX TABLE of
              the delegations learned (longest-prefix match) and an answer cache honouring each record's TTL: after the
              first lookup in a zone, later lookups go straight to the authoritative server (1 message), and a cached answer
              needs none.
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


class _Base:
    def __init__(self):
        self.stats = {"lookups": 0, "messages": 0, "cache_hits": 0, "failures": 0, "stale": 0, "hops": 0}
        self.s = requests.Session()

    def _get(self, url, path, **params):
        self.stats["messages"] += 1
        return self.s.get(url + path, params=params, timeout=1.5)


class StaticResolver(_Base):
    def __init__(self, table):
        super().__init__()
        self.table = dict(table)

    def resolve(self, name):
        self.stats["lookups"] += 1
        return self.table.get(name)


class BroadcastResolver(_Base):
    def __init__(self, nodes, ttl=0.0):
        super().__init__()
        self.nodes, self.ttl, self.cache = list(nodes), ttl, {}
        self.pool = ThreadPoolExecutor(max(4, len(nodes)))

    def _ask(self, url, name):
        try:
            r = requests.get(url + "/who_has", params={"name": name}, timeout=1.5)
            return r.json()["answer"]["location"] if r.status_code == 200 else None
        except requests.RequestException:
            return None

    def resolve(self, name):
        self.stats["lookups"] += 1
        c = self.cache.get(name)
        if c and c[1] > time.time():
            self.stats["cache_hits"] += 1
            return c[0]
        self.stats["messages"] += len(self.nodes)
        futs = [self.pool.submit(self._ask, u, name) for u in self.nodes]
        for f in as_completed(futs):
            loc = f.result()
            if loc:
                if self.ttl:
                    self.cache[name] = (loc, time.time() + self.ttl)
                return loc
        self.stats["failures"] += 1
        return None


class NameServerResolver(_Base):
    def __init__(self, url, ttl=0.0):
        super().__init__()
        self.url, self.ttl, self.cache = url.rstrip("/"), ttl, {}

    def resolve(self, name):
        self.stats["lookups"] += 1
        c = self.cache.get(name)
        if c and c[1] > time.time():
            self.stats["cache_hits"] += 1
            return c[0]
        try:
            r = self._get(self.url, "/resolve", name=name)
        except requests.RequestException:
            self.stats["failures"] += 1
            return None
        if r.status_code != 200 or "answer" not in r.json():
            self.stats["failures"] += 1
            return None
        rec = r.json()["answer"]
        if self.ttl or rec.get("ttl"):
            self.cache[name] = (rec["location"], time.time() + (self.ttl or rec["ttl"]))
        return rec["location"]


class PrefixResolver(_Base):
    def __init__(self, root_url, use_cache=True, max_hops=8):
        super().__init__()
        self.root, self.use_cache, self.max_hops = root_url.rstrip("/"), use_cache, max_hops
        self.prefix_table = {"": self.root}               # zone prefix -> server url (learned from referrals)
        self.cache = {}                                   # name -> (location, expiry)

    def _start(self, name):
        best = max((z for z in self.prefix_table if z == "" or name == z or name.startswith(z + ".")), key=len)
        return self.prefix_table[best]

    def resolve(self, name):
        self.stats["lookups"] += 1
        if self.use_cache:
            c = self.cache.get(name)
            if c and c[1] > time.time():
                self.stats["cache_hits"] += 1
                return c[0]
        server = self._start(name) if self.use_cache else self.root
        for _ in range(self.max_hops):
            self.stats["hops"] += 1
            try:
                r = self._get(server, "/resolve", name=name)
            except requests.RequestException:
                self.stats["failures"] += 1
                if server != self.root and self.use_cache:       # cached delegation may be dead: fall back to the root
                    self.prefix_table = {"": self.root}
                    server = self.root
                    continue
                return None
            if r.status_code != 200:
                self.stats["failures"] += 1
                return None
            body = r.json()
            if "referral" in body:
                if self.use_cache:
                    self.prefix_table[body["referral"]["zone"]] = body["referral"]["server"]
                server = body["referral"]["server"]
                continue
            rec = body["answer"]
            if self.use_cache:
                self.cache[name] = (rec["location"], time.time() + rec.get("ttl", 30))
            return rec["location"]
        self.stats["failures"] += 1
        return None
