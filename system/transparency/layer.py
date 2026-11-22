"""
Transparency layer (Milestone 9): applications use LOGICAL resources, never physical ones.

    app code:    layer.write("site/NAI-01/threshold", {"cpu": 85});   layer.read("site/NAI-01/threshold")
    never sees:  which host, which replica, how many replicas, protocol, or that the data moved.

  Access        one read/write API whatever the backend (replicated key-value store, local file) and wire protocol.
  Location      a logical name is resolved at run time through the naming service:
                    res.<namespace>  -> placement {groups, read_from, epoch, backend}
                    group.<id>       -> replica set  "r1=url,r2=url,r3=url"
                Placement is cached for CACHE_TTL_S; a miss on a cached placement forces ONE re-resolution and retry.
  Replication   each group is a quorum-replicated store (ReplicatedStore: W+R>N, read repair, hinted handoff).
  Migration     migrate(namespace, src, dst) moves a namespace between groups while clients keep working:
                    1 dual-write   placement = {groups:[src,dst], read_from:src}   (new writes go to both)
                    2 copy         state transfer src -> dst (versions preserved, so concurrent writes win by version)
                    3 flip reads   placement = {groups:[src,dst], read_from:dst}
                    4 retire       placement = {groups:[dst]} ; src copies deleted
                Each phase waits one cache TTL so every client has seen the previous placement.
  Failure       a crashed replica is masked by the quorum + failover; the application sees no error.
"""
import json
import os
import time

import requests

from system.naming.resolver import NameServerResolver
from system.replication.client import ReplicatedStore, Unavailable

CACHE_TTL_S = 0.5


class FileBackend:
    """Second backend behind the same API (access transparency): a JSON file on local disk."""

    def __init__(self, path):
        self.path = path

    def read(self, key):
        try:
            return json.load(open(self.path)).get(key)
        except FileNotFoundError:
            return None

    def write(self, key, value):
        d = {}
        if os.path.exists(self.path):
            d = json.load(open(self.path))
        d[key] = value
        json.dump(d, open(self.path + ".tmp", "w"))
        os.replace(self.path + ".tmp", self.path)


class Directory:
    def __init__(self, naming_url, ttl=CACHE_TTL_S):
        self.resolver = NameServerResolver(naming_url, ttl=ttl)
        self.naming_url = naming_url.rstrip("/")
        self.ttl = ttl
        self.lookups = 0

    def _get(self, name, fresh=False):
        if fresh:
            self.resolver.cache.pop(name, None)
        self.lookups += 1
        loc = self.resolver.resolve(name)
        return loc

    def placement(self, namespace, fresh=False):
        loc = self._get(f"res.{namespace}", fresh)
        return json.loads(loc) if loc else None

    def group(self, gid, fresh=False):
        loc = self._get(f"group.{gid}", fresh)
        return dict(p.split("=", 1) for p in loc.split(",")) if loc else None

    # administration
    def set_placement(self, namespace, placement):
        requests.post(self.naming_url + "/register", json={"name": f"res.{namespace}", "location": json.dumps(placement), "ttl": self.ttl}, timeout=2)

    def set_group(self, gid, replicas):
        requests.post(self.naming_url + "/register", json={"name": f"group.{gid}", "location": ",".join(f"{k}={v}" for k, v in replicas.items()), "ttl": 3600}, timeout=2)


def namespace_of(key):
    return key.rsplit("/", 1)[0]


class LogicalStore:
    def __init__(self, naming_url, name="app", files_dir=None, w=2, r=2):
        self.dir, self.name, self.w, self.r, self.files_dir = Directory(naming_url), name, w, r, files_dir
        self.stores = {}
        self.stats = {"reads": 0, "writes": 0, "reresolutions": 0, "errors": 0, "dual_writes": 0}

    def _store(self, gid, fresh=False):
        reps = self.dir.group(gid, fresh)
        if not reps:
            raise KeyError(f"unknown group {gid}")
        sig = tuple(sorted(reps.items()))
        st = self.stores.get(gid)
        if st is None or st[0] != sig:
            st = (sig, ReplicatedStore(reps, w=self.w, r=self.r, timeout=0.6, name=self.name))
            self.stores[gid] = st
        return st[1]

    def _backend(self, p):
        if p.get("backend") == "file":
            return FileBackend(os.path.join(self.files_dir, p["file"]))
        return None

    def write(self, key, value):
        self.stats["writes"] += 1
        ns = namespace_of(key)
        p = self.dir.placement(ns)
        if p is None:
            raise KeyError(f"unknown logical resource {ns}")
        fb = self._backend(p)
        if fb:
            return fb.write(key, value)
        version = int(time.time() * 1e6)
        try:
            for gid in p["groups"]:
                self._store(gid).put(key, value, version=version)
                if len(p["groups"]) > 1:
                    self.stats["dual_writes"] += 1
        except Unavailable:
            self.stats["errors"] += 1
            raise

    def read(self, key, default=None):
        self.stats["reads"] += 1
        ns = namespace_of(key)
        for attempt in (0, 1):
            p = self.dir.placement(ns, fresh=bool(attempt))
            if p is None:
                raise KeyError(f"unknown logical resource {ns}")
            fb = self._backend(p)
            if fb:
                return fb.read(key)
            try:
                v = self._store(p["read_from"], fresh=bool(attempt)).get(key, None)
            except Unavailable:
                self.stats["errors"] += 1
                raise
            if v is not None or attempt:
                return v if v is not None else default
            self.stats["reresolutions"] += 1                  # miss on a possibly stale placement: look again, once
        return default


def migrate(directory, namespace, src, dst, settle=CACHE_TTL_S * 2.2, log=None):
    """Move a namespace from group src to group dst without stopping clients. Returns phase timings."""
    t = {}
    t0 = time.time()
    say = log or (lambda *_: None)
    backend = (directory.placement(namespace) or {}).get("backend", "kv")
    directory.set_placement(namespace, {"groups": [src, dst], "read_from": src, "epoch": 1, "backend": backend})
    time.sleep(settle)
    t["dual_write_started_s"] = round(time.time() - t0, 3)
    src_reps = directory.group(src, fresh=True)
    dst_reps = directory.group(dst, fresh=True)
    copied = 0
    for rid, url in src_reps.items():                           # state transfer: take the entries from every src replica (union by version)
        try:
            ents = [e for e in requests.get(url + "/dump", params={"live": 1}, timeout=5).json()["entries"] if e["key"].startswith(namespace + "/")]
        except requests.RequestException:
            continue
        for d_url in dst_reps.values():
            requests.post(d_url + "/load", json={"entries": ents}, timeout=5)
        copied = max(copied, len(ents))
    t["copied_keys"] = copied
    t["copy_done_s"] = round(time.time() - t0, 3)
    directory.set_placement(namespace, {"groups": [src, dst], "read_from": dst, "epoch": 2, "backend": backend})
    time.sleep(settle)
    t["reads_flipped_s"] = round(time.time() - t0, 3)
    directory.set_placement(namespace, {"groups": [dst], "read_from": dst, "epoch": 3, "backend": backend})
    time.sleep(settle)
    for url in src_reps.values():
        try:
            for e in [e for e in requests.get(url + "/dump", params={"live": 1}, timeout=5).json()["entries"] if e["key"].startswith(namespace + "/")]:
                requests.delete(url + "/kv/" + e["key"], timeout=2)
        except requests.RequestException:
            pass
    t["retired_s"] = round(time.time() - t0, 3)
    return t
