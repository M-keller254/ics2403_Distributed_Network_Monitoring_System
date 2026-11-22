"""
Quorum client for the replicated store: failure detection, failover, hinted handoff, read repair, Byzantine voting.

    store = ReplicatedStore({"r1": url1, "r2": url2, "r3": url3}, w=2, r=2)
    store.put("site/NAI-01/threshold", {"cpu": 85})
    store.get("site/NAI-01/threshold")      -> value (latest version in the read quorum)

  - writes go to ALL replicas in parallel and succeed once W acknowledge   (W + R > N  => read and write quorums intersect)
  - reads ask R replicas (fastest/healthiest first) and return the highest version; stale responders are repaired
  - vote=True: contact `r` replicas and require a strict MAJORITY of identical (version, value) answers -> tolerates
    f Byzantine replicas when r >= 2f+1 (a lying replica can sign its lie, so signatures alone do not help)
  - failure detector: a replica is SUSPECTED after `suspect_after` consecutive failures and re-tried (probe) after
    `retry_after` seconds; suspected replicas are skipped for reads (that is the failover) but still get writes
  - hinted handoff: writes a replica missed are queued and replayed when it answers again
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests


class Unavailable(Exception):
    pass


class ReplicatedStore:
    def __init__(self, replicas, w=2, r=2, timeout=0.6, vote=False, suspect_after=2, retry_after=1.0, name="client", read_repair=True):
        self.replicas, self.w, self.r, self.timeout, self.vote = dict(replicas), w, r, timeout, vote
        self.suspect_after, self.retry_after, self.name, self.read_repair = suspect_after, retry_after, name, read_repair
        self.fails = {k: 0 for k in replicas}
        self.suspected_at = {}
        self.hints = {k: {} for k in replicas}
        self.pool = ThreadPoolExecutor(max(8, 4 * len(replicas)))
        self.lock = threading.Lock()
        self._tls = threading.local()
        self.counters = {"puts": 0, "gets": 0, "put_failures": 0, "get_failures": 0, "suspicions": 0, "read_repairs": 0, "hints_replayed": 0,
                         "vote_disagreements": 0, "requests": 0}
        self.events = []                                     # (t, kind, replica) for detection-time measurements
        self._ver = 0

    # ---------------------------------------------------------------- helpers
    def _s(self):
        s = getattr(self._tls, "s", None)
        if s is None:
            s = self._tls.s = requests.Session()
            s.headers.update({"X-From": self.name})
        return s

    def _next_version(self):
        with self.lock:
            self._ver = max(self._ver + 1, int(time.time() * 1000))
            return self._ver

    def _ok(self, rid):
        with self.lock:
            self.fails[rid] = 0
            if rid in self.suspected_at:
                del self.suspected_at[rid]
                self.events.append((time.time(), "recovered", rid))

    def _bad(self, rid):
        with self.lock:
            self.fails[rid] += 1
            if self.fails[rid] >= self.suspect_after and rid not in self.suspected_at:
                self.suspected_at[rid] = time.time()
                self.counters["suspicions"] += 1
                self.events.append((time.time(), "suspected", rid))

    def healthy_order(self):
        """Replicas to read from: unsuspected first; suspected ones are probed again after retry_after."""
        now = time.time()
        good = [k for k in self.replicas if k not in self.suspected_at]
        probe = [k for k, t in self.suspected_at.items() if now - t >= self.retry_after]
        return good + probe

    def _call(self, rid, method, path, payload=None):
        self.counters["requests"] += 1
        try:
            r = self._s().request(method, self.replicas[rid] + path, json=payload, timeout=self.timeout)
        except requests.RequestException:
            self._bad(rid)
            return rid, None
        if r.status_code >= 500:
            self._bad(rid)
            return rid, None
        self._ok(rid)
        return rid, (r.status_code, r.json())

    # ---------------------------------------------------------------- write
    def put(self, key, value, version=None):
        version = version or self._next_version()
        body = {"value": value, "version": version, "writer": self.name}
        futs = [self.pool.submit(self._call, rid, "PUT", f"/kv/{key}", body) for rid in self.replicas]
        acks, missed, pending = 0, [], set(futs)
        deadline = time.time() + self.timeout + 0.5
        while pending and acks < self.w and time.time() < deadline:
            done, pending = wait(pending, timeout=max(0.01, deadline - time.time()), return_when=FIRST_COMPLETED)
            for f in done:
                rid, res = f.result()
                if res is not None and res[0] == 200:
                    acks += 1
                else:
                    missed.append(rid)
        for f in pending:                                    # late finishers: record hints for replicas that never answered
            f.add_done_callback(lambda ff, b=body, k=key: self._late(ff, k, b))
        for rid in missed:
            self.hints[rid][key] = body
        self.counters["puts"] += 1
        if acks < self.w:
            self.counters["put_failures"] += 1
            raise Unavailable(f"write quorum not reached for {key}: {acks}/{self.w}")
        self._replay_hints()
        return version

    def _late(self, fut, key, body):
        try:
            rid, res = fut.result()
            if res is None or res[0] != 200:
                self.hints[rid][key] = body
        except Exception:
            pass

    def _replay_hints(self):
        for rid, hs in self.hints.items():
            if hs and rid not in self.suspected_at:
                for k, body in list(hs.items()):
                    _, res = self._call(rid, "PUT", f"/kv/{k}", body)
                    if res is not None and res[0] == 200:
                        hs.pop(k, None)
                        self.counters["hints_replayed"] += 1

    # ---------------------------------------------------------------- read
    def get(self, key, default=None):
        order = self.healthy_order()
        n_read = self.r
        targets = order[:n_read] if len(order) >= n_read else order
        if len(targets) < n_read:
            self.counters["get_failures"] += 1
            raise Unavailable(f"only {len(targets)} usable replicas for read quorum {n_read}")
        futs = [self.pool.submit(self._call, rid, "GET", f"/kv/{key}") for rid in targets]
        answers, deadline = {}, time.time() + self.timeout + 0.5
        pending = set(futs)
        while pending and time.time() < deadline:
            done, pending = wait(pending, timeout=max(0.01, deadline - time.time()), return_when=FIRST_COMPLETED)
            for f in done:
                rid, res = f.result()
                if res is not None:
                    answers[rid] = res
            if len(answers) >= n_read:
                break
        spare = [rid for rid in order if rid not in targets and rid not in answers]        # failover: top up from the rest
        while len(answers) < n_read and spare:
            rid, res = self._call(spare.pop(0), "GET", f"/kv/{key}")
            if res is not None:
                answers[rid] = res
        self.counters["gets"] += 1
        if len(answers) < n_read:
            self.counters["get_failures"] += 1
            raise Unavailable(f"read quorum not reached for {key}: {len(answers)}/{n_read}")
        found = {rid: a[1] for rid, a in answers.items() if a[0] == 200}
        if not found:
            return default
        if self.vote:
            tally = {}
            for rid, a in found.items():
                tally.setdefault((a["version"], repr(a["value"])), []).append(rid)
            best_key, voters = max(tally.items(), key=lambda kv: (len(kv[1]), kv[0][0]))
            if len(tally) > 1:
                self.counters["vote_disagreements"] += 1
            if len(voters) * 2 <= len(found):
                self.counters["get_failures"] += 1
                raise Unavailable(f"no majority among {len(found)} answers for {key}")
            best = found[voters[0]]
        else:
            best = max(found.values(), key=lambda a: a["version"])
        if self.read_repair:
            stale = [rid for rid, a in found.items() if a["version"] < best["version"]]
            for rid in stale:
                self.pool.submit(self._call, rid, "PUT", f"/kv/{key}", {"value": best["value"], "version": best["version"], "writer": self.name + "-repair"})
                self.counters["read_repairs"] += 1
        return best["value"]

    def detection_time(self, rid, since):
        for t, kind, r in self.events:
            if r == rid and kind == "suspected" and t >= since:
                return t - since
        return None
