"""
Resource manager with deadlock handling (Milestone 6).

Processes P_i request and hold resources R_j (R_j has `capacity` identical units; capacity 1 = a mutex).
The manager keeps the state needed to draw the resource-allocation graph
        request edge  P_i -> R_j   (P_i is waiting for R_j)
        assignment    R_j -> P_i   (R_j is held by P_i)
and the derived wait-for graph  P_i -> P_k  (P_i waits for a resource held by P_k).
A cycle in the wait-for graph is a deadlock when every resource has a single unit.

Policies (how deadlock is dealt with):
  none         no handling: requests simply wait (they can only time out) -> deadlock is permanent
  detect       a detector thread looks for wait-for cycles every `detect_interval` seconds and RECOVERS by
               aborting a victim (the youngest process in the cycle): its acquire() raises Deadlock and the
               caller must release_all() and retry
  ordered      PREVENTION by resource ordering: acquiring a resource that ranks lower than one already held
               is refused -> no circular wait can form (breaks the "circular wait" Coffman condition)
  wait-die     PREVENTION by timestamps: an older process may wait for a younger holder; a younger process
               requesting from an older holder "dies" (Deadlock raised) and restarts with its ORIGINAL
               timestamp, so it eventually becomes the oldest and cannot starve.
Fairness:
  fair=True    FIFO per resource: the request at the head of the queue blocks later (even smaller) ones ->
               no starvation of large requests.
  fair=False   barging: any request that fits is served -> a large request can starve.
"""
import threading
import time
from collections import defaultdict


class Deadlock(Exception):
    pass


class OrderViolation(Exception):
    pass


class _Req:
    __slots__ = ("pid", "res", "units", "t", "seq")

    def __init__(self, pid, res, units, seq):
        self.pid, self.res, self.units, self.t, self.seq = pid, res, units, time.time(), seq


class ResourceManager:
    def __init__(self, resources, policy="none", fair=True, detect_interval=0.2):
        self.cap = dict(resources)
        self.rank = {r: i for i, r in enumerate(sorted(self.cap))}
        self.policy, self.fair, self.detect_interval = policy, fair, detect_interval
        self.cv = threading.Condition()
        self.held = defaultdict(dict)                     # res -> {pid: units}
        self.queue = defaultdict(list)                    # res -> [_Req]  (FIFO order)
        self.birth = {}                                   # pid -> timestamp (priority for wait-die; set once via begin())
        self.victims = set()
        self.seq = 0
        self.stats = {"acquired": 0, "waits": 0, "timeouts": 0, "deadlocks_detected": 0, "victims_aborted": 0,
                      "wait_die_aborts": 0, "order_violations": 0, "wait_s_total": 0.0, "detect_latency_s": [],
                      "unit_seconds_busy": 0.0}
        self.t0 = time.time()
        self._last = defaultdict(float)
        self.cycles_log = []
        self._stop = False
        if policy == "detect":
            threading.Thread(target=self._detector, daemon=True).start()

    # ------------------------------------------------------------ bookkeeping
    def free(self, res):
        return self.cap[res] - sum(self.held[res].values())

    def _busy_account(self, res):
        now = time.time()
        used = sum(self.held[res].values())
        self.stats["unit_seconds_busy"] += used * (now - self._last[res]) if self._last[res] else 0
        self._last[res] = now

    def begin(self, pid, ts=None):
        with self.cv:
            self.birth.setdefault(pid, ts if ts is not None else time.time())

    def end(self, pid):
        with self.cv:
            self.birth.pop(pid, None)

    # ------------------------------------------------------------ acquire / release
    def acquire(self, pid, res, units=1, timeout=None):
        deadline = None if timeout is None else time.time() + timeout
        with self.cv:
            self.birth.setdefault(pid, time.time())
            if self.policy == "ordered":
                mine = [r for r, h in self.held.items() if pid in h]
                if any(self.rank[r] > self.rank[res] for r in mine):
                    self.stats["order_violations"] += 1
                    raise OrderViolation(f"{pid} holds a higher-ranked resource than {res}")
            self.seq += 1
            req = _Req(pid, res, units, self.seq)
            self.queue[res].append(req)
            waited = False
            t_wait = time.time()
            try:
                while True:
                    if pid in self.victims:
                        self.victims.discard(pid)
                        raise Deadlock(f"{pid} chosen as deadlock victim")
                    if self._grantable(req):
                        self._busy_account(res)
                        self.held[res][pid] = self.held[res].get(pid, 0) + units
                        self.stats["acquired"] += 1
                        if waited:
                            self.stats["wait_s_total"] += time.time() - t_wait
                        return True
                    if self.policy == "wait-die":
                        older_holder = [h for h in self.held[res] if h != pid and self.birth.get(h, 1e18) < self.birth[pid]]
                        if older_holder:                 # younger requester vs older holder -> die
                            self.stats["wait_die_aborts"] += 1
                            raise Deadlock(f"{pid} (younger) dies rather than wait for {older_holder[0]}")
                    if not waited:
                        waited = True
                        self.stats["waits"] += 1
                    left = None if deadline is None else deadline - time.time()
                    if left is not None and left <= 0:
                        self.stats["timeouts"] += 1
                        raise TimeoutError(f"{pid} timed out waiting for {res}")
                    self.cv.wait(0.05 if left is None else min(0.05, left))
            finally:
                if req in self.queue[res]:
                    self.queue[res].remove(req)
                self.cv.notify_all()

    def _grantable(self, req):
        if self.free(req.res) < req.units:
            return False
        if self.fair:                                    # FIFO: only the head of the queue may be served
            head = self.queue[req.res][0] if self.queue[req.res] else None
            return head is req
        return True

    def release(self, pid, res, units=None):
        with self.cv:
            self._busy_account(res)
            have = self.held[res].get(pid, 0)
            left = have - (units if units is not None else have)
            if left > 0:
                self.held[res][pid] = left
            else:
                self.held[res].pop(pid, None)
            self.cv.notify_all()

    def release_all(self, pid):
        with self.cv:
            for res in list(self.held):
                if pid in self.held[res]:
                    self._busy_account(res)
                    del self.held[res][pid]
            self.victims.discard(pid)
            self.cv.notify_all()

    # ------------------------------------------------------------ graphs and detection
    def graph(self):
        with self.cv:
            return self._graph()

    def _graph(self):
        request = [(r.pid, r.res) for q in self.queue.values() for r in q if self.free(r.res) < r.units or (self.fair and q[0] is not r)]
        assign = [(res, pid) for res, h in self.held.items() for pid in h]
        wait_for = set()
        for pid, res in request:
            for holder in self.held[res]:
                if holder != pid:
                    wait_for.add((pid, holder))
        return {"request_edges": request, "assignment_edges": assign, "wait_for_edges": sorted(wait_for)}

    @staticmethod
    def find_cycle(edges):
        adj = defaultdict(list)
        for a, b in edges:
            adj[a].append(b)
        state, stack = {}, []

        def dfs(u):
            state[u] = 1
            stack.append(u)
            for v in adj[u]:
                if state.get(v) == 1:
                    return stack[stack.index(v):]
                if state.get(v) is None:
                    c = dfs(v)
                    if c:
                        return c
            stack.pop()
            state[u] = 2
            return None
        for n in list(adj):
            if state.get(n) is None:
                c = dfs(n)
                if c:
                    return c
        return None

    def to_dot(self, title="resource-allocation graph"):
        g = self.graph()
        lines = [f'digraph "{title}" {{', "  rankdir=LR;", "  node [fontname=Helvetica];"]
        procs = {p for p, _ in g["request_edges"]} | {p for _, p in g["assignment_edges"]}
        ress = {r for _, r in g["request_edges"]} | {r for r, _ in g["assignment_edges"]}
        lines += [f'  "{p}" [shape=circle];' for p in sorted(procs)] + [f'  "{r}" [shape=box];' for r in sorted(ress)]
        lines += [f'  "{p}" -> "{r}" [label="request", style=dashed];' for p, r in g["request_edges"]]
        lines += [f'  "{r}" -> "{p}" [label="held by"];' for r, p in g["assignment_edges"]]
        lines.append("}")
        return "\n".join(lines)

    def detect_once(self):
        with self.cv:
            cyc = self.find_cycle(self._graph()["wait_for_edges"])
            if not cyc:
                return None
            victim = max(cyc, key=lambda p: self.birth.get(p, 0))      # youngest in the cycle
            self.victims.add(victim)
            self.stats["deadlocks_detected"] += 1
            self.stats["victims_aborted"] += 1
            oldest_wait = min((r.t for q in self.queue.values() for r in q if r.pid in cyc), default=time.time())
            self.stats["detect_latency_s"].append(round(time.time() - oldest_wait, 3))
            self.cycles_log.append({"cycle": cyc, "victim": victim, "t": round(time.time() - self.t0, 3)})
            self.cv.notify_all()
            return cyc

    def _detector(self):
        while not self._stop:
            time.sleep(self.detect_interval)
            self.detect_once()

    def utilization(self):
        with self.cv:
            for res in self.cap:
                self._busy_account(res)
            total_units = sum(self.cap.values())
            elapsed = max(time.time() - self.t0, 1e-9)
            return round(self.stats["unit_seconds_busy"] / (total_units * elapsed), 4)

    def close(self):
        self._stop = True
