"""
Distributed process management (Milestone 10):  Create -> Locate -> Schedule -> Coordinate -> Terminate.

    ctl = Controller({"n1": url, ...}, naming_url, policy="least-loaded")
    result = ctl.run_job("job1", tasks)      # tasks: list of site lists, one worker process per task

  Create      POST /spawn on the chosen node agent (an OS process is started there and registered in the naming service)
  Locate      resolve "proc.<id>" through the naming service to the node that runs it
  Schedule    policy: round-robin | random (static, decided up front) | least-loaded (dynamic: the next task goes to the
              node with the smallest expected completion time = (running+1) x measured seconds-per-row)
  Coordinate  a barrier: wait for every worker, gather the partial results (map -> reduce); a dead node or lost process is
              detected by polling and its tasks are RE-SCHEDULED on live nodes
  Terminate   kill any worker still running when the job ends or is cancelled and unregister it
"""
import random
import time

import requests

from system.naming.resolver import NameServerResolver


class Controller:
    def __init__(self, nodes, naming_url=None, policy="least-loaded", cap=1, seed=2403, invalidate_stale=True):
        self.nodes, self.policy, self.cap = dict(nodes), policy, cap
        self.invalidate_stale = invalidate_stale             # drop the cached name->node binding when a process is re-placed (False = the original, buggy behaviour)
        self.resolver = NameServerResolver(naming_url) if naming_url else None
        self.rng = random.Random(seed)
        self.rr = 0
        self.s = requests.Session()
        self.stats = {"spawned": 0, "rescheduled": 0, "locate_calls": 0, "schedule_ms": [], "node_failures": 0}

    def _alive(self):
        up = []
        for n, u in self.nodes.items():
            try:
                if self.s.get(u + "/health", timeout=0.5).status_code == 200:
                    up.append(n)
            except requests.RequestException:
                pass
        return up

    def pick(self, alive, running, n_assigned):
        t0 = time.perf_counter()
        if self.policy == "round-robin":
            choice = alive[self.rr % len(alive)]
            self.rr += 1
        elif self.policy == "random":
            choice = self.rng.choice(alive)
        else:
            def eta(n):
                try:
                    L = self.s.get(self.nodes[n] + "/load", timeout=1).json()
                    spr = L["s_per_row"] or 4e-4
                except requests.RequestException:
                    return 1e9
                return (running[n] + 1) * spr
            choice = min(alive, key=eta)
        self.stats["schedule_ms"].append((time.perf_counter() - t0) * 1000)
        return choice

    def _forget(self, proc):
        """A re-placed process has a new home; a cached binding to the old (dead) node would be followed for a whole name TTL (60 s) otherwise."""
        if self.invalidate_stale and self.resolver:
            self.resolver.cache.pop(f"proc.{proc}", None)

    def locate(self, proc, fallback=None):
        self.stats["locate_calls"] += 1
        if self.resolver:
            return self.resolver.resolve(f"proc.{proc}") or fallback
        return fallback

    def run_job(self, job_id, tasks, timeout=120):
        t0 = time.time()
        pending = {f"{job_id}.w{i}": t for i, t in enumerate(tasks)}
        placed, results, running = {}, {}, {n: 0 for n in self.nodes}
        # static policies decide the whole assignment now
        static = {}
        if self.policy in ("round-robin", "random"):
            alive = self._alive()
            for proc in pending:
                static[proc] = self.pick(alive, running, 0)
        while pending or placed:
            if time.time() - t0 > timeout:
                raise TimeoutError("job timed out")
            alive = self._alive()
            if len(alive) < len(self.nodes):
                for n in set(self.nodes) - set(alive):
                    for proc, (node, sites) in list(placed.items()):
                        if node == n:                                     # node died: its processes are lost -> reschedule
                            self.stats["node_failures"] += 1
                            self.stats["rescheduled"] += 1
                            pending[proc] = sites
                            del placed[proc]
                            running[n] = max(0, running[n] - 1)
                            static.pop(proc, None)
                            self._forget(proc)
            # create + schedule
            for proc in list(pending):
                if not alive:
                    break
                node = static.get(proc)
                if node is None or node not in alive:
                    node = self.pick(alive, running, 0) if self.policy == "least-loaded" or node not in alive else node
                if running[node] >= self.cap:
                    if self.policy != "least-loaded":
                        continue
                    free = [n for n in alive if running[n] < self.cap]
                    if not free:
                        continue
                    node = self.pick(free, running, 0)
                sites = pending.pop(proc)
                try:
                    self.s.post(self.nodes[node] + "/spawn", json={"proc": proc, "sites": sites}, timeout=3)
                except requests.RequestException:
                    pending[proc] = sites
                    continue
                self.stats["spawned"] += 1
                placed[proc] = (node, sites)
                running[node] += 1
            # coordinate: collect finished workers; locate them through the naming service
            for proc, (node, sites) in list(placed.items()):
                loc = self.locate(proc, self.nodes[node]) if self.resolver else self.nodes[node]
                try:
                    r = self.s.get((loc or self.nodes[node]) + f"/result/{proc}", timeout=2)
                except requests.RequestException:
                    continue
                if r.status_code == 200:
                    results[proc] = r.json()
                    del placed[proc]
                    running[node] = max(0, running[node] - 1)
                elif r.status_code == 404 or r.json().get("state") in ("failed", "terminated"):
                    self.stats["rescheduled"] += 1
                    pending[proc] = sites
                    del placed[proc]
                    running[node] = max(0, running[node] - 1)
                    self._forget(proc)
            time.sleep(0.05)
        wall = time.time() - t0
        return {"job": job_id, "wall_s": round(wall, 3), "workers": len(results), "flagged": sum(r["flagged"] for r in results.values()),
                "rows": sum(r["rows"] for r in results.values()),
                "per_node": {n: sum(1 for r in results.values() if r["node"] == n) for n in self.nodes}, "results": results}

    def terminate_job(self, job_id):
        for n, u in self.nodes.items():
            try:
                for p in self.s.get(u + "/ps", timeout=1).json()["procs"]:
                    if p["proc"].startswith(job_id + ".") and p["state"] == "running":
                        self.s.post(u + "/terminate", json={"proc": p["proc"]}, timeout=1)
            except requests.RequestException:
                pass
