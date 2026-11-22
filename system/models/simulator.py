"""
Discrete-event simulator for the five distributed-system models (Milestone 8).

Same workload, five architectures. The compute cost per row is MEASURED on this machine (see
experiments/m08_models.py -> calibrate()), the network figures are stated assumptions, everything is seeded so a run
is exactly reproducible.

Workload: S monitored sites each produce analysis jobs (a job = JOB_ROWS telemetry rows to be scanned for anomalies).
Site rates are uneven (a few hot sites), arrivals are Poisson.

Models
  host-based      all jobs are shipped to ONE central host (H cores). Terminals do nothing. (centralised)
  workstation     every site analyses its own jobs on its own 1-core machine; nothing is shared.
  server-based    sites are clients of K dedicated compute servers (fixed site->server assignment, C cores each).
  processor-pool  P identical processors, one dispatcher sends each job to the least-loaded UP processor.
  integrated      edge nodes pre-aggregate locally (cost EDGE_COST of the job, data shrinks to RHO), then the reduced
                  work is dispatched to a processor pool with replicated storage: the architecture of this project.

Failure model: compute nodes alternate UP (exponential MTBF) and DOWN (exponential MTTR). When a node goes down its
queued and running jobs are returned to the model's dispatcher; models that cannot re-route park them until the node
returns (that is where their reliability differs).
"""
import heapq
import random
import statistics
from collections import deque

ROW_BYTES = 380.0
JOB_ROWS = 1000


class Node:
    def __init__(self, sim, name, cores, mtbf=None, mttr=None, cost_per_hour=1.0):
        self.sim, self.name, self.cores = sim, name, cores
        self.queue, self.running, self.up = deque(), {}, True
        self.busy_core_s, self.mtbf, self.mttr, self.cost_per_hour = 0.0, mtbf, mttr, cost_per_hour
        self.parked = []
        self.owner = None
        self.downtime = 0.0
        self.went_down = None
        if mtbf:
            sim.at(sim.rng.expovariate(1.0 / mtbf), self.fail)

    def load(self):
        return len(self.queue) + len(self.running)

    def submit(self, job, service):
        if not self.up:
            self.parked.append((job, service))
            return
        self.queue.append((job, service))
        self._dispatch()

    def _dispatch(self):
        while self.up and self.queue and len(self.running) < self.cores:
            job, service = self.queue.popleft()
            token = object()
            self.running[token] = (job, service, self.sim.t)
            self.sim.at(service, lambda tk=token: self._finish(tk))

    def _finish(self, token):
        if token not in self.running:
            return                                           # the node failed while this job was running
        job, service, t0 = self.running.pop(token)
        self.busy_core_s += service
        self.sim.on_done(job)
        self._dispatch()

    def fail(self):
        self.up, self.went_down = False, self.sim.t
        lost = [(j, s) for j, s, _ in self.running.values()] + list(self.queue)
        self.running.clear()
        self.queue.clear()
        self.sim.metrics["jobs_interrupted"] += len(lost)
        for job, service in lost:
            self.sim.model.redispatch(job, service, self)
        self.sim.at(self.sim.rng.expovariate(1.0 / self.mttr), self.recover)

    def recover(self):
        self.up = True
        self.downtime += self.sim.t - self.went_down
        pend, self.parked = self.parked, []
        for job, service in pend:
            self.submit(job, service)
        self.sim.at(self.sim.rng.expovariate(1.0 / self.mtbf), self.fail)


class Sim:
    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.t, self.heap, self.seq = 0.0, [], 0
        self.metrics = {"jobs_interrupted": 0, "bytes_wan": 0.0}
        self.latencies, self.done, self.model = [], 0, None

    def at(self, delay, fn):
        self.seq += 1
        heapq.heappush(self.heap, (self.t + delay, self.seq, fn))

    def on_done(self, job):
        job["t_done"] = self.t
        self.latencies.append(self.t - job["t_arrive"])
        self.done += 1

    def run(self, horizon):
        while self.heap and self.heap[0][0] <= horizon:
            self.t, _, fn = heapq.heappop(self.heap)
            fn()


class Network:
    def __init__(self, wan_mbps=50.0, wan_latency_s=0.020):
        self.bw, self.lat = wan_mbps * 1e6 / 8, wan_latency_s

    def transfer_s(self, nbytes):
        return self.lat + nbytes / self.bw


class Model:
    kind = "?"
    price_core_hour, price_gb = 1.0, 0.09

    def __init__(self, sim, net, sites, cpu_per_row, params):
        self.sim, self.net, self.sites, self.cpu_row, self.p = sim, net, sites, cpu_per_row, params
        sim.model = self
        self.nodes = []
        self.build()

    def job_cpu(self):
        return self.cpu_row * JOB_ROWS

    def job_bytes(self):
        return ROW_BYTES * JOB_ROWS

    def new_node(self, name, cores, fail=True):
        n = Node(self.sim, name, cores, self.p.get("mtbf") if fail else None, self.p.get("mttr"))
        self.nodes.append(n)
        return n

    def build(self): ...
    def route(self, job): ...

    def redispatch(self, job, service, failed):
        failed.parked.append((job, service))

    def cost(self, horizon):
        cores = sum(n.cores for n in self.nodes)
        return cores * self.price_core_hour * horizon / 3600.0 + self.sim.metrics["bytes_wan"] / 1e9 * self.price_gb


class HostBased(Model):
    kind = "host-based"

    def build(self):
        self.host = self.new_node("host", self.p["host_cores"])

    def route(self, job):
        self.sim.metrics["bytes_wan"] += self.job_bytes()
        self.sim.at(self.net.transfer_s(self.job_bytes()), lambda: self.host.submit(job, self.job_cpu()))


class Workstation(Model):
    kind = "workstation"

    def build(self):
        self.ws = {s: self.new_node(f"ws-{s}", 1) for s in self.sites}

    def route(self, job):
        self.ws[job["site"]].submit(job, self.job_cpu())


class ServerBased(Model):
    kind = "server-based"

    def build(self):
        self.servers = [self.new_node(f"srv{i}", self.p["server_cores"]) for i in range(self.p["servers"])]

    def server_of(self, site):
        """Regional assignment: contiguous blocks of sites share a server (hot sites are NOT all on one server)."""
        return self.servers[self.sites.index(site) * len(self.servers) // len(self.sites)]

    def route(self, job):
        self.sim.metrics["bytes_wan"] += self.job_bytes()
        srv = self.server_of(job["site"])
        self.sim.at(self.net.transfer_s(self.job_bytes()), lambda: srv.submit(job, self.job_cpu()))


class ProcessorPool(Model):
    kind = "processor-pool"

    def build(self):
        self.pool = [self.new_node(f"p{i}", 1) for i in range(self.p["pool"])]

    def pick(self):
        up = [n for n in self.pool if n.up] or self.pool
        return min(up, key=lambda n: n.load())

    def route(self, job):
        self.sim.metrics["bytes_wan"] += self.job_bytes()
        self.sim.at(self.net.transfer_s(self.job_bytes()), lambda: self.pick().submit(job, self.job_cpu()))

    def redispatch(self, job, service, failed):
        self.pick().submit(job, service)                          # any other live processor can take over


class Integrated(ProcessorPool):
    kind = "integrated"

    def build(self):
        super().build()
        # edge pre-aggregation machines exist at every site already; they are cheap and modelled as always up
        self.edge = {s: self.new_node(f"edge-{s}", 1, fail=False) for s in self.sites}
        self.busy_until = {s: 0.0 for s in self.sites}

    def route(self, job):
        site = job["site"]
        edge_cpu = self.p["edge_cost"] * self.job_cpu()
        sent = self.job_bytes() * self.p["rho"]                       # only the reduced summary crosses the WAN
        start = max(self.sim.t, self.busy_until[site])                # the 1-core edge machine works through jobs one at a time
        self.busy_until[site] = start + edge_cpu
        self.edge[site].busy_core_s += edge_cpu

        def after_edge():
            self.sim.metrics["bytes_wan"] += sent
            self.sim.at(self.net.transfer_s(sent), lambda: self.pick().submit(job, self.job_cpu() * (1 - self.p["edge_cost"])))
        self.sim.at(self.busy_until[site] - self.sim.t, after_edge)


MODELS = {"host-based": HostBased, "workstation": Workstation, "server-based": ServerBased, "processor-pool": ProcessorPool, "integrated": Integrated}


def simulate(kind, seed=2403, sites=24, load=1.0, horizon=600.0, cpu_per_row=2e-4, params=None, wan_mbps=50.0):
    """Run one model. `load` scales arrival rates relative to the reference (host with 4 cores ~85 % busy)."""
    p = {"host_cores": 4, "servers": 2, "server_cores": 2, "pool": 5, "edge_cost": 0.10, "rho": 0.10, "mtbf": None, "mttr": 60.0}
    p.update(params or {})
    sim = Sim(seed)
    site_names = [f"S{i:02d}" for i in range(sites)]
    m = MODELS[kind](sim, Network(wan_mbps), site_names, cpu_per_row, p)
    job_cpu = cpu_per_row * JOB_ROWS
    ref_total = 0.85 * 4 / job_cpu * load * (sites / 24.0)           # jobs/s that keep a 4-core host 85 % busy at 24 sites
    weights = [4.0 if i % 6 == 0 else 1.0 for i in range(sites)]       # a few hot sites
    wsum = sum(weights)
    rates = {s: ref_total * w / wsum for s, w in zip(site_names, weights)}

    arrivals = [0]

    def arrival(site):
        arrivals[0] += 1
        job = {"site": site, "t_arrive": sim.t}
        m.route(job)
        sim.at(sim.rng.expovariate(rates[site]), lambda: arrival(site))
    for s in site_names:
        sim.at(sim.rng.expovariate(rates[s]), lambda s=s: arrival(s))
    sim.run(horizon)
    lat = sorted(sim.latencies)
    pct = lambda q: lat[min(len(lat) - 1, int(q * len(lat)))] if lat else None
    total_cores = sum(n.cores for n in m.nodes)
    compute_nodes = [n for n in m.nodes if not n.name.startswith("edge-")] if kind == "integrated" else m.nodes
    comp_cores = sum(n.cores for n in compute_nodes)
    util = sum(n.busy_core_s for n in compute_nodes) / (comp_cores * horizon)
    offered = sum(rates.values()) * horizon
    return {"model": kind, "sites": sites, "load": load, "horizon_s": horizon, "offered_jobs": round(offered), "completed_jobs": sim.done,
            "throughput_jobs_s": round(sim.done / horizon, 3), "completion_pct": round(100 * sim.done / max(1, arrivals[0]), 1), "arrived_jobs": arrivals[0],
            "lat_mean_s": round(statistics.mean(lat), 3) if lat else None, "lat_p95_s": round(pct(0.95), 3) if lat else None,
            "lat_p99_s": round(pct(0.99), 3) if lat else None,
            "compute_cores": comp_cores, "total_cores": total_cores, "compute_utilization": round(util, 3),
            "wan_gb": round(sim.metrics["bytes_wan"] / 1e9, 3), "cost_units": round(m.cost(horizon), 2),
            "jobs_interrupted": sim.metrics["jobs_interrupted"]}
