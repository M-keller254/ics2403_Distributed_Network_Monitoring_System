"""
Milestone 12 (part 2) -- CAPSTONE EXPERIMENT: baseline vs proposed vs proposed + improvement, nine factors, three groups of metrics.

Systems compared (same dataset, same driver, same host):
  baseline-flat       Week 1/2 design. Edge nodes POST straight to ONE collector (validate + SQLite). No bus, no replicas, no election.
  proposed            Final architecture without the improvement: edge gateway -> Kafka-style log -> 3 core replicas (Bully leader) -> cloud.
                      Fixed ROWS_PER_MSG=100, uncompressed rows on the edge->core link.
  proposed+improved   Same, plus the improvement of brief section 6: ADAPTIVE site-affine coalescing (bigger messages under backlog, bounded
                      wait COALESCE_MS) and zlib compression of row batches on the bandwidth-limited edge->core link.

NOTE service ports are kept below 32768: Linux hands those above to client sockets, which collided with service ports in the first run.

Factors (one-factor-at-a-time sweeps around a centre point; centre = 60,000 rows, 4 users, unlimited rate, no bandwidth cap, 3 cores,
2 CPUs, no memory cap, no cross-traffic, no failures):
  nodes        number of core replicas / processing nodes                    1, 2, 3, 5            (baseline has no scale-out: 1)
  users        concurrent edge clients sending                                1, 2, 4, 8, 16
  bandwidth    capacity of the edge->core link (Mbit/s)                       unlimited, 100, 25, 10, 5    (full 120,000 rows)
  cpu          CPU available to the core / collector processes                2 CPUs, 1 CPU, 1 CPU shared with a busy-loop process
  memory       address-space cap on the core / collector processes            unlimited, 256, 160, 128 MB
  net_load     cross-traffic at the entry point (threads posting 32 KB live reports)    0, 2, 8, 16
  failure_rate core/collector crashes per minute (killed, restarted after 1 s)   0, 6, 12, 20
  request_rate offered load (rows/s, paced)                                      2k, 5k, 10k, 20k, unlimited
  data_volume  rows sent                                                          10k, 30k, 60k, 120k
Metrics:
  performance   request latency (mean/p95/p99), jitter (stdev and RFC-3550-style mean |delta|), ingest throughput, end-to-end throughput, e2e lag
  resources     CPU %, resident memory, wire bandwidth (MB, Mbit/s), compression ratio
  reliability   ingest availability (1-row write probe every 100 ms), request failure rate, mean time to resume processing after a crash (MTTR),
                rows lost
Also: consistency (stored rows are checked against the dataset: lost / duplicates), scalability (nodes + users + volume sweeps), coordination overhead
(election + heartbeat messages are read from the cores in the nodes sweep).

Hypotheses (written before running):
  H1 Under an unconstrained link the improvement costs CPU (compression) and gives no throughput gain; baseline-flat is fastest (fewest hops).
  H2 Under a constrained link (<= 25 Mbit/s) compression cuts wire bytes ~4-6x and so end-to-end time by a similar factor; below the crossover
     bandwidth proposed+improved beats both other systems.
  H3 Under crashes the flat collector loses availability in proportion to downtime; the proposed system keeps accepting data (edge outbox + log)
     and loses nothing, at the price of a processing pause (MTTR) when the leader dies.
  H4 More core replicas do not raise throughput on 2 CPUs (they share the CPUs) but raise coordination overhead.
"""
import csv
import glob
import json
import os
import random
import shutil
import sqlite3
import statistics
import subprocess
import sys
import threading
import time

import requests

from system.experiments.common import RESULTS, ResourceSampler, dataset, save_csv, save_json
from system.scripts.cluster import Cluster
from system.scripts.stack import edge_core_cloud

PROBE_BASE = 10 ** 9
RAW = os.path.join(RESULTS, "m12_capstone_raw.csv")
SYSTEMS = ["baseline-flat", "proposed", "proposed+improved"]
CENTRE = dict(rows=60000, users=4, rate=0, bw=0, nodes=3, cpus=None, mem_mb=None, noise=0, fail_per_min=0, hog=False)
_BODY_CACHE = {}


def n_cpu():
    return len(os.sched_getaffinity(0))


# ----------------------------------------------------------------------------------------------- driver
def bodies(rows, chunk):
    key = (id(rows), len(rows), chunk)
    if key not in _BODY_CACHE:
        _BODY_CACHE.clear()
        _BODY_CACHE[key] = [json.dumps({"rows": rows[i:i + chunk]}).encode() for i in range(0, len(rows), chunk)]
    return _BODY_CACHE[key]


class Driver:
    """`users` concurrent edge clients pulling chunks from a shared list. Optional global pacing (rows/s) and a shared emulated uplink."""

    def __init__(self, url, rows, chunk=1000, users=4, rate=0, link_mbps=0.0):
        self.url, self.users, self.rate, self.link_mbps, self.chunk = url.rstrip("/"), users, rate, link_mbps, chunk
        self.rows = rows
        self.bodies = bodies(rows, chunk)
        self.next = 0
        self.lock = threading.Lock()
        self.link_free = 0.0
        self.pace_next = 0.0
        self.lat, self.attempts, self.failures, self.bytes = [], 0, 0, 0
        self.t_start = self.t_end = None

    def _take(self):
        with self.lock:
            if self.next >= len(self.bodies):
                return None
            i = self.next
            self.next += 1
            return i

    def _worker(self):
        s = requests.Session()
        while True:
            i = self._take()
            if i is None:
                return
            body = self.bodies[i]
            n_rows = min(self.chunk, len(self.rows) - i * self.chunk)
            if self.rate:                                              # pacing: this chunk may not start before its slot
                with self.lock:
                    self.pace_next = max(self.pace_next, time.time()) + n_rows / float(self.rate)
                    slot = self.pace_next - n_rows / float(self.rate)
                w = slot - time.time()
                if w > 0:
                    time.sleep(w)
            if self.link_mbps:                                         # emulated shared uplink (client -> collector in the flat design)
                with self.lock:
                    self.link_free = max(self.link_free, time.time()) + len(body) * 8.0 / (self.link_mbps * 1e6)
                    w = self.link_free - time.time()
                if w > 0:
                    time.sleep(w)
            while True:
                t0 = time.perf_counter()
                try:
                    r = s.post(self.url + "/bulk", data=body, headers={"Content-Type": "application/json"}, timeout=20)
                    dt = time.perf_counter() - t0
                    with self.lock:
                        self.attempts += 1
                        self.bytes += len(body)
                    if r.status_code == 200:
                        with self.lock:
                            self.lat.append(dt)
                        break
                    with self.lock:
                        self.failures += 1
                    time.sleep(0.2 if r.status_code == 429 else 0.3)
                except requests.RequestException:
                    with self.lock:
                        self.attempts += 1
                        self.failures += 1
                    time.sleep(0.4)

    def run(self):
        self.t_start = time.perf_counter()
        ths = [threading.Thread(target=self._worker, daemon=True) for _ in range(self.users)]
        [t.start() for t in ths]
        [t.join() for t in ths]
        self.t_end = time.perf_counter()
        return self.t_end - self.t_start


class Noise:
    """Cross-traffic at the entry point: threads posting 32 KB live-agent reports back to back."""

    def __init__(self, url, threads):
        self.url, self.threads, self.stop, self.sent = url.rstrip("/"), threads, threading.Event(), [0]
        self.ths = []

    def _run(self, i):
        s = requests.Session()
        rep = {"node_id": f"noise-{i}", "node_role": "edge", "cpu_percent": 30, "mem_percent": 50, "timestamp": time.time(), "report_interval": 1.0,
               "compute_time_ms": 5.0, "pad": "x" * 32768}
        while not self.stop.is_set():
            try:
                rep["timestamp"] = time.time()
                s.post(self.url + "/metrics", json=rep, timeout=5)
                self.sent[0] += 1
            except requests.RequestException:
                time.sleep(0.1)

    def __enter__(self):
        for i in range(self.threads):
            t = threading.Thread(target=self._run, args=(i,), daemon=True)
            t.start()
            self.ths.append(t)
        return self

    def __exit__(self, *a):
        self.stop.set()


class Probe:
    """Ingest availability: a real 1-row write every 100 ms; 'up' if it is accepted within 1 s."""

    def __init__(self, url, template):
        self.url, self.tpl, self.stop, self.ok, self.n, self.fail_runs = url.rstrip("/"), template, threading.Event(), 0, 0, []
        self.t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        s, i, down_since = requests.Session(), 0, None
        while not self.stop.wait(0.1):
            row = dict(self.tpl, record_id=PROBE_BASE + i)
            i += 1
            up = False
            try:
                up = s.post(self.url + "/bulk", json={"rows": [row]}, timeout=1.0).status_code == 200
            except requests.RequestException:
                pass
            self.n += 1
            self.ok += int(up)
            if not up and down_since is None:
                down_since = time.time()
            if up and down_since is not None:
                self.fail_runs.append(time.time() - down_since)
                down_since = None

    def __enter__(self):
        self.t.start()
        return self

    def __exit__(self, *a):
        self.stop.set()
        self.t.join()

    def availability(self):
        return round(100.0 * self.ok / self.n, 2) if self.n else None


# ----------------------------------------------------------------------------------------------- one trial
def stored_ids(c, system, workdir):
    """(distinct data rows stored, total stored rows incl. duplicates) from the SQLite files, ignoring probe rows."""
    if system == "baseline-flat":
        paths = glob.glob(f"{workdir}/baseline/*.sqlite3")
    else:
        paths = glob.glob(f"{workdir}/core-data/core-*.sqlite3")
    ids, total = set(), 0
    for p in paths:
        try:
            con = sqlite3.connect(p, timeout=30)
            got = [r[0] for r in con.execute("SELECT record_id FROM readings WHERE record_id < ?", (PROBE_BASE,))]
            con.close()
        except sqlite3.Error:
            continue
        total += len(got)
        ids.update(got)
    return len(ids), total


def live_count(c, system, names):
    """Cheap progress counter used while the trial runs (sum over live processes, probe rows included)."""
    tot = 0
    targets = ["collector"] if system == "baseline-flat" else names["cores"]
    for n in targets:
        if c.alive(n):
            try:
                tot += c.get(n, "/readings/count", timeout=2)["rows_in_db"]
            except Exception:
                pass
    return tot


def trial(system, rows, users=4, rate=0, bw=0, nodes=3, cpus=None, mem_mb=None, noise=0, fail_per_min=0, hog=False, transport="kafka", chunk=1000,
          base=20000, seed=2403, drain_timeout=240):
    t_wall = time.time()
    with Cluster() as c:
        names = {"cores": ["collector"]}
        hog_proc = None
        if hog:                                                          # a busy-loop pinned to the same CPU as the processing node(s)
            hog_proc = subprocess.Popen([sys.executable, "-c", "import os\nos.sched_setaffinity(0,{%d})\nwhile True: pass" % (cpus[0] if cpus else 0)])
        try:
            if system == "baseline-flat":
                c.start("collector", "system.baseline.collector", base, env={"DATA_DIR": f"{c.workdir}/baseline"}, cpus=cpus, mem_mb=mem_mb)
                entry, link = c.url("collector"), bw
            else:
                improved = system == "proposed+improved"
                genv = {"TRANSPORT": transport, "LINK_MBPS": bw, "COMPRESS": "zlib" if improved else "none", "ADAPTIVE": "true" if improved else "false",
                        "ROWS_PER_MSG": 100, "OUTBOX_MAX": 4000}
                names = edge_core_cloud(c, base=base, n_cores=nodes, transport=transport, gateway_env=genv, core_env={"HEARTBEAT_INTERVAL": 0.5},
                                        core_cpus=cpus, core_mem_mb=mem_mb)
                entry, link = c.url("gateway"), 0
            kill_targets = ["collector"] if system == "baseline-flat" else list(names["cores"])
            pids = [p.pid for p in c.procs.values()]
            drv = Driver(entry, rows, chunk=chunk, users=users, rate=rate, link_mbps=link)
            kills, mttrs, stop_chaos = [], [], threading.Event()
            progress = {"n": 0}
            probes_ok = lambda: probe.ok

            def monitor():                                               # resume time after each crash = first moment the stored-row counter moves again
                waiting = None
                last = live_count(c, system, names)
                while not stop_chaos.is_set():
                    time.sleep(0.1)
                    cur = live_count(c, system, names)
                    progress["n"] = cur
                    if waiting and cur > waiting[1] + 20:
                        mttrs.append(time.time() - waiting[0])
                        waiting = None
                    if kills and kills[-1]["t"] is not None and kills[-1].get("armed"):
                        kills[-1]["armed"] = False
                        waiting = (kills[-1]["t"], last)
                    last = cur
                    if waiting and drv.t_end is not None and cur - probe.ok >= len(rows):
                        waiting = None

            def chaos():
                rng = random.Random(seed)
                period = 60.0 / fail_per_min
                time.sleep(1.0)
                while not stop_chaos.is_set():
                    alive = [n for n in kill_targets if c.alive(n)]
                    if alive and drv.t_end is None:
                        v = rng.choice(alive)
                        c.kill(v)
                        kills.append({"victim": v, "t": time.time(), "armed": True})
                        time.sleep(1.0)
                        try:
                            c.restart(v, wait=True, timeout=20)
                        except Exception:
                            pass
                    if stop_chaos.wait(max(0.0, period - 1.0)):
                        return

            tpl = rows[0]
            with ResourceSampler(pids) as rs, Probe(entry, tpl) as probe:
                th = [threading.Thread(target=monitor, daemon=True)]
                if fail_per_min:
                    th.append(threading.Thread(target=chaos, daemon=True))
                [t.start() for t in th]
                noise_ctx = Noise(entry, noise) if noise else None
                if noise_ctx:
                    noise_ctx.__enter__()
                t0 = time.perf_counter()
                send_s = drv.run()
                deadline = time.time() + drain_timeout
                while time.time() < deadline:
                    if live_count(c, system, names) - probe.ok >= len(rows) and stored_ids(c, system, c.workdir)[0] >= len(rows):
                        break                                            # cheap counter first (may include duplicates), then verify distinct ids
                    time.sleep(0.25)
                complete_s = time.perf_counter() - t0
                stop_chaos.set()
                if noise_ctx:
                    noise_ctx.__exit__()
                for t in th:
                    t.join(timeout=30)          # a chaos restart may still be in flight; never leave an orphan process behind
            time.sleep(0.3)
            distinct, total = stored_ids(c, system, c.workdir)
            wire_raw = wire = drv.bytes
            if system != "baseline-flat":
                link_stats = c.get("gateway", "/metrics_summary")["link"]
                wire, wire_raw = link_stats["wire"], link_stats["raw"]
            lat = sorted(drv.lat)
            pct = lambda q: round(1000 * lat[min(len(lat) - 1, int(q * len(lat)))], 2) if lat else None
            deltas = [abs(a - b) for a, b in zip(drv.lat[1:], drv.lat[:-1])]
            out = {
                "system": system, "transport": transport, "rows": len(rows), "users": users, "rate_rows_s": rate or "unlimited", "bandwidth_mbps": bw or "unlimited",
                "nodes": nodes if system != "baseline-flat" else 1, "cpus": len(cpus) if cpus else n_cpu(), "cpu_contended": hog, "mem_cap_mb": mem_mb or "none",
                "cross_traffic_threads": noise, "failures_per_min": fail_per_min,
                "stored_distinct": distinct, "stored_total_incl_dup": total, "rows_lost": len(rows) - distinct, "duplicates": total - distinct,
                "send_s": round(send_s, 2), "complete_s": round(complete_s, 2), "e2e_lag_s": round(complete_s - send_s, 2),
                "ingest_rows_s": round(len(rows) / send_s, 0), "e2e_rows_s": round(distinct / complete_s, 0),
                "req_lat_mean_ms": round(1000 * statistics.mean(lat), 2) if lat else None, "req_lat_p95_ms": pct(0.95), "req_lat_p99_ms": pct(0.99),
                "jitter_sd_ms": round(1000 * statistics.pstdev(lat), 2) if len(lat) > 1 else None,
                "jitter_rfc3550_ms": round(1000 * statistics.mean(deltas), 2) if deltas else None,
                "client_attempts": drv.attempts, "client_failures": drv.failures,
                "request_failure_rate_pct": round(100.0 * drv.failures / max(1, drv.attempts), 2),
                "ingest_availability_pct": probe.availability(), "ingest_outages": len(probe.fail_runs),
                "kills": len(kills), "mttr_resume_s": round(statistics.mean(mttrs), 2) if mttrs else None, "mttr_samples": len(mttrs),
                "cpu_pct_mean": rs.summary()["svc_cpu_pct_mean"], "cpu_pct_max": rs.summary()["svc_cpu_pct_max"], "rss_mb_mean": rs.summary()["svc_rss_mb_mean"],
                "host_cpu_pct_mean": rs.summary()["host_cpu_pct_mean"],
                "wire_mb": round(wire / 1e6, 2), "raw_mb": round(wire_raw / 1e6, 2), "compression_ratio": round(wire_raw / wire, 2) if wire else None,
                "wire_mbps_mean": round(wire * 8 / 1e6 / complete_s, 2) if complete_s else None,
                "trial_wall_s": round(time.time() - t_wall, 1),
            }
            return out
        except Exception as e:                                           # a trial that cannot even start (e.g. memory cap too small) is a result
            return {"system": system, "transport": transport, "rows": len(rows), "users": users, "rate_rows_s": rate or "unlimited", "bandwidth_mbps": bw or "unlimited",
                    "nodes": nodes if system != "baseline-flat" else 1, "cpus": len(cpus) if cpus else n_cpu(), "cpu_contended": hog, "mem_cap_mb": mem_mb or "none",
                    "cross_traffic_threads": noise, "failures_per_min": fail_per_min, "stored_distinct": 0, "rows_lost": len(rows),
                    "error": f"{type(e).__name__}: {str(e)[:120]}", "trial_wall_s": round(time.time() - t_wall, 1)}
        finally:
            if hog_proc:
                hog_proc.kill()
            shutil.rmtree(c.workdir, ignore_errors=True)


# ----------------------------------------------------------------------------------------------- sweeps
def plan():
    allrows = 120000
    sw = {}
    sw["bandwidth"] = ("bandwidth_mbps", [dict(bw=b, rows=allrows) for b in (0, 100, 25, 10, 5)], SYSTEMS, 3, "bw")
    sw["users"] = ("users", [dict(users=u) for u in (1, 2, 4, 8, 16)], SYSTEMS, 2, "users")
    sw["nodes"] = ("nodes", [dict(nodes=n) for n in (1, 2, 3, 5)], SYSTEMS[1:], 2, "nodes")
    sw["cpu"] = ("cpu", [dict(cpus=[0, 1]), dict(cpus=[0]), dict(cpus=[0], hog=True)], SYSTEMS, 2, "cpu")
    sw["memory"] = ("memory", [dict(mem_mb=m) for m in (None, 256, 160, 128)], SYSTEMS, 2, "mem_mb")
    sw["net_load"] = ("net_load", [dict(noise=n) for n in (0, 2, 8, 16)], SYSTEMS, 2, "noise")
    sw["failure_rate"] = ("failure_rate", [dict(fail_per_min=f, rate=4000) for f in (0, 6, 12, 20)], SYSTEMS, 3, "fail_per_min")
    sw["request_rate"] = ("request_rate", [dict(rate=r) for r in (2000, 5000, 10000, 20000, 0)], SYSTEMS, 2, "rate")
    sw["data_volume"] = ("data_volume", [dict(rows=n) for n in (10000, 30000, 60000, 120000)], SYSTEMS, 2, "rows")
    sw["transport"] = ("transport", [dict(transport=t, fail_per_min=12, rate=4000) for t in ("kafka", "http")], SYSTEMS[1:2], 3, "transport")
    return sw


def append_raw(row):
    new = not os.path.exists(RAW)
    fields = None
    rows = []
    if not new:
        with open(RAW, newline="") as f:
            rd = csv.DictReader(f)
            fields = rd.fieldnames
            rows = list(rd)
    allf = list(fields or [])
    for k in row:
        if k not in allf:
            allf.append(k)
    rows.append(row)
    with open(RAW, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=allf, restval="")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def done_keys():
    if not os.path.exists(RAW):
        return set()
    with open(RAW, newline="") as f:
        return {(r["sweep"], r["level"], r["system"] if r["sweep"] != "transport" else r["transport"], r["rep"]) for r in csv.DictReader(f)}


def run_sweeps(names):
    """Resumable: a (sweep, level, system, rep) already present in the raw CSV is skipped, so an interrupted run can simply be restarted."""
    data = dataset()
    sw = plan()
    k = 0
    finished = done_keys()
    for name in names:
        label, levels, systems, reps, key = sw[name]
        for lv in levels:
            for system in systems:
                for rep in range(1, reps + 1):
                    lvl_label = str(lv["bw"] or "unlimited") if name == "bandwidth" else (
                        ("2 CPUs" if lv.get("cpus") == [0, 1] else ("1 CPU" if not lv.get("hog") else "1 CPU shared with busy-loop")) if name == "cpu" else (
                            str(lv["mem_mb"] or "unlimited") if name == "memory" else (str(lv.get(key, "")) if key in lv else str(lv))))
                    tag = lv.get("transport") if name == "transport" else system
                    k += 1
                    if (name, lvl_label, tag, str(rep)) in finished:
                        continue
                    kw = dict(CENTRE)
                    kw.update(lv)
                    n_rows = kw.pop("rows")
                    rows = data[:n_rows] if n_rows < len(data) else data
                    est_s = (n_rows * 370 * 8 / (kw["bw"] * 1e6) * 1.6 + 40) if kw["bw"] else 90          # generous drain allowance; a stuck trial is a result, not a hang
                    res = trial(system, rows, base=20000 + 100 * (k % 20), drain_timeout=est_s, **kw)
                    res.update({"sweep": name, "rep": rep, "level": str(lv.get(key, "")) if key in lv else str(lv)})
                    if name == "bandwidth":
                        res["level"] = str(lv["bw"] or "unlimited")
                    if name == "cpu":
                        res["level"] = "2 CPUs" if lv.get("cpus") == [0, 1] else ("1 CPU" if not lv.get("hog") else "1 CPU shared with busy-loop")
                    if name == "memory":
                        res["level"] = str(lv["mem_mb"] or "unlimited")
                    append_raw(res)
                    print(f"  {name:13} {str(res['level']):28} {system:18} rep{rep}: "
                          + (f"ERROR {res['error']}" if "error" in res else
                             f"complete {res['complete_s']:>6}s  e2e {res['e2e_rows_s']:>7} rows/s  ingest-lat p95 {res['req_lat_p95_ms']} ms  "
                             f"wire {res['wire_mb']} MB  cpu {res['cpu_pct_mean']}%  lost {res['rows_lost']}  avail {res['ingest_availability_pct']}%  mttr {res['mttr_resume_s']}"), flush=True)


if __name__ == "__main__":
    args = sys.argv[1:] or list(plan())
    run_sweeps(args)
