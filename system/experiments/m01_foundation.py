"""
Milestone 1 -- Distributed OS foundation:  Nodes + Processes + Resources + Communication.

The model (see docs/milestones/01-distributed-os-foundation.md):
  Node        = an OS process with its own address space, port and resource envelope (a container in docker-compose).
  Process     = a unit of work a node runs (here: Python worker processes created through the cluster harness / process manager).
  Resource    = CPU time, memory, network (measured by psutil in every agent; ENFORCED here with CPU affinity and RLIMIT_AS).
  Communication = request/response over HTTP between nodes (the Week-1 agents push to the collector).

Parts
  A  The Week-1 prototype: 3 agent nodes (edge-a, edge-b, core) + 1 collector, started as real processes with the unmodified agent.py /
     collector.py. Check that every node is known to the collector and report each node's resources.
  B  Resource allocation: the same CPU-bound job on 1, 2, 4 worker processes pinned to ONE CPU vs allowed BOTH CPUs of this machine.
  C  Memory allocation: a node with a 96 MB address-space limit is asked to hold 32 MB, 64 MB, 128 MB, 256 MB: where does the OS say no?
  D  Inter-node communication cost: round-trip time of an empty request, and of 1 KB / 64 KB / 1 MB payloads between two nodes.

Hypotheses
  H1 All four nodes register with the collector and report non-zero CPU / memory within 3 report intervals.
  H2 CPU-bound throughput saturates at (number of CPUs the node set may use): 4 workers on 1 CPU give the same total throughput as 1 worker.
  H3 allocation beyond the limit fails with MemoryError in the offending node only; other nodes are unaffected.
  H4 round-trip time grows linearly with payload size above ~64 KB (bandwidth-bound) and is flat below it (latency-bound).
"""
import hashlib
import os
import statistics
import subprocess
import sys
import time

import requests

from system.experiments.common import REPO, save_csv, save_json
from system.scripts.cluster import Cluster

BASE = 32000


def part_a():
    rows = []
    with Cluster() as c:
        env0 = {"PYTHONPATH": REPO}
        pcol = subprocess.Popen([sys.executable, os.path.join(REPO, "collector.py")], env={**os.environ, **env0},
                                cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        agents = []
        try:
            time.sleep(2.0)
            for nid, role in (("edge-node-a", "edge"), ("edge-node-b", "edge"), ("core-node", "core")):
                agents.append(subprocess.Popen([sys.executable, os.path.join(REPO, "agent.py")], cwd=REPO,
                                               env={**os.environ, **env0, "NODE_ID": nid, "NODE_ROLE": role, "REPORT_INTERVAL": "1",
                                                    "COLLECTOR_URL": "http://127.0.0.1:5000/metrics"},
                                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            t0 = time.time()
            known = {}
            while time.time() - t0 < 15:
                try:
                    st = requests.get("http://127.0.0.1:5000/status", timeout=2).json()
                    known = st.get("latest", {})
                    if len(known) == 3 and all(v.get("cpu_percent") is not None for v in known.values()):
                        break
                except requests.RequestException:
                    pass
                time.sleep(0.5)
            register_s = round(time.time() - t0, 2)
            for nid, v in sorted(known.items()):
                rows.append({"node": nid, "role": v.get("node_role"), "cpu_percent": v.get("cpu_percent"), "mem_percent": v.get("mem_percent"),
                             "registered_after_s": register_s})
            print(f"  A {len(known)}/3 nodes registered after {register_s}s: {sorted(known)}")
            for r in rows:
                print(f"    {r['node']:12} role={r['role']:5} cpu={r['cpu_percent']}%  mem={r['mem_percent']}%")
        finally:
            for p in agents + [pcol]:
                p.kill()
    save_csv("m01_prototype_nodes.csv", rows)
    return rows


CPU_JOB = "import os,sys,time,hashlib\nos.sched_setaffinity(0,{%s})\nt=time.time();n=0;b=b'x'*4096\nwhile time.time()-t<%s:\n    hashlib.sha256(b).digest();n+=1\nprint(n)"


def part_b(dur=2.5, reps=3):
    rows = []
    ncpu = len(os.sched_getaffinity(0))
    for cpus_label, cpus in (("1 CPU", [0]), (f"{ncpu} CPUs", list(range(ncpu)))):
        for workers in (1, 2, 4):
            tot = []
            for _ in range(reps):
                procs = [subprocess.Popen([sys.executable, "-c", CPU_JOB % (",".join(map(str, cpus)), dur)], stdout=subprocess.PIPE) for _ in range(workers)]
                tot.append(sum(int(p.communicate()[0]) for p in procs) / dur)
            rows.append({"cpus_allowed": cpus_label, "worker_processes": workers, "hashes_per_s_total": round(statistics.mean(tot)), "sd": round(statistics.pstdev(tot)),
                         "per_worker": round(statistics.mean(tot) / workers)})
            print(f"  B {cpus_label:7} x {workers} workers: {rows[-1]['hashes_per_s_total']:>9} hashes/s total  ({rows[-1]['per_worker']} per worker)")
    save_csv("m01_cpu_allocation.csv", rows)
    return rows


MEM_JOB = ("import resource,sys\nresource.setrlimit(resource.RLIMIT_AS,(96<<20,96<<20))\n"
           "try:\n    b=bytearray(%d<<20)\n    print('ok')\nexcept MemoryError:\n    print('MemoryError')")


def part_c():
    rows = []
    for mb in (32, 64, 128, 256):
        out = subprocess.run([sys.executable, "-c", MEM_JOB % mb], capture_output=True, text=True).stdout.strip()
        rows.append({"limit_mb": 96, "request_mb": mb, "result": out})
        print(f"  C limit 96 MB, request {mb:>3} MB -> {out}")
    save_csv("m01_memory_allocation.csv", rows)
    return rows


def part_d(n=300):
    rows = []
    with Cluster() as c:
        c.start("echo", "system.rpc.rest_server", BASE + 50, env={"SEMANTICS": "at-most-once"})
        s = requests.Session()
        for size in (0, 1024, 65536, 1 << 20):
            body = ('{"readings": ["' + "x" * max(0, size - 20) + '"]}').encode()      # collector counts the readings list; size ~ payload
            lat = []
            for _ in range(n if size < (1 << 20) else n // 5):
                t0 = time.perf_counter()
                s.post(c.url("echo") + "/rest/readings", data=body, headers={"Content-Type": "application/json"}, timeout=10)
                lat.append((time.perf_counter() - t0) * 1000)
            lat.sort()
            rows.append({"payload_bytes": len(body), "mean_ms": round(statistics.mean(lat), 3), "p95_ms": round(lat[int(.95 * len(lat))], 3), "jitter_ms": round(statistics.pstdev(lat), 3),
                         "mb_per_s": round(size / 1e6 / (statistics.mean(lat) / 1000), 1) if size else None})
            print(f"  D payload {size:>8} B: mean {rows[-1]['mean_ms']} ms  p95 {rows[-1]['p95_ms']} ms  jitter {rows[-1]['jitter_ms']} ms")
    save_csv("m01_comm_cost.csv", rows)
    return rows


def main():
    print("A prototype nodes"); a = part_a()
    print("B cpu allocation"); b = part_b()
    print("C memory allocation"); m = part_c()
    print("D communication cost"); d = part_d()
    save_json("m01_summary.json", {"A": a, "B": b, "C": m, "D": d})


if __name__ == "__main__":
    main()
