"""
Milestone 7 -- Fault Tolerance and Recovery.

Fault-injection framework: system/common/faults.py (installed in every service) + process-level crash/pause from the
cluster harness. Seven fault types are injected into replica r1 for 5 s of a 12 s run while a client issues
write-then-read probes every 50 ms against the replicated store:

   crash          process killed (SIGKILL) then restarted           (crash failure / node failure)
   node-hang      process frozen (SIGSTOP) then resumed             (node failure: alive but unresponsive)
   omission       50 % of requests accepted but never answered      (omission failure)
   communication  r1 unreachable from the client (partition)        (communication failure)
   service-down  process alive, every request answered 503         (service failure)
   app-error      50 % of requests answered 500                     (application failure)
   byzantine      r1 answers with plausible but WRONG values        (Byzantine behaviour)

Three configurations are compared:
   single             1 replica,           no redundancy                  (baseline)
   replicated         3 replicas, W=2 R=2  quorum + failover + hints + read repair
   replicated+vote    3 replicas, W=2 R=3  majority vote on reads (Byzantine-tolerant for f=1 on reads)
   replicated+vote N=4  4 replicas, W=3 R=3  vote over 3 of 4: masks one Byzantine replica AND one crashed replica
                        (the textbook N >= 3f+1 requirement when crash and Byzantine faults must both be tolerated)

Measured:  Availability = T_uptime / T_total  (a probe window counts as 'up' when the write AND the read complete),
           correctness = completed reads returning the value just written,
           MTTR = mean duration of an outage episode (consecutive failed probes), detection time, failure rate,
           component recovery = time for the repaired replica to be fully caught up (anti-entropy).

Hypotheses
  H1 single instance availability drops to ~58 % (7/12) under crash/hang/down; replicated stays >= 99 %.
  H2 failover (MTTR) is bounded by the detection time (a few probe timeouts), not by the repair time.
  H3 only voting protects against Byzantine replies: single and plain quorum return wrong data.

Run:  python -m system.experiments.m07_fault_tolerance
"""
import os
import statistics
import threading
import time

import requests

from system.experiments.common import RESULTS, save_csv, save_json
from system.replication.client import ReplicatedStore, Unavailable
from system.scripts.cluster import Cluster

BASE, DURATION, T_FAULT, T_REPAIR, INTERVAL = 28000, 12.0, 3.0, 8.0, 0.05
FAULTS = ["crash", "node-hang", "omission", "communication", "service-down", "app-error", "byzantine"]
CONFIGS = {"single": dict(n=1, w=1, r=1, vote=False), "replicated": dict(n=3, w=2, r=2, vote=False), "replicated+vote": dict(n=3, w=2, r=3, vote=True),
           "replicated+vote N=4": dict(n=4, w=3, r=3, vote=True)}


def start_replicas(c, n):
    urls = {f"r{i}": f"http://127.0.0.1:{BASE + i}" for i in range(1, n + 1)}
    peers = ",".join(f"{k}={v}" for k, v in urls.items())
    for rid, u in urls.items():
        c.start(rid, "system.replication.replica", int(u.rsplit(":", 1)[1]), env={"REPLICA_ID": rid, "DATA_DIR": f"{c.workdir}/db", "PEERS": peers, "ANTI_ENTROPY_S": 0.5})
    return urls


def inject(c, urls, kind):
    u = urls["r1"]
    if kind == "crash":
        c.kill("r1")
    elif kind == "node-hang":
        c.pause("r1")
    elif kind == "omission":
        requests.post(u + "/fault", json={"omit_prob": 0.5, "hang_s": 5})
    elif kind == "communication":
        requests.post(u + "/fault", json={"blocked_from": ["client"], "hang_s": 5})
    elif kind == "service-down":
        requests.post(u + "/fault", json={"down": True})
    elif kind == "app-error":
        requests.post(u + "/fault", json={"error_prob": 0.5})
    elif kind == "byzantine":
        requests.post(u + "/fault", json={"byzantine": True})


def repair(c, urls, kind):
    if kind == "crash":
        c.restart("r1")
    elif kind == "node-hang":
        c.resume("r1")
    else:
        requests.delete(urls["r1"] + "/fault")


def run_one(kind, cfg_name, rep):
    cfg = CONFIGS[cfg_name]
    with Cluster() as c:
        urls = start_replicas(c, cfg["n"])
        store = ReplicatedStore(urls, w=cfg["w"], r=cfg["r"], vote=cfg["vote"], timeout=0.6, name="client")
        for i in range(5):
            store.put("probe", -1)
        probes, stop, marks = [], threading.Event(), {}

        def prober():
            i = 0
            t_start = time.perf_counter()
            while not stop.is_set():
                t0 = time.perf_counter()
                done, correct = False, False
                try:
                    store.put("probe", i)
                    v = store.get("probe")
                    done, correct = True, (v == i)
                except Unavailable:
                    pass
                except Exception:
                    pass
                probes.append({"t": t0 - t_start, "dur": time.perf_counter() - t0, "done": done, "correct": correct})
                i += 1
                time.sleep(max(0, INTERVAL - (time.perf_counter() - t0)))
        th = threading.Thread(target=prober)
        th.start()
        time.sleep(T_FAULT)
        t_inj = time.time()
        inject(c, urls, kind)
        time.sleep(T_REPAIR - T_FAULT)
        t_rep = time.time()
        repair(c, urls, kind)
        catch_up = None
        if cfg["n"] > 1 and kind in ("crash", "node-hang", "communication", "omission", "service-down", "app-error"):
            t0 = time.perf_counter()
            while time.perf_counter() - t0 < 6:
                try:
                    d1, d2 = requests.get(urls["r1"] + "/digest", timeout=0.5).json(), requests.get(urls["r2"] + "/digest", timeout=0.5).json()
                    if d1 == d2 and d1:
                        catch_up = time.perf_counter() - t0
                        break
                except Exception:
                    pass
                time.sleep(0.05)
        time.sleep(max(0, DURATION - (time.time() - t_inj) - T_FAULT))
        stop.set()
        th.join()
        # ---- metrics
        n = len(probes)
        up = sum(p["done"] for p in probes)
        # outage episodes = maximal runs of failed probes; duration = from start of first failed probe to end of last
        episodes, cur = [], None
        for p in probes:
            if not p["done"]:
                cur = [p["t"], p["t"] + p["dur"]] if cur is None else [cur[0], p["t"] + p["dur"]]
            elif cur is not None:
                episodes.append(cur[1] - cur[0]); cur = None
        if cur is not None:
            episodes.append(cur[1] - cur[0])
        down_s = sum(episodes)
        wrong = sum(1 for p in probes if p["done"] and not p["correct"])
        det = store.detection_time("r1", t_inj) if cfg["n"] > 1 else None
        return {"fault": kind, "config": cfg_name, "rep": rep, "probes": n, "availability_pct": round(100 * up / n, 2), "downtime_s": round(down_s, 2),
                "outage_episodes": len(episodes), "mttr_s": round(statistics.mean(episodes), 2) if episodes else 0.0,
                "failure_rate_pct": round(100 * (n - up) / n, 2), "wrong_answers": wrong, "correctness_pct": round(100 * (up - wrong) / up, 2) if up else 0,
                "detection_s": round(det, 2) if det is not None else None, "component_catch_up_s": round(catch_up, 2) if catch_up is not None else None,
                "hints_replayed": store.counters["hints_replayed"], "read_repairs": store.counters["read_repairs"], "suspicions": store.counters["suspicions"]}


def main(reps=2, only=None):
    """only: run just these configs and merge them into the saved raw results (used to add a config without re-running all)."""
    rows = []
    if only:
        import csv
        with open(os.path.join(RESULTS, "m07_fault_injection_raw.csv"), newline="") as f:
            for r in csv.DictReader(f):
                if r["config"] not in only:
                    rows.append({k: (float(v) if v.replace(".", "", 1).replace("-", "", 1).isdigit() else (None if v in ("", "None") else v)) for k, v in r.items()})
    for kind in FAULTS:
        for cfg in (only or CONFIGS):
            for rep in range(1, reps + 1):
                r = run_one(kind, cfg, rep)
                rows.append(r)
                print(f"  {kind:14} {cfg:16} rep{rep}: availability {r['availability_pct']:>6}%  downtime {r['downtime_s']:>5}s  MTTR {r['mttr_s']:>5}s  "
                      f"correct {r['correctness_pct']:>6}%  detect {r['detection_s']}  catch-up {r['component_catch_up_s']}")
    save_csv("m07_fault_injection_raw.csv", rows)
    summ = []
    for kind in FAULTS:
        for cfg in CONFIGS:
            g = [r for r in rows if r["fault"] == kind and r["config"] == cfg]
            m = lambda k: round(statistics.mean(x[k] for x in g if x[k] is not None), 2) if any(x[k] is not None for x in g) else None
            summ.append({"fault": kind, "config": cfg, "availability_pct": m("availability_pct"), "downtime_s": m("downtime_s"), "mttr_s": m("mttr_s"),
                         "failure_rate_pct": m("failure_rate_pct"), "correctness_pct": m("correctness_pct"), "detection_s": m("detection_s"), "component_catch_up_s": m("component_catch_up_s")})
    save_csv("m07_failure_comparison.csv", summ)
    save_json("m07_summary.json", summ)


if __name__ == "__main__":
    import sys
    main(only=sys.argv[1:] or None)
