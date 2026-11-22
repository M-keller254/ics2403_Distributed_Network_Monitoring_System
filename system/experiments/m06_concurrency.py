"""
Milestone 6 -- Concurrency and Deadlock Management.

A  Race condition: 8 clients x 100 increments of a shared counter with / without mutual exclusion.
B  Deadlock: 4 services need two resources each in a circle (P_i holds R_i, wants R_{i+1}). Policies:
   none (no handling), detect+recover (wait-for-graph cycle, abort youngest), ordered (prevention), wait-die (prevention).
C  Starvation: a request for the whole pool (4 units) against a stream of 1-unit requests; barging vs FIFO-fair queue.
D  Resource utilisation and contention: 4-unit pool, 1..16 clients: throughput, mean wait, utilisation vs the analytic bound.

Hypotheses
  H1 unprotected read-modify-write loses updates (> 50 % with 8 clients); the mutex gives exactly 800.
  H2 with policy 'none' the 4-cycle deadlocks forever; detection resolves it within ~detect_interval + victim restart;
     prevention never forms a cycle (at the cost of restarts for wait-die).
  H3 barging starves the whole-pool request; FIFO serves it within about one hold time.
  H4 throughput rises linearly to capacity/hold (200/s) and then flattens; waiting time grows past saturation.

Run:  python -m system.experiments.m06_concurrency
"""
import os
import statistics
import subprocess
import threading
import time

import requests

from system.experiments.common import RESULTS, save_csv, save_json
from system.scripts.cluster import Cluster

PORT = 27000


def start_lm(c, resources, policy="none", fair=True, detect_interval=0.2):
    c.start("lm", "system.concurrency.service", PORT, env={"RESOURCES": resources, "POLICY": policy, "FAIR": str(fair).lower(), "DETECT_INTERVAL": detect_interval})
    return c.url("lm")


def part_a():
    rows = []
    for mode in ("unsafe", "safe"):
        with Cluster() as c:
            url = start_lm(c, "counter:1")

            def worker(i):
                s = requests.Session()
                for _ in range(100):
                    s.post(f"{url}/counter/{mode}", json={"pid": f"w{i}"}, timeout=30)
            t0 = time.perf_counter()
            ths = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
            [t.start() for t in ths]
            [t.join() for t in ths]
            wall = time.perf_counter() - t0
            v = requests.get(url + "/counter").json()["v"]
            rows.append({"mode": "no mutual exclusion" if mode == "unsafe" else "mutex via lock manager", "increments_issued": 800, "final_value": v,
                         "lost_updates": 800 - v, "lost_pct": round(100 * (800 - v) / 800, 1), "wall_s": round(wall, 2)})
            print("  A", rows[-1])
    save_csv("m06_race_condition.csv", rows)
    return rows


def run_ring(policy, n=4, jobs=5, hold=0.05, window=8.0, timeout=2.5, render=False, label=None):
    res = ",".join(f"R{i}:1" for i in range(n))
    with Cluster() as c:
        url = start_lm(c, res, policy)
        done, restarts, stuck, lock = [0], [0], [0], threading.Lock()
        t_start = time.perf_counter()
        finish = []

        def worker(i):
            pid, s = f"P{i}", requests.Session()
            first, second = f"R{i}", f"R{(i + 1) % n}"
            order = sorted([first, second]) if policy == "ordered" else [first, second]
            for j in range(jobs):
                ts = time.time()                                  # fixed for all restarts of this job (wait-die)
                while time.perf_counter() - t_start < window:
                    s.post(url + "/begin", json={"pid": pid, "ts": ts})
                    ok = True
                    for k, r in enumerate(order):
                        resp = s.post(url + "/acquire", json={"pid": pid, "res": r, "timeout": timeout}, timeout=timeout + 2)
                        if resp.status_code != 200:
                            ok = False
                            with lock:
                                if resp.status_code == 408:
                                    stuck[0] += 1
                                else:
                                    restarts[0] += 1
                            break
                        if k == 0:
                            time.sleep(0.02)                      # work while holding the first resource: widens the deadlock window
                    if ok:
                        time.sleep(hold)
                    s.post(url + "/release_all", json={"pid": pid})
                    if ok:
                        with lock:
                            done[0] += 1
                        break
                    time.sleep(0.01 * (i + 1))                    # small backoff before retrying
                else:
                    return
            finish.append(time.perf_counter() - t_start)
        ths = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        [t.start() for t in ths]
        if render:
            time.sleep(0.8)
            g = requests.get(url + "/graph").json()
            tag = label or policy
            tag = "".join(ch if ch.isalnum() else "_" for ch in tag).strip("_")
            open(os.path.join(RESULTS, f"m06_rag_{tag}.dot"), "w").write(g["dot"])
            try:
                subprocess.run(["dot", "-Tpng", os.path.join(RESULTS, f"m06_rag_{tag}.dot"), "-o", os.path.join(RESULTS, f"m06_rag_{tag}.png")], check=True, timeout=20)
            except Exception:
                pass
            if g["wait_for_edges"]:
                open(os.path.join(RESULTS, f"m06_wait_for_{tag}.txt"), "w").write("\n".join(f"{a} -> {b}" for a, b in g["wait_for_edges"]))
        [t.join() for t in ths]
        wall = time.perf_counter() - t_start
        st = requests.get(url + "/stats").json()
        total = n * jobs
        return {"policy": label or policy, "jobs_total": total, "jobs_completed": done[0], "completed_pct": round(100 * done[0] / total, 1), "wall_s": round(wall, 2),
                "jobs_per_s": round(done[0] / wall, 2), "deadlocks_detected": st["deadlocks_detected"], "victims_or_dies": st["victims_aborted"] + st["wait_die_aborts"],
                "client_timeouts": stuck[0], "restarts": restarts[0],
                "detect_latency_s": round(statistics.mean(st["detect_latency_s"]), 3) if st["detect_latency_s"] else None,
                "utilization": st["utilization"], "stuck": done[0] < total}


def part_b():
    rows = []
    # 'none' = no deadlock handling and clients wait (effectively) forever inside the 8 s observation window;
    # 'timeout-only' = the crude recovery every RPC system gets for free: give up after 2.5 s, release everything, retry.
    for label, policy, tmo in (("none (wait forever)", "none", 30.0), ("timeout-only (2.5 s)", "none", 2.5), ("detect+recover", "detect", 30.0),
                               ("ordered (prevention)", "ordered", 30.0), ("wait-die (prevention)", "wait-die", 30.0)):
        for rep in range(3):
            r = run_ring(policy, timeout=tmo, render=(rep == 0 and label != "timeout-only (2.5 s)"), label=label)
            r["rep"] = rep + 1
            rows.append(r)
            print(f"  B {label:24} rep{rep + 1}: completed {r['jobs_completed']}/{r['jobs_total']} in {r['wall_s']}s  deadlocks {r['deadlocks_detected']}  victims/dies {r['victims_or_dies']}  timeouts {r['client_timeouts']}")
    save_csv("m06_deadlock.csv", rows)
    return rows


def part_c():
    rows = []
    for fair in (False, True):
        for rep in range(3):
            with Cluster() as c:
                url = start_lm(c, "pool:4", "none", fair)
                stop = threading.Event()

                def small(i):
                    s = requests.Session()
                    while not stop.is_set():
                        r = s.post(url + "/acquire", json={"pid": f"s{i}", "res": "pool", "units": 1, "timeout": 5})
                        if r.status_code == 200:
                            time.sleep(0.05)
                            s.post(url + "/release", json={"pid": f"s{i}", "res": "pool", "units": 1})
                ths = [threading.Thread(target=small, args=(i,)) for i in range(6)]
                [t.start() for t in ths]
                time.sleep(0.5)
                t0 = time.perf_counter()
                r = requests.post(url + "/acquire", json={"pid": "BIG", "res": "pool", "units": 4, "timeout": 6}, timeout=10)
                waited = time.perf_counter() - t0
                stop.set()
                [t.join() for t in ths]
                rows.append({"queue": "FIFO (fair)" if fair else "barging (unfair)", "rep": rep + 1, "big_request_granted": r.status_code == 200, "big_wait_s": round(waited, 2),
                             "starved": r.status_code != 200})
                print("  C", rows[-1])
    save_csv("m06_starvation.csv", rows)
    return rows


def part_d():
    rows = []
    hold, think, cap = 0.02, 0.005, 4
    for clients in (1, 2, 4, 8, 16):
        with Cluster() as c:
            url = start_lm(c, f"pool:{cap}", "none", True)
            waits, count, lock = [], [0], threading.Lock()
            stop = threading.Event()

            def worker(i):
                s = requests.Session()
                while not stop.is_set():
                    t = time.perf_counter()
                    r = s.post(url + "/acquire", json={"pid": f"w{i}", "res": "pool", "timeout": 10})
                    w = time.perf_counter() - t
                    time.sleep(hold)
                    s.post(url + "/release", json={"pid": f"w{i}", "res": "pool"})
                    with lock:
                        waits.append(w)
                        count[0] += 1
                    time.sleep(think)
            t0 = time.perf_counter()
            ths = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
            [t.start() for t in ths]
            time.sleep(4)
            stop.set()
            [t.join() for t in ths]
            wall = time.perf_counter() - t0
            st = requests.get(url + "/stats").json()
            rows.append({"clients": clients, "capacity_units": cap, "hold_ms": hold * 1000, "throughput_per_s": round(count[0] / wall, 1),
                         "bound_cap_over_hold": round(cap / hold, 1), "mean_wait_ms": round(1000 * statistics.mean(waits), 2),
                         "p95_wait_ms": round(1000 * sorted(waits)[int(0.95 * len(waits))], 2), "utilization": st["utilization"]})
            print("  D", rows[-1])
    save_csv("m06_utilization.csv", rows)
    return rows


def main(parts="abcd"):
    import json, os
    path = os.path.join(RESULTS, "m06_summary.json")
    summary = json.load(open(path)) if os.path.exists(path) else {}
    if "a" in parts:
        print("A race"); summary["race"] = part_a()
    if "b" in parts:
        print("B deadlock"); summary["deadlock"] = part_b()
    if "c" in parts:
        print("C starvation"); summary["starvation"] = part_c()
    if "d" in parts:
        print("D utilisation"); summary["utilization"] = part_d()
    save_json("m06_summary.json", summary)


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else "abcd")
