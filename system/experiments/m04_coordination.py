"""
Milestone 4 -- Distributed Algorithms and Coordination.

A  Bully election: message complexity, election time and consistency vs cluster size n = 2..6
   (startup election and re-election after the leader is crashed), plus steady-state heartbeat overhead.
B  Synchronization delay vs coordination overhead: failure-detection time against heartbeat interval.
C  Logical vs physical clocks: agents with skewed wall clocks; count causal-order violations seen by the core.
D  Coordination under load: the leader crashes while 120,000 rows stream edge -> bus -> core;
   rows must neither be lost nor double counted (leader-only consumption + committed offsets + idempotent writes).

Hypotheses
  H1 Bully sends O(n^2) messages in the worst case, between n-1 and n^2 in practice.
  H2 Failover time ~ heartbeat_interval x miss_limit + election time; shorter intervals trade delay for overhead.
  H3 Wall clocks with skew violate causal order; Lamport clocks never do.
  H4 Killing the leader mid-stream loses zero rows; the replayed tail is absorbed (duplicates bounded by one poll batch).

Run:  python -m system.experiments.m04_coordination
"""
import glob
import sqlite3
import statistics
import sys
import threading
import time

import requests

from system.common.metrics import mean_ci95
from system.edge.replayer import run_replay
from system.experiments.common import dataset, save_csv, save_json
from system.scripts.cluster import Cluster
from system.scripts.stack import current_leader, edge_core_cloud, wait_for

REPS = 3


def peers_for(base, n):
    return ",".join(f"{i}=http://127.0.0.1:{base + i}" for i in range(1, n + 1))


def start_cores(c, base, n, hb=1.0, miss=2):
    for i in range(1, n + 1):
        c.start(f"core{i}", "system.core.service", base + i, wait=False, health="/health", env={
            "NODE_ID": i, "PEERS": peers_for(base, n), "TRANSPORT": "http", "DATA_DIR": f"{c.workdir}/cd",
            "STARTUP_DELAY": 1.5, "HEARTBEAT_INTERVAL": hb, "HEARTBEAT_MISS_LIMIT": miss, "DIGEST_INTERVAL": 999})
    for i in range(1, n + 1):
        c.wait_healthy(f"core{i}")


def leaders_view(c, names):
    out = {}
    for n in names:
        if c.alive(n):
            try:
                out[n] = c.get(n, "/leader", timeout=0.5)
            except Exception:
                pass
    return out


def wait_agree(c, names, want_id, timeout=40):
    """Poll every 25 ms until all live nodes name `want_id` as leader. Returns (seconds, split_brain_samples)."""
    t0, split, end = time.perf_counter(), 0, time.time() + timeout
    while time.time() < end:
        v = leaders_view(c, names)
        if sum(1 for x in v.values() if x["is_leader"]) > 1:
            split += 1
        if v and all(x["leader_id"] == want_id for x in v.values()):
            return time.perf_counter() - t0, split
        time.sleep(0.025)
    raise TimeoutError("no agreement")


def msgs(c, names):
    tot = {"election": 0, "ok": 0, "coordinator": 0, "heartbeat": 0}
    for n in names:
        if c.alive(n):
            s = c.get(n, "/election_stats")["sent"]
            for k in tot:
                tot[k] += s.get(k, 0)
    return tot


def part_a_b():
    rows = []
    configs = [(n, 1.0) for n in range(2, 7)] + [(4, 0.25), (4, 0.5), (4, 2.0)]
    for n, hb in configs:
        for rep in range(1, REPS + 1):
            with Cluster() as c:
                names = [f"core{i}" for i in range(1, n + 1)]
                t0 = time.perf_counter()
                start_cores(c, 24000, n, hb)
                t_start, split0 = wait_agree(c, names, n)
                startup_msgs = msgs(c, names)
                # steady-state heartbeat overhead over 6 s
                h0 = msgs(c, names)["heartbeat"]
                time.sleep(6)
                hb_rate = (msgs(c, names)["heartbeat"] - h0) / 6.0
                before = msgs(c, names[:-1])                 # survivors only (the dying leader's counters vanish with it)
                c.kill(f"core{n}")                           # crash the leader
                t_kill = time.perf_counter()
                alive = [x for x in names if c.alive(x)]
                t_fail, split1 = wait_agree(c, alive, n - 1, timeout=60)
                time.sleep(1.0)
                after = msgs(c, alive)
                re_msgs = {k: after[k] - before[k] for k in ("election", "ok", "coordinator")}
                row = {"n": n, "heartbeat_interval_s": hb, "rep": rep,
                       "startup_agree_s": round(t_start + 1.5, 2), "startup_election_msgs": sum(startup_msgs[k] for k in ("election", "ok", "coordinator")),
                       "heartbeat_msgs_per_s": round(hb_rate, 2),
                       "failover_s": round(t_fail, 2), "reelection_msgs": sum(re_msgs.values()),
                       "reelection_election": re_msgs["election"], "reelection_ok": re_msgs["ok"], "reelection_coordinator": re_msgs["coordinator"],
                       "split_brain_samples": split0 + split1}
                rows.append(row)
                print(f"  n={n} hb={hb}s rep{rep}: startup {row['startup_election_msgs']} msgs | failover {row['failover_s']}s, "
                      f"{row['reelection_msgs']} msgs (E{re_msgs['election']}/OK{re_msgs['ok']}/C{re_msgs['coordinator']}) | hb {row['heartbeat_msgs_per_s']}/s | split-brain samples {row['split_brain_samples']}")
    save_csv("m04_election_raw.csv", rows)
    summ = []
    for n, hb in configs:
        g = [r for r in rows if r["n"] == n and r["heartbeat_interval_s"] == hb]
        f = lambda k: round(statistics.mean(r[k] for r in g), 2)
        summ.append({"n": n, "heartbeat_interval_s": hb, "startup_msgs": f("startup_election_msgs"), "reelection_msgs": f("reelection_msgs"),
                     "failover_s": f("failover_s"), "failover_sd": round(statistics.stdev(r["failover_s"] for r in g), 2),
                     "heartbeat_msgs_per_s": f("heartbeat_msgs_per_s"), "split_brain_samples": sum(r["split_brain_samples"] for r in g),
                     "bound_n_minus_1": n - 1, "bound_n_squared": n * n})
    save_csv("m04_election_summary.csv", summ)
    return summ


def part_c():
    out = []
    for label, skews in (("no skew", (0, 0, 0)), ("skew 0/+3s/-3s", (0, 3, -3))):
        with Cluster() as c:
            names = edge_core_cloud(c, base=24300, transport="http", gateway_env={"BATCH_SIZE": 1})
            for i, sk in enumerate(skews):
                c.start(f"agent{i}", "system.edge.agent", 24390 + i, wait=False, health=None, env={
                    "NODE_ID": f"agent-{i}", "COLLECTOR_URL": c.url("gateway") + "/metrics", "REPORT_INTERVAL": 0.3,
                    "WORK_UNITS": 3000, "SKEW_SECONDS": sk})
            time.sleep(12)
            lead = current_leader(c, names)
            o = c.get(lead, "/ordering")
            chk = max(o["traces_checked"], 1)
            row = {"scenario": label, "traces_checked": o["traces_checked"], "wall_clock_violations": o["wall_clock_violations"],
                   "lamport_violations": o["lamport_violations"], "wall_violation_pct": round(100 * o["wall_clock_violations"] / chk, 1),
                   "lamport_violation_pct": round(100 * o["lamport_violations"] / chk, 1)}
            out.append(row)
            print(f"  C {label:16}: {row['traces_checked']} traces  wall-clock violations {row['wall_violation_pct']}%  Lamport violations {row['lamport_violation_pct']}%")
    save_csv("m04_clock_ordering.csv", out)
    return out


def distinct_ids(c, names):
    ids, total = set(), 0
    for path in glob.glob(f"{c.workdir}/core-data/core-*.sqlite3"):
        con = sqlite3.connect(path, timeout=30)
        got = [r[0] for r in con.execute("SELECT record_id FROM readings")]
        con.close()
        total += len(got)
        ids.update(got)
    return len(ids), total


def part_d():
    data = dataset()
    out = []
    for scenario, rep in [("control (no failure)", 1)] + [("leader crash at ~35%", r) for r in range(1, REPS + 1)]:
        with Cluster() as c:
            names = edge_core_cloud(c, base=24500, transport="kafka", core_env={"HEARTBEAT_INTERVAL": 0.5})
            killed = {"t": None, "leader": None}
            stop = threading.Event()

            def killer():
                while not stop.is_set():
                    try:
                        if sum(c.get(n, "/readings/count")["rows_in_db"] for n in names["cores"] if c.alive(n)) >= int(len(data) * 0.35):
                            killed["leader"] = current_leader(c, names)
                            c.kill(killed["leader"])
                            killed["t"] = time.perf_counter()
                            return
                    except Exception:
                        pass
                    time.sleep(0.05)
            if scenario.startswith("leader"):
                threading.Thread(target=killer, daemon=True).start()
            t0 = time.perf_counter()
            run_replay(c.url("gateway"), rows=data, chunk=2000, quiet=True)
            sent_s = time.perf_counter() - t0
            deadline = time.time() + 240
            n_distinct = total = 0
            while time.time() < deadline:
                n_distinct, total = distinct_ids(c, names["cores"])
                if n_distinct >= len(data):
                    break
                time.sleep(1)
            wall = time.perf_counter() - t0
            stop.set()
            row = {"scenario": scenario, "rep": rep, "rows_sent": len(data), "distinct_rows_stored": n_distinct, "rows_lost": len(data) - n_distinct,
                   "rows_stored_incl_dup": total, "duplicate_rows_across_cores": total - n_distinct,
                   "sender_s": round(sent_s, 1), "complete_s": round(wall, 1), "killed": killed["leader"],
                   "new_leader": current_leader(c, names)}
            out.append(row)
            print(f"  D {scenario:22} rep{rep}: stored {n_distinct}/{len(data)} (lost {row['rows_lost']}, duplicates {row['duplicate_rows_across_cores']}), "
                  f"complete in {row['complete_s']}s, killed {killed['leader']} -> leader now {row['new_leader']}")
    save_csv("m04_leader_crash_stream.csv", out)
    return out


def main(parts="abcd"):
    import json, os
    from system.experiments.common import RESULTS
    path = os.path.join(RESULTS, "m04_summary.json")
    summary = json.load(open(path)) if os.path.exists(path) else {}
    if "a" in parts:
        print("Part A/B: Bully election vs n and heartbeat interval"); summary["election"] = part_a_b()
    if "c" in parts:
        print("Part C: logical vs physical clocks"); summary["clocks"] = part_c()
    if "d" in parts:
        print("Part D: leader crash during a 120,000-row stream"); summary["crash_stream"] = part_d()
    save_json("m04_summary.json", summary)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "acd")
