"""
Milestone 12 (part 3): COORDINATION OVERHEAD and scalability of the core tier.

The capstone sweeps (m12_capstone.py) measure throughput/latency/resources/availability. The brief also asks for *coordination overhead*
(what does it cost to keep N core replicas agreeing on a leader?) and *scalability*. This experiment measures it directly from the
message counters the core replicas keep (GET /election_stats):

  steady state   N replicas, 60,000 rows replayed, then idle until 20 s have passed: heartbeat messages per second (whole cluster)
                 and per replica, elections started
  leader crash   kill the leader (highest id): time until every survivor agrees on a new leader, election messages exchanged
  leader return  restart it: it has the highest id, so it calls an election and takes over again (Bully): time and messages

Theory (Week 4): steady-state heartbeats = (N-1) per interval; one Bully election after the leader dies costs at most O(N^2) messages
(every lower replica may start one), usually far fewer.

Hypotheses
  H1 heartbeat traffic grows linearly with N: 2(N-1) messages/s at a 0.5 s interval.
  H2 election messages after a leader crash grow with N but stay well below N^2 (replicas notice the failure within one heartbeat of each other).
  H3 a leader change is visible to every survivor within a few heartbeat intervals (detection) + one election round trip.

Run:  python -m system.experiments.m12_coordination
"""
import time

import requests

from system.edge.replayer import run_replay
from system.experiments.common import dataset, save_csv, save_json
from system.scripts.cluster import Cluster
from system.scripts.stack import edge_core_cloud, wait_for

BASE = 25000
HB = 0.5          # heartbeat interval, seconds


def stats(c, names):
    out = {}
    for n in names:
        if not c.alive(n):
            continue
        try:
            out[n] = c.get(n, "/election_stats")
        except Exception:
            pass
    return out


def totals(st):
    return (sum(s["heartbeat_messages_sent"] for s in st.values()), sum(s["election_messages_sent"] for s in st.values()),
            sum(s["elections_started"] for s in st.values()))


def leader_view(st):
    return {s["leader_id"] for s in st.values()}


def run(n, rep, data):
    with Cluster() as c:
        names = edge_core_cloud(c, base=BASE + 100 * ((n * 3 + rep) % 20), n_cores=n, transport="kafka", core_env={"HEARTBEAT_INTERVAL": HB})
        cores = names["cores"]
        wait_for(lambda: len(leader_view(stats(c, cores))) == 1 and None not in leader_view(stats(c, cores)) or None, 30, what="leader elected")
        t_start = time.time()
        s0 = stats(c, cores)
        t0 = time.perf_counter()
        run_replay(c.url("gateway"), rows=data, chunk=2000, quiet=True)
        send_s = time.perf_counter() - t0
        time.sleep(max(0.0, 20.0 - (time.time() - t_start)))
        s1 = stats(c, cores)
        window = time.time() - t_start
        hb0, el0, es0 = totals(s0)
        hb1, el1, es1 = totals(s1)
        row = {"replicas": n, "rep": rep, "window_s": round(window, 1), "rows_replayed": len(data), "send_s": round(send_s, 2),
               "heartbeats_total": hb1 - hb0, "heartbeats_per_s": round((hb1 - hb0) / window, 2), "theory_heartbeats_per_s": round((n - 1) / HB, 2),
               "elections_started_steady": es1 - es0, "initial_leader": next(iter(leader_view(s1)))}
        # ---- leader crash
        leader = max(int(s["node_id"]) for s in s1.values())
        victim = f"core{leader}"
        survivors = [x for x in cores if x != victim]
        before = totals(stats(c, survivors))               # survivors only: the victim's own counters disappear with its process
        t0 = time.time()
        c.kill(victim)
        new_leader = None
        if survivors:
            try:
                while time.time() - t0 < 30:
                    st = stats(c, survivors)
                    lv = leader_view(st)
                    if len(st) == len(survivors) and len(lv) == 1 and leader not in lv and None not in lv:
                        new_leader = next(iter(lv))
                        break
                    time.sleep(0.05)
            except Exception:
                pass
        row["leader_crash_detect_and_elect_s"] = round(time.time() - t0, 2) if new_leader is not None else None
        time.sleep(1.0)
        after = totals(stats(c, survivors)) if survivors else before
        row["election_messages_after_crash"] = after[1] - before[1]
        row["elections_started_after_crash"] = after[2] - before[2]
        row["n_squared_bound"] = n * n
        # ---- leader returns
        if survivors:
            before = totals(stats(c, survivors))
            t0 = time.time()
            c.restart(victim)
            while time.time() - t0 < 30:
                st = stats(c, cores)
                lv = leader_view(st)
                if len(st) == n and lv == {leader}:
                    break
                time.sleep(0.05)
            row["leader_return_takeover_s"] = round(time.time() - t0, 2)
            time.sleep(1.0)
            after = totals(stats(c, cores))
            row["election_messages_on_return"] = after[1] - before[1]
        row["rows_in_db_total_core_view"] = sum(c.get(x, "/readings/count")["rows_in_db"] for x in cores if c.alive(x))
        return row


def main(reps=2):
    data = dataset(60000)
    rows = []
    for n in (1, 2, 3, 5):
        for rep in range(1, reps + 1):
            r = run(n, rep, data)
            rows.append(r)
            print(f"  N={n} rep{rep}: heartbeats {r['heartbeats_per_s']}/s cluster-wide, crash->new leader {r.get('leader_crash_detect_and_elect_s')} s with "
                  f"{r.get('election_messages_after_crash')} election msgs ({r.get('elections_started_after_crash')} elections started, bound N^2={r['n_squared_bound']}), "
                  f"return takeover {r.get('leader_return_takeover_s')} s with {r.get('election_messages_on_return')} msgs", flush=True)
    save_csv("m12_coordination.csv", rows)
    save_json("m12_coordination.json", rows)


if __name__ == "__main__":
    main()
