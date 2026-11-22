"""
Oral-defence demo (Part E): ONE deliberate failure, shown as   Failure -> Detection -> Recovery -> Service restoration.

    python -m system.scripts.demo_failure            # ~30 s, prints a timestamped timeline

What happens
  1. start the final stack: edge gateway -> log -> 3 core replicas (Bully) -> cloud
  2. stream the 120,000-row dataset at 6,000 rows/s from an edge node
  3. at 40 % of the stream SIGKILL the LEADER core replica                        <- FAILURE
  4. watch the survivors: first heartbeat miss, then agreement on a new leader     <- DETECTION
  5. the new leader resumes consuming the log from the committed offset            <- RECOVERY
  6. the stream ends; every row is checked against the dataset                     <- SERVICE RESTORATION
  7. restart the dead replica: it has the highest id, so it takes leadership back (Bully)
"""
import threading
import time

import requests

from system.edge.replayer import run_replay
from system.experiments.common import dataset
from system.experiments.m12_capstone import stored_ids
from system.scripts.cluster import Cluster
from system.scripts.stack import edge_core_cloud, wait_for

BASE = 21800
HB = 0.5
RATE = 6000


def leader_of(c, names):
    seen = {}
    for n in names:
        if c.alive(n):
            try:
                seen[n] = c.get(n, "/election_stats", timeout=1)["leader_id"]
            except requests.RequestException:
                pass
    return seen


def stored(c, names):
    t = 0
    for n in names:
        if c.alive(n):
            try:
                t += c.get(n, "/readings/count", timeout=2)["rows_in_db"]
            except requests.RequestException:
                pass
    return t


def main():
    data = dataset()
    with Cluster() as c:
        t0 = time.perf_counter()
        T = lambda: f"[{time.perf_counter() - t0:6.2f}s]"
        names = edge_core_cloud(c, base=BASE, n_cores=3, transport="kafka", core_env={"HEARTBEAT_INTERVAL": HB})
        cores = names["cores"]
        wait_for(lambda: len(set(leader_of(c, cores).values())) == 1 and None not in leader_of(c, cores).values() or None, 30, what="leader elected")
        leader = next(iter(set(leader_of(c, cores).values())))
        print(f"{T()} stack up: 3 core replicas, leader = core{leader}; streaming {len(data):,} rows at {RATE:,} rows/s")
        th = threading.Thread(target=lambda: run_replay(c.url("gateway"), rows=data, chunk=1000, rate=RATE, quiet=True), daemon=True)
        th.start()
        wait_for(lambda: stored(c, cores) >= 0.4 * len(data) or None, 60, interval=0.1, what="40 % streamed")
        before = stored(c, cores)
        victim = f"core{leader}"
        t_fail = time.perf_counter()
        c.kill(victim)
        print(f"{T()} FAILURE    : SIGKILL {victim} (the leader) with {before:,} rows stored")
        survivors = [n for n in cores if n != victim]
        detected = recovered = None
        base_surv = stored(c, survivors)             # replicas keep separate databases: progress of the NEW leader = growth of the survivors' own counts
        while time.perf_counter() - t_fail < 30:
            lv = leader_of(c, survivors)
            if detected is None and (None in lv.values() or any(v != leader for v in lv.values())):
                detected = time.perf_counter() - t_fail
                print(f"{T()} DETECTION  : survivors stopped hearing {victim} (+{detected:.2f} s) and started an election")
            if detected is not None and len(lv) == len(survivors) and len(set(lv.values())) == 1 and None not in lv.values() and leader not in lv.values():
                recovered = time.perf_counter() - t_fail
                print(f"{T()} RECOVERY   : new leader = core{next(iter(lv.values()))} agreed by all survivors (+{recovered:.2f} s)")
                break
            time.sleep(0.05)
        wait_for(lambda: stored(c, survivors) > base_surv or None, 30, interval=0.05, what="consumption resumed")
        t_resume = time.perf_counter() - t_fail
        print(f"{T()} RECOVERY   : new leader is consuming the log again from the committed offset (+{t_resume:.2f} s after the crash)")
        th.join(timeout=120)
        wait_for(lambda: stored_ids(c, "proposed", c.workdir)[0] >= len(data) or None, 120, interval=0.5, what="all rows stored")
        distinct, total = stored_ids(c, "proposed", c.workdir)
        print(f"{T()} RESTORATION: stream finished; distinct record_ids stored = {distinct:,} of {len(data):,}  (lost: {len(data) - distinct}; duplicate copies across replicas: {total - distinct:,})")
        c.restart(victim)
        t_back = time.perf_counter()
        wait_for(lambda: set(leader_of(c, cores).values()) == {leader} or None, 30, interval=0.1, what="old leader takes over")
        print(f"{T()} RETURN     : {victim} restarted; as the highest id it bullies its way back to leadership in {time.perf_counter() - t_back:.2f} s")
        print("\nSummary: failure injected at +0 s, detected +%.2f s, new leader +%.2f s, no row lost." % (detected or float("nan"), recovered or float("nan")))


if __name__ == "__main__":
    main()
