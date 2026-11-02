"""
Milestone 4 failover experiment (run on the HOST while the system is up).

For each trial it:
  1. finds the current leader aggregator (asks both /leader endpoints)
  2. stops that aggregator's container            <- deliberate failure
  3. measures (a) election time: until the survivor reports is_leader
                (b) forwarding-resume time: until the cloud collector receives
                    a summary forwarded by the survivor
  4. restarts the stopped container and waits for the system to settle

Usage:  python experiments/failover_test.py --trials 3
Output: experiments/results/failover.csv and a printed summary.
"""
import argparse, csv, os, subprocess, time, statistics
from datetime import datetime
import requests

AGG = {1: "http://localhost:6001", 2: "http://localhost:6002"}
CONTAINER = {1: "aggregator-1", 2: "aggregator-2"}
COLLECTOR = "http://localhost:5000/status"


def leader_info(i):
    try:
        return requests.get(AGG[i] + "/leader", timeout=1).json()
    except requests.exceptions.RequestException:
        return None


def find_leader(timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        for i in AGG:
            info = leader_info(i)
            if info and info["is_leader"]:
                return i
        time.sleep(0.5)
    raise SystemExit("No leader found. Is the system running (docker compose up)?")


def last_forwarder():
    rec = requests.get(COLLECTOR, timeout=2).json()["latest"].get("edge-region")
    if not rec:
        return None, 0.0
    return rec["forwarded_by"], datetime.fromisoformat(rec["received_at"]).timestamp()


def run_trial(n):
    old = find_leader()
    new = [i for i in AGG if i != old][0]
    print(f"Trial {n}: leader is aggregator-{old}; stopping it...")
    t0 = time.time()
    subprocess.run(["docker", "stop", "-t", "0", CONTAINER[old]], check=True, capture_output=True)
    t_elect = t_forward = None
    while time.time() - t0 < 60 and (t_elect is None or t_forward is None):
        if t_elect is None:
            info = leader_info(new)
            if info and info["is_leader"]:
                t_elect = time.time() - t0
        if t_forward is None:
            who, ts = last_forwarder()
            if who == new and ts > t0:
                t_forward = time.time() - t0
        time.sleep(0.25)
    print(f"   election time = {t_elect}s, forwarding resumed after = {t_forward}s")
    subprocess.run(["docker", "start", CONTAINER[old]], check=True, capture_output=True)
    time.sleep(15)      # let heartbeats settle before the next trial
    return {"trial": n, "failed_aggregator": old, "new_leader": new,
            "election_time_s": round(t_elect, 2) if t_elect else "",
            "forwarding_resume_s": round(t_forward, 2) if t_forward else ""}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=3)
    a = ap.parse_args()
    rows = [run_trial(i + 1) for i in range(a.trials)]
    os.makedirs("experiments/results", exist_ok=True)
    with open("experiments/results/failover.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    for key in ("election_time_s", "forwarding_resume_s"):
        vals = [r[key] for r in rows if r[key] != ""]
        if vals:
            print(f"{key}: mean={statistics.mean(vals):.2f}s min={min(vals):.2f}s max={max(vals):.2f}s (n={len(vals)})")
    print("Saved experiments/results/failover.csv")
