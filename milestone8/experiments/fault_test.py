"""
Milestone 7 - Fault Tolerance and Recovery.

Injects controlled failures into the TWO subsystems that already know
how to recover on their own (built in Milestones 4 and 5), and
measures how well that recovery actually works:

  component=aggregator  -> crashes/pauses the current leader aggregator.
                            Recovery = the standby (Milestone 4 election)
                            takes over and resumes forwarding.
  component=store        -> crashes/pauses store-b, a 2PC participant.
                            Recovery = the coordinator (Milestone 5)
                            stops aborting and starts committing again
                            once store-b is restored.

Failure kinds:
  crash          docker stop   (process gone outright)
  omission       docker pause  (process alive, but does not respond -
                                 a request to it hangs/times out rather
                                 than being cleanly refused)
  communication  approximated as docker pause on a single host, which
                 has the same externally-observable effect (no
                 response) as a cut network link for this experiment.
                 A genuine partition needs separate hosts or network-
                 level tooling, out of scope for a single-machine demo.

Byzantine failures (a node responding with plausible but WRONG data)
are not injected here - detecting them needs redundancy and voting
beyond what a 2-replica / 2-aggregator design can support, and is
noted as a limitation rather than attempted.

For each trial, reports:
  mttr_s         time from failure to confirmed recovery
  availability   (trial_window - downtime) / trial_window

Usage:
    python experiments/fault_test.py --component aggregator --fault crash --trials 3
    python experiments/fault_test.py --component store      --fault omission --trials 3
"""
import argparse, csv, os, subprocess, time, statistics
import requests

AGG = {1: "http://localhost:6001", 2: "http://localhost:6002"}
AGG_CONTAINER = {1: "aggregator-1", 2: "aggregator-2"}
COORD = "http://localhost:7000"
STORE_B_CONTAINER = "store-b"


def docker(*args):
    subprocess.run(["docker", *args], check=True, capture_output=True)


def inject(container, fault_kind):
    if fault_kind in ("crash", "communication"):
        docker("stop", "-t", "0", container)
    else:  # omission
        docker("pause", container)


def restore(container, fault_kind):
    if fault_kind in ("crash", "communication"):
        docker("start", container)
    else:
        docker("unpause", container)


def leader_of_aggregators():
    for i in AGG:
        try:
            if requests.get(AGG[i] + "/leader", timeout=1).json().get("is_leader"):
                return i
        except requests.exceptions.RequestException:
            continue
    return None


def aggregator_trial(fault_kind, window_s=60):
    old = leader_of_aggregators()
    if old is None:
        raise SystemExit("No aggregator leader found - is the system up?")
    new = [i for i in AGG if i != old][0]
    t_fail = time.time()
    inject(AGG_CONTAINER[old], fault_kind)

    t_recovered = None
    while time.time() - t_fail < window_s and t_recovered is None:
        try:
            if requests.get(AGG[new] + "/leader", timeout=1).json().get("is_leader"):
                t_recovered = time.time() - t_fail
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.25)

    restore(AGG_CONTAINER[old], fault_kind)
    time.sleep(10)
    downtime = t_recovered if t_recovered else window_s
    availability = round((window_s - downtime) / window_s, 4)
    return {"component": "aggregator", "fault": fault_kind,
            "mttr_s": round(t_recovered, 2) if t_recovered else None,
            "availability": availability}


def store_trial(fault_kind, window_s=60):
    t_fail = time.time()
    inject(STORE_B_CONTAINER, fault_kind)

    # confirm the coordinator is now aborting (expected during the outage)
    time.sleep(2)
    r = requests.post(f"{COORD}/write", json={"key": "probe", "value": 1}, timeout=5)
    aborting_during_failure = (r.json()["decision"] == "ABORT")

    restore(STORE_B_CONTAINER, fault_kind)
    t_recovered = None
    while time.time() - t_fail < window_s and t_recovered is None:
        try:
            r = requests.post(f"{COORD}/write", json={"key": "probe", "value": 2}, timeout=5)
            if r.json()["decision"] == "COMMIT":
                t_recovered = time.time() - t_fail
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.5)

    downtime = t_recovered if t_recovered else window_s
    availability = round((window_s - downtime) / window_s, 4)
    return {"component": "store", "fault": fault_kind,
            "aborted_during_failure": aborting_during_failure,
            "mttr_s": round(t_recovered, 2) if t_recovered else None,
            "availability": availability}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--component", choices=["aggregator", "store"], required=True)
    ap.add_argument("--fault", choices=["crash", "omission", "communication"], default="crash")
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--window", type=float, default=60, help="trial window in seconds, for the availability calculation")
    a = ap.parse_args()

    rows = []
    for i in range(a.trials):
        print(f"Trial {i+1}/{a.trials}: component={a.component} fault={a.fault}")
        row = (aggregator_trial(a.fault, a.window) if a.component == "aggregator"
               else store_trial(a.fault, a.window))
        rows.append(row)
        print(" ", row)

    os.makedirs("experiments/results", exist_ok=True)
    path = f"experiments/results/fault_{a.component}_{a.fault}.csv"
    fieldnames = sorted({k for r in rows for k in r})
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames); w.writeheader(); w.writerows(rows)

    mttrs = [r["mttr_s"] for r in rows if r["mttr_s"] is not None]
    avails = [r["availability"] for r in rows]
    print(f"\nRecovered in {len(mttrs)}/{a.trials} trials.")
    if mttrs:
        print(f"MTTR: mean={statistics.mean(mttrs):.2f}s min={min(mttrs):.2f}s max={max(mttrs):.2f}s")
    print(f"Availability (this window): mean={statistics.mean(avails):.4f}")
    print(f"Saved {path}")
