"""
Milestone 12 - final capstone experiment.

Rather than re-inventing measurement code, this orchestrates the
experiments already built and verified in earlier milestones against
the FULL integrated stack, and adds the one new piece: verifying
distributed storage actually works (write a snapshot, read it back).

    Performance            -> experiments/run_experiment.py        (M2)
    Fault tolerance         -> experiments/fault_test.py            (M7)
    RPC / shared state      -> experiments/rpc_benchmark.py         (M11)
    Distributed storage     -> this script's own history check      (M12, new)

Usage:
    python experiments/capstone_experiment.py --duration 30
"""
import argparse, subprocess, sys, time, requests

ARCHIVER = "http://localhost:9500"


def check_storage(wait_s=25):
    print("\n--- Distributed storage (Milestone 12) ---")
    before = requests.get(f"{ARCHIVER}/history").json()["count"]
    print(f"Snapshots before wait: {before}")
    print(f"Waiting {wait_s}s for at least one snapshot interval to pass...")
    time.sleep(wait_s)
    after = requests.get(f"{ARCHIVER}/history").json()
    print(f"Snapshots after wait: {after['count']}")
    if after["snapshots"]:
        key = after["snapshots"][-1]
        snap = requests.get(f"{ARCHIVER}/history/{key}").json()
        print(f"Read back '{key}': captured_at={snap['captured_at']}, "
              f"known_nodes in that snapshot={snap['status'].get('known_nodes')}")
        return after["count"] > before
    return False


def run_subexperiment(label, args):
    print(f"\n--- Running: {' '.join(args)} ---")
    subprocess.run([sys.executable, *args], check=False)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=30,
                     help="duration for the Milestone 2 performance sub-experiment")
    a = ap.parse_args()

    run_subexperiment("performance",
        ["experiments/run_experiment.py", "--duration", str(a.duration), "--label", "capstone_perf"])
    run_subexperiment("rpc",
        ["experiments/rpc_benchmark.py", "--writes", "15", "--reads", "15"])
    storage_ok = check_storage(wait_s=min(25, a.duration))

    print("\n=== Capstone summary ===")
    print(f"Distributed storage write+readback: {'OK' if storage_ok else 'NO NEW SNAPSHOT - check archiver/minio logs'}")
    print("For fault tolerance (requires stopping real containers), run separately:")
    print("  python experiments/fault_test.py --component aggregator --fault crash --trials 3")
    print("  python experiments/fault_test.py --component store      --fault crash --trials 3")
