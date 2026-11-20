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
import argparse, csv, subprocess, sys, time, requests
from pathlib import Path

ARCHIVER = "http://localhost:9500"


def check_storage(wait_s=25):
    print("\n--- Distributed storage (Milestone 12) ---")
    before_response = requests.get(f"{ARCHIVER}/history", timeout=10)
    before_response.raise_for_status()
    before = before_response.json()["count"]
    print(f"Snapshots before wait: {before}")
    print(f"Waiting {wait_s}s for at least one snapshot interval to pass...")
    time.sleep(wait_s)
    after_response = requests.get(f"{ARCHIVER}/history", timeout=10)
    after_response.raise_for_status()
    after = after_response.json()
    print(f"Snapshots after wait: {after['count']}")
    key = ""
    readback_ms = ""
    readback_ok = False
    if after["snapshots"]:
        key = after["snapshots"][-1]
        read_started = time.perf_counter()
        snap_response = requests.get(f"{ARCHIVER}/history/{key}", timeout=10)
        readback_ms = round((time.perf_counter() - read_started) * 1000, 3)
        snap_response.raise_for_status()
        snap = snap_response.json()
        readback_ok = "captured_at" in snap and "status" in snap
        print(f"Read back '{key}': captured_at={snap['captured_at']}, "
              f"known_nodes in that snapshot={snap['status'].get('known_nodes')}")
    success = after["count"] > before and readback_ok
    output = Path(__file__).resolve().parent / "results" / "capstone_storage.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "captured_at_utc", "wait_seconds", "snapshots_before",
            "snapshots_after", "latest_snapshot_key", "readback_latency_ms",
            "readback_success", "storage_check_success",
        ])
        writer.writeheader()
        writer.writerow({
            "captured_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "wait_seconds": wait_s,
            "snapshots_before": before,
            "snapshots_after": after["count"],
            "latest_snapshot_key": key,
            "readback_latency_ms": readback_ms,
            "readback_success": readback_ok,
            "storage_check_success": success,
        })
    print(f"Saved storage evidence: {output}")
    return success


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
