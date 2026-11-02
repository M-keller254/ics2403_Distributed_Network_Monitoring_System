"""
Week 2 experiment script.

Polls the collector's /status endpoint repeatedly over a fixed
duration, and derives:
    - throughput (records received per second, from the running total)
    - latency samples (using each node's self-reported 'sent_at' vs
      the collector's 'received_at')
    - jitter (stdev of latency samples per node)
    - packet loss (approximated as expected reports vs actual growth
      in total_records_received)

Usage:
    python experiments/run_experiment.py --duration 60 --label window5

Run this AFTER `docker compose up --build` is already running, in a
separate terminal, with the venv/python that has `requests` installed
on your HOST machine (not inside a container).

Output: a CSV file in experiments/results/ named after --label.
"""

import argparse
import csv
import os
import time
import statistics
from datetime import datetime, timezone
import requests

COLLECTOR_STATUS_URL = "http://localhost:5000/status"
POLL_INTERVAL_SECONDS = 1.0


def parse_iso(ts):
    if ts is None:
        return None
    return datetime.fromisoformat(ts)


def run(duration_seconds, label):
    os.makedirs("experiments/results", exist_ok=True)
    out_path = f"experiments/results/{label}.csv"

    samples = []  # one row per poll
    latencies_by_node = {}

    start = time.time()
    prev_total = None
    first_total = None
    poll_count = 0

    print(f"Running experiment '{label}' for {duration_seconds}s, "
          f"polling {COLLECTOR_STATUS_URL} every {POLL_INTERVAL_SECONDS}s")

    while time.time() - start < duration_seconds:
        poll_time = time.time()
        try:
            resp = requests.get(COLLECTOR_STATUS_URL, timeout=3)
            data = resp.json()
        except requests.exceptions.RequestException as e:
            print(f"  poll failed: {e}")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        total = data.get("total_records_received", 0)
        if first_total is None:
            first_total = total
        failed = data.get("total_records_failed", 0)
        latest = data.get("latest", {})

        # Instantaneous throughput since last poll
        if prev_total is not None:
            delta = total - prev_total
            throughput = delta / POLL_INTERVAL_SECONDS
        else:
            throughput = 0.0
        prev_total = total

        # Per-node latency: received_at (collector, UTC ISO) - sent_at (agent, unix time)
        row = {
            "elapsed_s": round(poll_time - start, 2),
            "total_records_received": total,
            "total_records_failed": failed,
            "throughput_records_per_s": round(throughput, 3),
        }

        for node, record in latest.items():
            sent_at = record.get("sent_at")
            received_at = record.get("received_at")
            if sent_at is not None and received_at is not None:
                try:
                    recv_dt = parse_iso(received_at)
                    recv_unix = recv_dt.timestamp()
                    latency = recv_unix - float(sent_at)
                    latencies_by_node.setdefault(node, []).append(latency)
                    row[f"{node}_latency_s"] = round(latency, 4)
                except (ValueError, TypeError):
                    pass
            row[f"{node}_cpu"] = record.get("cpu")
            row[f"{node}_mem"] = record.get("mem")

        samples.append(row)
        poll_count += 1
        time.sleep(POLL_INTERVAL_SECONDS)

    # Write raw per-poll CSV
    all_fields = set()
    for row in samples:
        all_fields.update(row.keys())
    fieldnames = sorted(all_fields)

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(samples)

    print(f"\nWrote {len(samples)} rows to {out_path}")

    # Print summary stats (latency, jitter) per node
    print("\n--- Summary ---")
    for node, lat_list in latencies_by_node.items():
        if len(lat_list) >= 2:
            jitter = statistics.stdev(lat_list)
        else:
            jitter = 0.0
        print(
            f"{node}: n={len(lat_list)} "
            f"avg_latency={statistics.mean(lat_list):.4f}s "
            f"min={min(lat_list):.4f}s max={max(lat_list):.4f}s "
            f"jitter(stdev)={jitter:.4f}s"
        )

    if samples:
        final_total = samples[-1]["total_records_received"]
        run_records = final_total - first_total      # only records received during THIS run
        overall_throughput = run_records / duration_seconds
        print(f"\nOverall throughput: {overall_throughput:.3f} records/s "
              f"over {duration_seconds}s ({run_records} records during this run)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Week 2 load/performance experiment")
    parser.add_argument("--duration", type=int, default=60,
                         help="How long to run the experiment, in seconds")
    parser.add_argument("--label", type=str, default="experiment",
                         help="Name used for the output CSV file")
    args = parser.parse_args()
    run(args.duration, args.label)
