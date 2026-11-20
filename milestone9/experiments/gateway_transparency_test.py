"""Measure Milestone 9 transparent gateway reads and export Power BI CSV.

Run from the milestone9 directory while its Compose stack is running.
Each trial reads through the same gateway URL with both stores healthy,
then stops store-a and reads through that same URL again.
"""
import argparse
import csv
import os
import subprocess
import time
from datetime import datetime, timezone

import requests

COORDINATOR = "http://localhost:7000"
GATEWAY_READ = "http://localhost:8000/read"
STORE_A_CONTAINER = "store-a"
FIELDS = [
    "recorded_at_utc", "trial", "scenario", "status_code", "served_by",
    "replicas_tried", "gateway_overhead_s", "failover_succeeded",
]


def docker(*args):
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True)


def create_key(trial):
    key = f"powerbi-transparency-{int(time.time())}-{trial}"
    response = requests.post(
        f"{COORDINATOR}/write", json={"key": key, "value": trial}, timeout=10
    )
    payload = response.json()
    if payload.get("decision") != "COMMIT":
        raise RuntimeError(f"Could not seed test key: {payload}")
    return key


def measure_read(trial, scenario, key, expect_failover=False):
    response = requests.get(f"{GATEWAY_READ}/{key}", timeout=8)
    try:
        body = response.json()
    except ValueError:
        body = {}
    served_by = body.get("_served_by", "")
    tried = body.get("_tried", 0)
    if not isinstance(tried, int):
        tried = len(tried) if isinstance(tried, list) else 0
    success = (
        response.status_code == 200
        and "store-b" in served_by
        and tried >= 2
    ) if expect_failover else ""
    return {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "trial": trial,
        "scenario": scenario,
        "status_code": response.status_code,
        "served_by": served_by,
        "replicas_tried": tried,
        "gateway_overhead_s": body.get("_gateway_overhead_s", ""),
        "failover_succeeded": success,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument(
        "--output", default="experiments/results/powerbi_gateway_transparency.csv"
    )
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("--trials must be at least 1")

    rows = []
    for trial in range(1, args.trials + 1):
        key = create_key(trial)
        rows.append(measure_read(trial, "both_stores_healthy", key))
        stopped = False
        try:
            docker("stop", "-t", "0", STORE_A_CONTAINER)
            stopped = True
            time.sleep(0.5)
            rows.append(measure_read(trial, "store_a_stopped", key, expect_failover=True))
        finally:
            if stopped:
                docker("start", STORE_A_CONTAINER)
        time.sleep(1)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    recovered = [r for r in rows if r["scenario"] == "store_a_stopped" and r["failover_succeeded"]]
    print(f"Captured {len(rows)} reads across {args.trials} trials.")
    print(f"Transparent store-a failure recovered in {len(recovered)}/{args.trials} trials.")
    print(f"Saved Power BI data to {args.output}")


if __name__ == "__main__":
    main()
