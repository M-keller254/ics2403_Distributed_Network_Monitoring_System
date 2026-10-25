"""Exercise live-agent 2PC commit and abort paths and save latency evidence."""
import argparse
import csv
import os
import time

import requests


def run(base_url, value, fail_node=None):
    payload = {"value": value}
    if fail_node:
        payload["fail_prepare_node"] = fail_node
    started = time.perf_counter()
    response = requests.post(base_url.rstrip("/") + "/transactions/two-phase",
                             json=payload, timeout=15)
    elapsed_ms = (time.perf_counter() - started) * 1000
    response.raise_for_status()
    result = response.json()
    return {
        "transaction_id": result["transaction_id"],
        "decision": result["decision"],
        "participants": len(result["votes"]),
        "prepared": len(result["prepared"]),
        "completed": len(result["completed"]),
        "recovery_pending": result["recovery_pending"],
        "latency_ms": round(elapsed_ms, 3),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:5000")
    parser.add_argument("--output", default="results/transactions.csv")
    args = parser.parse_args()
    rows = [
        run(args.url, {"alert_threshold": 75}),
        run(args.url, {"alert_threshold": 80}, fail_node="edge-node-b"),
    ]
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    for row in rows:
        print(row)
    print(f"Saved transaction measurements to {args.output}")


if __name__ == "__main__":
    main()
