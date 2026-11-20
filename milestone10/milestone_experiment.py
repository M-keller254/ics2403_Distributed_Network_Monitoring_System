"""Measure live /resolve latency through the ZooKeeper-backed name server."""
import argparse
import csv
import os
import statistics
import time

import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", default="store-a")
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument("--url", default="http://localhost:9000")
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("--trials must be at least 1")

    rows = []
    for trial in range(1, args.trials + 1):
        started = time.perf_counter()
        response = requests.get(f"{args.url}/resolve/{args.service}", timeout=3)
        elapsed = time.perf_counter() - started
        if response.status_code != 200:
            raise SystemExit(f"Resolve failed ({response.status_code}): {response.text}")
        payload = response.json()
        rows.append({
            "trial": trial,
            "service": args.service,
            "url": payload["url"],
            "alive": payload["alive"],
            "http_latency_s": round(elapsed, 6),
            "registry_latency_s": payload["resolved_in_s"],
        })
        time.sleep(0.05)

    os.makedirs("results", exist_ok=True)
    output = "results/zookeeper_resolution.csv"
    with open(output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    values = [row["http_latency_s"] for row in rows]
    print(f"Resolved {args.service} in {len(rows)} trials; alive={rows[-1]['alive']}")
    print(f"HTTP resolution latency: mean={statistics.mean(values):.6f}s, "
          f"min={min(values):.6f}s, max={max(values):.6f}s")
    print(f"Saved {output}")


if __name__ == "__main__":
    main()
