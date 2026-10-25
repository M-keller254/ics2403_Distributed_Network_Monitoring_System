"""Compare serial host-style polling with a parallel integrated poll over live agents."""
import argparse
import csv
import os
import statistics
import time

import requests


def sample(base_url, mode, timeout):
    started = time.perf_counter()
    response = requests.get(
        base_url.rstrip("/") + "/aggregate",
        params={"mode": mode}, timeout=timeout
    )
    response.raise_for_status()
    payload = response.json()
    elapsed = (time.perf_counter() - started) * 1000
    failures = sum("error" in result for result in payload["edges"].values())
    return {
        "model_proxy": "serial-centralized" if mode == "serial" else "parallel-integrated",
        "poll_mode": mode,
        "elapsed_ms": round(elapsed, 3),
        "core_elapsed_ms": payload["elapsed_ms"],
        "nodes": len(payload["edges"]),
        "failed_nodes": failures,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:5000")
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--output", default="results/model_comparison.csv")
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("--trials must be positive")
    rows = []
    for trial in range(1, args.trials + 1):
        for mode in ("serial", "parallel"):
            row = sample(args.url, mode, args.timeout)
            row["trial"] = trial
            rows.append(row)
            print(f"trial={trial} mode={mode} elapsed={row['elapsed_ms']}ms failures={row['failed_nodes']}")
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    for mode in ("serial", "parallel"):
        values = [row["elapsed_ms"] for row in rows if row["poll_mode"] == mode]
        print(f"{mode}: mean={statistics.mean(values):.3f}ms trials={len(values)}")
    print(f"Saved raw trials to {args.output}")


if __name__ == "__main__":
    main()
