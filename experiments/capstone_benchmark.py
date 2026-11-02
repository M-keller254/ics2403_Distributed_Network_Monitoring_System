"""Measure ingestion latency and throughput on the running collector."""
import argparse
import csv
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import psutil
import requests

def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]

def run_case(base_url, count, workers, label):
    url = base_url.rstrip("/") + "/ingest"
    records = []
    before_net = psutil.net_io_counters()
    cpu_start = psutil.cpu_percent(interval=0.2)
    memory_start = psutil.virtual_memory().percent
    started = time.perf_counter()

    def send(index):
        payload = {
            "node_id": f"benchmark-{label}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cpu_percent": float(index % 100),
            "memory_percent": float((index * 3) % 100),
            "bytes_sent": index * 100,
            "bytes_recv": index * 200,
        }
        request_started = time.perf_counter()
        try:
            response = requests.post(url, json=payload, timeout=5)
            elapsed_ms = (time.perf_counter() - request_started) * 1000
            response.raise_for_status()
            return index, elapsed_ms, response.status_code
        except requests.RequestException:
            return index, (time.perf_counter() - request_started) * 1000, 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(send, i) for i in range(count)]
        for future in as_completed(futures):
            records.append(future.result())
    records.sort(key=lambda record: record[0])
    latencies = [record[1] for record in records]
    outcomes = [record[2] for record in records]
    elapsed = time.perf_counter() - started
    after_net = psutil.net_io_counters()
    try:
        status_response = requests.get(base_url.rstrip("/") + "/status", timeout=5)
        status_response.raise_for_status()
        nodes = status_response.json().get("known_nodes", {})
        monitored = [node for node_id, node in nodes.items()
                     if not node_id.startswith("benchmark-")]
    except (requests.RequestException, ValueError):
        monitored = []
    return {
        "run": label, "requests": count, "workers": workers,
        "successes": sum(200 <= status < 300 for status in outcomes),
        "failures": sum(not 200 <= status < 300 for status in outcomes),
        "elapsed_seconds": round(elapsed, 4),
        "throughput_requests_per_second": round(count / elapsed, 3),
        "mean_latency_ms": round(statistics.mean(latencies), 3),
        "p95_latency_ms": round(percentile(latencies, 0.95), 3),
        "max_latency_ms": round(max(latencies), 3),
        "mean_latency_jitter_ms": round(
            statistics.mean(abs(b - a) for a, b in zip(latencies, latencies[1:])), 3
        ) if len(latencies) > 1 else 0.0,
        "request_failure_rate_percent": round(
            100 * sum(not 200 <= status < 300 for status in outcomes) / count, 2
        ),
        "client_cpu_start_percent": cpu_start,
        "client_cpu_end_percent": psutil.cpu_percent(interval=None),
        "client_memory_start_percent": memory_start,
        "client_memory_end_percent": psutil.virtual_memory().percent,
        "client_network_sent_bytes": after_net.bytes_sent - before_net.bytes_sent,
        "client_network_recv_bytes": after_net.bytes_recv - before_net.bytes_recv,
        "monitored_nodes": len(monitored),
        "mean_monitored_cpu_percent": round(
            statistics.mean(node["cpu_percent"] for node in monitored), 3
        ) if monitored else "",
        "mean_monitored_memory_percent": round(
            statistics.mean(node["memory_percent"] for node in monitored), 3
        ) if monitored else "",
    }

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:5000")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/capstone_benchmark.csv")
    args = parser.parse_args()
    if args.requests < 1 or args.workers < 1:
        parser.error("--requests and --workers must be positive")
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    summaries = [
        run_case(args.url, args.requests, 1, "serial-baseline"),
        run_case(args.url, args.requests, args.workers, "parallel-probe"),
    ]
    with open(args.output, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=summaries[0].keys())
        writer.writeheader()
        writer.writerows(summaries)
    for row in summaries:
        print(row)
    print(f"Wrote benchmark summary to {args.output}")

if __name__ == "__main__":
    main()
