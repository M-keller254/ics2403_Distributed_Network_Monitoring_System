import requests, time, csv, statistics, argparse

NODES = {
    "core-node": "http://127.0.0.1:5000/status",
    "edge-node-a": "http://127.0.0.1:5001/status",
    "edge-node-b": "http://127.0.0.1:5002/status",
}

def run_experiment(duration, label):
    rows = []
    start = time.time()
    print(f"Running experiment '{label}' for {duration}s, polling every 1.0s")

    while time.time() - start < duration:
        loop_start = time.time()
        for node_name, url in NODES.items():
            req_time = time.time()
            try:
                resp = requests.get(url, timeout=2)
                resp_time = time.time()
                data = resp.json()
                rows.append({
                    "node": node_name, "timestamp": req_time,
                    "latency": resp_time - req_time,
                    "cpu_percent": data.get("cpu_percent"),
                    "memory_percent": data.get("memory_percent"),
                    "success": 1
                })
            except requests.exceptions.RequestException:
                rows.append({
                    "node": node_name, "timestamp": req_time,
                    "latency": None, "cpu_percent": None,
                    "memory_percent": None, "success": 0
                })
        elapsed = time.time() - loop_start
        time.sleep(max(0, 1.0 - elapsed))

    out_path = f"results/{label}.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nWrote {len(rows)} rows to {out_path}")

    print("\n--- Summary ---")
    total_success = 0
    for node_name in NODES:
        node_rows = [r for r in rows if r["node"] == node_name]
        successes = [r for r in node_rows if r["success"] == 1]
        latencies = [r["latency"] for r in successes]
        cpu_vals = [r["cpu_percent"] for r in successes if r["cpu_percent"] is not None]
        mem_vals = [r["memory_percent"] for r in successes if r["memory_percent"] is not None]
        n = len(node_rows)
        n_success = len(successes)
        total_success += n_success
        packet_loss = 100 * (1 - n_success / n) if n else 0
        avg_lat = statistics.mean(latencies) if latencies else 0
        min_lat = min(latencies) if latencies else 0
        max_lat = max(latencies) if latencies else 0
        jitter = statistics.stdev(latencies) if len(latencies) > 1 else 0
        avg_cpu = statistics.mean(cpu_vals) if cpu_vals else 0
        avg_mem = statistics.mean(mem_vals) if mem_vals else 0
        print(f"{node_name}: n={n} success={n_success} loss={packet_loss:.1f}% "
              f"avg_latency={avg_lat:.4f}s min={min_lat:.4f}s max={max_lat:.4f}s "
              f"jitter={jitter:.4f}s avg_cpu={avg_cpu:.1f}% avg_mem={avg_mem:.1f}%")

    total_time = time.time() - start
    throughput = total_success / total_time
    print(f"\nOverall throughput: {throughput:.3f} records/s over {total_time:.0f}s ({total_success} records)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--label", type=str, default="run1")
    args = parser.parse_args()
    run_experiment(args.duration, args.label)