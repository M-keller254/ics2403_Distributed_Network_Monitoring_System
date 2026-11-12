import csv
import math
import os
import random
from datetime import datetime, timedelta

SEED = 42
ROWS = 150_000
NODES = ["edge-node-a", "edge-node-b"]
START = datetime(2026, 10, 1, 0, 0, 0)
STEP_SECONDS = 2
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "network_metrics.csv")

PROFILE = {
    "edge-node-a": {"cpu": 35, "mem": 48, "lat": 12, "loss": 0.4},
    "edge-node-b": {"cpu": 45, "mem": 55, "lat": 18, "loss": 0.8},
}


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def main():
    rng = random.Random(SEED)
    per_node = ROWS // len(NODES)
    rows = []
    for n_idx, node in enumerate(NODES):
        p = PROFILE[node]
        spike_left = 0
        for i in range(per_node):
            ts = START + timedelta(seconds=i * STEP_SECONDS)
            hour = ts.hour + ts.minute / 60
            wave = math.sin((hour - 8) / 24 * 2 * math.pi)
            if spike_left == 0 and rng.random() < 0.0006:
                spike_left = rng.randint(20, 60)
            spiking = spike_left > 0
            if spiking:
                spike_left -= 1
            cpu = p["cpu"] + 15 * wave + rng.gauss(0, 5) + (35 if spiking else 0)
            mem = p["mem"] + 6 * wave + rng.gauss(0, 2) + (10 if spiking else 0)
            lat = p["lat"] * (1 + 0.3 * wave) + abs(rng.gauss(0, 3)) + (60 if spiking else 0)
            loss = p["loss"] + abs(rng.gauss(0, 0.3)) + (4 if spiking else 0)
            cpu, mem = clamp(cpu, 1, 100), clamp(mem, 5, 100)
            lat, loss = max(1.0, lat), clamp(loss, 0, 100)
            bytes_in = int(max(0, 80_000 + 40_000 * wave + rng.gauss(0, 10_000)))
            bytes_out = int(max(0, 50_000 + 25_000 * wave + rng.gauss(0, 8_000)))
            if cpu >= 85 or lat >= 80:
                status = "critical"
            elif cpu >= 65 or lat >= 40:
                status = "degraded"
            else:
                status = "normal"
            rows.append((ts, node, round(cpu, 2), round(mem, 2), round(lat, 2),
                         round(loss, 2), bytes_in, bytes_out, status))
    rows.sort(key=lambda r: (r[0], r[1]))

    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["row_id", "timestamp", "node_id", "cpu_percent", "memory_percent",
                    "latency_ms", "packet_loss_percent", "bytes_in", "bytes_out", "status"])
        for k, r in enumerate(rows, start=1):
            w.writerow([k, r[0].strftime("%Y-%m-%d %H:%M:%S"), *r[1:]])
    print(f"Wrote {len(rows):,} rows to {OUT} (seed={SEED})")


if __name__ == "__main__":
    main()