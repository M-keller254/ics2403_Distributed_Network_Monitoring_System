#!/usr/bin/env python3
"""
Telecom site telemetry dataset generator (Theme 2: Distributed Network Monitoring).

Produces data/telemetry_120k.csv: 24 cell sites x 5,000 readings = 120,000 rows
(the lecturer asked for 100,000+). Fully deterministic: the same seed always
gives byte-identical output, so a second researcher can reproduce it
(brief section 9, reproducibility).

Why synthetic: a real operator trace is not available for a coursework project.
The generator is built so that the data has the structure a monitoring system
has to cope with, which is what the experiments need:
  * a daily (diurnal) load cycle and a weekend dip
  * per-site capacity differences (macro / micro / small cell)
  * correlated degradation: high CPU -> higher latency/jitter -> packet loss
  * labelled anomalies (single-site incidents) and regional incidents that hit
    several sites of one region at the same time (feeds correlated alerts)

Columns
  record_id, timestamp (UTC ISO-8601), site_id, region, edge_node (which edge
  node ingests this site), site_type, cpu_pct, mem_pct, rx_mbps, tx_mbps,
  latency_ms, jitter_ms, packet_loss_pct, active_users, signal_dbm,
  temperature_c, is_anomaly (0/1 ground-truth label)

Usage:  python data/generate_dataset.py [--rows-per-site 5000] [--seed 2403]
"""
import argparse
import csv
import hashlib
import math
import os
import random
from datetime import datetime, timedelta, timezone

REGIONS = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Nyeri"]
SITES_PER_REGION = 4
SITE_TYPES = [("macro", 1.0), ("micro", 0.55), ("small_cell", 0.3)]
INTERVAL_MIN = 5
START = datetime(2026, 9, 1, tzinfo=timezone.utc)

FIELDS = [
    "record_id", "timestamp", "site_id", "region", "edge_node", "site_type",
    "cpu_pct", "mem_pct", "rx_mbps", "tx_mbps", "latency_ms", "jitter_ms",
    "packet_loss_pct", "active_users", "signal_dbm", "temperature_c", "is_anomaly",
]


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def build_sites(rng):
    sites = []
    for ri, region in enumerate(REGIONS):
        for k in range(SITES_PER_REGION):
            stype, cap = SITE_TYPES[(ri + k) % 3] if k else SITE_TYPES[0]
            idx = len(sites)
            sites.append({
                "site_id": f"{region[:3].upper()}-{k + 1:02d}",
                "region": region,
                "edge_node": "edge-node-a" if idx < (len(REGIONS) * SITES_PER_REGION) // 2 else "edge-node-b",
                "site_type": stype,
                "capacity": cap,
                "base_cpu": rng.uniform(18, 32),
                "base_mem": rng.uniform(35, 55),
                "phase": rng.uniform(-1.5, 1.5),
                "signal_base": rng.uniform(-92, -68),
            })
    return sites


def diurnal(t, phase):
    """0..1 daily load curve peaking around 19:00 with a smaller morning peak, weekend dip."""
    h = t.hour + t.minute / 60.0 + phase
    evening = math.exp(-((h - 19) ** 2) / 14.0)
    morning = 0.55 * math.exp(-((h - 9) ** 2) / 8.0)
    base = 0.18 + 0.82 * min(1.0, evening + morning)
    return base * (0.82 if t.weekday() >= 5 else 1.0)


def plan_incidents(rng, sites, n_steps):
    """Return {(site_index, step): severity}. Single-site and regional incidents."""
    inc = {}
    by_region = {}
    for i, s in enumerate(sites):
        by_region.setdefault(s["region"], []).append(i)
    # single-site incidents: ~1.0% of rows
    target_single = int(0.010 * n_steps * len(sites))
    placed = 0
    while placed < target_single:
        i = rng.randrange(len(sites))
        start = rng.randrange(20, n_steps - 20)
        length = rng.randint(3, 8)
        sev = rng.uniform(0.6, 1.0)
        for d in range(length):
            if (i, start + d) not in inc:
                inc[(i, start + d)] = sev
                placed += 1
    # regional incidents: 2-3 sites of one region at the same time
    for _ in range(int(0.0012 * n_steps * len(sites) / 4)):
        region = rng.choice(list(by_region))
        members = rng.sample(by_region[region], rng.randint(2, 3))
        start = rng.randrange(20, n_steps - 20)
        length = rng.randint(4, 9)
        sev = rng.uniform(0.7, 1.0)
        for i in members:
            for d in range(length):
                inc[(i, start + d)] = sev
    return inc


def generate(rows_per_site, seed):
    rng = random.Random(seed)
    sites = build_sites(rng)
    incidents = plan_incidents(rng, sites, rows_per_site)
    rows = []
    for step in range(rows_per_site):
        t = START + timedelta(minutes=INTERVAL_MIN * step)
        for si, s in enumerate(sites):
            load = clamp(diurnal(t, s["phase"]) * (0.8 + 0.4 * s["capacity"]) + rng.gauss(0, 0.04), 0.02, 1.0)
            sev = incidents.get((si, step), 0.0)
            cpu = s["base_cpu"] + 48 * load + rng.gauss(0, 2.2) + 38 * sev
            mem = s["base_mem"] + 14 * load + 0.0008 * step % 6 + rng.gauss(0, 1.1) + 12 * sev
            users = max(0, int(s["capacity"] * 420 * load * (1 + rng.gauss(0, 0.07)) * (1 - 0.35 * sev)))
            rx = max(0.0, users * 0.045 * (1 + rng.gauss(0, 0.1)) * (1 - 0.25 * sev))
            tx = max(0.0, rx * rng.uniform(0.18, 0.32))
            cpu, mem = clamp(cpu, 1, 100), clamp(mem, 5, 99)
            stress = max(0.0, (cpu - 70) / 30.0)
            latency = 11 + 0.18 * cpu + 55 * stress ** 2 + rng.gauss(0, 1.4) + 95 * sev
            jitter = 0.9 + 0.05 * cpu + 18 * stress + abs(rng.gauss(0, 0.5)) + 14 * sev
            loss = clamp(0.03 + 3.5 * stress ** 2 + abs(rng.gauss(0, 0.04)) + 4.5 * sev, 0, 30)
            signal = s["signal_base"] - 6 * load - 9 * sev + rng.gauss(0, 1.2)
            temp = 24 + 0.16 * cpu + 5 * math.sin((t.hour - 14) / 24 * 2 * math.pi) + rng.gauss(0, 0.6)
            rows.append([
                0, t.strftime("%Y-%m-%dT%H:%M:%SZ"), s["site_id"], s["region"], s["edge_node"], s["site_type"],
                round(cpu, 2), round(mem, 2), round(rx, 3), round(tx, 3), round(latency, 2), round(jitter, 2),
                round(loss, 3), users, round(signal, 1), round(temp, 1), 1 if sev > 0 else 0,
            ])
    for n, r in enumerate(rows, start=1):
        r[0] = n
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows-per-site", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=2403)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "telemetry_120k.csv"))
    a = ap.parse_args()
    rows = generate(a.rows_per_site, a.seed)
    with open(a.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(FIELDS)
        w.writerows(rows)
    h = hashlib.sha256(open(a.out, "rb").read()).hexdigest()
    anomalies = sum(r[-1] for r in rows)
    print(f"wrote {len(rows):,} rows x {len(FIELDS)} columns to {a.out}")
    print(f"anomalous rows: {anomalies:,} ({100 * anomalies / len(rows):.2f}%)   sha256: {h}")


if __name__ == "__main__":
    main()
