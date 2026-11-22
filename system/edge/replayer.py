"""
Dataset replayer: the 'edge node' that streams the 120,000-row telemetry dataset.

Reads data/telemetry_120k.csv, keeps only the rows belonging to this edge node
(edge_node column), and POSTs them to the edge gateway /bulk in chunks. The gateway
publishes them to Kafka; the core consumes them. Honours HTTP 429 backpressure.

  python -m system.edge.replayer --edge edge-node-a --gateway http://localhost:5000 --rate 0

--rate = rows per second (0 = as fast as the gateway accepts).
--limit = stop after N rows (for quick experiments).
Usable as a library:  run_replay(...) -> dict of counters.
"""
import argparse
import csv
import json
import os
import time

import requests

DEFAULT_CSV = os.environ.get("DATASET_CSV", os.path.join(os.path.dirname(__file__), "..", "..", "data", "telemetry_120k.csv"))
INT_FIELDS = {"record_id", "active_users", "is_anomaly"}
STR_FIELDS = {"timestamp", "site_id", "region", "edge_node", "site_type"}


def load_rows(csv_path=DEFAULT_CSV, edge=None, limit=None):
    rows = []
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            if edge and r["edge_node"] != edge:
                continue
            for k in r:
                if k in INT_FIELDS:
                    r[k] = int(r[k])
                elif k not in STR_FIELDS:
                    r[k] = float(r[k])
            rows.append(r)
            if limit and len(rows) >= limit:
                break
    return rows


def run_replay(gateway, edge=None, csv_path=DEFAULT_CSV, rate=0, chunk=500, limit=None, rows=None, quiet=False, order="time", link_mbps=0.0):
    """order="time": rows as recorded (sites interleaved, like live traffic).
    order="site": site-major (a backlog upload: many consecutive readings of one site per chunk)."""
    rows = rows if rows is not None else load_rows(csv_path, edge, limit)
    if order == "site":
        rows = sorted(rows, key=lambda r: (r["site_id"], r["timestamp"]))
    sess = requests.Session()
    sent = retries = 0
    lat, link_free_at = [], 0.0                   # link_mbps: emulate a slow uplink between this device and `gateway`
    t0 = time.time()
    for i in range(0, len(rows), chunk):
        part = rows[i:i + chunk]
        while True:
            try:
                if link_mbps:
                    nbytes = len(json.dumps({"rows": part}))
                    link_free_at = max(link_free_at, time.time()) + nbytes * 8.0 / (link_mbps * 1e6)
                    wait = link_free_at - time.time()
                    if wait > 0:
                        time.sleep(wait)
                tr = time.perf_counter()
                r = sess.post(gateway.rstrip("/") + "/bulk", json={"rows": part}, timeout=15)
                lat.append(time.perf_counter() - tr)
            except requests.RequestException:
                retries += 1
                time.sleep(0.5)
                continue
            if r.status_code == 200:
                break
            retries += 1
            time.sleep(0.2)                       # 429 backpressure or transient error
        sent += len(part)
        if rate:
            ahead = sent / float(rate) - (time.time() - t0)
            if ahead > 0:
                time.sleep(ahead)
        if not quiet and (i // chunk) % 20 == 0:
            print(f"[replayer:{edge}] {sent:,}/{len(rows):,} rows", flush=True)
    elapsed = time.time() - t0
    return {"edge": edge, "rows_sent": sent, "retries": retries, "elapsed_s": round(elapsed, 3),
            "rows_per_s": round(sent / elapsed, 1) if elapsed else None, "request_latencies_s": lat}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--edge", default=os.environ.get("EDGE_NAME"))
    ap.add_argument("--gateway", default=os.environ.get("GATEWAY_URL", "http://localhost:5000"))
    ap.add_argument("--csv", default=DEFAULT_CSV)
    ap.add_argument("--rate", type=float, default=float(os.environ.get("REPLAY_RATE", 0)))
    ap.add_argument("--chunk", type=int, default=500)
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()
    # wait for the gateway to come up (compose starts everything together)
    for _ in range(60):
        try:
            if requests.get(a.gateway.rstrip("/") + "/health", timeout=2).status_code == 200:
                break
        except requests.RequestException:
            time.sleep(1)
    res = run_replay(a.gateway, a.edge, a.csv, a.rate, a.chunk, a.limit)
    res.pop("request_latencies_s", None)
    print(res)


if __name__ == "__main__":
    main()
