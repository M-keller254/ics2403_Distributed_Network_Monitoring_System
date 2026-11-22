"""
BASELINE system: one flat collector (Milestones 1-2, and the 'baseline' side of milestones 3 and 12).

Everything the proposed system spreads over tiers is done here by one process:
every edge node posts straight to it, it validates, stores to SQLite, and answers /status.
No message bus, no leader election, no replication, no transactions. It is deliberately the
simplest design that works, which makes it the control for the baseline-vs-proposed
comparison (brief section 6).

  POST /bulk     {"rows": [...]}   dataset chunk (same payload the edge gateway accepts)
  POST /metrics  {...report...}    live agent report (Week 1-2 payload)
  GET  /status   /readings/count   /health
"""
import os
import sqlite3
import threading
import time

from flask import Flask, jsonify, request

from system.common.config import env_int, env_str
from system.common.faults import FaultInjector

app = Flask(__name__)
faults = FaultInjector("baseline-collector").install(app)
PORT = env_int("PORT", 5000)
DATA_DIR = env_str("DATA_DIR", "/tmp/baseline-data")
os.makedirs(DATA_DIR, exist_ok=True)
COLS = ["record_id", "timestamp", "site_id", "region", "edge_node", "site_type", "cpu_pct", "mem_pct", "rx_mbps",
        "tx_mbps", "latency_ms", "jitter_ms", "packet_loss_pct", "active_users", "signal_dbm", "temperature_c", "is_anomaly"]
db = sqlite3.connect(os.path.join(DATA_DIR, "baseline.sqlite3"), check_same_thread=False, timeout=30)
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute(f"CREATE TABLE IF NOT EXISTS readings ({', '.join(COLS)}, PRIMARY KEY(record_id))")
db.commit()
lock = threading.Lock()
latest = {}
stats = {"rows": 0, "chunks": 0, "reports": 0}
START = time.time()


@app.route("/bulk", methods=["POST"])
def bulk():
    rows = (request.get_json(force=True) or {}).get("rows", [])
    with lock:
        db.executemany(f"INSERT OR IGNORE INTO readings VALUES ({','.join('?' * len(COLS))})", [tuple(r[c] for c in COLS) for r in rows])
        db.commit()
        stats["rows"] += len(rows)
        stats["chunks"] += 1
    return jsonify({"status": "ok", "rows": len(rows)})


@app.route("/metrics", methods=["POST"])
def metrics():
    d = request.get_json(force=True)
    with lock:
        latest[d["node_id"]] = d
        stats["reports"] += 1
    return jsonify({"status": "ok"})


@app.route("/status")
def status():
    with lock:
        return jsonify({"known_nodes": list(latest), "latest": latest, "stats": dict(stats), "uptime_s": round(time.time() - START, 1)})


@app.route("/readings/count")
def count():
    with lock:
        return jsonify({"rows_in_db": db.execute("SELECT COUNT(*) FROM readings").fetchone()[0]})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "tier": "flat-collector"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
