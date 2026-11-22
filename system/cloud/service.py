"""
Cloud service: the CLOUD tier (Milestone 3; ported from Week 4, now with fault injection and
an optional durable sink: SINK_URL receives each digest as a transactional write -- Milestone 12).

Receives periodic digests from the core and keeps the long-term view: history
and per-node lifetime analytics. It is deliberately the slowest-moving tier --
it never sees individual reports, only digests -- which is the point: the
further up the stack you go, the less data arrives and the longer it is kept.

History is held in memory for now. Real durable, distributed storage is the
Week 12 milestone.
"""

import os
import threading
import time
from collections import deque

from flask import Flask, jsonify, request

from system.common.faults import FaultInjector
from system.common.netcall import Net

app = Flask(__name__)
faults = FaultInjector('cloud').install(app)
net = Net('cloud')
SINK_URL = os.environ.get('SINK_URL', '')        # transparency gateway /write (replicated, transactional store)
sink = {'writes': 0, 'failures': 0, 'last_decision': None}

PORT = int(os.environ.get("PORT", "5002"))
lock = threading.Lock()
history = deque(maxlen=1000)
lifetime = {}   # node_id -> running analytics across all digests
digests_received = 0
digests_by_core = {}   # Week 4: which core replica sent each digest (shows failover)
START = time.time()


@app.route("/digest", methods=["POST"])
def digest():
    global digests_received
    data = request.get_json(force=True)
    with lock:
        digests_received += 1
        cid = data.get("core_id")
        digests_by_core[cid] = digests_by_core.get(cid, 0) + 1
        history.append({"received_at": time.time(), "core_id": data.get("core_id"), "nodes": data["nodes"], "alerts_total": data.get("alerts_total", 0)})
        for node_id, n in data["nodes"].items():
            a = lifetime.setdefault(node_id, {"digests": 0, "cpu_sum": 0.0, "mem_sum": 0.0, "peak_cpu": 0.0})
            a["digests"] += 1
            a["cpu_sum"] += n.get("avg_cpu", 0)
            a["mem_sum"] += n.get("avg_mem", 0)
            a["peak_cpu"] = max(a["peak_cpu"], n.get("max_cpu", 0))
            a["total_reports_seen"] = n.get("total_reports")
            a["total_anomalies"] = n.get("total_anomalies")
    if SINK_URL:                                     # persist the digest through the transactional store
        try:
            st, body = net.post_json(SINK_URL, {"writes": {f"digest/{data.get('core_id')}/latest": {"nodes": len(data["nodes"]), "alerts_total": data.get("alerts_total", 0),
                                                                                      "rows_ingested": data.get("rows_ingested", 0), "at": time.time()}}}, timeout=5)
            with lock:
                sink["writes" if st == 200 else "failures"] += 1
                sink["last_decision"] = (body or {}).get("decision")
        except Exception:
            with lock:
                sink["failures"] += 1
    return jsonify({"status": "ok"})


@app.route("/sink", methods=["GET"])
def sink_status():
    with lock:
        return jsonify(dict(sink, sink_url=SINK_URL))


@app.route("/analytics", methods=["GET"])
def analytics():
    with lock:
        out = {}
        for node_id, a in lifetime.items():
            out[node_id] = {
                "digests": a["digests"],
                "lifetime_avg_cpu": round(a["cpu_sum"] / a["digests"], 2),
                "lifetime_avg_mem": round(a["mem_sum"] / a["digests"], 2),
                "peak_cpu": a["peak_cpu"],
                "total_reports_seen": a.get("total_reports_seen"),
                "total_anomalies": a.get("total_anomalies"),
            }
        return jsonify({"digests_received": digests_received, "digests_by_core": digests_by_core, "uptime_seconds": round(time.time() - START, 1), "nodes": out})


@app.route("/history", methods=["GET"])
def get_history():
    limit = int(request.args.get("limit", 10))
    with lock:
        return jsonify({"stored": len(history), "latest": list(history)[-limit:]})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "tier": "cloud"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
