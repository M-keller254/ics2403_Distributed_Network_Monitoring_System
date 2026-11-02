"""
Collector service — Week 2 version.

Receives aggregated metric summaries from edge/core nodes, stores the
latest reading per node, tracks total records, and exposes /status.
Also records server-side receive time so latency can be cross-checked
against the client-side latency the agent measures.
"""

from flask import Flask, request, jsonify
from datetime import datetime, timezone
import threading

app = Flask(__name__)

_lock = threading.Lock()
_state = {
    "latest": {},          # node_name -> latest report dict
    "total_records_received": 0,
    "total_records_failed": 0,
    "forwarded_counts": {},   # aggregator id -> summaries received from it
}


@app.route("/report", methods=["POST"])
def receive_report():
    data = request.get_json(silent=True)
    if not data or "node" not in data:
        with _lock:
            _state["total_records_failed"] += 1
        return jsonify({"error": "invalid payload"}), 400

    node = data["node"]
    role = data.get("role", "unknown")
    received_at = datetime.now(timezone.utc).isoformat()

    record = {
        "node": node,
        "role": role,
        "cpu": data.get("cpu"),
        "mem": data.get("mem"),
        "cpu_min": data.get("cpu_min"),
        "cpu_max": data.get("cpu_max"),
        "mem_min": data.get("mem_min"),
        "mem_max": data.get("mem_max"),
        "window_size": data.get("window_size"),
        "sent_at": data.get("sent_at"),
        "received_at": received_at,
        "forwarded_by": data.get("forwarded_by"),
        "leader_term": data.get("leader_term"),
        "members": data.get("members"),
    }

    with _lock:
        _state["latest"][node] = record
        _state["total_records_received"] += 1
        fb = data.get("forwarded_by")
        if fb is not None:
            fc = _state["forwarded_counts"]
            fc[str(fb)] = fc.get(str(fb), 0) + 1

    print(
        f"[collector] received from {node} (role={role}): "
        f"cpu={record['cpu']}% mem={record['mem']}% "
        f"window={record['window_size']}",
        flush=True,
    )

    return jsonify({"status": "ok", "received_at": received_at}), 200


@app.route("/status", methods=["GET"])
def status():
    with _lock:
        known_nodes = list(_state["latest"].keys())
        latest = dict(_state["latest"])
        total = _state["total_records_received"]
        failed = _state["total_records_failed"]
        forwarded = dict(_state["forwarded_counts"])

    return jsonify({
        "known_nodes": known_nodes,
        "latest": latest,
        "total_records_received": total,
        "total_records_failed": failed,
        "forwarded_counts": forwarded,
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
