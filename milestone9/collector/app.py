"""
Collector service — Week 2 version.

Receives aggregated metric summaries from edge/core nodes, stores the
latest reading per node, tracks total records, and exposes /status.
Also records server-side receive time so latency can be cross-checked
against the client-side latency the agent measures.
"""

from flask import Flask, request, jsonify
from deadlock_demo import (
    unsafe_transfer, unsafe_reconcile, safe_transfer, safe_reconcile,
    detect_transfer, detect_reconcile,
)
from concurrent.futures import ThreadPoolExecutor
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


@app.route("/demo/unsafe", methods=["POST"])
def demo_unsafe():
    """Fires transfer and reconcile CONCURRENTLY with opposite lock
    order. Expect this to hang until the request timeout if a real
    deadlock forms - that IS the demonstration."""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(unsafe_transfer)
        f2 = ex.submit(unsafe_reconcile)
        r1 = f1.result(timeout=request.args.get("timeout", default=10, type=float))
        r2 = f2.result(timeout=request.args.get("timeout", default=10, type=float))
    return jsonify({"transfer": r1, "reconcile": r2}), 200


@app.route("/demo/safe", methods=["POST"])
def demo_safe():
    """Same concurrent operations, but both acquire locks in the same
    fixed order (registry, then stats). No deadlock is possible."""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(safe_transfer)
        f2 = ex.submit(safe_reconcile)
        r1 = f1.result(timeout=5)
        r2 = f2.result(timeout=5)
    return jsonify({"transfer": r1, "reconcile": r2}), 200


@app.route("/demo/detect", methods=["POST"])
def demo_detect():
    """Same opposite lock order as /demo/unsafe, but using try-acquire
    with a timeout: a deadlock is detected and one side aborts and
    recovers instead of the request hanging forever."""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(detect_transfer)
        f2 = ex.submit(detect_reconcile)
        r1 = f1.result(timeout=5)
        r2 = f2.result(timeout=5)
    return jsonify({"transfer": r1, "reconcile": r2}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
