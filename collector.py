"""
Week 1 - Distributed OS Foundation
Collector process: the single central node that all agents report to.

Responsibility (Process model, Week 1):
    - Accept incoming metric reports over HTTP (POST /metrics).
    - Keep the latest reading per node, plus a full in-memory log.
    - Expose current state for inspection (GET /status).

NOTE: In-memory storage and a single collector are deliberate Week 1
simplifications -- see "Design justification" in the project doc.
Week 4 adds a second collector + leader election; Week 12 replaces this
with real distributed storage.
"""

import time

from flask import Flask, jsonify, request

app = Flask(__name__)

metrics_log = []       # full history, Week 1 stand-in for distributed storage
latest_by_node = {}    # node_id -> most recent report


@app.route("/metrics", methods=["POST"])
def receive_metrics():
    data = request.get_json(force=True)
    data["received_at"] = time.time()

    metrics_log.append(data)
    latest_by_node[data["node_id"]] = data

    print(
        f"[collector] received from {data['node_id']} "
        f"(role={data.get('node_role')}): "
        f"cpu={data['cpu_percent']:.1f}% mem={data['mem_percent']:.1f}%"
    )
    return jsonify({"status": "ok"}), 200


@app.route("/status", methods=["GET"])
def status():
    return jsonify(
        {
            "known_nodes": list(latest_by_node.keys()),
            "latest": latest_by_node,
            "total_records_received": len(metrics_log),
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
