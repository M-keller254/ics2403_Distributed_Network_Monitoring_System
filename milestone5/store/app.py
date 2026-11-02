"""
Storage replica for the Milestone 5 transaction demo.

Implements the participant side of Two-Phase Commit:
  POST /prepare  {txn_id, key, value} -> votes YES (locks the key) or NO
  POST /commit   {txn_id}             -> makes the prepared write durable
  POST /abort    {txn_id}             -> discards the prepared write
  GET  /data                          -> committed key/value store (for consistency checks)
  POST /fail_mode {"down": true/false} -> simulate a crashed/unreachable participant
"""
import os, threading
from flask import Flask, request, jsonify

STORE_ID = os.environ.get("STORE_ID", "A")
app = Flask(__name__)
lock = threading.Lock()
committed = {}          # key -> value, durable
pending = {}            # txn_id -> {key, value}
down = False


@app.route("/fail_mode", methods=["POST"])
def fail_mode():
    global down
    down = bool((request.get_json(silent=True) or {}).get("down", False))
    return jsonify({"down": down}), 200


@app.route("/prepare", methods=["POST"])
def prepare():
    if down:
        return jsonify({"vote": "NO", "reason": "participant down"}), 503
    d = request.get_json(silent=True) or {}
    with lock:
        pending[d["txn_id"]] = {"key": d["key"], "value": d["value"]}
    return jsonify({"vote": "YES", "store": STORE_ID}), 200


@app.route("/commit", methods=["POST"])
def commit():
    if down:
        return jsonify({"error": "participant down"}), 503
    txn_id = (request.get_json(silent=True) or {}).get("txn_id")
    with lock:
        rec = pending.pop(txn_id, None)
        if rec:
            committed[rec["key"]] = rec["value"]
    return jsonify({"status": "committed", "store": STORE_ID}), 200


@app.route("/abort", methods=["POST"])
def abort():
    txn_id = (request.get_json(silent=True) or {}).get("txn_id")
    with lock:
        pending.pop(txn_id, None)
    return jsonify({"status": "aborted", "store": STORE_ID}), 200


@app.route("/data", methods=["GET"])
def data():
    with lock:
        return jsonify({"store": STORE_ID, "committed": dict(committed), "pending": len(pending)}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "down" if down else "healthy"}), 200 if not down else 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "7001")))
