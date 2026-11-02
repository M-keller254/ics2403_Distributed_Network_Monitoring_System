"""
Name server - Milestone 10.

A small registry: services REGISTER their logical name and current
network location, and clients RESOLVE a logical name to a location
instead of hardcoding host:port. Models:
    Name -> Name Resolution -> Resource Location
Also logs process lifecycle events (create/locate/terminate) for the
distributed process-management requirement.
"""
import os, time, threading
from flask import Flask, request, jsonify

app = Flask(__name__)
lock = threading.Lock()
REGISTRY = {}     # logical_name -> {"url": ..., "registered_at": ..., "last_heartbeat": ...}
EVENTS = []
STALE_AFTER = 10.0


def log_event(kind, name):
    EVENTS.append({"event": kind, "name": name, "ts": time.time()})


@app.route("/register", methods=["POST"])
def register():
    d = request.get_json(silent=True) or {}
    name, url = d.get("name"), d.get("url")
    if not name or not url:
        return jsonify({"error": "name and url required"}), 400
    with lock:
        is_new = name not in REGISTRY
        REGISTRY[name] = {"url": url, "registered_at": time.time(), "last_heartbeat": time.time()}
    log_event("create" if is_new else "re-register", name)
    return jsonify({"status": "registered", "name": name, "url": url}), 200


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    d = request.get_json(silent=True) or {}
    name = d.get("name")
    with lock:
        if name in REGISTRY:
            REGISTRY[name]["last_heartbeat"] = time.time()
            return jsonify({"status": "ok"}), 200
    return jsonify({"error": "not registered"}), 404


@app.route("/resolve/<name>", methods=["GET"])
def resolve(name):
    t0 = time.time()
    with lock:
        entry = REGISTRY.get(name)
    log_event("locate", name)
    if not entry:
        return jsonify({"error": "not found", "resolved_in_s": time.time() - t0}), 404
    alive = (time.time() - entry["last_heartbeat"]) <= STALE_AFTER
    return jsonify({"name": name, "url": entry["url"], "alive": alive,
                    "resolved_in_s": round(time.time() - t0, 6)}), 200


@app.route("/deregister", methods=["POST"])
def deregister():
    d = request.get_json(silent=True) or {}
    name = d.get("name")
    with lock:
        REGISTRY.pop(name, None)
    log_event("terminate", name)
    return jsonify({"status": "deregistered", "name": name}), 200


@app.route("/directory", methods=["GET"])
def directory():
    now = time.time()
    with lock:
        entries = {n: {**e, "alive": (now - e["last_heartbeat"]) <= STALE_AFTER}
                   for n, e in REGISTRY.items()}
    return jsonify({"services": entries, "count": len(entries)}), 200


@app.route("/events", methods=["GET"])
def events():
    return jsonify({"events": EVENTS[-200:], "total": len(EVENTS)}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "9000")))
