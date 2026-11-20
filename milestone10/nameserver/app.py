"""HTTP name-service facade backed by Apache ZooKeeper (Milestone 10).

Store services keep the existing /register and /heartbeat calls. The
registrations live as ZooKeeper ephemeral znodes under /services, and
clients resolve logical names through this facade.
"""
import atexit
import json
import os
import re
import threading
import time

from flask import Flask, jsonify, request
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NodeExistsError, NoNodeError
from kazoo.protocol.states import KazooState

app = Flask(__name__)
ZOOKEEPER_HOSTS = os.environ.get("ZOOKEEPER_HOSTS", "zookeeper:2181")
STALE_AFTER = float(os.environ.get("STALE_AFTER", "10"))
ROOT = "/services"
event_lock = threading.Lock()
EVENTS = []

zk = KazooClient(hosts=ZOOKEEPER_HOSTS, timeout=10.0)
zk.start(timeout=30)
zk.ensure_path(ROOT)
atexit.register(zk.stop)


def log_event(kind, name):
    with event_lock:
        EVENTS.append({"event": kind, "name": name, "ts": time.time()})


def service_path(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        raise ValueError("name must contain only letters, digits, '.', '_' or '-'")
    return f"{ROOT}/{name}"


def read_entry(name):
    data, _ = zk.get(service_path(name))
    return json.loads(data.decode("utf-8"))


def write_entry(name, entry):
    path = service_path(name)
    payload = json.dumps(entry, separators=(",", ":")).encode("utf-8")
    try:
        zk.create(path, payload, ephemeral=True)
        return True
    except NodeExistsError:
        zk.set(path, payload)
        return False


@app.errorhandler(KazooException)
def registry_unavailable(error):
    app.logger.error("ZooKeeper request failed: %s", error)
    return jsonify({"error": "ZooKeeper registry unavailable"}), 503


@app.route("/register", methods=["POST"])
def register():
    data = request.get_json(silent=True) or {}
    name, url = data.get("name"), data.get("url")
    if not name or not url:
        return jsonify({"error": "name and url required"}), 400
    try:
        path = service_path(name)
    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    now = time.time()
    try:
        old = read_entry(name)
        entry = {**old, "url": url, "last_heartbeat": now}
        is_new = False
    except NoNodeError:
        entry = {"url": url, "registered_at": now, "last_heartbeat": now}
        is_new = True
    created = write_entry(name, entry)
    log_event("create" if is_new or created else "re-register", name)
    return jsonify({"status": "registered", "name": name, "url": url,
                    "registry": "zookeeper", "path": path}), 200


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json(silent=True) or {}
    name = data.get("name")
    try:
        entry = read_entry(name)
    except (NoNodeError, ValueError, TypeError):
        return jsonify({"error": "not registered"}), 404
    entry["last_heartbeat"] = time.time()
    zk.set(service_path(name), json.dumps(entry, separators=(",", ":")).encode("utf-8"))
    return jsonify({"status": "ok", "name": name}), 200


@app.route("/resolve/<name>", methods=["GET"])
def resolve(name):
    started = time.perf_counter()
    log_event("locate", name)
    try:
        entry = read_entry(name)
    except (NoNodeError, ValueError):
        return jsonify({"error": "not found",
                        "resolved_in_s": round(time.perf_counter() - started, 6)}), 404
    alive = time.time() - float(entry.get("last_heartbeat", 0)) <= STALE_AFTER
    return jsonify({"name": name, "url": entry["url"], "alive": alive,
                    "resolved_in_s": round(time.perf_counter() - started, 6),
                    "registry": "zookeeper"}), 200


@app.route("/deregister", methods=["POST"])
def deregister():
    data = request.get_json(silent=True) or {}
    name = data.get("name")
    try:
        zk.delete(service_path(name))
    except (NoNodeError, ValueError, TypeError):
        pass
    log_event("terminate", name)
    return jsonify({"status": "deregistered", "name": name}), 200


@app.route("/directory", methods=["GET"])
def directory():
    now = time.time()
    entries = {}
    for name in zk.get_children(ROOT):
        try:
            entry = read_entry(name)
        except NoNodeError:
            continue
        entry["alive"] = now - float(entry.get("last_heartbeat", 0)) <= STALE_AFTER
        entries[name] = entry
    return jsonify({"services": entries, "count": len(entries),
                    "registry": "zookeeper"}), 200


@app.route("/events", methods=["GET"])
def events():
    with event_lock:
        current = list(EVENTS[-200:])
        total = len(EVENTS)
    return jsonify({"events": current, "total": total}), 200


@app.route("/health", methods=["GET"])
def health():
    connected = zk.state == KazooState.CONNECTED
    return jsonify({"status": "healthy" if connected else "unavailable",
                    "registry": "zookeeper", "connected": connected}), (200 if connected else 503)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "9000")))
