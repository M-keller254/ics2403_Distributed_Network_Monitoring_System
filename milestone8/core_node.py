"""Milestone 4 core node: exposes status and aggregates edge status."""
import threading
import time

import psutil
import requests
from flask import Flask, jsonify, request

EDGES = {
    "edge-node-a": "http://127.0.0.1:5001",
    "edge-node-b": "http://127.0.0.1:5002",
}
app = Flask(__name__)
clock = 0
lock = threading.Lock()

def tick(incoming=None):
    global clock
    with lock:
        clock = max(clock, incoming or 0) + 1
        return clock

@app.get("/status")
def status():
    current = tick()
    return jsonify(node="core-node", timestamp=time.time(), lamport_clock=current,
                   cpu_percent=psutil.cpu_percent(interval=0.1),
                   memory_percent=psutil.virtual_memory().percent)

@app.post("/sync")
def sync():
    payload = request.get_json(silent=True) or {}
    incoming = payload.get("lamport_clock", 0)
    if not isinstance(incoming, int) or incoming < 0:
        return jsonify(error="lamport_clock must be a non-negative integer"), 400
    return jsonify(node="core-node", lamport_clock=tick(incoming), timestamp=time.time())

@app.get("/aggregate")
def aggregate():
    edges = {}
    for name, url in EDGES.items():
        try:
            response = requests.get(url + "/status", timeout=2)
            response.raise_for_status()
            reading = response.json()
            tick(reading.get("lamport_clock", 0))
            edges[name] = reading
        except (requests.RequestException, ValueError):
            edges[name] = {"error": "unreachable"}
    current = tick()
    return jsonify(node="core-node", timestamp=time.time(),
                   lamport_clock=current, edges=edges)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
