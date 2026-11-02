"""Milestone 4 edge node with a Lamport logical clock."""
import sys
import threading
import time

import psutil
from flask import Flask, jsonify, request

NODE_NAME = sys.argv[1] if len(sys.argv) > 1 else "edge-node-a"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5001
SKEW = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0

app = Flask(__name__)
clock = 0
lock = threading.Lock()

def now():
    return time.time() + SKEW

@app.get("/status")
def status():
    global clock
    cpu = psutil.cpu_percent(interval=0.1)
    with lock:
        clock += 1
        current = clock
    return jsonify(node=NODE_NAME, timestamp=now(), lamport_clock=current,
                   cpu_percent=cpu, memory_percent=psutil.virtual_memory().percent)

@app.post("/sync")
def sync():
    global clock
    payload = request.get_json(silent=True) or {}
    incoming = payload.get("lamport_clock", 0)
    if not isinstance(incoming, int) or incoming < 0:
        return jsonify(error="lamport_clock must be a non-negative integer"), 400
    with lock:
        clock = max(clock, incoming) + 1
        current = clock
    return jsonify(node=NODE_NAME, lamport_clock=current, timestamp=now())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
