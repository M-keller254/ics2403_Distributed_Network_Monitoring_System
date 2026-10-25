import time, threading
import psutil, requests
from flask import Flask, jsonify, request

EDGES = {
    "edge-node-a": "http://127.0.0.1:5001",
    "edge-node-b": "http://127.0.0.1:5002",
}

app = Flask(__name__)
clock = 0
lock = threading.Lock()

@app.route("/status")
def status():
    global clock
    cpu = psutil.cpu_percent(interval=0.1)
    with lock:
        clock += 1
        c = clock
    return jsonify({
        "node": "core-node",
        "timestamp": time.time(),
        "lamport_clock": c,
        "cpu_percent": cpu,
        "memory_percent": psutil.virtual_memory().percent,
    })

@app.route("/sync", methods=["POST"])
def sync():
    global clock
    incoming = request.get_json().get("lamport_clock", 0)
    with lock:
        clock = max(clock, incoming) + 1
        c = clock
    return jsonify({"node": "core-node", "lamport_clock": c, "timestamp": time.time()})

@app.route("/aggregate")
def aggregate():
    global clock
    edges = {}
    for name, url in EDGES.items():
        try:
            r = requests.get(url + "/status", timeout=2).json()
            with lock:
                clock = max(clock, r["lamport_clock"]) + 1
            edges[name] = r
        except requests.RequestException:
            edges[name] = {"error": "unreachable"}
    with lock:
        clock += 1
        c = clock
    return jsonify({"node": "core-node", "timestamp": time.time(),
                    "lamport_clock": c, "edges": edges})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)