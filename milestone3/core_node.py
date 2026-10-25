import time
import psutil, requests
from flask import Flask, jsonify

EDGES = {
    "edge-node-a": "http://127.0.0.1:5001",
    "edge-node-b": "http://127.0.0.1:5002",
}

app = Flask(__name__)

@app.route("/status")
def status():
    return jsonify({
        "node": "core-node",
        "timestamp": time.time(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
    })

@app.route("/aggregate")
def aggregate():
    edges = {}
    for name, url in EDGES.items():
        try:
            edges[name] = requests.get(url + "/status", timeout=2).json()
        except requests.RequestException:
            edges[name] = {"error": "unreachable"}
    return jsonify({
        "node": "core-node",
        "timestamp": time.time(),
        "edges": edges,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)