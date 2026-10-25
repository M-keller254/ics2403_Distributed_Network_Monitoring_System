import sys, time, threading
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

@app.route("/status")
def status():
    global clock
    cpu = psutil.cpu_percent(interval=0.1)
    with lock:
        clock += 1
        c = clock
    return jsonify({
        "node": NODE_NAME,
        "timestamp": now(),
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
    return jsonify({"node": NODE_NAME, "lamport_clock": c, "timestamp": now()})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)