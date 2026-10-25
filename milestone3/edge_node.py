import sys, time
import psutil
from flask import Flask, jsonify

NODE_NAME = sys.argv[1] if len(sys.argv) > 1 else "edge-node-a"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5001

app = Flask(__name__)

@app.route("/status")
def status():
    return jsonify({
        "node": NODE_NAME,
        "timestamp": time.time(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)