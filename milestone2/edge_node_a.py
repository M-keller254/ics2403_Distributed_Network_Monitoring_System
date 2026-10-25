from flask import Flask, jsonify
import psutil, time
app = Flask(__name__)
NODE_NAME = "edge-node-a"

@app.route("/status")
def status():
    return jsonify({
        "node": NODE_NAME,
        "timestamp": time.time(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)