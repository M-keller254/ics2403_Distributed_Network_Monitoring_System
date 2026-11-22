"""REST/JSON + hand-written RPC server for the monitoring service (Milestone 11 benchmark target).

  REST     GET /rest/site/<id>   POST /rest/readings {"readings":[...]}   POST /rest/increment   GET /rest/stream?count=N (NDJSON)
  RPC      POST /rpc  methods: get_site, put_readings, increment, stream_readings   (system/rpc/framework.py)
"""
import json
import threading

from flask import Flask, Response, jsonify, request

from system.common.config import env_int, env_str
from system.common.faults import FaultInjector
from system.rpc.framework import RpcServer

app = Flask(__name__)
PORT = env_int("PORT", 7700)
faults = FaultInjector("rpc-rest").install(app)
rpc = RpcServer(app, semantics=env_str("SEMANTICS", "at-most-once"))
state = {"counter": 0, "readings": 0}
lock = threading.Lock()
SITE = {"site": "NAI-01", "cpu_pct": 41.5, "mem_pct": 62.1, "latency_ms": 23.4, "readings": 1234}


def sample(i):
    return {"record_id": i, "site": "NAI-01", "cpu_pct": 41.5, "mem_pct": 62.1, "rx_mbps": 120.5, "tx_mbps": 80.25, "latency_ms": 23.4,
            "jitter_ms": 2.1, "packet_loss_pct": 0.05, "active_users": 321, "is_anomaly": False}


@rpc.register("get_site")
def get_site(site):
    return {**SITE, "site": site}


@rpc.register("put_readings")
def put_readings(readings):
    with lock:
        state["readings"] += len(readings)
    return {"accepted": len(readings)}


@rpc.register("increment")
def increment(counter="c"):
    with lock:
        state["counter"] += 1
        return {"value": state["counter"]}


@rpc.register("stream_readings")
def stream_readings(count):
    return [sample(i) for i in range(count)]


@app.route("/rest/site/<site>")
def rest_site(site):
    return jsonify({**SITE, "site": site})


@app.route("/rest/readings", methods=["POST"])
def rest_readings():
    n = len(request.get_json(force=True)["readings"])
    with lock:
        state["readings"] += n
    return jsonify({"accepted": n})


@app.route("/rest/increment", methods=["POST"])
def rest_increment():
    with lock:
        state["counter"] += 1
        return jsonify({"value": state["counter"]})


@app.route("/rest/stream")
def rest_stream():
    n = int(request.args.get("count", 100))
    return Response((json.dumps(sample(i)) + "\n" for i in range(n)), mimetype="application/x-ndjson")


@app.route("/state", methods=["GET", "DELETE"])
def get_state():
    if request.method == "DELETE":
        state.update(counter=0, readings=0)
    return jsonify({**state, "rpc": rpc.stats, "executions": rpc.executions})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
