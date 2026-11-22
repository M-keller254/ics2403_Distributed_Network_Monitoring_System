"""Lock-manager service: processes on different nodes request/release shared resources over HTTP.

  POST /acquire {pid,res,units,timeout}   200 granted | 409 deadlock victim / wait-die | 408 timeout | 422 order violation
  POST /release {pid,res} | /release_all {pid} | /begin {pid,ts}
  GET  /graph (JSON + DOT)   /stats   POST /detect (run detector once)
Env: RESOURCES="R1:1,R2:1,pool:4"  POLICY=none|detect|ordered|wait-die  FAIR=true|false  DETECT_INTERVAL=0.2
"""
import time

from flask import Flask, jsonify, request

from system.common.config import env_bool, env_float, env_int, env_str
from system.concurrency.lockmgr import Deadlock, OrderViolation, ResourceManager

app = Flask(__name__)
PORT = env_int("PORT", 7000)
res = {k.split(":")[0]: int(k.split(":")[1]) for k in env_str("RESOURCES", "R1:1,R2:1,R3:1,R4:1").split(",")}
rm = ResourceManager(res, env_str("POLICY", "none"), env_bool("FAIR", True), env_float("DETECT_INTERVAL", 0.2))


@app.route("/acquire", methods=["POST"])
def acquire():
    d = request.get_json(force=True)
    try:
        rm.acquire(d["pid"], d["res"], d.get("units", 1), d.get("timeout"))
        return jsonify({"granted": True})
    except Deadlock as e:
        return jsonify({"granted": False, "error": "deadlock", "detail": str(e)}), 409
    except TimeoutError as e:
        return jsonify({"granted": False, "error": "timeout", "detail": str(e)}), 408
    except OrderViolation as e:
        return jsonify({"granted": False, "error": "order", "detail": str(e)}), 422


@app.route("/release", methods=["POST"])
def release():
    d = request.get_json(force=True)
    rm.release(d["pid"], d["res"], d.get("units"))
    return jsonify({"ok": True})


@app.route("/release_all", methods=["POST"])
def release_all():
    rm.release_all(request.get_json(force=True)["pid"])
    return jsonify({"ok": True})


@app.route("/begin", methods=["POST"])
def begin():
    d = request.get_json(force=True)
    rm.begin(d["pid"], d.get("ts"))
    return jsonify({"ok": True})


@app.route("/graph")
def graph():
    return jsonify({**rm.graph(), "dot": rm.to_dot()})


@app.route("/detect", methods=["POST"])
def detect():
    return jsonify({"cycle": rm.detect_once()})


@app.route("/stats")
def stats():
    s = dict(rm.stats)
    s["utilization"] = rm.utilization()
    s["policy"], s["fair"], s["cycles"] = rm.policy, rm.fair, rm.cycles_log[-10:]
    return jsonify(s)


# ---- race-condition demo: the same read-modify-write with and without mutual exclusion
counter = {"v": 0, "calls": 0}


@app.route("/counter/<mode>", methods=["POST"])
def counter_inc(mode):
    pid = (request.get_json(silent=True) or {}).get("pid", "anon")
    counter["calls"] += 1
    if mode == "safe":
        rm.acquire(pid, "counter")
    try:
        v = counter["v"]
        time.sleep(0.001)                       # widen the race window (a DB read/network hop in real life)
        counter["v"] = v + 1
    finally:
        if mode == "safe":
            rm.release(pid, "counter")
    return jsonify({"v": counter["v"]})


@app.route("/counter", methods=["GET", "DELETE"])
def counter_get():
    if request.method == "DELETE":
        counter.update(v=0, calls=0)
    return jsonify(counter)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
