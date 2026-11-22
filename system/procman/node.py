"""
Node agent = the per-machine process manager (Milestone 10).

    POST /spawn {"proc","sites":[...]}   create a worker OS process, register its name in the naming service
    GET  /ps                              list processes and their state
    GET  /result/<proc>                   the worker's output (404 until finished)
    POST /terminate {"proc"}              SIGTERM, unregister, reclaim
    GET  /load                            running count, cpu %, speed estimate (EWMA seconds per row), slow factor
Env: NODE_NAME, PORT, NAMING_URL (name server to register processes in), WORK_REPEAT, SLOW_FACTOR
"""
import json
import os
import signal
import subprocess
import sys
import threading
import time

import psutil
import requests
from flask import Flask, jsonify, request

from system.common.config import env_float, env_int, env_str
from system.common.faults import FaultInjector

app = Flask(__name__)
NODE_NAME = env_str("NODE_NAME", "node1")
PORT = env_int("PORT", 7400)
NAMING_URL = env_str("NAMING_URL", "")
SLOW = env_float("SLOW_FACTOR", 1.0)
OUT_DIR = env_str("OUT_DIR", f"/tmp/procman-{NODE_NAME}")
os.makedirs(OUT_DIR, exist_ok=True)
faults = FaultInjector(NODE_NAME).install(app)
procs, lock = {}, threading.Lock()
ewma = {"s_per_row": None}
SELF_URL = f"http://127.0.0.1:{PORT}"


def name_of(proc):
    return f"proc.{proc}"


def register(proc):
    if NAMING_URL:
        try:
            requests.post(NAMING_URL + "/register", json={"name": name_of(proc), "location": SELF_URL, "ttl": 60}, timeout=1.0)
        except requests.RequestException:
            pass


def unregister(proc):
    if NAMING_URL:
        try:
            requests.delete(NAMING_URL + "/unregister", params={"name": name_of(proc)}, timeout=1.0)
        except requests.RequestException:
            pass


def refresh(p):
    if p["state"] == "running" and p["popen"].poll() is not None:
        out = p["out"]
        p["state"] = "done" if (p["popen"].returncode == 0 and os.path.exists(out)) else ("terminated" if p["state"] == "terminating" else "failed")
        p["finished"] = time.time()
        if p["state"] == "done":
            r = json.load(open(out))
            if r["rows"]:
                v = (p["finished"] - p["started"]) / r["rows"]
                ewma["s_per_row"] = v if ewma["s_per_row"] is None else 0.7 * ewma["s_per_row"] + 0.3 * v
        unregister(p["proc"])


@app.route("/spawn", methods=["POST"])
def spawn():
    d = request.get_json(force=True)
    proc = d["proc"]
    out = os.path.join(OUT_DIR, proc + ".json")
    if os.path.exists(out):
        os.remove(out)
    env = dict(os.environ, SLOW_FACTOR=str(SLOW), PYTHONPATH=os.environ.get("PYTHONPATH", "."))
    p = subprocess.Popen([sys.executable, "-m", "system.procman.worker", "--proc", proc, "--sites", ",".join(d["sites"]), "--out", out],
                         env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    with lock:
        procs[proc] = {"proc": proc, "pid": p.pid, "popen": p, "state": "running", "started": time.time(), "sites": d["sites"], "out": out}
    register(proc)
    return jsonify({"proc": proc, "pid": p.pid, "node": NODE_NAME})


@app.route("/ps")
def ps():
    with lock:
        for p in procs.values():
            refresh(p)
        return jsonify({"node": NODE_NAME, "procs": [{k: v for k, v in p.items() if k not in ("popen", "out")} for p in procs.values()]})


@app.route("/result/<proc>")
def result(proc):
    with lock:
        p = procs.get(proc)
        if p is None:
            return jsonify({"error": "unknown process"}), 404
        refresh(p)
        if p["state"] != "done":
            return jsonify({"state": p["state"]}), 202
        return jsonify({"state": "done", "node": NODE_NAME, **json.load(open(p["out"]))})


@app.route("/terminate", methods=["POST"])
def terminate():
    proc = request.get_json(force=True)["proc"]
    with lock:
        p = procs.get(proc)
        if p and p["state"] == "running":
            p["state"] = "terminating"
            p["popen"].send_signal(signal.SIGTERM)
    unregister(proc)
    return jsonify({"ok": True})


@app.route("/load")
def load():
    with lock:
        for p in procs.values():
            refresh(p)
        running = sum(1 for p in procs.values() if p["state"] == "running")
    return jsonify({"node": NODE_NAME, "running": running, "cpu_percent": psutil.cpu_percent(interval=None),
                    "s_per_row": ewma["s_per_row"], "slow_factor": SLOW})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "node": NODE_NAME})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
