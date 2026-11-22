"""
Name server (Milestone 10): one node of the naming system. The same program plays every role.

    Name  ->  Name Resolution  ->  Resource Location

  Records            name -> {"location": "http://node:port", "ttl": seconds, "version": n}
  Zones (DNS/LDAP style): a server is AUTHORITATIVE for the names under its ZONE prefix ("" = root). It can DELEGATE a
  sub-zone ("ke.nai") to another server: resolving a name under a delegated zone returns a REFERRAL instead of an answer.

  GET  /resolve?name=ke.nai.NAI-01.agent  -> {"answer": {...}} | {"referral": {"zone":"ke.nai","server":url}} | 404
  GET  /who_has?name=...                  -> broadcast query: answers only if the resource is hosted AT this node
  POST /register   {"name","location","ttl"} | {"records":[...]}     DELETE /unregister?name=
  POST /delegate   {"zone","server"}
  GET  /stats  /health
Env: NODE_NAME, PORT, ZONE
"""
import threading
import time

from flask import Flask, jsonify, request

from system.common.config import env_int, env_str
from system.common.faults import FaultInjector

app = Flask(__name__)
NODE_NAME = env_str("NODE_NAME", "ns")
PORT = env_int("PORT", 7300)
ZONE = env_str("ZONE", "")
faults = FaultInjector(NODE_NAME).install(app)
records, delegations = {}, {}                      # name -> record ; zone -> server url
lock = threading.Lock()
stats = {"resolve": 0, "who_has": 0, "referrals": 0, "answers": 0, "misses": 0, "registers": 0}


def in_zone(name):
    return ZONE == "" or name == ZONE or name.startswith(ZONE + ".")


@app.route("/resolve")
def resolve():
    name = request.args["name"]
    stats["resolve"] += 1
    if not in_zone(name):
        stats["misses"] += 1
        return jsonify({"error": "not authoritative", "zone": ZONE}), 404
    with lock:
        # longest delegated prefix wins
        best = max((z for z in delegations if name == z or name.startswith(z + ".")), key=len, default=None)
        if best is not None:
            stats["referrals"] += 1
            return jsonify({"referral": {"zone": best, "server": delegations[best]}})
        rec = records.get(name)
    if rec is None:
        stats["misses"] += 1
        return jsonify({"error": "no such name"}), 404
    stats["answers"] += 1
    return jsonify({"answer": rec, "authority": NODE_NAME})


@app.route("/who_has")
def who_has():
    stats["who_has"] += 1
    with lock:
        rec = records.get(request.args["name"])
    return jsonify({"answer": rec, "node": NODE_NAME}) if rec else (jsonify({"error": "not here"}), 404)


@app.route("/register", methods=["POST"])
def register():
    d = request.get_json(force=True)
    batch = d.get("records") or [d]
    with lock:
        for r in batch:
            old = records.get(r["name"])
            records[r["name"]] = {"location": r["location"], "ttl": r.get("ttl", 30), "version": (old["version"] + 1) if old else 1}
    stats["registers"] += len(batch)
    return jsonify({"ok": True, "count": len(batch)})


@app.route("/unregister", methods=["DELETE"])
def unregister():
    with lock:
        records.pop(request.args["name"], None)
    return jsonify({"ok": True})


@app.route("/delegate", methods=["POST"])
def delegate():
    d = request.get_json(force=True)
    with lock:
        delegations[d["zone"]] = d["server"].rstrip("/")
    return jsonify({"ok": True})


@app.route("/stats")
def get_stats():
    with lock:
        return jsonify({"node": NODE_NAME, "zone": ZONE, "records": len(records), "delegations": dict(delegations), **stats})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "node": NODE_NAME})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
