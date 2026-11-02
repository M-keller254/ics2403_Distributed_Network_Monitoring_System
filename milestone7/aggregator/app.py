"""
Aggregator service (core tier) - Milestones 3 and 4.

Milestone 3: sits between the edge nodes and the cloud collector. It
receives edge reports, and every FLUSH_INTERVAL seconds forwards ONE
regional summary upstream instead of one message per edge node.

Milestone 4: several aggregators can run side by side. Edge nodes send
to all of them (so every aggregator holds fresh data), but only the
LEADER forwards to the cloud. Leadership uses a heartbeat-based
Bully-style rule: the alive aggregator with the highest ID is leader.
An aggregator is "alive" if its heartbeat arrived within HB_TIMEOUT.

Config (environment variables):
  AGG_ID          unique integer ID of this aggregator (default 1)
  PORT            port to listen on (default 6000)
  PEERS           comma-separated base URLs of the other aggregators
  CLOUD_URL       cloud collector /report URL
  HB_INTERVAL     seconds between heartbeats (default 1)
  HB_TIMEOUT      seconds without heartbeat before a peer is dead (default 3)
  FLUSH_INTERVAL  seconds between forwards to the cloud (default 5)
"""
import os
import time
import threading
import statistics
import requests
from flask import Flask, request, jsonify

AGG_ID = int(os.environ.get("AGG_ID", "1"))
PORT = int(os.environ.get("PORT", "6000"))
PEERS = [p.strip() for p in os.environ.get("PEERS", "").split(",") if p.strip()]
CLOUD_URL = os.environ.get("CLOUD_URL", "http://collector:5000/report")
HB_INTERVAL = float(os.environ.get("HB_INTERVAL", "1"))
HB_TIMEOUT = float(os.environ.get("HB_TIMEOUT", "3"))
FLUSH_INTERVAL = float(os.environ.get("FLUSH_INTERVAL", "5"))
STALE_AFTER = 15.0          # ignore edge readings older than this

app = Flask(__name__)
lock = threading.Lock()
START = time.time()
S = {
    "latest": {},           # edge node name -> latest report
    "last_seen": {},        # peer id -> time of last heartbeat
    "term": 0, "last_leader": None,
    "edge_received": 0, "hb_sent": 0, "hb_failed": 0, "hb_received": 0,
    "forwarded": 0, "forward_failed": 0,
}


def log(msg):
    print(f"[aggregator-{AGG_ID}] {msg}", flush=True)


def evaluate():
    """Return (leader_id, alive_ids, term). Leader = highest alive ID."""
    now = time.time()
    with lock:
        alive = [AGG_ID] + [i for i, t in S["last_seen"].items() if now - t <= HB_TIMEOUT]
        leader = max(alive)
        if now - START < HB_TIMEOUT:      # startup grace: learn peers first
            return None, sorted(alive), S["term"]
        if leader != S["last_leader"]:
            S["term"] += 1
            log(f"leader change: {S['last_leader']} -> {leader} "
                f"(term {S['term']}, alive={sorted(alive)})")
            S["last_leader"] = leader
        return leader, sorted(alive), S["term"]


@app.route("/report", methods=["POST"])
def report():
    d = request.get_json(silent=True)
    if not d or "node" not in d:
        return jsonify({"error": "invalid payload"}), 400
    d["received_ts"] = time.time()
    with lock:
        S["latest"][d["node"]] = d
        S["edge_received"] += 1
    return jsonify({"status": "ok", "aggregator": AGG_ID}), 200


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    d = request.get_json(silent=True) or {}
    if "id" in d:
        with lock:
            S["last_seen"][int(d["id"])] = time.time()
            S["hb_received"] += 1
    return jsonify({"status": "ok"}), 200


@app.route("/leader", methods=["GET"])
def leader():
    lid, alive, term = evaluate()
    with lock:
        counters = {k: S[k] for k in ("edge_received", "hb_sent", "hb_failed",
                                       "hb_received", "forwarded", "forward_failed")}
    return jsonify({"id": AGG_ID, "leader_id": lid, "is_leader": lid == AGG_ID,
                    "alive": alive, "term": term, **counters}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "id": AGG_ID}), 200


def heartbeat_loop():
    while True:
        for peer in PEERS:
            try:
                requests.post(peer + "/heartbeat", json={"id": AGG_ID}, timeout=0.5)
                with lock:
                    S["hb_sent"] += 1
            except requests.exceptions.RequestException:
                with lock:
                    S["hb_failed"] += 1
        time.sleep(HB_INTERVAL)


def flush_loop():
    while True:
        time.sleep(FLUSH_INTERVAL)
        lid, alive, term = evaluate()
        if lid != AGG_ID:
            continue                      # standby: hold data, do not forward
        now = time.time()
        with lock:
            fresh = {n: r for n, r in S["latest"].items()
                     if now - r["received_ts"] <= STALE_AFTER}
        if not fresh:
            continue
        payload = {
            "node": "edge-region", "role": "aggregate",
            "cpu": round(statistics.mean(r["cpu"] for r in fresh.values()), 2),
            "mem": round(statistics.mean(r["mem"] for r in fresh.values()), 2),
            "window_size": len(fresh), "sent_at": time.time(),
            "forwarded_by": AGG_ID, "leader_term": term,
            "members": {n: {"cpu": r["cpu"], "mem": r["mem"]} for n, r in fresh.items()},
        }
        try:
            requests.post(CLOUD_URL, json=payload, timeout=3)
            with lock:
                S["forwarded"] += 1
            log(f"forwarded regional summary ({len(fresh)} edge nodes) "
                f"cpu={payload['cpu']}% mem={payload['mem']}%")
        except requests.exceptions.RequestException as e:
            with lock:
                S["forward_failed"] += 1
            log(f"FAILED to forward to cloud: {e}")


if __name__ == "__main__":
    log(f"starting, peers={PEERS or 'none'}, cloud={CLOUD_URL}")
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=flush_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)
