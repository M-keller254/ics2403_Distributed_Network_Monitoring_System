"""
Two-Phase Commit coordinator - Milestone 5.

Model:  Prepare -> Commit -> Completion  (or Prepare -> Abort on any NO vote)

POST /write {"key": ..., "value": ...}
  1. PREPARE is sent to every participant in PARTICIPANTS.
  2. If ALL vote YES, COMMIT is sent to all (transaction succeeds).
  3. If ANY votes NO or is unreachable, ABORT is sent to all that voted YES
     (transaction fails, no partial writes are left behind).
Returns timing and outcome for each transaction so experiments can
measure transaction latency, throughput and failure behaviour.
"""
import os, time, uuid, threading
import requests
from flask import Flask, request, jsonify

PARTICIPANTS = [p.strip() for p in os.environ.get("PARTICIPANTS", "").split(",") if p.strip()]
app = Flask(__name__)
lock = threading.Lock()
STATS = {"committed": 0, "aborted": 0, "total_latency": 0.0}


def log(msg):
    print(f"[txn-coordinator] {msg}", flush=True)


@app.route("/write", methods=["POST"])
def write():
    d = request.get_json(silent=True) or {}
    key, value = d.get("key"), d.get("value")
    txn_id = str(uuid.uuid4())[:8]
    t0 = time.time()

    votes = {}
    for p in PARTICIPANTS:
        try:
            r = requests.post(p + "/prepare", json={"txn_id": txn_id, "key": key, "value": value}, timeout=2)
            votes[p] = (r.status_code == 200 and r.json().get("vote") == "YES")
        except requests.exceptions.RequestException:
            votes[p] = False

    all_yes = all(votes.values()) and len(votes) == len(PARTICIPANTS)
    decision = "COMMIT" if all_yes else "ABORT"
    log(f"txn {txn_id} PREPARE votes={votes} -> {decision}")

    for p, voted_yes in votes.items():
        if not voted_yes and decision == "ABORT":
            continue   # nothing was prepared there
        try:
            requests.post(p + ("/commit" if decision == "COMMIT" else "/abort"),
                          json={"txn_id": txn_id}, timeout=2)
        except requests.exceptions.RequestException as e:
            log(f"txn {txn_id}: failed to send {decision} to {p}: {e}")

    latency = time.time() - t0
    with lock:
        STATS["committed" if decision == "COMMIT" else "aborted"] += 1
        STATS["total_latency"] += latency

    return jsonify({"txn_id": txn_id, "decision": decision, "votes": votes,
                    "latency_s": round(latency, 4)}), (200 if decision == "COMMIT" else 409)


@app.route("/stats", methods=["GET"])
def stats():
    with lock:
        n = STATS["committed"] + STATS["aborted"]
        avg = STATS["total_latency"] / n if n else 0.0
        return jsonify({**STATS, "total": n, "avg_latency_s": round(avg, 4)}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "participants": PARTICIPANTS}), 200


if __name__ == "__main__":
    print(f"[txn-coordinator] starting, participants={PARTICIPANTS}", flush=True)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "7000")))
