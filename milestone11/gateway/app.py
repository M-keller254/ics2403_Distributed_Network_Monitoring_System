"""
Transparency gateway - Milestone 9.

Fronts the two Milestone 5 store replicas with ONE stable read path,
GET /read/<key>, so a client never needs to know:
  - which replica holds the answer (location transparency)
  - that there are two replicas at all (replication transparency)
  - that the first one it tried was down (failure transparency - the
    gateway retries the next replica automatically)

Writes still go through the existing txn-coordinator (/write), since
that is where the Two-Phase Commit guarantee lives; this gateway only
adds transparency to the READ path, which Milestone 5 did not cover.
"""
import os, time
import requests
from flask import Flask, jsonify

app = Flask(__name__)
REPLICAS = [u.strip() for u in os.environ.get("REPLICAS", "http://store-a:7001,http://store-b:7002").split(",") if u.strip()]
NAME_SERVER = os.environ.get("NAME_SERVER")          # optional, added in Milestone 10
LOGICAL_NAMES = [n.strip() for n in os.environ.get("LOGICAL_NAMES", "").split(",") if n.strip()]


def discover_replicas():
    """Milestone 10: resolve replica locations by logical name via the
    name server if one is configured, instead of a fixed env-var list.
    Falls back to REPLICAS if the name server is unreachable, so the
    gateway degrades gracefully rather than failing outright."""
    if not (NAME_SERVER and LOGICAL_NAMES):
        return REPLICAS
    urls = []
    for name in LOGICAL_NAMES:
        try:
            r = requests.get(f"{NAME_SERVER}/resolve/{name}", timeout=1).json()
            if r.get("alive"):
                urls.append(r["url"])
        except requests.exceptions.RequestException:
            continue
    return urls or REPLICAS


@app.route("/read/<key>", methods=["GET"])
def read(key):
    t0 = time.time()
    tried = []
    for base in discover_replicas():
        tried.append(base)
        try:
            r = requests.get(f"{base}/data", timeout=1.5)
            if r.status_code == 200:
                committed = r.json().get("committed", {})
                if key in committed:
                    return jsonify({"key": key, "value": committed[key],
                                    "_served_by": base, "_tried": len(tried),
                                    "_gateway_overhead_s": round(time.time() - t0, 4)}), 200
        except requests.exceptions.RequestException:
            continue
    return jsonify({"error": "not found or no replica available", "_tried": tried}), 404


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
