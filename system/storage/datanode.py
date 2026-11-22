"""
DataNode of the HDFS-style file system (Milestone 12): stores chunks as files, verifies checksums, replicates.

  PUT  /chunk/<id>   body=bytes, X-Checksum=sha256, X-Forward="url2,url3"   store, then forward down the replication PIPELINE
  GET  /chunk/<id>   -> bytes (409 if the stored bytes no longer match their checksum: bit-rot detected)
  POST /replicate_from {chunk_id, source}   pull a copy from another datanode (re-replication ordered by the namenode)
  POST /corrupt/<id>   flip a byte on disk (fault injection for the checksum experiment)
Env: DN_NAME, PORT, DATA_DIR, NAMENODE_URL, PUBLIC_URL
"""
import hashlib
import os
import threading
import time

import requests
from flask import Flask, Response, jsonify, request

from system.common.config import env_int, env_str
from system.common.faults import FaultInjector

app = Flask(__name__)
DN = env_str("DN_NAME", "dn1")
PORT = env_int("PORT", 9100)
DATA_DIR = env_str("DATA_DIR", f"/tmp/datanode-{DN}")
NAMENODE = env_str("NAMENODE_URL", "http://localhost:9000").rstrip("/")
PUBLIC = env_str("PUBLIC_URL", f"http://127.0.0.1:{PORT}")
faults = FaultInjector(DN).install(app)
os.makedirs(DATA_DIR, exist_ok=True)
stats = {"chunks_written": 0, "chunks_read": 0, "bytes_in": 0, "bytes_out": 0, "checksum_failures": 0, "pulled": 0, "forwarded": 0}


def path(cid):
    return os.path.join(DATA_DIR, cid)


def sha(b):
    return hashlib.sha256(b).hexdigest()


@app.route("/chunk/<cid>", methods=["PUT"])
def put(cid):
    data = request.get_data()
    want = request.headers.get("X-Checksum")
    if want and sha(data) != want:
        return jsonify({"error": "checksum mismatch on receive"}), 400
    with open(path(cid) + ".tmp", "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(path(cid) + ".tmp", path(cid))
    open(path(cid) + ".sum", "w").write(sha(data))
    stats["chunks_written"] += 1
    stats["bytes_in"] += len(data)
    fwd = [u for u in request.headers.get("X-Forward", "").split(",") if u]
    if fwd:                                                # pipeline: pass it on to the next datanode, wait for its ack
        r = requests.put(fwd[0] + f"/chunk/{cid}", data=data, headers={"X-Checksum": sha(data), "X-Forward": ",".join(fwd[1:])}, timeout=10)
        stats["forwarded"] += 1
        if r.status_code != 200:
            return jsonify({"error": "downstream failed", "detail": r.text}), 502
    return jsonify({"ok": True, "datanode": DN})


@app.route("/chunk/<cid>", methods=["GET"])
def get(cid):
    if not os.path.exists(path(cid)):
        return jsonify({"error": "not here"}), 404
    data = open(path(cid), "rb").read()
    if sha(data) != open(path(cid) + ".sum").read():
        stats["checksum_failures"] += 1
        return jsonify({"error": "corrupt"}), 409
    stats["chunks_read"] += 1
    stats["bytes_out"] += len(data)
    return Response(data, mimetype="application/octet-stream", headers={"X-Checksum": sha(data)})


@app.route("/replicate_from", methods=["POST"])
def replicate_from():
    d = request.get_json(force=True)
    r = requests.get(d["source"] + f"/chunk/{d['chunk_id']}", timeout=10)
    if r.status_code != 200:
        return jsonify({"error": "source unavailable"}), 502
    open(path(d["chunk_id"]), "wb").write(r.content)
    open(path(d["chunk_id"]) + ".sum", "w").write(sha(r.content))
    stats["pulled"] += 1
    return jsonify({"ok": True})


@app.route("/corrupt/<cid>", methods=["POST"])
def corrupt(cid):
    b = bytearray(open(path(cid), "rb").read())
    b[len(b) // 2] ^= 0xFF
    open(path(cid), "wb").write(bytes(b))
    return jsonify({"ok": True})


@app.route("/stats")
def get_stats():
    return jsonify({"datanode": DN, **stats, "chunks": len([f for f in os.listdir(DATA_DIR) if not f.endswith((".sum", ".tmp"))])})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "datanode": DN})


def heartbeat_loop():
    while True:
        try:
            names = [f for f in os.listdir(DATA_DIR) if not f.endswith((".sum", ".tmp"))]
            used = sum(os.path.getsize(path(n)) for n in names)
            r = requests.post(NAMENODE + "/heartbeat", json={"name": DN, "url": PUBLIC, "used": used, "chunks": names}, timeout=2).json()
            for cid in r.get("delete", []):
                for suf in ("", ".sum"):
                    try:
                        os.remove(path(cid) + suf)
                    except OSError:
                        pass
        except Exception:
            pass
        time.sleep(1.0)


if __name__ == "__main__":
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
