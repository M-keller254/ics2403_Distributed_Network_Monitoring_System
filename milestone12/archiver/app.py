"""
Milestone 12 - Distributed storage.

Periodically snapshots the collector's /status (the monitoring data
built up since Milestone 1) to MinIO, an S3-compatible distributed
object store, so monitoring history survives past whatever the
collector currently holds in memory. Exposes /history to read
snapshots back, demonstrating genuine storage AND retrieval, not just
a write-and-forget archive.

    Users / Devices -> Edge -> Telecom Core -> Cloud -> Distributed Storage
                                                  ^              ^
                                              collector      this service
"""
import os, io, json, time, threading
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify
from minio import Minio
from minio.error import S3Error

COLLECTOR_URL = os.environ.get("COLLECTOR_URL", "http://collector:5000/status")
SNAPSHOT_INTERVAL = float(os.environ.get("SNAPSHOT_INTERVAL", "10"))
BUCKET = os.environ.get("BUCKET", "monitoring-data")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

app = Flask(__name__)
client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY, secure=False)

_stats_lock = threading.Lock()
_stats = {"snapshots_written": 0, "write_failed": 0, "last_snapshot_at": None}


def log(msg):
    print(f"[archiver] {msg}", flush=True)


def ensure_bucket():
    for attempt in range(10):
        try:
            if not client.bucket_exists(BUCKET):
                client.make_bucket(BUCKET)
                log(f"created bucket '{BUCKET}'")
            return
        except S3Error as e:
            log(f"MinIO not ready yet ({e}); retrying...")
            time.sleep(2)
    raise SystemExit("Could not reach MinIO after 10 attempts")


def snapshot_key(ts):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return f"{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H-%M-%S')}.json"


def snapshot_loop():
    while True:
        time.sleep(SNAPSHOT_INTERVAL)
        try:
            status = requests.get(COLLECTOR_URL, timeout=3).json()
        except requests.exceptions.RequestException as e:
            log(f"could not reach collector: {e}")
            continue
        payload = json.dumps({"captured_at": time.time(), "status": status}).encode()
        key = snapshot_key(time.time())
        try:
            client.put_object(BUCKET, key, io.BytesIO(payload), length=len(payload),
                               content_type="application/json")
            with _stats_lock:
                _stats["snapshots_written"] += 1
                _stats["last_snapshot_at"] = time.time()
            log(f"wrote snapshot {key} ({len(payload)} bytes)")
        except S3Error as e:
            with _stats_lock:
                _stats["write_failed"] += 1
            log(f"FAILED to write snapshot: {e}")


@app.route("/history", methods=["GET"])
def history():
    """List every stored snapshot key - proves retrieval works, not just storage."""
    try:
        objs = client.list_objects(BUCKET, recursive=True)
        keys = sorted(o.object_name for o in objs)
    except S3Error as e:
        return jsonify({"error": str(e)}), 503
    return jsonify({"bucket": BUCKET, "count": len(keys), "snapshots": keys}), 200


@app.route("/history/<path:key>", methods=["GET"])
def read_snapshot(key):
    try:
        resp = client.get_object(BUCKET, key)
        data = json.loads(resp.read())
        resp.close(); resp.release_conn()
    except S3Error as e:
        return jsonify({"error": str(e)}), 404
    return jsonify(data), 200


@app.route("/stats", methods=["GET"])
def stats():
    with _stats_lock:
        return jsonify(dict(_stats)), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    ensure_bucket()
    threading.Thread(target=snapshot_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "9500")))
