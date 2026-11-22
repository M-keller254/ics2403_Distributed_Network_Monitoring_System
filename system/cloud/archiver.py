"""
Cloud-tier archiver (Milestone 12): core.processed topic -> distributed storage.

    Users/Devices -> Edge gateway -> [Kafka edge.telemetry] -> Core (leader) -> [Kafka core.processed] -> ARCHIVER -> storage

Consumes the rows the core has validated and writes them, partitioned like a data lake, as gzip CSV objects:

    telemetry/site=NAI-01/date=2026-09-01/p0-o120-o179.csv.gz

The object name is derived from the Kafka partition and offset range, so re-processing the same offsets after a crash
OVERWRITES the same object (idempotent) instead of creating a copy. Batches that overlap partially after a restart can
still duplicate individual rows, so readers de-duplicate on record_id (the verify endpoint does exactly that).

STORAGE=dfs     write to the HDFS-style file system (namenode, 3 replicas)
STORAGE=object  write to S3-compatible object storage (MinIO in docker-compose; a directory in development)
STORAGE=both    write to both (default)
Endpoints: GET /stats  GET /verify (distinct rows in storage)  GET /health
"""
import csv
import gzip
import io
import json
import os
import threading
import time
from collections import defaultdict

from flask import Flask, jsonify

from system.common.bus import get_bus
from system.common.config import env_int, env_str
from system.common.metrics import summarize
from system.common.objstore import get_store
from system.storage.dfs_client import DfsClient

app = Flask(__name__)
PORT = env_int("PORT", 5003)
TOPIC = env_str("ARCHIVE_TOPIC", "core.processed")
GROUP = env_str("ARCHIVER_GROUP", "archiver")
STORAGE = env_str("STORAGE", "both")
NAMENODE = env_str("NAMENODE_URL", "http://localhost:9000")
COLS = ["record_id", "timestamp", "site_id", "region", "edge_node", "site_type", "cpu_pct", "mem_pct", "rx_mbps",
        "tx_mbps", "latency_ms", "jitter_ms", "packet_loss_pct", "active_users", "signal_dbm", "temperature_c", "is_anomaly"]
stats = {"messages": 0, "rows": 0, "objects_written": 0, "bytes_written": 0, "write_errors": 0, "commits": 0, "last_error": None}
write_ms = []
dfs = store = None


def encode(rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(COLS)
    for r in rows:
        w.writerow([r[c] for c in COLS])
    return gzip.compress(buf.getvalue().encode(), 3)


def write_object(key, data):
    t0 = time.perf_counter()
    if STORAGE in ("dfs", "both"):
        dfs.put("/" + key, data)
    if STORAGE in ("object", "both"):
        store.put(key, data)
    write_ms.append((time.perf_counter() - t0) * 1000)
    stats["objects_written"] += 1
    stats["bytes_written"] += len(data)


def consume():
    global dfs, store
    if STORAGE in ("dfs", "both"):
        dfs = DfsClient(NAMENODE)
    if STORAGE in ("object", "both"):
        store = get_store()
    bus = get_bus()
    bus.ensure_topic(TOPIC, env_int("KAFKA_PARTITIONS", 3))
    cons = bus.consumer(TOPIC, GROUP)
    while True:
        try:
            recs = cons.poll(200, 0.5)
            if not recs:
                continue
            groups = defaultdict(list)                 # (partition, site, date) -> [rows], offsets
            span = {}
            for rec in recs:
                for r in rec.value["rows"]:
                    k = (rec.partition, r["site_id"], r["timestamp"][:10])
                    groups[k].append(r)
                    lo, hi = span.get(k, (rec.offset, rec.offset))
                    span[k] = (min(lo, rec.offset), max(hi, rec.offset))
            for (part, site, date), rows in groups.items():
                lo, hi = span[(part, site, date)]
                write_object(f"telemetry/site={site}/date={date}/p{part}-o{lo}-o{hi}.csv.gz", encode(rows))
            cons.commit()                              # only after every object of this poll is durably stored
            stats["messages"] += len(recs)
            stats["rows"] += sum(len(rec.value["rows"]) for rec in recs)
            stats["commits"] += 1
        except Exception as e:
            stats["write_errors"] += 1
            stats["last_error"] = f"{type(e).__name__}: {e}"
            time.sleep(1)


def read_all_ids(backend):
    ids, n_obj = set(), 0
    if backend == "dfs":
        for f in DfsClient(NAMENODE).ls("/telemetry/"):
            data = DfsClient(NAMENODE).get(f["path"])
            n_obj += 1
            ids.update(int(r["record_id"]) for r in csv.DictReader(io.StringIO(gzip.decompress(data).decode())))
    else:
        st = get_store()
        for k in st.list("telemetry/"):
            n_obj += 1
            ids.update(int(r["record_id"]) for r in csv.DictReader(io.StringIO(gzip.decompress(st.get(k)).decode())))
    return ids, n_obj


@app.route("/verify")
def verify():
    out = {}
    for b in (["dfs"] if STORAGE in ("dfs", "both") else []) + (["object"] if STORAGE in ("object", "both") else []):
        ids, n = read_all_ids(b)
        out[b] = {"objects": n, "distinct_record_ids": len(ids), "min_id": min(ids) if ids else None, "max_id": max(ids) if ids else None}
    return jsonify(out)


@app.route("/stats")
def get_stats():
    return jsonify({**stats, "storage": STORAGE, "write_ms": summarize(write_ms)})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "tier": "cloud-archiver"})


if __name__ == "__main__":
    threading.Thread(target=consume, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
