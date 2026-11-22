"""
Replicated key-value state store: one replica (Milestones 7, 9, 11, 12).

A versioned, durable (SQLite, synchronous=FULL) map. Several replicas hold copies; clients write to a write quorum
and read from a read quorum (system/replication/client.py). A replica that was down catches up by anti-entropy:
it periodically compares version digests with its peers and pulls every entry that is newer than its own.

  PUT  /kv/<key>   {"value":..., "version": int, "writer": str}  accept only if (version, writer) is newer (last-writer-wins)
  GET  /kv/<key>   -> {"value","version","checksum"}             404 if absent
  GET  /digest     -> {key: version}                              (anti-entropy)
  POST /pull       {"keys":[...]} -> entries                      (anti-entropy transfer)
  GET  /dump       -> every entry                                 (state transfer for migration / new replica)
  POST /load       {"entries":[...]}                              (install a state transfer)
  DELETE /kv/<key>   (migration: drop a key moved elsewhere)
  GET  /stats  /health  /fault (fault injection, see common/faults.py)
Env: REPLICA_ID, PORT, DATA_DIR, PEERS "r2=http://...,r3=http://...", ANTI_ENTROPY_S (default 1.0; 0 = off)
"""
import hashlib
import json
import os
import sqlite3
import threading
import time

import requests
from flask import Flask, jsonify, request

from system.common.config import env_float, env_int, env_str, parse_peers
from system.common.faults import FaultInjector

app = Flask(__name__)
REPLICA_ID = env_str("REPLICA_ID", "r1")
PORT = env_int("PORT", 7100)
DATA_DIR = env_str("DATA_DIR", "/tmp/replica-data")
PEERS = {k: v.rstrip("/") for k, v in parse_peers(env_str("PEERS", ""), key_type=str).items() if k != REPLICA_ID}
ANTI_ENTROPY_S = env_float("ANTI_ENTROPY_S", 1.0)
faults = FaultInjector(REPLICA_ID).install(app)

os.makedirs(DATA_DIR, exist_ok=True)
db = sqlite3.connect(os.path.join(DATA_DIR, f"{REPLICA_ID}.sqlite3"), check_same_thread=False, timeout=30, isolation_level=None)
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=FULL")
db.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, version INTEGER, writer TEXT, ts REAL)")
lock = threading.Lock()
stats = {"puts": 0, "stale_puts_ignored": 0, "gets": 0, "pulled_entries": 0, "anti_entropy_rounds": 0, "loaded": 0}
START = time.time()


def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:16]


TOMBSTONE = {"__tombstone__": True}


def is_tombstone(value):
    return isinstance(value, dict) and value.get("__tombstone__") is True


def row_to_entry(r):
    return {"key": r[0], "value": json.loads(r[1]), "version": r[2], "writer": r[3], "ts": r[4]}


def upsert(key, value, version, writer):
    with lock:
        cur = db.execute("SELECT version, writer FROM kv WHERE key=?", (key,)).fetchone()
        if cur is not None and (cur[0], cur[1]) >= (version, writer):
            stats["stale_puts_ignored"] += 1
            return False, cur[0]
        db.execute("INSERT OR REPLACE INTO kv VALUES (?,?,?,?,?)", (key, json.dumps(value), version, writer, time.time()))
        return True, version


@app.route("/kv/<path:key>", methods=["PUT"])
def put(key):
    d = request.get_json(force=True)
    applied, ver = upsert(key, d["value"], int(d["version"]), d.get("writer", ""))
    stats["puts"] += 1
    return jsonify({"ok": True, "applied": applied, "version": ver, "replica": REPLICA_ID})


@app.route("/kv/<path:key>", methods=["GET"])
def get(key):
    with lock:
        r = db.execute("SELECT key, value, version, writer, ts FROM kv WHERE key=?", (key,)).fetchone()
    stats["gets"] += 1
    if r is None:
        return jsonify({"error": "not found", "replica": REPLICA_ID}), 404
    e = row_to_entry(r)
    if is_tombstone(e["value"]):
        return jsonify({"error": "not found", "replica": REPLICA_ID, "deleted": True}), 404
    value = faults.corrupt(e["value"])                      # Byzantine replica lies, and signs its lie
    return jsonify({"value": value, "version": e["version"], "checksum": checksum(value), "replica": REPLICA_ID})


@app.route("/kv/<path:key>", methods=["DELETE"])
def delete(key):
    """Deletes are *versioned tombstones*, not row removals. A plain DELETE is undone by anti-entropy: a peer that still holds the
    key looks 'newer' than our empty slot and pushes it straight back (found in M9: retiring a migrated namespace left 12 resurrected copies)."""
    upsert(key, TOMBSTONE, int(time.time() * 1e6), f"del-{REPLICA_ID}")
    return jsonify({"ok": True})


@app.route("/digest")
def digest():
    with lock:
        rows = db.execute("SELECT key, version FROM kv").fetchall()
    return jsonify(dict(rows))


@app.route("/pull", methods=["POST"])
def pull():
    keys = request.get_json(force=True)["keys"]
    with lock:
        rows = [db.execute("SELECT key, value, version, writer, ts FROM kv WHERE key=?", (k,)).fetchone() for k in keys]
    return jsonify({"entries": [row_to_entry(r) for r in rows if r]})


@app.route("/dump")
def dump():
    """?live=1 hides tombstones (what a migration copies / what an operator wants to see); the default keeps them (what anti-entropy needs)."""
    with lock:
        rows = db.execute("SELECT key, value, version, writer, ts FROM kv").fetchall()
    ents = [row_to_entry(r) for r in rows]
    if request.args.get("live"):
        ents = [e for e in ents if not is_tombstone(e["value"])]
    return jsonify({"entries": ents})


@app.route("/load", methods=["POST"])
def load():
    n = 0
    for e in request.get_json(force=True)["entries"]:
        if upsert(e["key"], e["value"], e["version"], e.get("writer", ""))[0]:
            n += 1
    stats["loaded"] += n
    return jsonify({"loaded": n})


@app.route("/stats")
def get_stats():
    with lock:
        n = db.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
        dead = db.execute("SELECT COUNT(*) FROM kv WHERE value=?", (json.dumps(TOMBSTONE),)).fetchone()[0]
    return jsonify({"replica": REPLICA_ID, "keys": n - dead, "tombstones": dead, **stats, "uptime_s": round(time.time() - START, 1)})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "replica": REPLICA_ID})


def anti_entropy_loop():
    """Pull what we are missing: compare digests with each peer, fetch strictly newer entries."""
    time.sleep(0.5)
    while True:
        time.sleep(ANTI_ENTROPY_S)
        for pid, url in PEERS.items():
            try:
                theirs = requests.get(url + "/digest", timeout=1.0, headers={"X-From": REPLICA_ID}).json()
                with lock:
                    mine = dict(db.execute("SELECT key, version FROM kv").fetchall())
                need = [k for k, v in theirs.items() if v > mine.get(k, -1)]
                if need:
                    ents = requests.post(url + "/pull", json={"keys": need}, timeout=2.0, headers={"X-From": REPLICA_ID}).json()["entries"]
                    for e in ents:
                        if upsert(e["key"], e["value"], e["version"], e.get("writer", ""))[0]:
                            stats["pulled_entries"] += 1
            except Exception:
                continue
        stats["anti_entropy_rounds"] += 1


if __name__ == "__main__":
    if ANTI_ENTROPY_S > 0:
        threading.Thread(target=anti_entropy_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
