"""
Transaction participant / resource manager (Milestone 5).

Owns a small ledger of per-site capacity quotas (Mbps) in SQLite and takes part in
distributed transactions run by system/txn/coordinator.py:

    Prepare -> (Pre-commit, 3PC only) -> Commit | Abort -> Completion

ACID mapping
  Atomicity    all ops of a transaction are applied in ONE SQLite transaction at commit, or not at all.
  Consistency  invariant: a quota may never go negative (checked at prepare; a violating vote is NO).
  Isolation    strict two-phase locking: a per-account lock is taken at prepare and held to commit/abort;
               a lock wait that exceeds LOCK_TIMEOUT_S makes this participant vote NO (no deadlock possible
               across participants because waiting is bounded).
  Durability   the vote is forced to the transaction log (synchronous=FULL) BEFORE it is returned;
               on restart in-doubt transactions get their locks back and are resolved.

Recovery / termination (the part that differs between protocols)
  2PC  in-doubt (PREPARED) -> ask the coordinator for the decision; if it is down, ask the other participants
       (cooperative termination). If every reachable participant is also only PREPARED, we BLOCK: this is the
       known 2PC blocking problem, and we expose how long it lasts.
  3PC  in-doubt and coordinator silent -> ask peers: any COMMITTED/PRECOMMITTED => commit,
       any ABORTED or all only PREPARED => abort. No blocking (assumes fail-stop crashes, no partitions).

Env: PART_ID, PORT, DATA_DIR, ACCOUNTS ("site-a:1000,site-b:1000"), COORD_URL, PEERS ("p1=url,p2=url"),
     LOCK_TIMEOUT_S, INDOUBT_AFTER_S (how long to wait before running termination), PREPARE_DELAY_MS (simulated work).
"""
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
PART_ID = env_str("PART_ID", "p1")
PORT = env_int("PORT", 6001)
DATA_DIR = env_str("DATA_DIR", "/tmp/txn-data")
COORD_URL = env_str("COORD_URL", "http://localhost:6000").rstrip("/")
PEERS = {k: v.rstrip("/") for k, v in parse_peers(env_str("PEERS", ""), key_type=str).items()}
LOCK_TIMEOUT_S = env_float("LOCK_TIMEOUT_S", 1.0)
INDOUBT_AFTER_S = env_float("INDOUBT_AFTER_S", 2.0)
PREPARE_DELAY_MS = env_float("PREPARE_DELAY_MS", 0.0)
faults = FaultInjector(PART_ID).install(app)

os.makedirs(DATA_DIR, exist_ok=True)
db = sqlite3.connect(os.path.join(DATA_DIR, f"{PART_ID}.sqlite3"), check_same_thread=False, timeout=30, isolation_level=None)
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=FULL")                     # Durability: fsync on every commit
db.execute("CREATE TABLE IF NOT EXISTS accounts (name TEXT PRIMARY KEY, value INTEGER NOT NULL CHECK (value >= 0))")
db.execute("CREATE TABLE IF NOT EXISTS txlog (txid TEXT PRIMARY KEY, state TEXT, ops TEXT, participants TEXT, protocol TEXT, since REAL)")
for pair in env_str("ACCOUNTS", "").split(","):
    if ":" in pair:
        n, v = pair.split(":")
        db.execute("INSERT OR IGNORE INTO accounts VALUES (?,?)", (n.strip(), int(v)))

dblock = threading.Lock()
cond = threading.Condition()
locks = {}                                                   # account -> txid holding it
stats = {"prepared": 0, "committed": 0, "aborted": 0, "votes_no": 0, "lock_timeouts": 0, "recovered_in_doubt": 0,
         "resolved_by_coordinator": 0, "resolved_by_peers": 0, "terminated_3pc": 0, "blocked_checks": 0}
blocked_since = {}                                           # txid -> time we first found ourselves blocked
START = time.time()


def log_state(txid, state, ops=None, participants=None, protocol=None):
    with dblock:
        row = db.execute("SELECT state FROM txlog WHERE txid=?", (txid,)).fetchone()
        if row is None:
            db.execute("INSERT INTO txlog VALUES (?,?,?,?,?,?)", (txid, state, json.dumps(ops or []), json.dumps(participants or []), protocol, time.time()))
        else:
            db.execute("UPDATE txlog SET state=? WHERE txid=?", (state, txid))


def get_tx(txid):
    with dblock:
        r = db.execute("SELECT txid, state, ops, participants, protocol, since FROM txlog WHERE txid=?", (txid,)).fetchone()
    return None if r is None else {"txid": r[0], "state": r[1], "ops": json.loads(r[2]), "participants": json.loads(r[3]), "protocol": r[4], "since": r[5]}


def acquire(accounts, txid):
    deadline = time.time() + LOCK_TIMEOUT_S
    with cond:
        while True:
            if all(locks.get(a) in (None, txid) for a in accounts):
                for a in accounts:
                    locks[a] = txid
                return True
            left = deadline - time.time()
            if left <= 0:
                stats["lock_timeouts"] += 1
                return False
            cond.wait(left)


def release(txid):
    with cond:
        for a in [a for a, t in locks.items() if t == txid]:
            del locks[a]
        cond.notify_all()


@app.route("/prepare", methods=["POST"])
def prepare():
    d = request.get_json(force=True)
    txid, ops = d["txid"], d["ops"]
    existing = get_tx(txid)
    if existing:                                           # idempotent re-delivery
        return jsonify({"vote": "no" if existing["state"] == "ABORTED" else "yes", "state": existing["state"]})
    if PREPARE_DELAY_MS:
        time.sleep(PREPARE_DELAY_MS / 1000.0)
    accounts = sorted({o["account"] for o in ops})          # sorted acquisition order
    if not acquire(accounts, txid):
        log_state(txid, "ABORTED", ops, d.get("participants"), d.get("protocol"))
        stats["votes_no"] += 1
        return jsonify({"vote": "no", "reason": "lock-timeout"})
    with dblock:
        for o in ops:                                       # consistency check against current + pending effect
            row = db.execute("SELECT value FROM accounts WHERE name=?", (o["account"],)).fetchone()
            delta_total = sum(x["delta"] for x in ops if x["account"] == o["account"])
            if row is None or row[0] + delta_total < 0:
                bad = "unknown-account" if row is None else "would-go-negative"
                break
        else:
            bad = None
    if bad:
        release(txid)
        log_state(txid, "ABORTED", ops, d.get("participants"), d.get("protocol"))
        stats["votes_no"] += 1
        return jsonify({"vote": "no", "reason": bad})
    log_state(txid, "PREPARED", ops, d.get("participants"), d.get("protocol"))   # force-write BEFORE voting
    stats["prepared"] += 1
    return jsonify({"vote": "yes", "state": "PREPARED"})


@app.route("/precommit", methods=["POST"])
def precommit():
    txid = request.get_json(force=True)["txid"]
    t = get_tx(txid)
    if t is None or t["state"] == "ABORTED":
        return jsonify({"ack": False, "state": t["state"] if t else None})
    if t["state"] == "PREPARED":
        log_state(txid, "PRECOMMITTED")
    return jsonify({"ack": True, "state": "PRECOMMITTED" if t["state"] != "COMMITTED" else "COMMITTED"})


def apply_commit(txid):
    t = get_tx(txid)
    if t is None or t["state"] == "COMMITTED":
        return True
    if t["state"] == "ABORTED":
        return False
    with dblock:
        db.execute("BEGIN IMMEDIATE")                       # atomic: all ops + state change together
        try:
            for o in t["ops"]:
                db.execute("UPDATE accounts SET value = value + ? WHERE name=?", (o["delta"], o["account"]))
            db.execute("UPDATE txlog SET state='COMMITTED' WHERE txid=?", (txid,))
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise
    release(txid)
    stats["committed"] += 1
    blocked_since.pop(txid, None)
    return True


def apply_abort(txid):
    t = get_tx(txid)
    if t is None:
        log_state(txid, "ABORTED")                          # presumed abort: remember it so a late PREPARE is refused
    elif t["state"] == "COMMITTED":
        return False
    elif t["state"] != "ABORTED":
        log_state(txid, "ABORTED")
        stats["aborted"] += 1
    release(txid)
    blocked_since.pop(txid, None)
    return True


@app.route("/commit", methods=["POST"])
def commit():
    ok = apply_commit(request.get_json(force=True)["txid"])
    return jsonify({"ok": ok, "state": "COMMITTED" if ok else "ABORTED"})


@app.route("/abort", methods=["POST"])
def abort():
    ok = apply_abort(request.get_json(force=True)["txid"])
    return jsonify({"ok": ok})


@app.route("/txstate/<txid>")
def txstate(txid):
    """State of a transaction here. With ?final=1 a participant that has never seen the transaction
    records ABORTED, so a late PREPARE is refused and the asker may safely abort (cooperative termination)."""
    t = get_tx(txid)
    if t is None and request.args.get("final") == "1":
        log_state(txid, "ABORTED")
        return jsonify({"part": PART_ID, "state": "ABORTED"})
    return jsonify({"part": PART_ID, "state": t["state"] if t else None})


@app.route("/balance")
def balance():
    with dblock:
        rows = db.execute("SELECT name, value FROM accounts ORDER BY name").fetchall()
    return jsonify({"part": PART_ID, "accounts": dict(rows), "total": sum(v for _, v in rows)})


@app.route("/state")
def state():
    with dblock:
        indoubt = db.execute("SELECT txid, state, since FROM txlog WHERE state IN ('PREPARED','PRECOMMITTED')").fetchall()
        counts = dict(db.execute("SELECT state, COUNT(*) FROM txlog GROUP BY state").fetchall())
    now = time.time()
    return jsonify({"part": PART_ID, "stats": dict(stats), "log_counts": counts, "locks_held": dict(locks),
                    "in_doubt": [{"txid": t, "state": s, "age_s": round(now - since, 2)} for t, s, since in indoubt],
                    "blocked": {t: round(now - s, 2) for t, s in blocked_since.items()}})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "part": PART_ID})


# ------------------------------------------------------------------ recovery & termination
def recover_locks():
    """After a restart, in-doubt transactions must keep their locks until resolved."""
    with dblock:
        rows = db.execute("SELECT txid, ops FROM txlog WHERE state IN ('PREPARED','PRECOMMITTED')").fetchall()
    for txid, ops in rows:
        acquire(sorted({o["account"] for o in json.loads(ops)}), txid)
        stats["recovered_in_doubt"] += 1


def ask(url, path, payload=None, timeout=1.0):
    try:
        r = requests.get(url + path, timeout=timeout) if payload is None else requests.post(url + path, json=payload, timeout=timeout)
        return r.json()
    except Exception:
        return None


def resolve(t):
    txid, protocol = t["txid"], t["protocol"]
    dec = ask(COORD_URL, f"/decision/{txid}")
    if dec and dec.get("decision") in ("COMMIT", "ABORT"):
        (apply_commit if dec["decision"] == "COMMIT" else apply_abort)(txid)
        stats["resolved_by_coordinator"] += 1
        return
    if dec and dec.get("decision") == "PENDING":
        return                                              # coordinator is alive and still working on it
    # coordinator unreachable (or has no record): cooperative termination with the peers
    states = []
    for pid, url in PEERS.items():
        r = ask(url, f"/txstate/{txid}?final=1")
        if r is not None:
            states.append(r["state"])
    if "COMMITTED" in states:
        apply_commit(txid); stats["resolved_by_peers"] += 1; return
    if "ABORTED" in states:                                  # includes peers that had never seen the txn (they now refuse it)
        apply_abort(txid); stats["resolved_by_peers"] += 1; return
    if protocol == "3pc":
        if "PRECOMMITTED" in states or t["state"] == "PRECOMMITTED":
            apply_commit(txid)                               # everyone voted yes and a pre-commit was issued
        else:
            apply_abort(txid)                                # nobody saw pre-commit: coordinator cannot have committed
        stats["terminated_3pc"] += 1
        return
    # 2PC: every reachable participant is merely PREPARED and the coordinator is gone -> BLOCKED
    blocked_since.setdefault(txid, time.time())
    stats["blocked_checks"] += 1


def resolver_loop():
    time.sleep(1.0)
    recover_locks()
    while True:
        time.sleep(0.5)
        with dblock:
            rows = db.execute("SELECT txid FROM txlog WHERE state IN ('PREPARED','PRECOMMITTED')").fetchall()
        for (txid,) in rows:
            t = get_tx(txid)
            if t and t["state"] in ("PREPARED", "PRECOMMITTED") and time.time() - t["since"] >= INDOUBT_AFTER_S:
                try:
                    resolve(t)
                except Exception as e:
                    print(f"[{PART_ID}] resolve error {e}", flush=True)


if __name__ == "__main__":
    threading.Thread(target=resolver_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
