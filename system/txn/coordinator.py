"""
Transaction coordinator (Milestone 5): Two-Phase Commit and Three-Phase Commit.

    POST /txn   {"ops":[{"participant":"p1","account":"site-a","delta":-50}, ...], "protocol":"2pc"|"3pc"}
    GET  /decision/<txid>     used by in-doubt participants (COMMIT | ABORT | PENDING)
    POST /fault_point         {"crash_at": "after_prepare"|"mid_precommit"|"after_precommit"|"after_decision_log",
                               "skip_phase2_to": ["p2"], "count": 1}   (fault injection for the experiments)
    GET  /stats  /log  /health

Protocol          2PC:  PREPARE -> votes -> log decision -> COMMIT/ABORT -> acks
                  3PC:  PREPARE -> votes -> PRECOMMIT -> acks -> log decision -> COMMIT -> acks
The coordinator forces its decision to disk BEFORE telling anyone (the commit point), keeps retrying
unacknowledged participants from a background thread, and on restart finishes whatever it was doing:
  - decision logged, not yet delivered   -> re-send it
  - 2PC, no decision logged              -> ABORT (nobody can have committed)
  - 3PC, no decision logged              -> ask the participants' states and apply the 3PC termination rule
"""
import json
import os
import sqlite3
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, jsonify, request

from system.common.config import env_float, env_int, env_str, parse_peers
from system.common.faults import FaultInjector
from system.common.metrics import summarize

app = Flask(__name__)
PORT = env_int("PORT", 6000)
DATA_DIR = env_str("DATA_DIR", "/tmp/txn-coord")
PARTS = {k: v.rstrip("/") for k, v in parse_peers(env_str("PARTICIPANTS", ""), key_type=str).items()}
DEFAULT_PROTOCOL = env_str("PROTOCOL", "2pc")
PREPARE_MODE = env_str("PREPARE_MODE", "parallel")        # parallel | ordered (see run_txn)
PHASE_TIMEOUT_S = env_float("PHASE_TIMEOUT_S", 2.0)
faults = FaultInjector("txn-coordinator").install(app)

os.makedirs(DATA_DIR, exist_ok=True)
db = sqlite3.connect(os.path.join(DATA_DIR, "coordinator.sqlite3"), check_same_thread=False, timeout=30, isolation_level=None)
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=FULL")
db.execute("CREATE TABLE IF NOT EXISTS coordlog (txid TEXT PRIMARY KEY, protocol TEXT, participants TEXT, phase TEXT, decision TEXT, done INTEGER DEFAULT 0, ts REAL)")
dblock = threading.Lock()
pool = ThreadPoolExecutor(64)
active = set()
tls = threading.local()
fault_point = {"crash_at": None, "skip_phase2_to": [], "count": 0}
stats = {"started": 0, "committed": 0, "aborted": 0, "resent": 0, "recovered": 0}
latencies = deque(maxlen=5000)
phase_ms = {"prepare": deque(maxlen=5000), "precommit": deque(maxlen=5000), "decide": deque(maxlen=5000)}
START = time.time()


def sess():
    if not hasattr(tls, "s"):
        tls.s = requests.Session()
    return tls.s


def call(pid, path, payload, timeout=None):
    try:
        r = sess().post(PARTS[pid] + path, json=payload, timeout=timeout or PHASE_TIMEOUT_S)
        return r.json()
    except Exception:
        return None


def crash_if(point):
    if fault_point["crash_at"] == point:
        print(f"[coordinator] INJECTED CRASH at {point}", flush=True)
        os._exit(137)                                         # hard crash, like kill -9


def log_set(txid, **kw):
    with dblock:
        sets = ", ".join(f"{k}=?" for k in kw)
        db.execute(f"UPDATE coordlog SET {sets} WHERE txid=?", (*kw.values(), txid))


def deliver(txid, decision, parts):
    """Phase 2: send the decision; returns True when every participant acknowledged."""
    skip = set(fault_point["skip_phase2_to"]) if fault_point["count"] else set()
    path = "/commit" if decision == "COMMIT" else "/abort"
    futs = {p: pool.submit(call, p, path, {"txid": txid}) for p in parts if p not in skip}
    if skip:
        fault_point["count"] -= 1                             # omission failure: these messages are never sent
    return all(f.result() is not None and f.result().get("ok") is not False for f in futs.values()) and not skip


def run_txn(ops, protocol, prepare_mode=None):
    txid = uuid.uuid4().hex[:12]
    by_part = {}
    for o in ops:
        by_part.setdefault(o["participant"], []).append({"account": o["account"], "delta": o["delta"]})
    parts = sorted(by_part)
    with dblock:
        db.execute("INSERT INTO coordlog VALUES (?,?,?,?,?,0,?)", (txid, protocol, json.dumps(parts), "PREPARE", None, time.time()))
    active.add(txid)
    stats["started"] += 1
    t0 = time.perf_counter()
    try:
        # ---- phase 1: prepare
        mode = prepare_mode or PREPARE_MODE
        if mode == "ordered":
            # participants are asked ONE AT A TIME in a global order, so two transactions can never hold each other's
            # locks in opposite order on different participants (distributed deadlock prevention by resource ordering)
            votes = {}
            for p in parts:
                votes[p] = call(p, "/prepare", {"txid": txid, "ops": by_part[p], "participants": parts, "protocol": protocol})
                if votes[p] is None or votes[p].get("vote") != "yes":
                    break
        else:
            futs = {p: pool.submit(call, p, "/prepare", {"txid": txid, "ops": by_part[p], "participants": parts, "protocol": protocol}) for p in parts}
            votes = {p: f.result() for p, f in futs.items()}
        t1 = time.perf_counter()
        crash_if("after_prepare")
        all_yes = len(votes) == len(parts) and all(v is not None and v.get("vote") == "yes" for v in votes.values())
        decision = "COMMIT" if all_yes else "ABORT"
        t2 = t1
        # ---- 3PC extra phase
        if all_yes and protocol == "3pc":
            log_set(txid, phase="PRECOMMIT")
            acks = {}
            for i, p in enumerate(parts):                      # sequential so mid_precommit can stop halfway
                acks[p] = call(p, "/precommit", {"txid": txid})
                if i == 0:
                    crash_if("mid_precommit")
            crash_if("after_precommit")
            if not all(a is not None and a.get("ack") for a in acks.values()):
                decision = "ABORT"
            t2 = time.perf_counter()
        # ---- commit point: decision forced to the log
        log_set(txid, decision=decision, phase="DECIDED")
        t3 = time.perf_counter()
        crash_if("after_decision_log")
        done = deliver(txid, decision, parts)
        if done:
            log_set(txid, done=1)
        t4 = time.perf_counter()
        stats["committed" if decision == "COMMIT" else "aborted"] += 1
        latencies.append(t4 - t0)
        phase_ms["prepare"].append((t1 - t0) * 1000)
        if protocol == "3pc":
            phase_ms["precommit"].append((t2 - t1) * 1000)
        phase_ms["decide"].append((t4 - t2) * 1000)
        reasons = {p: v.get("reason") if v else "no-response" for p, v in votes.items() if not v or v.get("vote") != "yes"}
        return {"txid": txid, "outcome": decision, "protocol": protocol, "latency_ms": round((t4 - t0) * 1000, 2),
                "prepare_ms": round((t1 - t0) * 1000, 2), "precommit_ms": round((t2 - t1) * 1000, 2) if protocol == "3pc" else 0,
                "complete_ms": round((t4 - t2) * 1000, 2), "delivered": done, "abort_reasons": reasons}
    finally:
        active.discard(txid)


@app.route("/txn", methods=["POST"])
def txn():
    d = request.get_json(force=True)
    return jsonify(run_txn(d["ops"], d.get("protocol", DEFAULT_PROTOCOL), d.get("prepare_mode")))


@app.route("/decision/<txid>")
def decision(txid):
    if txid in active:
        return jsonify({"decision": "PENDING"})
    with dblock:
        r = db.execute("SELECT decision FROM coordlog WHERE txid=?", (txid,)).fetchone()
    if r is None:
        return jsonify({"decision": "ABORT", "note": "presumed abort"})
    if r[0] is None:
        return jsonify({"decision": "PENDING"})                # restart recovery has not decided yet
    return jsonify({"decision": r[0]})


@app.route("/fault_point", methods=["POST"])
def set_fault_point():
    d = request.get_json(force=True)
    fault_point.update({"crash_at": d.get("crash_at"), "skip_phase2_to": d.get("skip_phase2_to", []), "count": d.get("count", 1 if d.get("skip_phase2_to") else 0)})
    return jsonify(fault_point)


@app.route("/stats")
def get_stats():
    with dblock:
        undone = db.execute("SELECT COUNT(*) FROM coordlog WHERE decision IS NOT NULL AND done=0").fetchone()[0]
    return jsonify({**stats, "undelivered_decisions": undone, "latency_s": summarize(list(latencies)),
                    "phase_ms": {k: summarize(list(v)) for k, v in phase_ms.items()}, "uptime_s": round(time.time() - START, 1)})


@app.route("/log")
def get_log():
    with dblock:
        rows = db.execute("SELECT txid, protocol, phase, decision, done FROM coordlog ORDER BY ts DESC LIMIT 100").fetchall()
    return jsonify([dict(zip(("txid", "protocol", "phase", "decision", "done"), r)) for r in rows])


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ------------------------------------------------------------------ recovery and redelivery
def participant_states(txid, parts):
    out = {}
    for p in parts:
        try:
            out[p] = requests.get(PARTS[p] + f"/txstate/{txid}", timeout=1.0).json()["state"]
        except Exception:
            out[p] = "UNREACHABLE"
    return out


def recover():
    with dblock:
        rows = db.execute("SELECT txid, protocol, participants, phase, decision FROM coordlog WHERE done=0").fetchall()
    for txid, protocol, parts, phase, dec in rows:
        parts = json.loads(parts)
        if dec is None:
            if protocol == "3pc" and phase == "PRECOMMIT":
                st = participant_states(txid, parts).values()      # 3PC termination rule, same as the participants'
                dec = "COMMIT" if ("COMMITTED" in st or "PRECOMMITTED" in st) and "ABORTED" not in st else "ABORT"
            else:
                dec = "ABORT"                                      # 2PC or never got past prepare: nobody can have committed
            log_set(txid, decision=dec, phase="DECIDED")
            stats["recovered"] += 1
            print(f"[coordinator] recovered {txid}: no decision logged -> {dec}", flush=True)


def redeliver_loop():
    time.sleep(0.5)
    recover()
    while True:
        time.sleep(0.5)
        with dblock:
            rows = db.execute("SELECT txid, participants, decision FROM coordlog WHERE decision IS NOT NULL AND done=0").fetchall()
        for txid, parts, dec in rows:
            if txid in active:
                continue
            if deliver(txid, dec, json.loads(parts)):
                log_set(txid, done=1)
                stats["resent"] += 1


if __name__ == "__main__":
    threading.Thread(target=redeliver_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
