"""
Core service (Milestones 3, 4 and the Kafka consumer). A cluster of replicas that coordinate.
(Bully election + Lamport ordering ported from Week 4; Kafka consumption, persistence and
de-duplication are new -- see the section at the bottom.)

Two distributed algorithms are implemented here from scratch, over plain HTTP:

1. BULLY LEADER ELECTION (Garcia-Molina, 1982).
   Every replica has a numeric id; the live replica with the HIGHEST id must be
   the leader. Only the leader accepts data from the edge gateway; followers
   answer 409 "not leader". Messages:
     ELECTION     "I noticed no leader; are you alive?"  (sent to all HIGHER ids)
     OK           reply to ELECTION: "yes, I'm alive and higher -- I'll take over"
     COORDINATOR  "I am the leader"  (sent by the winner to all LOWER ids)
   A replica that gets no OK from any higher id wins. A replica that does get an
   OK waits for the higher one to announce; if nothing arrives, it retries.
   Failure detection: followers ping the leader every HEARTBEAT_INTERVAL and
   start an election after HEARTBEAT_MISS_LIMIT consecutive misses. A crashed
   replica that restarts with the highest id starts an election and takes over
   (that is the "bully" behaviour).

2. LAMPORT LOGICAL CLOCKS for event ordering. The core merges the gateway's
   clock on every ingest and checks each report's causal chain
   (agent -> gateway receive -> gateway send -> core receive) two ways: by
   physical timestamps and by Lamport stamps. GET /ordering shows how often
   each one violates causality. Physical clocks break under clock skew;
   Lamport clocks cannot, by construction.

Every message the election sends is counted, so message complexity is
measured, not just asserted (see GET /election_stats).

Limitations (deliberate, for later milestones): state is NOT replicated between
replicas, so a new leader starts with an empty network view and rebuilds it from
incoming data (replication is Week 7); Bully assumes a synchronous network with
reliable failure detection, so a network partition could produce two leaders.
"""

import base64
import json
import os
import threading
import time
import zlib
from collections import Counter, OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor

import sqlite3

from flask import Flask, jsonify, request

from system.common.bus import get_bus
from system.common.config import env_float, env_int, env_str, parse_peers
from system.common.faults import FaultInjector
from system.common.lamport import LamportClock
from system.common.metrics import summarize
from system.common.netcall import Net

app = Flask(__name__)

PORT = int(os.environ.get("PORT", "5001"))
NODE_ID = int(os.environ.get("NODE_ID", "1"))
CORE_ID = os.environ.get("CORE_ID", f"core-{NODE_ID}")
CLOUD_URL = os.environ.get("CLOUD_URL", "http://localhost:5002/digest")
DIGEST_INTERVAL = float(os.environ.get("DIGEST_INTERVAL", "5"))
TRANSPORT = env_str("TRANSPORT", "kafka")
TOPIC = env_str("KAFKA_TOPIC", "edge.telemetry")
GROUP = env_str("KAFKA_GROUP", "core")
DATA_DIR = env_str("DATA_DIR", "/tmp/core-data")
POLL_MAX = env_int("POLL_MAX", 500)
ARCHIVE_TOPIC = env_str("ARCHIVE_TOPIC", "")           # core -> cloud tier: validated rows go here for the archiver ("" = off)
_archive_producer = None


PEERS = parse_peers(os.environ.get("PEERS", f"{NODE_ID}=http://localhost:{PORT}"))

HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "1.0"))
HEARTBEAT_MISS_LIMIT = int(os.environ.get("HEARTBEAT_MISS_LIMIT", "2"))
ELECTION_REPLY_TIMEOUT = float(os.environ.get("ELECTION_REPLY_TIMEOUT", "1.0"))
COORDINATOR_WAIT = float(os.environ.get("COORDINATOR_WAIT", "3.0"))
STARTUP_DELAY = float(os.environ.get("STARTUP_DELAY", "2.0"))
CORRELATION_WINDOW_S = 10.0

# ------------------------------------------------------------------ Lamport clock


clock = LamportClock()
net = Net(CORE_ID)
faults = FaultInjector(CORE_ID).install(app)

# ------------------------------------------------------------------ election state

state_lock = threading.Lock()
leader_id = None
leader_set_at = 0.0
electing = False
counters = Counter()          # messages SENT, by type
received = Counter()          # messages RECEIVED, by type
election_info = {"elections_started": 0, "last_election_ms": None, "leader_changes": 0}
START = time.time()


def count_sent(kind):
    with state_lock:
        counters[kind] += 1


def count_received(kind):
    with state_lock:
        received[kind] += 1


def send_election(pid):
    count_sent("election")
    try:
        r = net.post(PEERS[pid] + "/election", {"from": NODE_ID}, timeout=ELECTION_REPLY_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False


def send_coordinator(pid):
    count_sent("coordinator")
    try:
        net.post(PEERS[pid] + "/coordinator", {"leader": NODE_ID}, timeout=ELECTION_REPLY_TIMEOUT)
    except Exception:
        pass


def set_leader(new_id):
    global leader_id, leader_set_at
    with state_lock:
        if new_id is not None and leader_id != new_id:
            election_info["leader_changes"] += 1
        leader_id = new_id
        leader_set_at = time.time()


def become_leader():
    set_leader(NODE_ID)
    lower = [p for p in PEERS if p < NODE_ID]
    if lower:
        with ThreadPoolExecutor(len(lower)) as ex:
            list(ex.map(send_coordinator, lower))


def run_election():
    """Bully algorithm, from this replica's point of view."""
    global electing
    with state_lock:
        if electing:
            return
        electing = True
        election_info["elections_started"] += 1
    t0 = time.time()
    try:
        for _attempt in range(5):
            higher = [p for p in PEERS if p > NODE_ID]
            got_ok = False
            if higher:
                with ThreadPoolExecutor(len(higher)) as ex:
                    got_ok = any(ex.map(send_election, higher))
            if not got_ok:
                become_leader()          # nobody higher is alive: we win
                break
            # A higher replica is alive and is taking over; wait for its announcement.
            deadline = time.time() + COORDINATOR_WAIT
            announced = False
            while time.time() < deadline:
                with state_lock:
                    announced = leader_id is not None and leader_set_at > t0
                if announced:
                    break
                time.sleep(0.05)
            if announced:
                break
            # else: silence -> the higher replica died mid-election; try again
    finally:
        with state_lock:
            electing = False
            election_info["last_election_ms"] = round((time.time() - t0) * 1000, 1)


def monitor_loop():
    """Failure detector: startup election, then heartbeat the leader."""
    time.sleep(STARTUP_DELAY)          # give peers a moment to come up
    run_election()
    misses = 0
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with state_lock:
            lid = leader_id
        if lid is None:
            run_election()
            continue
        if lid == NODE_ID:
            misses = 0
            continue
        count_sent("heartbeat")
        try:
            ok = net.get(PEERS[lid] + "/ping", timeout=1.0).status_code == 200
        except Exception:
            ok = False
        if ok:
            misses = 0
        else:
            misses += 1
            if misses >= HEARTBEAT_MISS_LIMIT:
                misses = 0
                set_leader(None)          # leader presumed dead
                run_election()


@app.route("/election", methods=["POST"])
def election():
    count_received("election")
    count_sent("ok")
    threading.Thread(target=run_election, daemon=True).start()   # Bully: reply OK, then run our own election
    return jsonify({"ok": True})


@app.route("/coordinator", methods=["POST"])
def coordinator():
    count_received("coordinator")
    set_leader(int(request.get_json(force=True)["leader"]))
    return jsonify({"ok": True})


@app.route("/ping", methods=["GET"])
def ping():
    count_received("heartbeat")
    return jsonify({"ok": True, "node_id": NODE_ID})


@app.route("/leader", methods=["GET"])
def get_leader():
    with state_lock:
        lid = leader_id
    return jsonify({"node_id": NODE_ID, "leader_id": lid, "leader_url": PEERS.get(lid), "is_leader": lid == NODE_ID})


@app.route("/election_stats", methods=["GET"])
def election_stats():
    with state_lock:
        sent = dict(counters)
        return jsonify(
            {
                "node_id": NODE_ID,
                "leader_id": leader_id,
                "sent": sent,
                "received": dict(received),
                "election_messages_sent": sent.get("election", 0) + sent.get("ok", 0) + sent.get("coordinator", 0),
                "heartbeat_messages_sent": sent.get("heartbeat", 0),
                **election_info,
                "uptime_seconds": round(time.time() - START, 1),
            }
        )


# ------------------------------------------------------------------ data path (Week 3, now leader-gated)

lock = threading.Lock()
nodes = {}
alerts = deque(maxlen=200)
recent_anomaly_at = {}
last_correlated_alert = 0.0
events = deque(maxlen=500)
ordering = {"traces_checked": 0, "wall_clock_violations": 0, "lamport_violations": 0, "by_node": {}}
stats = {
    "ingest_messages": 0, "raw_messages": 0, "batch_messages": 0, "ingest_bytes": 0,
    "reports_represented": 0, "latency_weighted_sum_ms": 0.0, "digests_sent": 0, "digest_failures": 0,
    "duplicates_skipped": 0,
}
SEEN_MAX = 50000
seen_msgs = OrderedDict()                 # msg_id -> True (LRU) for idempotent processing
pipeline_latency_s = deque(maxlen=5000)   # gateway enqueue -> core processed, per message


def to_summaries(msg):
    if msg.get("type") == "batch":
        return msg["summaries"]
    r = msg["report"]
    return [
        {
            "node_id": r["node_id"], "node_role": r.get("node_role"), "count": 1,
            "avg_cpu": r["cpu_percent"], "max_cpu": r["cpu_percent"], "avg_mem": r["mem_percent"],
            "anomaly_count": 1 if r.get("anomaly") else 0, "avg_ts": r["timestamp"],
            "newest_ts": r["timestamp"], "avg_compute_ms": r.get("compute_time_ms", 0),
        }
    ]


def apply_summary(s, recv):
    global last_correlated_alert
    count = s["count"]
    entry = nodes.setdefault(s["node_id"], {"total_reports": 0, "total_anomalies": 0, "node_role": s.get("node_role")})
    entry.update({"avg_cpu": s["avg_cpu"], "max_cpu": s["max_cpu"], "avg_mem": s["avg_mem"],
                  "avg_compute_ms": s.get("avg_compute_ms"), "last_update": recv})
    entry["total_reports"] += count
    entry["total_anomalies"] += s["anomaly_count"]
    stats["reports_represented"] += count
    stats["latency_weighted_sum_ms"] += (recv - s["avg_ts"]) * 1000 * count
    if s["anomaly_count"] > 0:
        recent_anomaly_at[s["node_id"]] = recv
        alerts.append({"time": recv, "type": "node_anomaly", "node": s["node_id"]})
        active = [n for n, t in recent_anomaly_at.items() if recv - t <= CORRELATION_WINDOW_S]
        if len(active) >= 2 and recv - last_correlated_alert > CORRELATION_WINDOW_S:
            last_correlated_alert = recv
            alerts.append({"time": recv, "type": "correlated_network_alert", "nodes": sorted(active)})


def check_ordering(traces, core_lamport, recv):
    """Does each report's causal chain look ordered? Two rulers, one truth."""
    for t in traces:
        if not t.get("agent_lamport"):
            continue                      # sender has no logical clock (e.g. load generator)
        gw_send = t["gw_send"]
        wall_chain = [t["agent_wall"], t["gw_recv_wall"], gw_send["wall"], recv]
        lamport_chain = [t["agent_lamport"], t["gw_recv_lamport"], gw_send["lamport"], core_lamport]
        wall_bad = any(a > b for a, b in zip(wall_chain, wall_chain[1:]))
        lamport_bad = any(a >= b for a, b in zip(lamport_chain, lamport_chain[1:]))
        ordering["traces_checked"] += 1
        ordering["wall_clock_violations"] += int(wall_bad)
        ordering["lamport_violations"] += int(lamport_bad)
        n = ordering["by_node"].setdefault(t["node"], {"checked": 0, "wall_violations": 0, "lamport_violations": 0})
        n["checked"] += 1
        n["wall_violations"] += int(wall_bad)
        n["lamport_violations"] += int(lamport_bad)


def handle_message(msg, raw_len, recv):
    """Process one edge message (live raw/batch summary, or a chunk of dataset rows).
    Shared by the HTTP /ingest path and the Kafka consumer. Idempotent by msg_id, so a
    Kafka replay after a leader failover (at-least-once delivery) cannot double count."""
    if msg.get("type") == "rows_z":                       # compressed on the edge->core link: restore the rows
        rows = json.loads(zlib.decompress(base64.b64decode(msg["z"])))
        msg = {**{k: v for k, v in msg.items() if k not in ("z", "n")}, "type": "rows", "rows": rows}
    mid = msg.get("msg_id")
    with lock:
        if mid is not None:
            if mid in seen_msgs:
                stats["duplicates_skipped"] += 1
                return None
            seen_msgs[mid] = True
            if len(seen_msgs) > SEEN_MAX:
                seen_msgs.popitem(last=False)
    core_lamport = clock.update(msg.get("gw_send", {}).get("lamport", 0))      # receive event
    if msg.get("type") == "rows":
        ingest_rows_chunk(msg, recv, raw_len)
        return core_lamport
    summaries = to_summaries(msg)
    with lock:
        stats["ingest_messages"] += 1
        stats["ingest_bytes"] += raw_len
        stats["batch_messages" if msg.get("type") == "batch" else "raw_messages"] += 1
        for s in summaries:
            apply_summary(s, recv)
        if "gw_send" in msg:
            for t in msg.get("traces", []):
                t["gw_send"] = msg["gw_send"]
            check_ordering(msg.get("traces", []), core_lamport, recv)
        events.append({"lamport": core_lamport, "wall": recv, "gateway": msg.get("gateway_id"),
                       "reports": 1 if msg.get("type") == "raw" else msg.get("report_count")})
    return core_lamport


@app.route("/ingest", methods=["POST"])
def ingest():
    with state_lock:
        lid = leader_id
    if lid != NODE_ID:
        return jsonify({"error": "not leader", "leader_id": lid, "leader_url": PEERS.get(lid)}), 409
    raw_body = request.get_data()
    lam = handle_message(json.loads(raw_body), len(raw_body), time.time())
    return jsonify({"status": "ok", "lamport": lam})


@app.route("/state", methods=["GET"])
def state():
    with lock:
        return jsonify({"core_id": CORE_ID, "node_id": NODE_ID, "nodes": nodes})


@app.route("/alerts", methods=["GET"])
def get_alerts():
    with lock:
        return jsonify({"count": len(alerts), "alerts": list(alerts)[-50:]})


@app.route("/ordering", methods=["GET"])
def get_ordering():
    with lock:
        return jsonify({"core_id": CORE_ID, **json.loads(json.dumps(ordering))})


@app.route("/events", methods=["GET"])
def get_events():
    limit = int(request.args.get("limit", 20))
    with lock:
        return jsonify({"events_by_lamport_order": sorted(events, key=lambda e: e["lamport"])[-limit:]})


@app.route("/stats", methods=["GET"])
def get_stats():
    with lock:
        elapsed = time.time() - START
        reps = stats["reports_represented"]
        return jsonify(
            {
                "core_id": CORE_ID, **stats,
                "elapsed_seconds": round(elapsed, 2),
                "avg_end_to_end_latency_ms": round(stats["latency_weighted_sum_ms"] / reps, 2) if reps else None,
            }
        )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "tier": "core", "id": CORE_ID})


def digest_loop():
    """Only the current leader sends digests to the cloud."""
    while True:
        time.sleep(DIGEST_INTERVAL)
        with state_lock:
            am_leader = leader_id == NODE_ID
        if not am_leader:
            continue
        with lock:
            snapshot = {n: dict(v) for n, v in nodes.items()}
            alert_total = len(alerts)
        if not snapshot:
            continue
        try:
            ok = net.post(CLOUD_URL, {"core_id": CORE_ID, "sent_at": time.time(), "nodes": snapshot, "alerts_total": alert_total, "rows_ingested": ingest_rows["total"]}, timeout=3).status_code == 200
        except Exception:
            ok = False
        with lock:
            stats["digests_sent" if ok else "digest_failures"] += 1


# ------------------------------------------------------------------ dataset rows + Kafka consumer (new)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, f"core-{NODE_ID}.sqlite3")
db_lock = threading.Lock()
ingest_rows = {"total": 0, "chunks": 0, "inserted": 0, "duplicate_rows": 0}
kafka_state = {"consumer_active": False, "polls": 0, "messages": 0, "commits": 0, "last_error": None}
region_anoms = OrderedDict()              # (region, timestamp) -> set(site)  for correlated incident detection

COLS = ["record_id", "timestamp", "site_id", "region", "edge_node", "site_type", "cpu_pct", "mem_pct", "rx_mbps",
        "tx_mbps", "latency_ms", "jitter_ms", "packet_loss_pct", "active_users", "signal_dbm", "temperature_c", "is_anomaly"]


def db_connect():
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute(f"CREATE TABLE IF NOT EXISTS readings ({', '.join(COLS)}, PRIMARY KEY(record_id))")
    c.execute("CREATE INDEX IF NOT EXISTS ix_site ON readings(site_id, timestamp)")
    c.commit()
    return c


db = db_connect()


def publish_processed(rows, msg):
    """Hand validated rows to the cloud tier (archiver) through the bus. Flushed before the input offset is committed."""
    global _archive_producer
    if not ARCHIVE_TOPIC:
        return
    if _archive_producer is None:
        bus = get_bus()
        bus.ensure_topic(ARCHIVE_TOPIC, env_int("KAFKA_PARTITIONS", 3))
        _archive_producer = bus.producer()
    _archive_producer.send(ARCHIVE_TOPIC, rows[0]["site_id"], {"type": "rows", "core": CORE_ID, "src_msg_id": msg.get("msg_id"), "rows": rows})


def ingest_rows_chunk(msg, recv, raw_len):
    rows = msg["rows"]
    with db_lock:
        before = db.total_changes
        db.executemany(f"INSERT OR IGNORE INTO readings VALUES ({','.join('?' * len(COLS))})",
                       [tuple(r[c] for c in COLS) for r in rows])
        db.commit()
        inserted = db.total_changes - before
    publish_processed(rows, msg)
    with lock:
        ingest_rows["chunks"] += 1
        ingest_rows["total"] += len(rows)
        ingest_rows["inserted"] += inserted
        ingest_rows["duplicate_rows"] += len(rows) - inserted
        stats["ingest_messages"] += 1
        stats["ingest_bytes"] += raw_len
        stats["batch_messages"] += 1
        if msg.get("gw_enqueue_wall"):
            pipeline_latency_s.append(recv - msg["gw_enqueue_wall"])
        # per-site rolling view + alerts (dataset time, not wall time, drives correlation)
        by_site = {}
        for r in rows:
            by_site.setdefault(r["site_id"], []).append(r)
        for site, rs in by_site.items():
            e = nodes.setdefault(site, {"total_reports": 0, "total_anomalies": 0, "node_role": "site"})
            e.update({"avg_cpu": round(sum(r["cpu_pct"] for r in rs) / len(rs), 2), "max_cpu": max(r["cpu_pct"] for r in rs),
                      "avg_mem": round(sum(r["mem_pct"] for r in rs) / len(rs), 2), "last_update": recv,
                      "region": rs[-1]["region"], "last_data_ts": rs[-1]["timestamp"]})
            e["total_reports"] += len(rs)
            e["total_anomalies"] += sum(r["is_anomaly"] for r in rs)
        for r in rows:
            if r["is_anomaly"]:
                key = (r["region"], r["timestamp"])
                sites = region_anoms.setdefault(key, set())
                sites.add(r["site_id"])
                if len(sites) == 2:           # second site of one region anomalous in the same interval
                    alerts.append({"time": recv, "type": "correlated_network_alert", "region": r["region"],
                                   "data_ts": r["timestamp"], "nodes": sorted(sites)})
                if len(region_anoms) > 500:
                    region_anoms.popitem(last=False)


def kafka_consume_loop():
    """Only the Bully LEADER consumes. Becoming leader -> subscribe (resumes from the group's
    committed offset); losing leadership -> close. poll -> process -> commit gives
    at-least-once delivery; handle_message() is idempotent so replays are harmless."""
    consumer = None
    bus = get_bus()
    try:
        bus.ensure_topic(TOPIC, env_int("KAFKA_PARTITIONS", 3))
    except Exception as e:
        kafka_state["last_error"] = f"ensure_topic: {e}"
    while True:
        with state_lock:
            am_leader = leader_id == NODE_ID
        if not am_leader:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer, kafka_state["consumer_active"] = None, False
            time.sleep(0.2)
            continue
        try:
            if consumer is None:
                consumer = bus.consumer(TOPIC, GROUP)
                kafka_state["consumer_active"] = True
            records = consumer.poll(POLL_MAX, 0.5)
            kafka_state["polls"] += 1
            if records:
                for rec in records:
                    handle_message(rec.value, len(json.dumps(rec.value)), time.time())
                if _archive_producer is not None:
                    _archive_producer.flush(10)        # downstream copy is durable before the input offset moves on
                consumer.commit()                      # after processing: at-least-once
                kafka_state["messages"] += len(records)
                kafka_state["commits"] += 1
        except Exception as e:
            kafka_state["last_error"] = str(e)
            time.sleep(1)


@app.route("/readings/count", methods=["GET"])
def readings_count():
    with db_lock:
        n = db.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    return jsonify({"core_id": CORE_ID, "rows_in_db": n})


@app.route("/analytics/regional", methods=["GET"])
def analytics_regional():
    with db_lock:
        cur = db.execute("""SELECT region, COUNT(*), ROUND(AVG(cpu_pct),2), ROUND(AVG(latency_ms),2),
                            ROUND(AVG(packet_loss_pct),3), SUM(is_anomaly), SUM(active_users)
                            FROM readings GROUP BY region ORDER BY region""")
        rows = cur.fetchall()
    keys = ["region", "rows", "avg_cpu", "avg_latency_ms", "avg_packet_loss", "anomalies", "user_samples"]
    return jsonify({"core_id": CORE_ID, "regions": [dict(zip(keys, r)) for r in rows]})


@app.route("/pipeline", methods=["GET"])
def pipeline():
    with lock:
        return jsonify({"core_id": CORE_ID, "transport": TRANSPORT, "ingest_rows": dict(ingest_rows), "kafka": dict(kafka_state),
                        "pipeline_latency_s": summarize(list(pipeline_latency_s)), "duplicates_skipped": stats["duplicates_skipped"]})


if __name__ == "__main__":
    if TRANSPORT == "kafka":
        threading.Thread(target=kafka_consume_loop, daemon=True).start()
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=digest_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
