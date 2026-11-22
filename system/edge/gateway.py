"""
Edge gateway (Milestones 2, 3, 4 and the Kafka link to the core).

The edge tier ingests two kinds of traffic:
  * live agent reports   POST /metrics   (one report per agent per interval; Week 1-4)
  * dataset chunks       POST /bulk      (rows replayed from data/telemetry_120k.csv)

and forwards them to the core in one of two ways (TRANSPORT):
  kafka  publish to the Kafka topic edge.telemetry; the core consumes from it.
         Kafka is the durable, decoupling link between edge and core: if the core
         leader dies, nothing is lost, the next leader resumes from the committed
         offset (this replaces the Week 4 retry-in-the-gateway approach).
  http   Week 4 behaviour: discover the Bully leader and POST /ingest, with retry.

Edge aggregation (Week 3): live reports can be batched (BATCH_SIZE) into one summary
per node per flush. Dataset rows are packed ROWS_PER_MSG per message.

PROPOSED IMPROVEMENT (milestone 12, brief section 6): ADAPTIVE=true makes ROWS_PER_MSG
adapt to the outbox backlog -- big messages when the edge is under pressure (amortise
per-message overhead), small messages when idle (keep latency low). The baseline is a
fixed ROWS_PER_MSG. experiments/capstone.py measures both.

Lamport clock: on every receive  clock = max(clock, sender) + 1  and every send is
stamped (Week 4). The core checks causal ordering against physical timestamps.
"""
import base64
import json
import os
import queue
import statistics
import threading
import time
import uuid
import zlib
from collections import deque

from flask import Flask, jsonify, request

from system.common.bus import get_bus
from system.common.config import env_bool, env_float, env_int, env_list, env_str
from system.common.faults import FaultInjector
from system.common.lamport import LamportClock
from system.common.netcall import Net

app = Flask(__name__)

GATEWAY_ID = env_str("GATEWAY_ID", "edge-gateway-1")
PORT = env_int("PORT", 5000)
TRANSPORT = env_str("TRANSPORT", "kafka")                    # kafka | http
TOPIC = env_str("KAFKA_TOPIC", "edge.telemetry")
TOPIC_PARTITIONS = env_int("KAFKA_PARTITIONS", 3)
CORE_URLS = [u.rstrip("/") for u in env_list("CORE_URLS", "http://localhost:5001")]
BATCH_SIZE = env_int("BATCH_SIZE", 1)                        # live reports per summary (Week 3)
FLUSH_INTERVAL = env_float("FLUSH_INTERVAL", 5)
RETRY_DEADLINE_S = env_float("RETRY_DEADLINE_S", 30)
ROWS_PER_MSG = env_int("ROWS_PER_MSG", 100)                  # dataset rows per Kafka message (baseline)
ADAPTIVE = env_bool("ADAPTIVE", False)                       # proposed improvement
ADAPT_MIN, ADAPT_MAX = env_int("ADAPT_MIN", 20), env_int("ADAPT_MAX", 500)
OUTBOX_MAX = env_int("OUTBOX_MAX", 2000)
COMPRESS = env_str("COMPRESS", "none")                       # none | zlib  (proposed improvement: compress rows on the edge->core link)
LINK_MBPS = env_float("LINK_MBPS", 0.0)                      # emulated edge->core uplink; 0 = unlimited
COALESCE_MS = env_float("COALESCE_MS", 100.0)                # ADAPTIVE: longest a row may wait to be batched with its site's other rows
site_buf, site_first = {}, {}                                # per-site coalescing buffers (ADAPTIVE)
link_bytes = {"wire": 0, "raw": 0, "throttle_sleep_s": 0.0}

START = time.time()
lock = threading.Lock()
clock = LamportClock()
net = Net(GATEWAY_ID)
faults = FaultInjector(GATEWAY_ID).install(app)

# ---- Week 2 instrumentation (live agents) ----
WINDOW = 50
total_records = 0
latest_by_node, reports_by_node, last_received_at = {}, {}, {}
inter_arrival, latencies_ms, expected_intervals, missed_reports = {}, {}, {}, {}

# ---- forwarding ----
buffer, last_flush = [], time.time()
outbox = queue.Queue(maxsize=OUTBOX_MAX)
fwd = {"messages": 0, "bytes": 0, "failures": 0, "dropped": 0, "reports_forwarded": 0, "rows_forwarded": 0,
       "retries": 0, "leader_rediscoveries": 0, "max_delivery_gap_s": 0.0, "rejected_429": 0}
bulk = {"rows_received": 0, "chunks_received": 0}
current_core, last_success_at = None, None
adaptive_state = {"rows_per_msg": ROWS_PER_MSG if not ADAPTIVE else 100, "adjustments": 0}
_producer = None


def _node_state(node_id):
    if node_id not in inter_arrival:
        inter_arrival[node_id] = deque(maxlen=WINDOW)
        latencies_ms[node_id] = deque(maxlen=WINDOW)
        missed_reports[node_id] = 0
        reports_by_node[node_id] = 0


# ------------------------------------------------------------------ adaptive batching
def current_rows_per_msg():
    """Baseline: constant. Proposed: scale with backlog (multiplicative up, gentle down)."""
    if not ADAPTIVE:
        return ROWS_PER_MSG
    fill = outbox.qsize() / float(OUTBOX_MAX)
    with lock:
        n = adaptive_state["rows_per_msg"]
        if fill > 0.25:
            n = min(ADAPT_MAX, int(n * 1.5) + 1)
        elif fill < 0.02:
            n = max(ADAPT_MIN, int(n * 0.9))
        if n != adaptive_state["rows_per_msg"]:
            adaptive_state["adjustments"] += 1
            adaptive_state["rows_per_msg"] = n
        return n


# ------------------------------------------------------------------ summaries for live reports
def build_summaries(items):
    grouped = {}
    for r in items:
        grouped.setdefault(r["node_id"], []).append(r)
    summaries, traces = [], []
    for node_id, reports in grouped.items():
        cpus, mems, stamps = [r["cpu_percent"] for r in reports], [r["mem_percent"] for r in reports], [r["timestamp"] for r in reports]
        summaries.append({
            "node_id": node_id, "node_role": reports[-1].get("node_role"), "count": len(reports),
            "avg_cpu": round(sum(cpus) / len(cpus), 2), "max_cpu": max(cpus),
            "avg_mem": round(sum(mems) / len(mems), 2),
            "anomaly_count": sum(1 for r in reports if r.get("anomaly")),
            "avg_ts": sum(stamps) / len(stamps), "newest_ts": max(stamps),
            "avg_compute_ms": round(sum(r.get("compute_time_ms", 0) for r in reports) / len(reports), 2),
        })
        traces.append(reports[-1]["_trace"])
    return summaries, traces


def enqueue(msg, weight):
    msg["msg_id"] = uuid.uuid4().hex
    msg["gw_enqueue_wall"] = time.time()
    try:
        outbox.put_nowait(msg)
        return True
    except queue.Full:
        with lock:
            fwd["dropped"] += weight
        return False


def flush_buffer():
    global last_flush
    with lock:
        if not buffer:
            return
        items = buffer[:]
        buffer.clear()
        last_flush = time.time()
    summaries, traces = build_summaries(items)
    enqueue({"type": "batch", "gateway_id": GATEWAY_ID, "report_count": len(items), "summaries": summaries, "traces": traces}, len(items))


# ------------------------------------------------------------------ delivery
def discover_leader():
    for base in CORE_URLS:
        try:
            status, info = net.get_json(base + "/leader", timeout=1)
            if info and info.get("leader_url"):
                return info["leader_url"].rstrip("/")
        except Exception:
            continue
    return None


def _account(msg, body_len):
    with lock:
        fwd["messages"] += 1
        fwd["bytes"] += body_len
        if msg["type"] == "rows":
            fwd["rows_forwarded"] += len(msg["rows"])
        else:
            fwd["reports_forwarded"] += 1 if msg["type"] == "raw" else msg["report_count"]


def deliver_http(msg):
    global current_core, last_success_at
    deadline, first = time.time() + RETRY_DEADLINE_S, True
    while time.time() < deadline:
        if current_core is None:
            found = discover_leader()
            if found is None:
                time.sleep(0.3)
                continue
            with lock:
                if last_success_at is not None:
                    fwd["leader_rediscoveries"] += 1
            current_core = found
        if not first:
            with lock:
                fwd["retries"] += 1
        first = False
        msg["gw_send"] = {"lamport": clock.tick(), "wall": time.time()}
        try:
            status, _ = net.post_json(current_core + "/ingest", msg, timeout=3)
            if status == 200:
                now = time.time()
                _account(msg, len(json.dumps(msg)))
                with lock:
                    if last_success_at is not None:
                        fwd["max_delivery_gap_s"] = round(max(fwd["max_delivery_gap_s"], now - last_success_at), 3)
                    last_success_at = now
                return True
        except Exception:
            pass
        current_core = None
        time.sleep(0.3)
    with lock:
        fwd["failures"] += 1
    return False


def deliver_kafka(msg):
    global last_success_at
    global _producer
    if _producer is None:
        bus = get_bus()
        bus.ensure_topic(TOPIC, TOPIC_PARTITIONS)
        _producer = bus.producer()
    msg["gw_send"] = {"lamport": clock.tick(), "wall": time.time()}
    key = msg["rows"][0]["site_id"] if msg["type"] == "rows" else (msg["summaries"][0]["node_id"] if msg["type"] == "batch" else msg["report"]["node_id"])
    wire_msg, wire_len = msg, None
    raw_len = None
    if msg["type"] == "rows" and COMPRESS == "zlib":
        raw = json.dumps(msg["rows"], separators=(",", ":")).encode()
        z = zlib.compress(raw, 1)
        wire_msg = {k: v for k, v in msg.items() if k != "rows"}
        wire_msg.update({"type": "rows_z", "n": len(msg["rows"]), "z": base64.b64encode(z).decode()})
        wire_len, raw_len = len(z), len(raw)
    _producer.send(TOPIC, key, wire_msg)
    if wire_len is None:
        wire_len = raw_len = len(json.dumps(msg))
    link_bytes["wire"] += wire_len
    link_bytes["raw"] += raw_len
    if LINK_MBPS:                                            # emulated uplink: the sender is busy for size/bandwidth seconds
        d = wire_len * 8.0 / (LINK_MBPS * 1e6)
        link_bytes["throttle_sleep_s"] += d
        time.sleep(d)
    _account(msg, wire_len)
    last_success_at = time.time()
    return True


def sender_loop():
    sender = deliver_kafka if TRANSPORT == "kafka" else deliver_http
    while True:
        msg = outbox.get()
        try:
            sender(msg)
        except Exception as e:                      # broker not ready yet etc.: retry with backoff, never lose the message
            print(f"[{GATEWAY_ID}] send error: {e}; retrying", flush=True)
            time.sleep(1)
            outbox.put(msg)
        if TRANSPORT == "kafka" and outbox.empty() and _producer is not None:
            _producer.flush(5)


def flush_timer_loop():  # live-report batching timer
    while True:
        time.sleep(0.5)
        if BATCH_SIZE > 1 and time.time() - last_flush >= FLUSH_INTERVAL:
            flush_buffer()


# ------------------------------------------------------------------ endpoints
@app.route("/metrics", methods=["POST"])
def receive_metrics():
    global total_records
    data = request.get_json(force=True)
    received_at = time.time()
    node_id = data["node_id"]
    agent_lamport = int(data.get("lamport", 0))
    recv_lamport = clock.update(agent_lamport)
    trace = {"node": node_id, "agent_lamport": agent_lamport, "agent_wall": data.get("timestamp"),
             "gw_recv_lamport": recv_lamport, "gw_recv_wall": received_at}
    flush_now = False
    with lock:
        _node_state(node_id)
        if "timestamp" in data:
            latencies_ms[node_id].append((received_at - data["timestamp"]) * 1000)
        if node_id in last_received_at:
            gap = received_at - last_received_at[node_id]
            inter_arrival[node_id].append(gap)
            exp = expected_intervals.get(node_id)
            if exp and gap > exp * 1.5:
                missed_reports[node_id] += 1
        last_received_at[node_id] = received_at
        if "report_interval" in data:
            expected_intervals[node_id] = data["report_interval"]
        data["received_at"] = received_at
        latest_by_node[node_id] = data
        reports_by_node[node_id] += 1
        total_records += 1
        if BATCH_SIZE > 1:
            data["_trace"] = trace
            buffer.append(data)
            flush_now = len(buffer) >= BATCH_SIZE
    if BATCH_SIZE <= 1:
        enqueue({"type": "raw", "gateway_id": GATEWAY_ID, "report": data, "traces": [trace]}, 1)
    elif flush_now:
        flush_buffer()
    return jsonify({"status": "ok", "lamport": recv_lamport}), 200


@app.route("/bulk", methods=["POST"])
def receive_bulk():
    """Dataset chunk from a replayer: {"rows": [ {...telemetry row...}, ... ]}.
    Re-packed into messages of current_rows_per_msg() rows, grouped by site so a
    message always has a single Kafka key (= partition affinity per site)."""
    rows = (request.get_json(force=True) or {}).get("rows", [])
    if outbox.qsize() > OUTBOX_MAX * 0.9:
        with lock:
            fwd["rejected_429"] += 1
        return jsonify({"error": "backpressure", "retry_after_s": 0.2}), 429
    per = current_rows_per_msg()
    if ADAPTIVE:
        return coalesce(rows, per)
    by_site = {}
    for r in rows:
        by_site.setdefault(r["site_id"], []).append(r)
    n_msgs = 0
    for site, rs in by_site.items():
        for i in range(0, len(rs), per):
            part = rs[i:i + per]
            if enqueue({"type": "rows", "gateway_id": GATEWAY_ID, "rows": part}, len(part)):
                n_msgs += 1
    with lock:
        bulk["rows_received"] += len(rows)
        bulk["chunks_received"] += 1
    return jsonify({"status": "ok", "rows": len(rows), "messages": n_msgs, "rows_per_msg": per}), 200


def coalesce(rows, per):
    """PROPOSED IMPROVEMENT: keep a buffer per site across requests and publish a message when it reaches the adaptive
    target (`per`: grows with backlog) or when its oldest row has waited COALESCE_MS (bounded added latency)."""
    ready = []
    with lock:
        for r in rows:
            sid = r["site_id"]
            site_buf.setdefault(sid, []).append(r)
            site_first.setdefault(sid, time.time())
        for sid in list(site_buf):
            if len(site_buf[sid]) >= per:
                ready.append(site_buf.pop(sid))
                site_first.pop(sid, None)
        bulk["rows_received"] += len(rows)
        bulk["chunks_received"] += 1
    n = 0
    for part in ready:
        for i in range(0, len(part), max(per, 1)):
            n += int(enqueue({"type": "rows", "gateway_id": GATEWAY_ID, "rows": part[i:i + per]}, len(part[i:i + per])))
    return jsonify({"status": "ok", "rows": len(rows), "messages": n, "rows_per_msg": per}), 200


def coalesce_timer_loop():
    while ADAPTIVE:
        time.sleep(COALESCE_MS / 4000.0 if COALESCE_MS else 0.05)
        now, due = time.time(), []
        with lock:
            for sid in list(site_buf):
                if now - site_first.get(sid, now) >= COALESCE_MS / 1000.0:
                    due.append(site_buf.pop(sid))
                    site_first.pop(sid, None)
        for part in due:
            enqueue({"type": "rows", "gateway_id": GATEWAY_ID, "rows": part}, len(part))


@app.route("/status", methods=["GET"])
def status():
    with lock:
        return jsonify({"known_nodes": list(latest_by_node), "latest": latest_by_node, "total_records_received": total_records})


@app.route("/metrics_summary", methods=["GET"])
def metrics_summary():
    with lock:
        elapsed = time.time() - START
        per_node = {}
        for node_id in latest_by_node:
            lat, gaps = list(latencies_ms.get(node_id, [])), list(inter_arrival.get(node_id, []))
            per_node[node_id] = {
                "reports_received": reports_by_node.get(node_id, 0),
                "avg_latency_ms": round(statistics.mean(lat), 2) if lat else None,
                "jitter_ms": round(statistics.pstdev(gaps) * 1000, 2) if len(gaps) > 1 else None,
                "estimated_missed_reports": missed_reports.get(node_id, 0),
            }
        return jsonify({
            "gateway_id": GATEWAY_ID,
            "config": {"transport": TRANSPORT, "batch_size": BATCH_SIZE, "rows_per_msg": ROWS_PER_MSG, "adaptive": ADAPTIVE,
                       "flush_interval_s": FLUSH_INTERVAL, "core_replicas": CORE_URLS},
            "elapsed_seconds": round(elapsed, 2), "total_records_received": total_records,
            "bulk": dict(bulk), "adaptive_state": dict(adaptive_state), "outbox_depth": outbox.qsize(),
            "current_leader_url": current_core, "forwarding": dict(fwd),
            "link": dict(link_bytes), "producer": {"delivered": getattr(_producer, "delivered", None), "failed": getattr(_producer, "failed", None)},
            "per_node": per_node,
        })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "tier": "edge", "id": GATEWAY_ID})


def main():
    threading.Thread(target=sender_loop, daemon=True).start()
    threading.Thread(target=flush_timer_loop, daemon=True).start()
    if ADAPTIVE:
        threading.Thread(target=coalesce_timer_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)


if __name__ == "__main__":
    main()
