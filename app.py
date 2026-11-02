"""HTTP ingestion service with SQLite persistence and Prometheus metrics."""
import os
import json
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
import requests

from distributed.concurrency import has_wait_cycle
from storage.minio_store import MinioObjectStore

REQUESTS = Counter("monitoring_ingest_total", "Accepted metric reports", ["node_id"])
CPU = Gauge("monitoring_cpu_percent", "Latest reported CPU percentage", ["node_id"])
MEMORY = Gauge("monitoring_memory_percent", "Latest reported memory percentage", ["node_id"])
LAST_SEEN = Gauge("monitoring_node_last_seen_timestamp_seconds", "Last report time", ["node_id"])


def create_app(db_path=None, stale_after=None, object_store=None, agent_urls=None):
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024
    default_db = "/data/metrics.sqlite3" if os.path.isdir("/data") else "metrics.sqlite3"
    db_path = db_path or os.getenv("DB_PATH", default_db)
    stale_after = float(stale_after if stale_after is not None
                        else os.getenv("STALE_AFTER_SECONDS", "10"))
    agent_urls = agent_urls if agent_urls is not None else json.loads(
        os.getenv("AGENT_URLS", "{}")
    )
    object_store = object_store or MinioObjectStore()

    @contextmanager
    def connect():
        conn = sqlite3.connect(db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    with connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY, node_id TEXT NOT NULL, timestamp TEXT NOT NULL,
            received_at REAL NOT NULL, cpu_percent REAL NOT NULL, memory_percent REAL NOT NULL,
            bytes_sent INTEGER NOT NULL, bytes_recv INTEGER NOT NULL)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_node_id ON readings(node_id, id DESC)")
        conn.execute("""CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY, value TEXT NOT NULL, decision TEXT NOT NULL,
            participants TEXT NOT NULL, prepared TEXT NOT NULL, created_at REAL NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS transaction_recovery (
            transaction_id TEXT PRIMARY KEY, decision TEXT NOT NULL,
            participants TEXT NOT NULL, completed TEXT NOT NULL, updated_at REAL NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS shared_state (
            state_key TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL,
            updated_at REAL NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS service_registry (
            node_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, expires_at REAL NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS archives (
            object_name TEXT PRIMARY KEY, size INTEGER NOT NULL, etag TEXT NOT NULL,
            created_at REAL NOT NULL)""")

    @app.get("/health")
    def health():
        return jsonify(status="ok")

    def poll_agent(node_id, endpoint, timeout=2):
        started = time.perf_counter()
        try:
            response = requests.get(endpoint.rstrip("/") + "/status", timeout=timeout)
            response.raise_for_status()
            return node_id, response.json(), (time.perf_counter() - started) * 1000
        except (requests.RequestException, ValueError) as exc:
            return node_id, {"error": str(exc)}, (time.perf_counter() - started) * 1000

    @app.get("/aggregate")
    def aggregate():
        mode = request.args.get("mode", "parallel")
        if mode not in {"parallel", "serial"}:
            return jsonify(error="mode must be parallel or serial"), 400
        started = time.perf_counter()
        readings = {}
        poll_latencies = {}
        if mode == "serial":
            results = [poll_agent(node, url) for node, url in agent_urls.items()]
        else:
            with ThreadPoolExecutor(max_workers=max(1, len(agent_urls))) as pool:
                futures = [pool.submit(poll_agent, node, url)
                           for node, url in agent_urls.items()]
                results = [future.result() for future in as_completed(futures)]
        for node, reading, latency in results:
            readings[node] = reading
            poll_latencies[node] = round(latency, 3)
        for node, reading in readings.items():
            if "lamport_clock" in reading:
                reading["core_observed_at"] = time.time()
        return jsonify(node="core-node", mode=mode, timestamp=time.time(),
                       elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
                       poll_latencies_ms=poll_latencies, edges=readings)

    @app.get("/nodes/<node_id>")
    def node_view(node_id):
        with connect() as conn:
            row = conn.execute("""SELECT * FROM readings WHERE node_id=?
                ORDER BY id DESC LIMIT 1""", (node_id,)).fetchone()
        if row is None:
            return jsonify(node_id=node_id, state="unknown"), 404
        age = max(0.0, time.time() - row["received_at"])
        return jsonify(node_id=node_id, state="stale" if age > stale_after else "healthy",
                       age_seconds=round(age, 2), cpu_percent=row["cpu_percent"],
                       memory_percent=row["memory_percent"], location="logical-node")

    @app.post("/transactions/two-phase")
    def two_phase_transaction():
        payload = request.get_json(silent=True) or {}
        txid = payload.get("transaction_id") or str(uuid.uuid4())
        value = payload.get("value")
        participants = payload.get("participants") or list(agent_urls)
        fail_node = payload.get("fail_prepare_node")
        if not isinstance(txid, str) or not txid or not isinstance(value, dict):
            return jsonify(error="transaction_id (optional) and object value are required"), 400
        if not participants or any(node not in agent_urls for node in participants):
            return jsonify(error="participants must be registered agent node IDs"), 400
        with connect() as conn:
            existing = conn.execute(
                "SELECT decision FROM transactions WHERE transaction_id=?", (txid,)
            ).fetchone()
        if existing:
            return jsonify(error="transaction_id already exists", decision=existing["decision"]), 409
        prepared = []
        votes = {}
        encoded_value = json.dumps(value, sort_keys=True)
        for node in participants:
            try:
                response = requests.post(agent_urls[node].rstrip("/") + "/prepare",
                    json={"transaction_id": txid, "value": value,
                          "force_no": node == fail_node}, timeout=3)
                response.raise_for_status()
                vote = response.json().get("vote", "no")
            except (requests.RequestException, ValueError):
                vote = "no"
            votes[node] = vote
            if vote == "yes":
                prepared.append(node)
            else:
                break
        decision = "commit" if len(prepared) == len(participants) else "abort"
        encoded_participants = json.dumps(participants)
        encoded_value = json.dumps(value, sort_keys=True)
        # Persist the coordinator decision before sending the second phase.
        with connect() as conn:
            conn.execute("INSERT INTO transactions VALUES(?,?,?,?,?,?)",
                (txid, encoded_value, decision, encoded_participants,
                 json.dumps(prepared), time.time()))
            conn.execute("INSERT INTO transaction_recovery VALUES(?,?,?,?,?)",
                (txid, decision, encoded_participants, "[]", time.time()))
        completed = []
        for node in participants:
            action = "commit" if decision == "commit" else "abort"
            try:
                response = requests.post(agent_urls[node].rstrip("/") + "/" + action,
                    json={"transaction_id": txid}, timeout=3)
                response.raise_for_status()
                completed.append(node)
            except requests.RequestException:
                pass
        with connect() as conn:
            conn.execute("UPDATE transaction_recovery SET completed=?,updated_at=? WHERE transaction_id=?",
                         (json.dumps(completed), time.time(), txid))
        return jsonify(transaction_id=txid, decision=decision, votes=votes,
                       prepared=prepared, completed=completed,
                       recovery_pending=len(completed) != len(participants),
                       note="2PC decision is durable at the coordinator; participants persist state in per-node SQLite volumes."), 200

    @app.post("/transactions/recover")
    def recover_transactions():
        recovered = []
        with connect() as conn:
            rows = conn.execute("SELECT * FROM transaction_recovery").fetchall()
        for row in rows:
            participants = json.loads(row["participants"])
            completed = set(json.loads(row["completed"]))
            action = "commit" if row["decision"] == "commit" else "abort"
            for node in participants:
                if node in completed or node not in agent_urls:
                    continue
                try:
                    response = requests.post(agent_urls[node].rstrip("/") + "/" + action,
                        json={"transaction_id": row["transaction_id"]}, timeout=3)
                    response.raise_for_status()
                    completed.add(node)
                except requests.RequestException:
                    pass
            with connect() as conn:
                conn.execute("UPDATE transaction_recovery SET completed=?,updated_at=? WHERE transaction_id=?",
                             (json.dumps(sorted(completed)), time.time(), row["transaction_id"]))
            recovered.append({"transaction_id": row["transaction_id"],
                              "decision": row["decision"],
                              "completed": sorted(completed),
                              "pending": sorted(set(participants) - completed)})
        return jsonify(recovered=recovered)

    @app.get("/transactions")
    def transaction_history():
        with connect() as conn:
            rows = conn.execute("SELECT * FROM transactions ORDER BY created_at DESC LIMIT 100").fetchall()
        return jsonify(transactions=[dict(row) for row in rows])

    @app.post("/deadlock/check")
    def deadlock_check():
        graph = (request.get_json(silent=True) or {}).get("wait_for_graph", {})
        if not isinstance(graph, dict):
            return jsonify(error="wait_for_graph must be an object"), 400
        return jsonify(deadlock_detected=has_wait_cycle(graph))

    @app.route("/registry", methods=["GET", "POST"])
    def registry():
        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            node, endpoint = payload.get("node_id"), payload.get("endpoint")
            try:
                lease = float(payload.get("lease_seconds", 60))
            except (TypeError, ValueError):
                return jsonify(error="lease_seconds must be a positive number"), 400
            if not node or not endpoint or lease <= 0:
                return jsonify(error="node_id, endpoint, and positive lease_seconds required"), 400
            expires = time.time() + lease
            with connect() as conn:
                conn.execute("INSERT OR REPLACE INTO service_registry VALUES(?,?,?)",
                             (node, endpoint, expires))
            agent_urls[node] = endpoint
            return jsonify(node_id=node, endpoint=endpoint, expires_at=expires), 201
        with connect() as conn:
            conn.execute("DELETE FROM service_registry WHERE expires_at <= ?", (time.time(),))
            rows = conn.execute("SELECT node_id,endpoint,expires_at FROM service_registry").fetchall()
        registered = {row["node_id"]: dict(row) for row in rows}
        for node, endpoint in agent_urls.items():
            registered.setdefault(node, {"node_id": node, "endpoint": endpoint,
                                         "expires_at": None})
        return jsonify(services=list(registered.values()))

    @app.route("/state/<state_key>", methods=["GET", "PUT"])
    def shared_state(state_key):
        if request.method == "GET":
            with connect() as conn:
                row = conn.execute("SELECT * FROM shared_state WHERE state_key=?",
                                   (state_key,)).fetchone()
            if row is None:
                return jsonify(error="state key not found"), 404
            return jsonify(key=state_key, value=json.loads(row["value"]),
                           version=row["version"], updated_at=row["updated_at"])
        payload = request.get_json(silent=True) or {}
        if "value" not in payload or not isinstance(payload.get("expected_version"), int):
            return jsonify(error="value and integer expected_version required"), 400
        encoded = json.dumps(payload["value"], sort_keys=True)
        with connect() as conn:
            current = conn.execute("SELECT version FROM shared_state WHERE state_key=?",
                                   (state_key,)).fetchone()
            version = current["version"] if current else 0
            if version != payload["expected_version"]:
                return jsonify(error="version conflict", current_version=version), 409
            new_version = version + 1
            conn.execute("INSERT OR REPLACE INTO shared_state VALUES(?,?,?,?)",
                         (state_key, encoded, new_version, time.time()))
        return jsonify(key=state_key, value=payload["value"], version=new_version), 200

    @app.post("/rpc")
    def rpc():
        payload = request.get_json(silent=True) or {}
        method = payload.get("method")
        if method == "status":
            return status()
        if method == "aggregate":
            return aggregate()
        if method == "history":
            node = payload.get("node_id", "")
            try:
                limit = min(1000, max(1, int(payload.get("limit", 100))))
            except (TypeError, ValueError):
                return jsonify(error="limit must be an integer"), 400
            with connect() as conn:
                rows = conn.execute("SELECT timestamp,cpu_percent,memory_percent,bytes_sent,bytes_recv FROM readings WHERE node_id=? ORDER BY id DESC LIMIT ?",
                                    (node, limit)).fetchall()
            return jsonify(node_id=node, readings=[dict(row) for row in reversed(rows)])
        if method == "state.get":
            key = payload.get("key")
            if not isinstance(key, str) or not key:
                return jsonify(error="key is required"), 400
            with connect() as conn:
                row = conn.execute("SELECT * FROM shared_state WHERE state_key=?", (key,)).fetchone()
            if row is None:
                return jsonify(error="state key not found"), 404
            return jsonify(key=key, value=json.loads(row["value"]),
                           version=row["version"], updated_at=row["updated_at"])
        if method == "state.put":
            key = payload.get("key")
            expected = payload.get("expected_version")
            if not isinstance(key, str) or not key or "value" not in payload \
                    or not isinstance(expected, int):
                return jsonify(error="key, value, and integer expected_version required"), 400
            encoded = json.dumps(payload["value"], sort_keys=True)
            with connect() as conn:
                current = conn.execute("SELECT version FROM shared_state WHERE state_key=?",
                                       (key,)).fetchone()
                version = current["version"] if current else 0
                if version != expected:
                    return jsonify(error="version conflict", current_version=version), 409
                new_version = version + 1
                conn.execute("INSERT OR REPLACE INTO shared_state VALUES(?,?,?,?)",
                             (key, encoded, new_version, time.time()))
            return jsonify(key=key, value=payload["value"], version=new_version)
        return jsonify(error="unknown RPC method"), 404

    @app.post("/storage/objects")
    def archive_object():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict) or "data" not in payload:
            return jsonify(error="JSON object with data field required"), 400
        object_name = payload.get("object_name") or (
            "telemetry/" + datetime.now(timezone.utc).strftime("%Y/%m/%d/")
            + str(uuid.uuid4()) + ".json")
        if (not isinstance(object_name, str) or ".." in object_name.split("/")
                or object_name.startswith("/")):
            return jsonify(error="invalid object_name"), 400
        body = json.dumps(payload["data"], sort_keys=True).encode("utf-8")
        try:
            etag = object_store.put_bytes(object_name, body)
        except Exception:
            app.logger.exception("MinIO archive write failed")
            return jsonify(error="object storage unavailable"), 503
        with connect() as conn:
            conn.execute("INSERT OR REPLACE INTO archives VALUES(?,?,?,?)",
                         (object_name, len(body), etag, time.time()))
        return jsonify(bucket=object_store.bucket, object_name=object_name,
                       size=len(body), etag=etag), 201

    @app.get("/storage/objects")
    def list_archives():
        prefix = request.args.get("prefix", "")
        try:
            objects = object_store.list_objects(prefix)
        except Exception:
            app.logger.exception("MinIO listing failed")
            return jsonify(error="object storage unavailable"), 503
        return jsonify(bucket=object_store.bucket, objects=objects)

    @app.get("/storage/objects/<path:object_name>")
    def get_archive(object_name):
        try:
            body = object_store.get_bytes(object_name)
            return app.response_class(body, content_type="application/json")
        except Exception:
            return jsonify(error="object not found or storage unavailable"), 404

    @app.post("/ingest")
    def ingest():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(error="JSON object required"), 400
        try:
            node = payload["node_id"]
            if not isinstance(node, str) or not node or len(node) > 128:
                raise ValueError("invalid node_id")
            timestamp = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
            cpu, memory = float(payload["cpu_percent"]), float(payload["memory_percent"])
            sent, recv = int(payload["bytes_sent"]), int(payload["bytes_recv"])
            if not 0 <= cpu <= 100 or not 0 <= memory <= 100 or sent < 0 or recv < 0:
                raise ValueError("metric value out of range")
        except (KeyError, TypeError, ValueError) as exc:
            return jsonify(error=f"invalid metric payload: {exc}"), 400
        received = time.time()
        with connect() as conn:
            conn.execute("INSERT INTO readings(node_id,timestamp,received_at,cpu_percent,memory_percent,bytes_sent,bytes_recv) VALUES(?,?,?,?,?,?,?)",
                         (node, str(timestamp), received, cpu, memory, sent, recv))
        REQUESTS.labels(node).inc()
        CPU.labels(node).set(cpu)
        MEMORY.labels(node).set(memory)
        LAST_SEEN.labels(node).set(received)
        return jsonify(status="accepted"), 202

    @app.get("/status")
    def status():
        with connect() as conn:
            rows = conn.execute("""SELECT r.* FROM readings r JOIN
                (SELECT node_id, MAX(id) id FROM readings GROUP BY node_id) latest
                ON r.id=latest.id ORDER BY r.node_id""").fetchall()
        now = time.time()
        nodes = {}
        for row in rows:
            age = max(0, now - row["received_at"])
            nodes[row["node_id"]] = {
                "timestamp": row["timestamp"], "cpu_percent": row["cpu_percent"],
                "memory_percent": row["memory_percent"], "bytes_sent": row["bytes_sent"],
                "bytes_recv": row["bytes_recv"], "age_seconds": round(age, 2),
                "stale": age > stale_after,
            }
        return jsonify(known_nodes=nodes)

    @app.get("/nodes/<node_id>/history")
    def history(node_id):
        try:
            limit = min(1000, max(1, int(request.args.get("limit", "100"))))
        except ValueError:
            return jsonify(error="limit must be an integer"), 400
        with connect() as conn:
            rows = conn.execute("SELECT timestamp,cpu_percent,memory_percent,bytes_sent,bytes_recv FROM readings WHERE node_id=? ORDER BY id DESC LIMIT ?",
                                (node_id, limit)).fetchall()
        return jsonify(node_id=node_id, readings=[dict(row) for row in reversed(rows)])

    @app.get("/metrics")
    def metrics():
        return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
