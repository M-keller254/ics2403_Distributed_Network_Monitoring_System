"""HTTP control plane for one edge/core monitoring node."""
import json
import os
import sqlite3
from contextlib import contextmanager
from threading import RLock
import time

from flask import Flask, jsonify, request


class AgentState:
    def __init__(self, node_id, metrics_reader, db_path=None):
        self.node_id = node_id
        self.metrics_reader = metrics_reader
        self.db_path = db_path or os.getenv("AGENT_DB_PATH", ":memory:")
        self.lock = RLock()
        self.clock = 0
        self._memory_db = (
            sqlite3.connect(":memory:", check_same_thread=False)
            if self.db_path == ":memory:" else None
        )
        if self._memory_db is not None:
            self._memory_db.row_factory = sqlite3.Row
        with self.connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS participant_transactions (
                transaction_id TEXT PRIMARY KEY, value TEXT NOT NULL, state TEXT NOT NULL,
                updated_at REAL NOT NULL)""")

    @contextmanager
    def connect(self):
        db = self._memory_db or sqlite3.connect(self.db_path, timeout=5)
        if self._memory_db is None:
            db.row_factory = sqlite3.Row
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            if self._memory_db is None:
                db.close()

    def create_app(self):
        app = Flask(__name__)

        @app.get("/health")
        def health():
            return jsonify(status="ok", node_id=self.node_id)

        @app.get("/status")
        def status():
            with self.lock:
                self.clock += 1
                current_clock = self.clock
            payload = self.metrics_reader()
            payload.update(node_id=self.node_id, timestamp=time.time(),
                           lamport_clock=current_clock)
            return jsonify(payload)

        @app.post("/sync")
        def sync():
            incoming = (request.get_json(silent=True) or {}).get("lamport_clock", 0)
            if not isinstance(incoming, int) or incoming < 0:
                return jsonify(error="lamport_clock must be a non-negative integer"), 400
            with self.lock:
                self.clock = max(self.clock, incoming) + 1
                current_clock = self.clock
            return jsonify(node_id=self.node_id, lamport_clock=current_clock,
                           timestamp=time.time())

        @app.post("/prepare")
        def prepare():
            payload = request.get_json(silent=True) or {}
            transaction_id, value = payload.get("transaction_id"), payload.get("value")
            if not isinstance(transaction_id, str) or not transaction_id:
                return jsonify(error="transaction_id is required"), 400
            with self.lock:
                if payload.get("force_no"):
                    return jsonify(node_id=self.node_id, vote="no"), 200
                encoded = json.dumps(value, sort_keys=True)
                with self.connect() as db:
                    previous = db.execute(
                        "SELECT value,state FROM participant_transactions WHERE transaction_id=?",
                        (transaction_id,)).fetchone()
                    if previous and previous["state"] == "committed":
                        return jsonify(node_id=self.node_id, vote="yes", state="committed"), 200
                    if previous and previous["state"] == "aborted":
                        return jsonify(node_id=self.node_id, vote="no", state="aborted"), 200
                    if previous and previous["value"] != encoded:
                        return jsonify(error="transaction value conflicts with prior prepare"), 409
                    db.execute("INSERT OR REPLACE INTO participant_transactions VALUES(?,?,?,?)",
                               (transaction_id, encoded, "prepared", time.time()))
            return jsonify(node_id=self.node_id, vote="yes", state="prepared"), 200

        @app.post("/commit")
        def commit():
            transaction_id = (request.get_json(silent=True) or {}).get("transaction_id")
            with self.lock:
                with self.connect() as db:
                    tx = db.execute("SELECT state FROM participant_transactions WHERE transaction_id=?",
                                    (transaction_id,)).fetchone()
                    if not tx:
                        return jsonify(error="transaction was not prepared"), 409
                    if tx["state"] == "aborted":
                        return jsonify(error="transaction was aborted"), 409
                    db.execute("UPDATE participant_transactions SET state='committed',updated_at=? WHERE transaction_id=?",
                               (time.time(), transaction_id))
            return jsonify(node_id=self.node_id, transaction_id=transaction_id,
                           state="committed"), 200

        @app.post("/abort")
        def abort():
            transaction_id = (request.get_json(silent=True) or {}).get("transaction_id")
            with self.lock:
                with self.connect() as db:
                    tx = db.execute("SELECT state,value FROM participant_transactions WHERE transaction_id=?",
                                    (transaction_id,)).fetchone()
                    if tx and tx["state"] == "committed":
                        return jsonify(error="transaction already committed"), 409
                    if tx:
                        db.execute("UPDATE participant_transactions SET state='aborted',updated_at=? WHERE transaction_id=?",
                                   (time.time(), transaction_id))
                    else:
                        db.execute("INSERT INTO participant_transactions VALUES(?,?,?,?)",
                                   (transaction_id, "{}", "aborted", time.time()))
            return jsonify(node_id=self.node_id, transaction_id=transaction_id,
                           state="aborted"), 200

        @app.get("/transactions/<transaction_id>")
        def transaction_status(transaction_id):
            with self.lock:
                with self.connect() as db:
                    tx = db.execute("SELECT value,state,updated_at FROM participant_transactions WHERE transaction_id=?",
                                    (transaction_id,)).fetchone()
                if tx is None:
                    return jsonify(error="transaction not found"), 404
                return jsonify(node_id=self.node_id, transaction_id=transaction_id,
                               value=json.loads(tx["value"]), state=tx["state"],
                               updated_at=tx["updated_at"])

        return app
