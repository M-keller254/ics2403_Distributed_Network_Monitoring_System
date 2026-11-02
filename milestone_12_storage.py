"""SQLite-backed metric history with bounded retention for capstone experiments."""
import sqlite3
from contextlib import closing

class MetricStore:
    def __init__(self, path):
        self.path = path
        with closing(self._connect()) as db:
            with db:
                db.execute("""CREATE TABLE IF NOT EXISTS samples (
                    id INTEGER PRIMARY KEY, node_id TEXT NOT NULL, recorded_at REAL NOT NULL,
                    cpu REAL NOT NULL, memory REAL NOT NULL)""")
                db.execute("CREATE INDEX IF NOT EXISTS samples_by_node_time ON samples(node_id, recorded_at)")

    def _connect(self):
        return sqlite3.connect(self.path, timeout=5)

    def append(self, node_id, recorded_at, cpu, memory):
        with closing(self._connect()) as db:
            with db:
                db.execute("INSERT INTO samples(node_id,recorded_at,cpu,memory) VALUES(?,?,?,?)",
                           (node_id, recorded_at, cpu, memory))

    def latest(self, node_id):
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT recorded_at,cpu,memory FROM samples WHERE node_id=? ORDER BY recorded_at DESC,id DESC LIMIT 1",
                (node_id,)).fetchone()
        return None if row is None else {"recorded_at": row[0], "cpu": row[1], "memory": row[2]}

    def prune_before(self, cutoff):
        with closing(self._connect()) as db:
            with db:
                cursor = db.execute("DELETE FROM samples WHERE recorded_at < ?", (cutoff,))
                return cursor.rowcount
