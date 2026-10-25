import tempfile
import unittest
from distributed.milestone_07_recovery import retry
from distributed.milestone_08_models import VectorClock
from distributed.milestone_09_transparency import node_state
from distributed.milestone_10_discovery import ServiceRegistry
from distributed.milestone_12_storage import MetricStore

class LaterMilestoneTests(unittest.TestCase):
    def test_retry_recovers_after_transient_failure(self):
        calls = []
        def operation():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("temporary")
            return "ok"
        self.assertEqual(retry(operation, sleep=lambda _: None), "ok")

    def test_vector_clock_orders_and_detects_concurrency(self):
        a = VectorClock(["a", "b"]).tick("a")
        b = VectorClock(["a", "b"]).tick("a").tick("b")
        c = VectorClock(["a", "b"]).tick("b")
        self.assertEqual(a.relation(b), "before")
        self.assertEqual(a.relation(c), "concurrent")

    def test_health_and_registry_expiration(self):
        self.assertEqual(node_state(9, 10, 2), "healthy")
        self.assertEqual(node_state(1, 10, 2), "stale")
        registry = ServiceRegistry()
        registry.register("collector", "http://collector:5000", 20)
        self.assertEqual(registry.resolve("collector", 19), "http://collector:5000")
        self.assertIsNone(registry.resolve("collector", 20))

    def test_sqlite_history_and_retention(self):
        with tempfile.TemporaryDirectory() as folder:
            store = MetricStore(folder + "/history.db")
            store.append("edge-a", 10, 30, 40)
            store.append("edge-a", 20, 31, 41)
            self.assertEqual(store.latest("edge-a")["cpu"], 31)
            self.assertEqual(store.prune_before(15), 1)

if __name__ == "__main__":
    unittest.main()
