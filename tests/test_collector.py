import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch
from collector.app import create_app

class FakeObjectStore:
    bucket = "test-bucket"
    def __init__(self):
        self.objects = {}
    def put_bytes(self, name, content):
        self.objects[name] = content
        return "fake-etag"
    def get_bytes(self, name):
        return self.objects[name]
    def list_objects(self, prefix=""):
        return [{"object_name": k, "size": len(v), "etag": "fake-etag"}
                for k, v in self.objects.items() if k.startswith(prefix)]

class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = FakeObjectStore()
        self.app = create_app(os.path.join(self.temp.name, "test.db"), stale_after=30,
                              object_store=self.store,
                              agent_urls={"edge-a": "http://edge-a:5001",
                                          "edge-b": "http://edge-b:5002"}).test_client()

    def tearDown(self):
        self.temp.cleanup()

    def test_ingest_status_and_history(self):
        payload = {"node_id":"edge-node-a","timestamp":"2026-01-01T00:00:00Z",
                   "cpu_percent":12.5,"memory_percent":20,"bytes_sent":12,"bytes_recv":34}
        self.assertEqual(self.app.post("/ingest", data=json.dumps(payload), content_type="application/json").status_code, 202)
        self.assertIn("edge-node-a", self.app.get("/status").get_json()["known_nodes"])
        self.assertEqual(len(self.app.get("/nodes/edge-node-a/history").get_json()["readings"]), 1)

    def test_reject_invalid_payload(self):
        self.assertEqual(self.app.post("/ingest", json={"node_id":"x"}).status_code, 400)

    def test_shared_state_uses_compare_and_swap_versions(self):
        created = self.app.put("/state/threshold", json={"value": 80, "expected_version": 0})
        conflict = self.app.put("/state/threshold", json={"value": 90, "expected_version": 0})
        current = self.app.get("/state/threshold")
        self.assertEqual(created.get_json()["version"], 1)
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(current.get_json()["value"], 80)

    @patch("collector.app.requests.post")
    def test_two_phase_commit_commits_and_aborts_across_agent_urls(self, post):
        def response_for(url, json, timeout):
            mock_response = Mock()
            mock_response.status_code = 200
            if url.endswith("/prepare"):
                mock_response.json.return_value = {
                    "vote": "no" if json.get("force_no") else "yes"
                }
            else:
                mock_response.json.return_value = {"state": url.rsplit("/", 1)[-1]}
            return mock_response
        post.side_effect = response_for
        committed = self.app.post("/transactions/two-phase", json={
            "transaction_id": "commit-1", "value": {"rule": 1}
        }).get_json()
        aborted = self.app.post("/transactions/two-phase", json={
            "transaction_id": "abort-1", "value": {"rule": 2},
            "fail_prepare_node": "edge-b"
        }).get_json()
        self.assertEqual(committed["decision"], "commit")
        self.assertEqual(aborted["decision"], "abort")
        self.assertEqual(aborted["completed"], ["edge-a", "edge-b"])

    def test_serial_parallel_aggregate_calls_live_agent_urls(self):
        fake_response = Mock()
        fake_response.json.return_value = {
            "node_id": "edge-a", "lamport_clock": 1, "cpu_percent": 15
        }
        @patch("collector.app.requests.get", return_value=fake_response)
        def run(mock_get):
            parallel = self.app.get("/aggregate?mode=parallel")
            serial = self.app.get("/aggregate?mode=serial")
            self.assertEqual(parallel.status_code, 200)
            self.assertEqual(serial.get_json()["mode"], "serial")
            self.assertEqual(len(parallel.get_json()["edges"]), 2)
            self.assertEqual(mock_get.call_count, 4)
        run()

    def test_registry_registers_and_resolves_node(self):
        registered = self.app.post("/registry", json={
            "node_id": "new-edge", "endpoint": "http://new-edge:5004",
            "lease_seconds": 60
        })
        services = self.app.get("/registry").get_json()["services"]
        self.assertEqual(registered.status_code, 201)
        self.assertTrue(any(s["node_id"] == "new-edge" for s in services))

    def test_archive_object_round_trip_uses_object_store(self):
        uploaded = self.app.post("/storage/objects", json={
            "object_name": "telemetry/test.json", "data": {"sample": 1}
        })
        downloaded = self.app.get("/storage/objects/telemetry/test.json")
        listed = self.app.get("/storage/objects?prefix=telemetry/")
        self.assertEqual(uploaded.status_code, 201)
        self.assertIn(b'"sample": 1', downloaded.data)
        self.assertEqual(len(listed.get_json()["objects"]), 1)

if __name__ == "__main__":
    unittest.main()
