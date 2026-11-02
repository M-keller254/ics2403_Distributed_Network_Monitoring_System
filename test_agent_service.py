import unittest
import tempfile
import os

from agent.service import AgentState


class AgentServiceTests(unittest.TestCase):
    def setUp(self):
        self.state = AgentState(
            "edge-a", lambda: {"cpu_percent": 10.0, "memory_percent": 20.0}
        )
        self.client = self.state.create_app().test_client()

    def test_status_and_lamport_sync_advance_clock(self):
        first = self.client.get("/status").get_json()
        synced = self.client.post("/sync", json={"lamport_clock": 8}).get_json()
        later = self.client.get("/status").get_json()
        self.assertEqual(first["lamport_clock"], 1)
        self.assertEqual(synced["lamport_clock"], 9)
        self.assertEqual(later["lamport_clock"], 10)

    def test_prepare_commit_and_abort_lifecycle(self):
        prepared = self.client.post("/prepare", json={
            "transaction_id": "tx-1", "value": {"threshold": 80}
        })
        committed = self.client.post("/commit", json={"transaction_id": "tx-1"})
        self.assertEqual(prepared.get_json()["vote"], "yes")
        self.assertEqual(committed.get_json()["state"], "committed")

        vote_no = self.client.post("/prepare", json={
            "transaction_id": "tx-2", "value": {}, "force_no": True
        })
        self.assertEqual(vote_no.get_json()["vote"], "no")

    def test_rejects_invalid_sync_clock(self):
        response = self.client.post("/sync", json={"lamport_clock": -1})
        self.assertEqual(response.status_code, 400)

    def test_participant_transaction_survives_agent_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            db_path = os.path.join(folder, "participant.sqlite3")
            first = AgentState("edge-a", lambda: {}, db_path).create_app().test_client()
            first.post("/prepare", json={
                "transaction_id": "durable-tx", "value": {"k": "v"}
            })
            restarted = AgentState("edge-a", lambda: {}, db_path).create_app().test_client()
            status = restarted.get("/transactions/durable-tx")
            self.assertEqual(status.get_json()["state"], "prepared")
            committed = restarted.post("/commit", json={"transaction_id": "durable-tx"})
            self.assertEqual(committed.get_json()["state"], "committed")


if __name__ == "__main__":
    unittest.main()
