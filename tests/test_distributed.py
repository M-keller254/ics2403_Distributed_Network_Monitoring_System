import unittest
from distributed.coordination import elect
from distributed.two_phase_commit import run_transaction
from distributed.concurrency import has_wait_cycle

class DistributedConceptTests(unittest.TestCase):
    def test_bully_election_selects_highest_available(self):
        self.assertEqual(elect(["node-1", "node-3", "node-2"]), "node-3")
        self.assertIsNone(elect([]))

    def test_two_phase_commit_aborts_when_prepare_fails(self):
        self.assertEqual(run_transaction(["a", "b"], "b")["decision"], "abort")
        self.assertEqual(run_transaction(["a", "b"])["committed"], ["a", "b"])

    def test_wait_for_cycle_detection(self):
        self.assertTrue(has_wait_cycle({"a": ["b"], "b": ["a"]}))
        self.assertFalse(has_wait_cycle({"a": ["b"], "b": []}))

if __name__ == "__main__":
    unittest.main()
