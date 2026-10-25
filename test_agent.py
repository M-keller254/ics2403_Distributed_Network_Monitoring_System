import unittest
from agent.agent import select_interval

class AdaptiveSamplingTests(unittest.TestCase):
    def test_fixed_interval_is_preserved_by_default(self):
        self.assertEqual(select_interval({"cpu_percent": 99, "memory_percent": 99}), 2.0)

    def test_quiet_metrics_use_longer_interval(self):
        self.assertEqual(
            select_interval({"cpu_percent": 20, "memory_percent": 30}, adaptive=True),
            5.0,
        )

    def test_hot_metrics_use_shorter_interval(self):
        self.assertEqual(
            select_interval({"cpu_percent": 90, "memory_percent": 30}, adaptive=True),
            1.0,
        )

if __name__ == "__main__":
    unittest.main()
