"""Run the milestone 5 two-phase-commit classroom scenario."""
from transactions import run_transaction

participants = ["edge-node-a", "edge-node-b"]
print("All participants ready:")
print(run_transaction(participants))
print("\nSimulate edge-node-b failing during prepare:")
print(run_transaction(participants, fail_prepare="edge-node-b"))
