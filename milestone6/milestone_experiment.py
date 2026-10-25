"""Run the milestone 6 wait-for graph/deadlock example."""
from concurrency import has_wait_cycle, OrderedLockPair

deadlock = {"worker-a": ["worker-b"], "worker-b": ["worker-a"]}
no_deadlock = {"worker-a": ["worker-b"], "worker-b": []}
print("Circular wait detected:", has_wait_cycle(deadlock))
print("Cycle-free graph detected:", has_wait_cycle(no_deadlock))
print("Ordered lock operation result:", OrderedLockPair().execute(lambda: "completed safely"))
