"""Run a vector-clock causal ordering example."""
from vector_clock import VectorClock

before = VectorClock(["edge-a", "edge-b"]).tick("edge-a")
after = VectorClock(["edge-a", "edge-b"], dict(before.values)).tick("edge-b")
independent = VectorClock(["edge-a", "edge-b"]).tick("edge-b")
print("Before vs after:", before.relation(after))
print("Independent events:", before.relation(independent))
after.merge(independent)
print("Merged clock:", after.values)
