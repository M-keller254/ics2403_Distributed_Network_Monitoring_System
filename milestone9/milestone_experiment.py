"""Show health transparency for fresh and stale node reports."""
from transparency import node_state, public_node_view

now = 100.0
print(public_node_view("edge-node-a", {
    "received_at": 98.0, "cpu_percent": 18.0, "memory_percent": 42.0
}, now, stale_after=10))
print("Old report state:", node_state(70.0, now, stale_after=10))
print("Never reported state:", node_state(None, now, stale_after=10))
