"""Helpers that turn last-seen timestamps into explicit node health state."""

def node_state(last_seen, now, stale_after):
    if last_seen is None:
        return "unknown"
    return "healthy" if now - last_seen <= stale_after else "stale"

def public_node_view(node_id, reading, now, stale_after):
    """Return a stable, storage-independent status object."""
    received_at = reading.get("received_at")
    return {
        "node_id": node_id,
        "state": node_state(received_at, now, stale_after),
        "cpu_percent": reading.get("cpu_percent"),
        "memory_percent": reading.get("memory_percent"),
        "age_seconds": None if received_at is None else max(0, round(now - received_at, 2)),
    }
