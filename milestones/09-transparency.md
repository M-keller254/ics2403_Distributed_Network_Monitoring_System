# Milestone 9 — Transparency and reliability

The collector exposes logical node identifiers and GET /nodes/<node_id> returns health, age, and metrics without requiring callers to know the node's container address. /status marks readings stale. This implements basic access, location, and failure transparency; replication and migration transparency remain future work.
