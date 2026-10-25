# Milestone 7 — Fault tolerance and recovery

Agents retry reports after communication failures; the collector marks readings stale; Compose can restart a stopped process; SQLite participant and collector data survive container recreation through named volumes. The collector has a durable 2PC decision log and POST /transactions/recover retries incomplete phase-two messages. experiments/PROTOCOL.md gives a controlled edge-node stop/restart procedure. This setup does not replicate the collector or guarantee zero downtime.
