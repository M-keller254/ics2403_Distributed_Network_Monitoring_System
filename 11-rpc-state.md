# Milestone 11 — RPC and distributed state

The collector exposes JSON RPC through POST /rpc, including status, aggregate, history, and versioned shared-state operations. POST/GET /state/<key> uses SQLite compare-and-swap versions to detect conflicting updates. This provides a small shared-state abstraction, not a distributed-memory system with replication or partition tolerance.
