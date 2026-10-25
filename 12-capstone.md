# Milestone 12 — Storage and capstone evaluation

The collector stores searchable telemetry history in SQLite and archives JSON objects through the MinIO S3-compatible API. MinIO uses a persistent named volume; Prometheus retains scraped metrics for seven days; Grafana is provisioned with a Prometheus data source and overview dashboard. This local single-server MinIO setup demonstrates object storage integration, not a distributed MinIO cluster or telecom-grade file system. Evaluate freshness, recovery, object retrieval, and report cadence in the capstone protocol.
