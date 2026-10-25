# Assignment coverage and evidence map

The assignment requires one system to evolve through 12 weeks, with implementation, measurement, analysis, and reproducibility at every stage. This map distinguishes what the starter demonstrates from what still needs evidence before claiming completion.

| Milestone | Requirement | Current implementation | Evidence still needed |
|---|---|---|---|
| 1 | Nodes, processes, resources, communication | Three Docker agent processes report to one collector | Document process/resource model and why the topology fits Theme 2 |
| 2 | Distribute work; measure throughput, latency, jitter, loss, CPU, memory | Periodic monitoring plus experiments/capstone_benchmark.py measures ingestion latency, latency jitter, request failures, throughput, client resource/network use, and monitored-node resources | Run at multiple request rates/node counts; add packet-level loss measurement |
| 3 | Edge-core-cloud architecture and trade-offs | HTTP-enabled edge/core agents, central collector, MinIO archive, Prometheus, Grafana | Record scaling trade-offs for more nodes and compare deployment alternatives |
| 4 | Coordination and event ordering | Lamport-clock experiment in milestone4 package; deterministic election helper | Save coordination CSV and analyze message complexity/delay across node counts |
| 5 | Distributed transaction and recovery | Core 2PC coordinator calls each live agent's prepare/commit/abort routes; decisions are logged in SQLite | Inject participant and network failures; report transaction latency and recovery limitations |
| 6 | Concurrent execution and deadlock management | Parallel polling, threaded Flask agents, lock-protected state, and live wait-for cycle endpoint | Vary concurrency and request rate; record contention and deadlock evidence |
| 7 | Fault injection, detection, recovery, availability, MTTR | Stale detection, report retries, SQLite persistence, Compose restart, documented stop/start experiment | Run the failure protocol and record detection/recovery times and availability |
| 8 | Compare host, processor-pool, workstation, server-based, integrated models | experiments/compare_models.py compares measured serial and parallel live-agent polling; model rationale is documented | Run across node counts and discuss model fit/cost/limits |
| 9 | Access, location, replication, migration, failure transparency | Logical node status hides endpoints; stale/unknown state is exposed via collector APIs | Demonstrate location/access/failure behavior; replication/migration remain explicit limitations |
| 10 | Naming, discovery, process lifecycle | Compose service DNS plus persistent lease registry; Compose supervises service processes | Demonstrate registration/expiry and document process start/stop scheduling |
| 11 | RPC and distributed shared state | JSON RPC endpoint plus SQLite compare-and-swap shared state | Benchmark RPC latency and consistency under concurrent updates |
| 12 | Distributed file storage and integrated telecom capstone | SQLite telemetry history plus MinIO S3-compatible object archive, Prometheus, Grafana | Run integrated failure/performance trials, compare baseline/adaptive reporting, and report limits |

## Proposed engineering improvement

The agent now supports optional adaptive reporting. With ADAPTIVE_REPORTING=true, it reports every QUIET_INTERVAL seconds under normal CPU/memory use and every BUSY_INTERVAL seconds when either configured threshold is crossed. The default remains the fixed two-second baseline. Unit checks cover the fixed, quiet, and high-load interval choices. This gives a measurable trade-off: fewer reports during quiet periods versus faster visibility during resource pressure.

Compare modes using the same node count and workload. Record report counts, freshness/latency, CPU, memory, and network-byte deltas. Preserve both CSV outputs and exact settings.

## Completion standard

Do not mark a milestone complete because a script prints an expected value. Keep its source, repeatable command, raw measurements or test output, analysis, and a failure-driven engineering-log entry. Limit conclusions to what the evidence demonstrates.
