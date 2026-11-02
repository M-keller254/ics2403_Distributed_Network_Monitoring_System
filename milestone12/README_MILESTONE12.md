# Milestone 12 - Distributed File System and Telecom Capstone

## Goal
Add distributed storage, integrate every component built since Milestone 1
into one running system, and present a baseline-vs-proposed comparison
backed by real measurements taken across the whole project.

## What's new: distributed storage
`archiver/app.py` snapshots the collector's `/status` to **MinIO**
(S3-compatible object storage) every `SNAPSHOT_INTERVAL` seconds, and
exposes `GET /history` (list snapshots) and `GET /history/<key>` (read
one back) - genuine storage AND retrieval, not write-and-forget.

```
Users/Devices -> Edge -> Telecom Core -> Cloud -> Distributed Storage
 edge-node-a/b    aggregator-1/2        collector      MinIO (archiver)
```

MinIO was chosen over HDFS: it is S3-compatible, drops into Docker
Compose exactly like every other service here, and matches this
project's data volume (small JSON snapshots, not big files) - HDFS's
NameNode/DataNode architecture would be considerable operational
overhead for no corresponding benefit at this scale.

## The complete system (12 services)
`nameserver, collector, aggregator-1, aggregator-2, edge-node-a, edge-node-b,
core-node, store-a, store-b, txn-coordinator, gateway, minio, archiver`

| Concept | Milestone | Component |
|---|---|---|
| Distributed OS foundation | 1 | agent + collector |
| Processing & performance | 2 | windowed local aggregation |
| Architecture (Edge/Core/Cloud) | 3 | aggregator tier |
| Algorithms & coordination | 4 | aggregator leader election |
| Transactions | 5 | txn-coordinator + store-a/store-b (2PC) |
| Concurrency & deadlock | 6 | collector's `/demo/*` endpoints |
| Fault tolerance | 7 | `fault_test.py`, MTTR/availability |
| System models | 8 | analysis only |
| Transparency | 9 | gateway |
| Naming & process management | 10 | nameserver |
| RPC & distributed memory | 11 | `rpc/client_stub.py` |
| Distributed storage | 12 | minio + archiver |

## Baseline vs proposed, using real measurements already taken

| Property | Baseline (Milestone 2, flat) | Proposed (this system) |
|---|---|---|
| Collector load as nodes grow | Grows with node count | Roughly constant (aggregator absorbs it) |
| Single point of failure | Yes - one collector | No, for the core tier (M4) and the write path (M5) |
| Write consistency under failure | Undefined (no transactions) | Atomic: commits or aborts, never partial (M5, verified) |
| Deadlock handling | None | Demonstrated unsafe/safe/detect, all three reproduced (M6) |
| Recovery | Manual (none built) | Automatic; MTTR measured, with an honest limit: a crashed replica's own prior state is not auto-repaired (M7) |
| Client-facing complexity | Caller must know the collector's address | One stable `/read/<key>` regardless of replica (M9) |
| Service discovery | Hardcoded hostnames | Resolved by logical name (M10) |
| Remote calls | Raw HTTP per caller | `client.put()/.get()`, ~9ms/~3ms (M11, measured) |
| Historical data | None - only "latest" in memory | Snapshotted to MinIO, retrievable by key (M12) |

**The one meaningful improvement this project is built around**: replacing
a single collector with an elected, replicated core tier backed by
atomic (2PC) writes - reducing the collector's bottleneck (M3) while
making the write path recoverable from a crash (M4, M5, M7), at a
measured cost in extra containers and messages.

## Honest limitations (carried forward, not hidden)
- A restarted store replica does not automatically recover data it held
  before crashing (M7) - transparency (M9) hides *which* replica answered,
  not *whether* replicas have reconciled.
- The name server (M10) and MinIO (M12) are each, themselves, unreplicated
  single points of failure - the same lesson Milestone 3 taught about the
  original collector, not yet solved at this scale.
- 2PC blocks if a participant never responds during PREPARE; no
  coordinator-side timeout/recovery protocol was implemented.
- All testing was done on one machine; real network latency and
  partitions were not exercised.

## How to run the full system
```powershell
docker compose up --build
python experiments/capstone_experiment.py --duration 30
python experiments/fault_test.py --component aggregator --fault crash --trials 3
```

## Deliverables
- [x] Fully working distributed system (12 services, `docker compose up --build`)
- [x] Complete source code (this folder)
- [x] Distributed storage (MinIO + archiver, write and read-back verified)
- [x] Baseline vs proposed comparison (table above, backed by M2-M11 measurements)
- [ ] Technical report (30-section structure - to be written from these README files)
- [ ] Research paper and oral defense materials
