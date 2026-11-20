# Milestone 12 - MinIO Archive and Telecom Capstone

## Goal
Add persistent object-storage archival to the monitoring system and present
a baseline-versus-proposed comparison backed by measurements from the earlier
milestones. Milestone 12 focuses on MinIO archival; Kafka event streaming is
demonstrated separately in Milestone 11.

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
overhead for no corresponding benefit at this scale. This is one MinIO
server with a persistent Docker volume, not a multi-node MinIO cluster or
HDFS deployment.

## The Milestone 12 stack (13 services)
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
| RPC & distributed memory | 11 | `rpc/client_stub.py` (Milestone 11 stack) |
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
# Run from the milestone12 folder. Stop other milestone stacks first because
# they reuse fixed container names.
docker compose up --build -d
docker compose ps
Start-Sleep -Seconds 30
Invoke-RestMethod http://localhost:9500/health
Invoke-RestMethod http://localhost:9500/stats | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:9500/history | ConvertTo-Json -Depth 5
python experiments/capstone_experiment.py --duration 30
```

To read back the newest archived snapshot after at least one snapshot interval:

```powershell
$history = Invoke-RestMethod http://localhost:9500/history
$key = $history.snapshots[-1]
Invoke-RestMethod "http://localhost:9500/history/$key" | ConvertTo-Json -Depth 8
```

The MinIO console is at `http://localhost:9001` (local demo credentials:
`minioadmin` / `minioadmin`). Open the `monitoring-data` bucket to show stored
JSON objects. The MinIO S3 API is at `http://localhost:9000`, and the archiver
service is at `http://localhost:9500`.

`capstone_experiment.py` writes `experiments/results/capstone_storage.csv`
with snapshot counts, the retrieved object key, read-back latency, and success
flags. It also runs the existing performance and RPC experiments. These are
run-specific local measurements, not guarantees. To load the storage evidence
in Power BI Desktop, choose **Get data > Text/CSV** and select
`experiments/results/capstone_storage.csv`. For Kafka producer/consumer results,
use the separate Milestone 11 benchmark CSV.

Stop this stack without deleting its MinIO archive volume:

```powershell
docker compose down
```

## Deliverables
- [x] Integrated monitoring, state, and storage stack (13 services)
- [x] Complete source code (this folder)
- [x] Persistent MinIO object storage (named volume) + archiver write and read-back
- [x] Baseline vs proposed comparison (table above, backed by M2-M11 measurements)
- [ ] Technical report (30-section structure - to be written from these README files)
- [ ] Research paper and oral defense materials
