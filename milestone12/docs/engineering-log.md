# Failure-Driven Engineering Log

Every week, answer all six questions below and append a new dated entry.
Do not delete previous weeks' entries.

---

## Week 1 — Distributed OS Foundation

**Date:** <fill in>

**What changed?**
Built the first prototype: two edge nodes (edge-node-a, edge-node-b) and
one core node, each running an agent that reads CPU/memory and reports
to a central collector over HTTP every 5 seconds. Collector exposes
`/report`, `/status`, and `/health` endpoints.

**What failed?**
(Fill in once you've run it — e.g. "All three nodes reported identical
memory percentages because psutil read host-wide memory instead of the
container's own usage.")

**Why did it fail?**
(e.g. "psutil.virtual_memory() reports the whole machine, not the
cgroup the container is confined to.")

**How was it fixed?**
(e.g. "Read /sys/fs/cgroup/memory.current and memory.max directly, and
set explicit mem_limit/cpus per container in docker-compose.yml so
each node has a genuinely different resource ceiling.")

**What alternative was considered?**
(e.g. "Considered using docker stats via the Docker API from inside
the collector instead of self-reporting agents. Rejected for Week 1
because it centralizes measurement logic instead of distributing it,
which defeats the purpose of the exercise.")

**What was learned?**
(e.g. "Containerized 'per-node' metrics require explicit resource
limits and cgroup-aware reading; naive psutil calls silently return
host-level data.")

---

## Week 2 — Distributed Processing and Performance

**Date:** <fill in>

**What changed?**
Agents no longer send one raw reading per report. Each node now
collects a local WINDOW_SIZE of CPU/memory samples and computes its
own min/avg/max before sending a single aggregated summary to the
collector — this is the distributed-processing element for this
milestone: computation (aggregation) happens on the node, not
centrally. Added `experiments/run_experiment.py`, which polls
`/status` over a fixed duration and derives throughput, per-node
latency, and jitter, and `experiments/plot_results.py` to graph them.

**What failed?**
(Fill in once you run it — e.g. "Latency looked artificially large at
first because agent 'sent_at' used time.time() (local clock, no
timezone) while the collector's 'received_at' used an ISO UTC
timestamp; comparing them directly gave wrong latency values.")

**Why did it fail?**
(e.g. "Clock representations didn't match: Unix timestamp vs ISO 8601
string in different timezones/formats.")

**How was it fixed?**
(e.g. "Standardized on: agent sends `sent_at` as a raw Unix timestamp;
collector's `received_at` is parsed back into a Unix timestamp with
`datetime.fromisoformat(...).timestamp()` before subtracting, in
`run_experiment.py`.")

**What alternative was considered?**
(e.g. "Considered using NTP-synced clocks or a monotonic clock shared
via the network for stricter latency accuracy. Rejected for Week 2 as
overkill on a single Docker host where all containers share the same
system clock; revisit if nodes ever run on physically separate
machines.")

**What was learned?**
(e.g. "In distributed systems, timestamp format and clock
synchronization matter as much as the logic itself — mismatched
timestamp formats silently produce plausible-looking but wrong
latency numbers.")

---

<!-- Copy the block above for Weeks 3–12 -->

---

## Week 3 - Distributed Architecture

**What changed?** Added an aggregator tier between edge nodes and the collector
(Edge -> Core -> Cloud). Added `docker-compose.flat.yml`, `.m3.yml` and the default file.
**What failed?** (fill in from your runs)
**Why did it fail?** (fill in)
**How was it fixed?** (fill in)
**What alternative was considered?** Sending all edge reports straight to the collector (the flat baseline).
**What was learned?** (fill in; e.g. the aggregator removes collector load growth but is a new single point of failure)

---

## Week 4 - Distributed Algorithms and Coordination

**What changed?** Two aggregators with heartbeat-based leader election; edge nodes fan out to both.
**What failed?** (fill in from your runs)
**Why did it fail?** (fill in)
**How was it fixed?** (fill in)
**What alternative was considered?** Full Bully or Raft; rejected as too heavy for two nodes.
**What was learned?** (fill in; e.g. failover time is bounded by heartbeat timeout plus flush interval)

---

## Correction to Week 2 (throughput)

Overall throughput was computed as the collector's lifetime record count divided by 60 s,
so it grew with uptime (this caused the 22.7 vs 93.45 records/s gap). Fixed in
`run_experiment.py` to count only records received during the run. Re-run Week 2
experiments to get valid throughput numbers.

---

## Week 5 - Distributed Transactions

**What changed?** Added a 2PC coordinator and two storage replicas. Writes now go
through Prepare -> Commit/Abort instead of being written directly.
**What failed?** (fill in from your own run)
**Why did it fail?** (fill in)
**How was it fixed?** (fill in)
**What alternative was considered?** Writing to both stores directly with no
coordinator; rejected because a crash mid-write could leave the replicas inconsistent.
**What was learned?** (fill in; e.g. 2PC trades availability for consistency - a single
unreachable participant blocks/aborts the whole transaction)

---

## Week 6 - Concurrency and Deadlock Management

**What changed?** Added two shared locks in the collector and three strategies
(unsafe, safe-by-ordering, detect-and-recover) against the same two operations.
**What failed?** (fill in; e.g. the first /demo/unsafe call permanently hung the
collector process, confirming a genuine deadlock rather than a slow response)
**Why did it fail?** Two threads acquired the same two locks in opposite order, so
each held one lock the other needed - a classic circular wait.
**How was it fixed?** /demo/safe fixes it by always acquiring locks in the same
global order; /demo/detect fixes it by using try-acquire with a timeout and
aborting one side on failure.
**What alternative was considered?** A global single lock for all shared state;
rejected because it would serialise unrelated operations unnecessarily.
**What was learned?** (fill in)

---

## Week 7 - Fault Tolerance and Recovery

**What changed?** Added `experiments/fault_test.py`: injects crash/omission/
communication failures into the aggregator tier and the store tier, measures
MTTR and availability.
**What failed?** After killing and restarting store-b, it was missing data
that store-a still had, even though the coordinator reported new writes as
successfully committed again.
**Why did it fail?** `store/app.py` holds `committed = {}` purely in memory.
A restarted process starts with an empty dict - it has no way to recover
data it held before the crash, since nothing was ever persisted or
transferred from the surviving replica.
**How was it fixed?** Not fixed - documented as a known limitation instead.
Fixing it properly would need either a write-ahead log / disk-backed commits,
or a state-transfer step when a replica rejoins (out of scope for this
course-scale demo).
**What alternative was considered?** Having the coordinator replay all past
commits to a rejoining participant; rejected as a significant feature on its
own (would need a durable transaction log at the coordinator too) and better
suited to a dedicated future milestone than a quick fix here.
**What was learned?** "The write path is available again" and "the data is
consistent again" are different claims. Milestone 5's 2PC correctly prevents
*new* inconsistency; it says nothing about *repairing* a replica that lost
state while it was down. Verify data state directly after a recovery test,
not just whether new requests succeed.

---

## Week 9 - Transparency and Reliability

**What changed?** Added `gateway/app.py` exposing `GET /read/<key>`, trying each
store replica in turn and returning the first that answers.
**What failed?** Nothing failed in the gateway itself; the test exposed a limitation
already logged in Week 7 - the gateway cannot tell a restarted store-a is missing data
store-b still has, since it just returns whichever replica answers first.
**Why did it fail?** The gateway only implements failure transparency (try the next
replica), not consistency checking across replicas.
**How was it fixed?** Not fixed - documented as a limitation. True read-repair would
need version numbers or timestamps per key, out of scope here.
**What alternative was considered?** Always reading from both replicas and comparing;
rejected as doubling read load for a check that would rarely catch anything in this
course-scale demo.
**What was learned?** Transparency about *where* data comes from is a different,
easier problem than transparency about *whether* that data is correct.

---

## Week 10 - Naming and Distributed Process Management

**What changed?** Added `nameserver/app.py`. `store/app.py` now self-registers and
heartbeats via `register_client.py`. Gateway resolves replica locations by logical
name when a name server is configured.
**What failed?** First attempt put `register_client.py` only in `nameserver/` and
imported it cross-folder from `store/app.py`; this works for local testing but would
fail in Docker, since each service's Dockerfile only copies its own build context.
**Why did it fail?** Docker build contexts are isolated per service folder; a file
outside `store/` is invisible to `store`'s image build.
**How was it fixed?** Copied `register_client.py` into `store/` directly and updated
`store/Dockerfile` to `COPY app.py register_client.py .`.
**What alternative was considered?** A shared base image all services inherit from;
rejected as unnecessary complexity for one small helper file at this project's scale.
**What was learned?** Code that will run in Docker needs to respect build-context
boundaries from the start - "it imports fine locally" is not the same test as "it
will build correctly."

---

## Week 11 - RPC and Distributed Shared Memory

**What changed?** Added `rpc/client_stub.py`, wrapping the existing
txn-coordinator/gateway HTTP endpoints as `put()`/`get()` Python calls, and
`experiments/rpc_benchmark.py` to measure latency, throughput and consistency.
**What failed?** First version of `rpc_benchmark.py` imported `rpc.client_stub`
using a relative import that only worked when run from the project root -
failed with ModuleNotFoundError when run from elsewhere.
**Why did it fail?** The script didn't add the project root to `sys.path`
before importing a sibling package.
**How was it fixed?** Added `sys.path.insert(0, ...)` pointing at the project
root at the top of the benchmark script, before importing `rpc.client_stub`.
**What alternative was considered?** Making `rpc/` an installable package
with `pip install -e .`; rejected as unnecessary overhead for a course project
with only one consumer of the package.
**What was learned?** A "server stub" does not have to be new code - the
Flask routes built in Milestones 5 and 9 already marshal/unmarshal requests,
which is exactly what a server stub does. The missing half was only the
client side.

---

## Week 12 - Distributed File System and Telecom Capstone

**What changed?** Added `archiver/app.py` (MinIO-backed snapshot storage),
wired `minio` + `archiver` into the final `docker-compose.yml`, and added
`capstone_experiment.py` to orchestrate the performance, RPC and storage
checks together against the full 12-service stack.
**What failed?** Could not run an actual MinIO server in the development
sandbox used while building this (no Docker available there) - only the
`minio` Python client library's method signatures could be checked against
what `archiver/app.py` calls.
**Why did it fail?** The sandbox environment used for development had no
Docker daemon; MinIO ships as a server binary/container image, not a pure
Python package that can be run standalone.
**How was it fixed?** Verified `bucket_exists`, `make_bucket`, `put_object`,
`list_objects` and `get_object` signatures directly against the installed
`minio` package before writing the calls, to catch parameter-order mistakes
without a live server. The real end-to-end test still needs to run once on
a machine with Docker.
**What alternative was considered?** HDFS, as literally named in the brief;
rejected as significant unnecessary operational overhead (NameNode +
DataNodes) for this project's small JSON-snapshot data volume - MinIO gives
the same "distributed storage" learning outcome at a fraction of the setup
cost.
**What was learned?** Not every piece of a distributed system can be fully
verified without the real infrastructure it depends on - be explicit about
what was tested locally (API shape) versus what still needs verification on
the actual deployment target (an end-to-end Docker run), rather than
presenting both with the same confidence.
