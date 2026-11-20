# Milestone 11 - RPC, Kafka, and Distributed Shared Memory

## Goal
Make the Client -> Client Stub -> Network -> Server Stub -> Server structure
explicit, and treat the Milestone 5 store/coordinator pair as what it
already is: a distributed shared-state abstraction. It also adds Kafka as
the event transport for the edge monitoring stream.

## Kafka event stream

The two edge agents still aggregate their local CPU and memory samples. They
publish each summary to the Kafka topic `monitoring.reports` instead of making
direct HTTP calls to the aggregators. Both aggregators consume the complete
topic using separate consumer groups (`monitoring-aggregator-1` and
`monitoring-aggregator-2`), so the existing leader election continues to have
a complete standby view. Only the elected leader forwards the regional
summary to the collector. The core node continues to report directly.

```text
edge agents -> Kafka topic monitoring.reports -> aggregator consumer groups
                                               -> elected leader -> collector
core node -------------------------------------------------------> collector
```

The local broker uses Kafka's KRaft mode and does not depend on the separate
Milestone 10 ZooKeeper naming service. This Compose setup has one Kafka broker
and replication factor one: it demonstrates event buffering, consumer groups,
and replay after an aggregator restart, but it is not a highly available Kafka
cluster. Aggregators do not commit Kafka offsets, so a restarted aggregator
replays retained events; the original `produced_at` timestamp prevents old
replayed measurements from being counted as fresh. The topic has Kafka's
normal retention policy, so this is bounded replay rather than permanent
archival.

Check the Kafka topic and live consumer counters:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic monitoring.reports
Invoke-RestMethod http://localhost:6001/leader | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:6002/leader | ConvertTo-Json -Depth 5
docker compose logs --tail 30 edge-node-a edge-node-b aggregator-1 aggregator-2
```

`kafka_received` counts reports consumed by that aggregator; `forwarded` counts
regional summaries sent upstream by the elected leader. Each aggregator has a
separate consumer group, so both counters should rise as new edge windows are
published. To measure broker acknowledgement latency, throughput, and
producer-to-consumer delay on your run, use the dedicated experiment below.

## Architecture
```
Caller code
    |
    v
rpc/client_stub.py (SharedStateClient)   <- Client Stub: looks like .put()/.get()
    |  marshals to JSON, sends HTTP
    v
Network
    |
    v
txn-coordinator /write  or  gateway /read/<key>   <- Server Stub: Flask route
    |  unmarshals JSON, calls the real logic
    v
Server (2PC across store-a / store-b)
```
The Flask routes already added in Milestones 5 and 9 ARE the server stubs -
this milestone adds the missing client-side half, so a caller writes
`client.put("key", value)` instead of hand-building an HTTP request, and
gets a Python exception (`RPCError`) instead of an HTTP status code to
handle.

## Distributed shared memory
`store-a` + `store-b`, kept consistent via the Milestone 5 coordinator, are
the distributed shared-state abstraction: multiple callers see the same
`key -> value` mapping no matter which replica answers, the same property
real distributed shared memory provides.

## Measured (real run, local)
| Operation | n | avg latency | throughput |
|---|---|---|---|
| `put()` | 15 | 0.0088 s | 113.6 calls/s |
| `get()` | 15 | 0.0033 s | 304.3 calls/s |

Consistency: 15/15 keys returned identical values on repeated reads through
the gateway. Message overhead per committed write: 2 participants x
(1 PREPARE + 1 COMMIT) = 4 messages, vs 1 for a non-replicated write - this
is the same 2PC synchronization cost Milestone 5 already isolated.

## How to run
```powershell
# Run this from the milestone11 folder. Stop any other milestone's Compose
# stack first because several milestones reuse fixed container names.
docker compose up --build -d
docker compose ps
Start-Sleep -Seconds 30
docker compose ps kafka
Invoke-RestMethod http://localhost:6001/leader | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:6002/leader | ConvertTo-Json -Depth 5
docker compose logs --tail 30 edge-node-a edge-node-b aggregator-1 aggregator-2
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic monitoring.reports
python -m pip install requests==2.32.3
python experiments/rpc_benchmark.py --writes 20 --reads 20
python -m pip install -r experiments/kafka-requirements.txt
python experiments/kafka_benchmark.py --messages 100
```

The Kafka benchmark writes its own run-specific topic and saves the per-event
measurements to `experiments/results/kafka_benchmark.csv`. It uses the host
listener at `localhost:9094`; applications inside Compose use `kafka:9092`.
Report the values printed and saved by your own run rather than treating
Kafka's timings as known in advance.

The Kafka broker is included in Docker Compose; a separate Kafka download or
ZooKeeper installation is not required. Kafka runs in KRaft mode. Wait for
`docker compose ps kafka` to report `healthy` before expecting the dependent
agents and aggregators to start. The health check allows 20 seconds per check
and a 90-second startup grace period because the broker CLI can be slow on a
local machine.

### Demonstrating the output

- **Live event flow:** show the edge-agent logs and the two `/leader` responses.
  The agent logs should say `published event=...`; each aggregator should show
  `kafka_received` increasing and `kafka_errors` remaining at zero. The leader
  response also identifies `leader_id` and `is_leader`.
- **Kafka topic:** show the `monitoring.reports` topic description. It has three
  partitions and replication factor one in this local demonstration.
- **Benchmark results:** the script writes one row per event to
  `experiments/results/kafka_benchmark.csv`. In Power BI Desktop, choose
  **Get data > Text/CSV**, select that file, and load it. Useful visuals are a
  table with `sequence`, `broker_ack_s`, `consumer_delivery_s`, `partition`,
  `offset`, and `consumed`; cards for average broker acknowledgement and
  average consumer delivery; and a column chart with `sequence` on the X-axis
  and `broker_ack_s` on the Y-axis. Keep consumer delivery on its own visual
  because its seconds-scale values make acknowledgement times hard to read.
- **RPC results:** the RPC benchmark writes
  `experiments/results/rpc_benchmark.csv`; load it separately in Power BI to
  show RPC operation latency and decisions.

The checked-in Kafka benchmark CSV currently contains 100 consumed events:
mean broker acknowledgement was approximately **0.0107 s**, and mean
producer-to-consumer delivery was approximately **3.593 s**. These are from
one local run, not a performance guarantee. Rerun the benchmark for new
measurements; it creates a unique topic for each run and overwrites the CSV.

### Startup troubleshooting

If Compose says `condition "service_healthy" of service "kafka" not met`,
check the health details and broker logs before restarting dependent services:

```powershell
docker inspect milestone11-kafka-1 --format '{{json .State.Health}}'
docker compose logs --tail 100 kafka
```

The health-check timeout is 20 seconds and startup grace period is 90 seconds.
Once Kafka reports healthy, Compose can start the edge agents and aggregators.
If `kafka_received` remains zero, check the edge-agent logs for `published
event=` lines; a crashed agent cannot produce events. Rebuild the agents after
changing their code:

```powershell
docker compose up -d --build --force-recreate edge-node-a edge-node-b
docker compose logs --tail 30 edge-node-a edge-node-b
```

To demonstrate aggregator recovery, stop the current leader (check `is_leader`
in `/leader` first), wait longer than the heartbeat timeout, then check that
the remaining aggregator becomes leader and continues consuming its own Kafka
group. Restart the stopped aggregator afterward:

```powershell
docker compose stop aggregator-2
Start-Sleep -Seconds 5
Invoke-RestMethod http://localhost:6001/leader | ConvertTo-Json -Depth 5
docker compose start aggregator-2
```

Replace `aggregator-2` with `aggregator-1` if aggregator 1 is the one you stop.

## Deliverables
- [x] RPC implementation (`rpc/client_stub.py`, server stubs = existing Flask routes)
- [x] Kafka producer/consumer event path for edge monitoring reports
- [x] Distributed memory/state mechanism (store-a/store-b + 2PC, reused from M5)
- [x] Communication benchmarks (latency and throughput, measured above)
- [x] Run and save Kafka acknowledgement/delivery measurements (`experiments/results/kafka_benchmark.csv`)
- [x] Consistency analysis (15/15 identical reads through the gateway)
