# Capstone experiment protocol

## Hypothesis

Adaptive reporting will reduce report traffic while nodes are quiet and shorten the time to observe a node entering a high-resource state.

## Variables

- Independent: fixed reporting versus adaptive reporting.
- Dependent: accepted reports per node, report age, ingestion latency/jitter, request failures, client network-byte delta, node CPU and memory.
- Controlled: same machine, three agent services, collector, software versions, two-minute observation window, and benchmark request count/workers.

## Procedure

1. Start the stack with ADAPTIVE_REPORTING=false and let agents report for two minutes.
2. Record collector status and Prometheus accepted-report counts. Save a baseline benchmark CSV.
3. Restart with ADAPTIVE_REPORTING=true, QUIET_INTERVAL=5, BUSY_INTERVAL=1, and the same observation window and workload.
4. Save the second benchmark CSV. Compare results and discuss the trade-off; do not infer a general improvement from one run.
5. Repeat at least three times if presenting a research conclusion. Record every run in ENGINEERING_LOG_TEMPLATE.md.

Use the Prometheus query sum by (node_id) (increase(monitoring_ingest_total[2m])) for accepted reports per node. Use the collector /status endpoint to inspect age and stale state.

## Failure-recovery run

With the stack active, stop edge-node-a using docker compose stop edge-node-a. Observe /status until that node is marked stale. Record the stop time, first stale observation, restart time, and first fresh report. Restore it with docker compose start edge-node-a. Other agents should continue reporting during the outage.

The measured stale threshold is configured by STALE_AFTER_SECONDS. This experiment demonstrates crash detection and service continuity for the remaining agents; it does not demonstrate replication or failover of the stopped agent.

## Load measurement

Run python experiments/capstone_benchmark.py --requests 100 --workers 8. It writes serial-baseline and parallel-probe rows to results/capstone_benchmark.csv. The tool measures HTTP request-response latency, latency variation between indexed requests, request failures, throughput, client-host counters, and real agent resource values exposed by /status. This measures HTTP request loss, not packet-level network loss.
