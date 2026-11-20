# Milestone 8 — Distributed System Models

## Purpose

Milestone 8 evaluates the Theme 2 Distributed Network Monitoring System using five distributed-system models. It uses the architecture and measurements from earlier milestones; **it does not require a new experiment or a new service**.

The best overall description is an **integrated model**: different parts of the monitoring system use different arrangements. This is an architectural classification, not a claim that every fault-tolerance feature has been proven.

## How the live system fits together

```text
edge-node-a ─┬─> aggregator-1 ─┐
             └─> aggregator-2 ─┴─> elected aggregator forwards a regional summary ─> collector
edge-node-b ─┬─> aggregator-1
             └─> aggregator-2
core-node ───────────────────────────────────────────────────────────────────────> collector

txn-coordinator ──> store-a and store-b   (separate replicated control-state writes)
```

The edge agents send reports to both aggregators. The elected aggregator forwards a regional summary to the collector. The core agent reports directly to the collector. The transaction coordinator and stores handle control-state transactions; they do not receive every periodic metrics sample.

## The five models

| Model | What it means | Fit to this project |
|---|---|---|
| Host-based | Many clients rely on one central host or service. | Describes the flat Milestone 2 baseline, where nodes report directly to one collector. It is simple, but concentrates load and failure risk at that collector. |
| Processor pool | Equivalent workers share jobs from a common pool. | Poor fit: edge nodes, aggregators, collector, coordinator, and stores have different assigned roles. |
| Workstation | Independent nodes do local work and occasionally cooperate. | Partly fits the edge agents: they collect and summarize local measurements before reporting upstream. |
| Server-based | Dedicated services provide specific functions to clients or other services. | Partly fits the collector, aggregators, coordinator, and stores, each of which has a defined role. |
| Integrated | A system deliberately combines multiple models. | Best overall fit: edge nodes do local work, service tiers provide dedicated functions, and services cooperate over the network. |

## Evaluation

| Criterion | Flat baseline (Milestone 2) | Integrated design (Milestones 3–7) |
|---|---|---|
| Data path | Nodes report directly to the collector. | Edge reports pass through aggregators; the core node reports directly to the collector. |
| Latency | Fewer service hops in the direct path. | Edge reports add an aggregator hop and wait for a summary flush. Use matched experiment CSVs for a numeric comparison. |
| Load distribution | Collector accepts each node's reports directly. | Aggregators summarize edge reports before forwarding. Do not claim a specific throughput improvement without matching measurements. |
| Failure handling | A single collector can interrupt the direct reporting path. | Redundant aggregators and replicated stores are configured, but configuration alone does not prove successful failover. |
| Resource use | Fewer services and containers. | More containers and inter-service messages, in exchange for role separation and redundancy mechanisms. |
| Limits | Central service can become a bottleneck or single point of failure. | Leader election and transaction coordination add complexity. The coordinator and state durability still need careful evaluation. |

### Evidence and limits from the captured runs

- **Milestone 5:** the latest run attempted 20 normal writes; 19 committed and one aborted. Five further writes aborted during the deliberate store-B failure. The two stores had matching 19-key committed contents. This local run used in-memory state, so it does not establish durable recovery.
- **Milestone 7 store crash:** the captured summary reported recovery in 3/3 trials, mean MTTR 11.06 seconds, and mean availability 0.8156.
- **Milestone 7 store omission:** the captured summary reported recovery in 3/3 trials, mean MTTR 4.79 seconds, and mean availability 0.9202.
- **Milestone 7 aggregator crash:** the displayed run reported “No aggregator leader found” on trial 2 and did not show a completed summary. Aggregator failover therefore remains unconfirmed by that run. A Docker view showing containers running is not proof that a leader was elected or telemetry successfully failed over.

These are measurements from particular local runs, not service-level guarantees. Keep each result with its matching terminal output and CSV. Do not combine numbers from different runs.

## Live Grafana view for Milestones 7 and 8

Milestone 8 remains an analysis milestone; these additions provide a teaching/demo view without changing its model comparison. Prometheus scrapes the collector and both aggregators. A small read-only exporter loads the already-saved Milestone 7 fault CSVs from `../milestone7/experiments/results`. Grafana provisions two separate dashboards automatically.

- **Milestone 7 | Fault Tolerance and Leader Failover:** live node CPU/memory, aggregator leader and forwarding/heartbeat metrics, collector report rate, and saved fault-test summaries (MTTR, availability, trial counts).
- **Milestone 8 | Distributed System Models:** architecture diagram, five-model comparison, trade-offs, and an explanation of the earlier milestone evidence used to support the integrated-model conclusion.

Milestone 7 fault charts are calculated from the saved CSV rows; they do not rerun experiments. Keep each CSV with its matching terminal output when presenting results. The two dashboards have separate titles and can be opened from **Dashboards → Distributed Network Monitoring**.

### Start it in PowerShell

First stop any other milestone stack that is using the same fixed container names. Go to that milestone's folder and run `docker compose down` (do not add `-v`). Then run:

```powershell
(Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned) ; (& C:\Users\USER\Desktop\-ics2403-distributed-network-monitoring\venv\Scripts\Activate.ps1)
Set-Location "C:\Users\USER\Desktop\-ics2403-distributed-network-monitoring\milestone8"
docker compose up --build -d
docker compose ps
```

Open Grafana at [http://localhost:3001](http://localhost:3001). Sign in with username `admin` and password `change-this-password`. Choose **Dashboards → Distributed Network Monitoring**, then open either **Milestone 7 | Fault Tolerance and Leader Failover** or **Milestone 8 | Distributed System Models**. Prometheus is available at [http://localhost:9091](http://localhost:9091); open **Status → Targets** to see whether each scrape is `UP`.

The results exporter reads the sibling Milestone 7 results directory when the M8 stack starts. If you rerun a Milestone 7 experiment, its CSV values refresh in Prometheus on the next scrape; the historical summary reflects the CSV files currently in that folder. It does not create a separate archive per run.

If a service is stopped or restarting, inspect its logs:

```powershell
docker compose logs --tail 50 collector aggregator-1 aggregator-2 prometheus fault-results grafana
```

### Other live status pages

Open the JSON status endpoints in a browser while the stack is running:

- Collector status and most recent reports: [http://localhost:5000/status](http://localhost:5000/status)
- Aggregator 1 leadership and counters: [http://localhost:6001/leader](http://localhost:6001/leader)
- Aggregator 2 leadership and counters: [http://localhost:6002/leader](http://localhost:6002/leader)

Metrics can also be viewed directly at [collector metrics](http://localhost:5000/metrics), [aggregator 1 metrics](http://localhost:6001/metrics), and [aggregator 2 metrics](http://localhost:6002/metrics). The port mappings are in `docker-compose.yml`; use the left-hand port on your computer.

To watch reports move through the services, open a second PowerShell terminal and run:

```powershell
Set-Location "C:\Users\USER\Desktop\-ics2403-distributed-network-monitoring\milestone8"
docker compose logs -f --tail 20 edge-node-a edge-node-b core-node aggregator-1 aggregator-2 collector
```

Look for edge-agent report lines, aggregator heartbeat/leader and forwarding lines, and collector receipt lines. Press **Ctrl+C** to stop following logs; this does not stop the containers.

You can also request the endpoints directly from PowerShell:

```powershell
Invoke-RestMethod http://localhost:5000/status | ConvertTo-Json -Depth 6
Invoke-RestMethod http://localhost:6001/leader | ConvertTo-Json -Depth 4
Invoke-RestMethod http://localhost:6002/leader | ConvertTo-Json -Depth 4
```

Check the `leader_id`, `is_leader`, `alive`, and `term` fields in the two aggregator responses. If `leader_id` is temporarily null, allow the startup heartbeat grace period, refresh, and inspect both aggregator logs. Do not claim leader failover succeeded unless the response and an end-to-end report confirm it.

When you have finished the live demonstration, stop this milestone's stack from the Milestone 8 folder:

```powershell
docker compose down
```

This does not remove named volumes; do not add `-v` unless you intentionally want to delete stored volume data.



[Theme 2 Distributed Network Monitoring System on GitHub](https://github.com/M-keller254/ics2403_Distributed_Network_Monitoring_System)
