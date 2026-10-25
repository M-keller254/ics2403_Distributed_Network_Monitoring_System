# Milestone 2 — Distributed Processing and Performance

## 1. Reproducibility
- Hardware: Intel Core i5-8265U @ 1.60GHz (4 cores, 8 threads), 15.8 GB RAM
- OS: Microsoft Windows 11 Pro
- Python 3.14, Flask, requests, psutil (see requirements.txt)
- Topology: 3 Flask processes on ONE machine (loopback network):
  core-node :5000, edge-node-a :5001, edge-node-b :5002
- Workload: client polls each node's /status once per second for 60s
- Parameters: `python run_experiment.py --duration 60 --label <name>`
- Random seeds: none (no randomness used)
- Raw data: experiments/results/milestone2run1.csv, milestone2run2.csv
- Full hardware details: SYSTEM_INFO.txt

## 2. Metrics
- Latency = T_response − T_request
- Jitter = standard deviation of latency
- Throughput = successful records / total time
- Packet loss = failed polls / total polls
- CPU, memory = psutil readings returned by each node

## 3. Experiment
Hypothesis: with identical workloads and a round-robin client, the three
nodes will show equal latency and no loss, and throughput will be limited by
the client polling rate.
Independent variable: client hostname (localhost vs 127.0.0.1).
Dependent variables: latency, jitter, throughput, loss, CPU, memory.
Controlled: nodes, code, machine, duration (60s), poll interval (1s).

## 4. Results

### Run 1 — target host `localhost`
| Node | Polls | Loss | Avg latency | Min | Max | Jitter | CPU | Mem |
|---|---|---|---|---|---|---|---|---|
| core-node | 10 | 0% | 2.1311s | 2.1173s | 2.1409s | 0.0085s | 25.2% | 67.3% |
| edge-node-a | 10 | 0% | 2.1355s | 2.1167s | 2.1596s | 0.0118s | 19.1% | 67.1% |
| edge-node-b | 10 | 0% | 2.1311s | 2.1191s | 2.1414s | 0.0063s | 22.2% | 67.1% |
Throughput: 0.469 records/s (30 records in 64s)

### Run 2 — target host `127.0.0.1`
| Node | Polls | Loss | Avg latency | Min | Max | Jitter | CPU | Mem |
|---|---|---|---|---|---|---|---|---|
| core-node | 60 | 0% | 0.1205s | 0.1102s | 0.1402s | 0.0080s | 20.4% | 64.4% |
| edge-node-a | 60 | 0% | 0.1196s | 0.1099s | 0.1416s | 0.0077s | 34.6% | 64.4% |
| edge-node-b | 60 | 0% | 0.1207s | 0.1070s | 0.1391s | 0.0082s | 20.1% | 64.4% |
Throughput: 2.997 records/s (180 records in 60s)

## 5. Load-Distribution Analysis
Latency was equal across nodes in both runs (run 2: 0.1205s, 0.1196s, 0.1207s).
The differences are far smaller than the jitter (~0.008s), so no node was
measurably slower: the round-robin client distributed requests evenly.
edge-node-a showed higher CPU (34.6% vs ~20%) but its latency did not change.
CPU and memory come from psutil system-wide counters, and all nodes share one
machine, so these values reflect the whole PC, not individual node load.
The identical memory reading (64.4%) confirms this. Per-process metrics
would be needed to attribute load to individual nodes.

## 6. Bottleneck Analysis
1. Run 1: hostname resolution. Windows tried IPv6 (::1) for `localhost`, the
   connection was refused, and it fell back to IPv4 after ~2s. This added ~2s
   to every request and cut throughput to 0.469 records/s (sequential polling:
   3 nodes x 2.13s = 6.4s per round).
2. Run 2: latency floor is ~0.1s, the psutil sampling window inside /status.
3. Run 2 throughput (2.997 records/s) is capped by the client poll rate
   (3 nodes / 1s). A round takes ~0.36s, so the system is idle ~64% of the time:
   the nodes are not the bottleneck at this load.
4. Loss was 0%: no reliability bottleneck observed at this load.

## 7. Failure-Driven Engineering Log
- What changed: built 3 nodes and a polling experiment runner.
- What failed: run 1 latency was ~2.1s and throughput 0.469 records/s.
- Why: Windows resolves `localhost` to IPv6 first, waits ~2s, then uses IPv4.
- How it was fixed: client targets 127.0.0.1 directly. Latency fell ~17x,
  throughput rose ~6.4x.
- Alternative considered: bind servers to IPv6 too, or poll nodes in parallel
  with threads.
- What was learned: sanity-check benchmark results; a client-side artifact
  can dominate measurements and hide true node behaviour.

## 8. Limitations
- All nodes run on one machine, so results exclude real network delay.
- Nodes only report status; no computational workload yet (added with the
  100k-row dataset stream in a later step).
- CPU and memory are system-wide, not per node.
- No failures injected yet (planned for Milestone 7).