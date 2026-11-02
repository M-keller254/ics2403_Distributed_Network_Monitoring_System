# Milestone 2 — Distributed Processing and Performance

**Course:** ICS 2403 — Distributed Computing & Applications
**Theme:** Theme 2 — Distributed Network Monitoring System
**Week:** 2

## 1. Overview

This milestone extends the Week 1 prototype (3 nodes + 1 collector)
by distributing computation across the nodes and measuring system
performance quantitatively. Each node now performs local processing
on its own metrics before reporting, and the system's throughput,
latency, jitter, and packet loss are measured under different load
conditions.

## 2. What Changed From Milestone 1

| Milestone 1 | Milestone 2 |
|---|---|
| Each node read CPU/memory once and sent one raw reading | Each node collects a **window** of samples (`WINDOW_SIZE`) and computes its own min/avg/max locally before sending one summary |
| No formal measurement | Throughput, latency, jitter, and packet loss are measured via a dedicated experiment script |
| Collector only stored the latest value | Collector also timestamps receipt of each report, enabling latency calculation |

The key idea: **computation is distributed to the edge**. Nodes no
longer just forward raw numbers — they aggregate locally, which is
the actual "distribute processing across nodes" requirement for this
milestone.

## 3. System Architecture

```
edge-node-a ─┐
edge-node-b ─┼─> collector (HTTP :5000) ─> /status, /report, /health
core-node   ─┘
```

Each node:
1. Samples its own CPU% and memory% `WINDOW_SIZE` times (default 5)
2. Computes min / avg / max locally
3. Sends ONE summarized report to the collector
4. Repeats

## 4. Metrics Measured (as required by the brief)

| Metric | Definition used | Where it's computed |
|---|---|---|
| **Throughput** | Completed reports ÷ time | `experiments/run_experiment.py`, from the collector's running total |
| **Latency** | T_response − T_request | Measured client-side in `agent/app.py` around each HTTP POST |
| **Jitter** | Standard deviation of latency samples | Computed per node in `run_experiment.py` |
| **Packet loss** | Failed/timed-out POST attempts | Tracked via `total_records_failed` on the collector |
| **CPU / memory utilization** | Per-container cgroup readings | Already implemented in Milestone 1, carried forward |

## 5. Experiments Run

Two configurations were tested, each with two independent trials to
check consistency:

| Configuration | Trial 1 | Trial 2 |
|---|---|---|
| `WINDOW_SIZE = 5` | `window5.csv` — 22.7 records/s | `window5b.csv` — 93.45 records/s |
| `WINDOW_SIZE = 2` | `window2.csv` — 13.7 records/s | `window2_run2.csv` — 39.0 records/s |

**Independent variable:** `WINDOW_SIZE` (report interval / batch size)
**Dependent variables:** throughput, latency, jitter, packet loss
**Controlled variables:** 3 nodes, same hardware, same Docker network,
same per-container resource limits, same 60-second run duration

## 6. Key Findings

- **Throughput varied significantly between repeated trials of the
  same configuration** (e.g. 22.7 vs 93.45 records/s at
  `WINDOW_SIZE=5`). This is attributed to container warm-up effects
  (cold `--build` vs an already-running system), not to the
  configuration itself. This highlights the importance of running
  multiple trials rather than relying on a single measurement.
- **`core-node` consistently showed the highest average latency and
  jitter** across all four runs, despite having more CPU/memory
  allocated (1.0 CPU / 256MB vs 0.5 CPU / 128MB for edge nodes).
  Resource allocation did not correlate with reporting stability in
  this design, likely because `psutil.cpu_percent(interval=1)`
  introduces its own timing variability independent of the
  container's resource ceiling.
- **A smaller `WINDOW_SIZE` (more frequent, smaller reports) did not
  straightforwardly increase throughput** in these trials — this is
  discussed further in the full technical report rather than assumed.

## 7. How to Reproduce

```powershell
# 1. Start the system
docker compose up --build

# 2. In a second terminal, set up the experiment environment
python -m venv venv
venv\Scripts\activate
pip install -r experiments/requirements.txt

# 3. Run an experiment
python experiments/run_experiment.py --duration 60 --label <label>

# 4. Generate graphs (one graph per experiment, single line)
python experiments/plot_results.py --files <label1> <label2>
```

To test a different load level, edit `WINDOW_SIZE` for **all three**
node blocks in `docker-compose.yml`, save, then repeat step 1.

## 8. Files Added/Changed in This Milestone

```
agent/app.py                    <- windowed local aggregation + latency timing
collector/app.py                <- richer payload handling, receive timestamping
docker-compose.yml               <- WINDOW_SIZE env var per node
experiments/run_experiment.py    <- NEW: measures throughput, latency, jitter
experiments/plot_results.py      <- NEW: generates per-experiment graphs
experiments/requirements.txt     <- NEW: host-side deps (requests, pandas, matplotlib)
experiments/results/*.csv        <- raw experiment data
experiments/results/*.png        <- generated graphs
docs/engineering-log.md          <- Week 2 entry added
```

## 9. Deliverables Checklist (per the course brief)

- [x] Distributed processing implementation
- [x] Performance measurements (throughput, latency, jitter, packet loss)
- [x] Load-distribution analysis (see Section 6)
- [ ] Bottleneck analysis (in progress — see full technical report)

## 10. Next Milestone

Milestone 3 will restructure this flat "many nodes → one collector"
shape into a proper multi-tier **Edge ↔ Core ↔ Cloud** architecture,
motivated directly by the latency/jitter asymmetry observed here.
