# Milestone 10 — Naming and Distributed Process Management

**System stage S10:** `Name → Name Resolution → Resource Location`, and `Create → Locate → Schedule → Coordinate → Terminate`.

| Brief deliverable | Where |
|---|---|
| Naming subsystem | `system/naming/server.py` (name server; also serves as a zone server in the hierarchy), `system/naming/resolver.py` (Static, Broadcast, NameServer, Prefix resolvers) |
| Process-management subsystem | `system/procman/controller.py` (Create/Locate/Schedule/Coordinate/Terminate), `node.py` (node agent), `worker.py` (the anomaly job) |
| Resolution latency analysis | `results/m10_resolution_latency.csv`, `m10_staleness.csv`, `m10_failure.csv` |
| Scalability analysis | `results/m10_broadcast_vs_nodes.csv`, `m10_resolution_load.csv`, `m10_resolution_vs_names.csv`, `m10_scheduling.csv`, `m10_process_failure.csv` |

Run: `python -m system.experiments.m10_naming_procman` (parts: `a c d e f`, ≈ 10 min total).

## Naming architecture
Names are `<site>.<region>.net` (e.g. `nai-01.nai.net`); resources are the node/service endpoints behind them. Four resolution schemes, same names, same 24 sites in 6 regions:

| Scheme | How a lookup works | Messages |
|---|---|---|
| **Static map** | local table compiled into the client | 0 |
| **Broadcast** | ask all N nodes, the owner answers | N |
| **Name server** (central) | one server holds every binding; client caches for the record TTL | 1 (0 on a cache hit) |
| **Prefix / hierarchical (DNS-style)** | root → regional zone server by name suffix; the client learns *prefix → server* referrals (prefix table) and caches answers for the TTL | 3 cold (root → zone → answer), 1 with a learned prefix table, 0 with an answer cache |

## Results — resolution latency (8 nodes, 1 500 lookups each)
| scheme | mean (ms) | p95 (ms) | jitter | messages/lookup | cache hits |
|---|---|---|---|---|---|
| static map | 0.000 | 0.000 | 0.000 | 0 | – |
| broadcast to 8 nodes | **9.37** | 14.4 | 3.03 | **8** | 0 % |
| central name server (TTL cache) | 0.025 | 0.000 | 0.201 | 0.02 | 98.4 % |
| prefix, cold (iterative from root) | 5.07 | 6.30 | 0.67 | 3 | 0 % |
| prefix, prefix table, answers not cached | 1.69 | 2.12 | 0.27 | 1 | 0 % |
| prefix, prefix table + answer cache (TTL 30 s) | **0.004** | 0.000 | 0.078 | 0 | 99.8 % |

## Results — scalability
**Broadcast vs number of nodes** (central server for reference): 4 nodes 4.43 ms / 4 msgs → 8: 8.44 / 8 → 16: 19.3 / 16 → 24: **31.3 ms / 24 msgs** (p95 47 ms), whereas the central server stays at ≈ 0.06 ms. Broadcast cost grows linearly with N in messages and slightly super-linearly in latency.

**Concurrent clients (uncached lookups)**

| clients | central: lookups/s · mean ms | hierarchy: lookups/s · mean ms |
|---|---|---|
| 1 | 651 · 1.5 | 570 · 1.7 |
| 4 | 874 · 4.6 | **1 121** · 3.4 |
| 16 | 973 · 16.3 | 907 · 14.7 |
| 32 | 886 · 35.3 | 627 · 33.8 |

Both saturate at ≈ 0.9–1.1 k lookups/s on this 2-vCPU host: the load generator and all servers share the CPU, so the *theoretical* advantage of spreading load over regional servers is mostly masked here (see H2 below).

**Central server vs number of registered names:** 1 000 → 1.07 ms, 10 000 → 1.51 ms, 100 000 → 1.70 ms per lookup (bulk registration of 100 000 names: 0.21 s) — lookup cost is nearly independent of table size (indexed).

**Consistency after a resource moves (7 s observation, one move):**

| client | time until the new location is seen | stale answers |
|---|---|---|
| static map | never (> 7 s) | **100 %** |
| name server / prefix, TTL 0 s | 0.0 s | 0 % |
| TTL 1 s | 1.01 s | 14.4 % |
| TTL 5 s | 5.01 s | **71.2 %** |

Staleness equals the TTL, as designed: the TTL is exactly the knob that trades lookup cost against freshness.

**Failure of the naming service itself (24 names each, all five client types)**

| failure | static | broadcast | central | prefix cold | prefix warm (table) |
|---|---|---|---|---|---|
| none | 100 % | 100 % | 100 % | 100 % | 100 % |
| central name server down | 100 % | 100 % | **0 %** | 100 % | 100 % |
| root server down | 100 % | 100 % | 100 % | **0 %** | **100 %** |
| one regional server down (zone NAI) | 100 % | 83.3 % | 100 % | 83.3 % | 83.3 % |

The central server is a single point of failure; the hierarchy's root is one *only for clients without a prefix table* (a warm client resolves through the zones directly); losing one zone costs exactly that zone's names (4 of 24 = 16.7 %). The static map never fails — and is never right after a move.

## Process management (Create → Locate → Schedule → Coordinate → Terminate)
The 24-site anomaly scan (120 000 rows, one worker process per site) runs on 4 node agents, one of them 3× slower (`SLOW_FACTOR=3`), cap = 1 concurrent worker per node, 3 reps per policy.

| step | implementation |
|---|---|
| Create | `POST /spawn` on the chosen node → an OS process is started and registered as `proc.<job>.w<k>` in the naming service (TTL 60 s) |
| Locate | the controller resolves `proc.<id>` through the naming service (420 lookups per job under round-robin) |
| Schedule | round-robin / random (decided up front) or **least-loaded** (next task to the node with the smallest expected completion time from its measured seconds-per-row) |
| Coordinate | barrier: poll results, gather partial results (map → reduce), re-schedule the processes of a node that stops answering |
| Terminate | kill workers still running at job end / cancel; unregister names |

**Scheduling with one slow node (makespan, s)**

| policy | rep 1 | rep 2 | rep 3 | tasks on slow node | scheduling cost |
|---|---|---|---|---|---|
| round-robin | 13.41 | 13.66 | 13.71 | 6 | ≈ 0 ms |
| random | 15.11 | 15.57 | 13.51 | 6–8 | ≈ 0 ms |
| **least-loaded** | **10.91** | **11.03** | **10.58** | **3** | 14–15 ms per decision |

Least-loaded is **≈ 20 % faster than round-robin and ≈ 25 % faster than random**, because it gives the slow node half the work; the price is a probe of every node per decision (≈ 15 ms, negligible against second-long tasks). Every run produced the identical result (1 524 flagged rows of 120 000), so scheduling changes *time*, not *answers*.

**A node is killed 2 s into the job** (4 nodes, 24 tasks, 1 rep per cell; control = no kill)

| policy | control (no failure) | kill, **stale name cache** (original controller) | kill, **cache invalidated on re-placement** |
|---|---|---|---|
| least-loaded | 9.4 s | **62.3 s** | **10.1 s** |
| round-robin | 8.5 s | **61.4 s** | **9.4 s** |

All 24 workers completed in every case and the result was complete and identical. Recovery costs ≈ 1 s once the controller stops following the cached binding — and ≈ 53 s (≈ the 60 s name TTL) when it does not (see log).

## Hypotheses
| Hypothesis | Outcome |
|---|---|
| H1 broadcast costs N messages and latency grows with N; central = 1; prefix = 1 after warm-up | **supported** (8 → 24 nodes: 8.4 → 31.3 ms; prefix table: 1 message, 1.69 ms) |
| H2 the central server saturates before the hierarchy | **not demonstrated**: both saturate at ≈ 0.9–1.1 k lookups/s on a shared 2-vCPU host (hierarchy better at 4 clients, worse at 32). The expected advantage needs servers on separate hardware. |
| H3 staleness is bounded by the TTL; a static map is stale forever | **supported** (1.01 s and 5.01 s for TTL 1 s and 5 s) |
| H4 least-loaded beats round-robin/random with a slow node; the job completes after a node dies | **supported** (10.9 vs 13.6 vs 14.7 s; completion 24/24 after a kill) |

## Failure-driven engineering log (week 10)
| Question | Answer |
|---|---|
| What changed? | Four resolvers, a hierarchical zone setup, a process controller with node agents and a barrier. |
| What failed? | (1) After a node was killed the job took **62 s instead of ≈ 9 s**. (2) My first failure experiment had no control run, so "62 s" looked like the cost of a crash. |
| Why? | The controller resolves worker names through a caching resolver (TTL 60 s). When a worker was re-placed on a live node the *old* binding (`proc.x → dead node`) was still cached, so the controller kept polling the dead node for the whole TTL. This is the same stale-binding effect measured in the staleness table, hitting the process manager. |
| How fixed? | `Controller._forget()` drops the cached binding whenever a process is re-placed (`invalidate_stale=True`; `False` reproduces the original behaviour); the experiment now has a no-failure control and both variants. Recovery time: 62 → 10 s (control 9.4 s). |
| Alternative considered | A shorter name TTL (60 → 5 s) — rejected as the primary fix: it only shrinks the window, and it raises lookup load; invalidation removes the stale window for exactly the names that changed. |
| What was learned? | Caches in one subsystem silently set the recovery time of another; every failure experiment needs a no-failure control; the CLI took only the first argument (`d e` ran just `d`), so a "re-run" had not actually happened — verify what ran, not what was typed. |

## Limits
2-vCPU shared host (see H2); 24 sites / 6 regions / 4 node agents; one kill per run; name TTL for process names is 60 s.
