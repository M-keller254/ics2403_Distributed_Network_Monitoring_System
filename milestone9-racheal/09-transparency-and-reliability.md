# Milestone 9 — Transparency and Reliability

**System stage S9:** applications talk to a **logical resource** (`site/NAI-01/threshold`); the layer decides which **physical resources** (replica rows on hosts, or a file) serve it, and can move them while the application keeps running.

| Brief deliverable | Where |
|---|---|
| Transparent service mechanism | `system/transparency/layer.py` (`LogicalStore`, `Directory`, `migrate`), placements published through the naming service (`system/naming/server.py`) |
| Replication/migration experiment | `system/experiments/m09_transparency.py` parts C, E → `results/m09_migration.csv`, `m09_replication_transparency.csv` |
| Reliability evaluation | part D → `results/m09_failure_transparency.csv` |
| Overhead analysis | part B → `results/m09_overhead.csv` |

Run: `python -m system.experiments.m09_transparency` (≈ 2 min). Tests: `tests/test_integration.py` (migration, tombstone regression).

## The five transparencies — how each is implemented and shown
| Transparency | Mechanism | Evidence |
|---|---|---|
| **Access** | one API (`read/write(logical_key)`) for every backend; the application never sees HTTP, versions, quorums or file formats | part A: identical application function against a replicated KV (group G1) and a local JSON file; `same_application_code = True` for all |
| **Location** | placement record `site/NAI-01 → {groups, read_from, epoch, backend}` kept in the name service; the layer resolves it and caches it for 0.5 s | part A: after the namespace was *moved* to group G2 the unchanged application read the same threshold `{"cpu": 85, "latency_ms": 120}` |
| **Replication** | one logical write → `W=2` of 3 replicas synchronously, the rest by hinted handoff / anti-entropy | part E: one write to `site/NAI-08/threshold` produced **3 physical copies, all equal** |
| **Migration** | 4-phase protocol: (1) dual-write to old+new group (epoch 1) → (2) bulk copy of the old group's live entries → (3) flip reads to the new group (epoch 2) → (4) drop the old group from the placement (epoch 3) and retire its copies. Each phase waits ≥ 2.2× the cache TTL so no client still acts on the previous phase. | part C below |
| **Failure** | quorum I/O + failure detector in the layer; the application never sees a replica crash | part D below |

## Results

**B — overhead of transparency (loopback, 1 thread, ms)**

| operation | mean | p95 | jitter | overhead vs direct |
|---|---|---|---|---|
| direct physical read (1 replica, no layer) | 1.57 | 1.94 | 0.32 | 1.00× |
| logical read (cached placement, R=2) | 2.66 | 3.41 | 0.66 | **1.70×** |
| logical read (placement looked up on every call) | 5.87 | 7.14 | 0.84 | **3.75×** |
| direct physical write (1 replica) | 2.38 | 3.10 | 0.60 | 1.00× |
| logical write (W=2 of 3 replicas) | 4.42 | 6.37 | 1.41 | **1.86×** |

The cost of transparency is ≈ 1.1 ms per read and ≈ 2 ms per write when the placement is cached; a directory lookup per call more than doubles it. This is the price of hiding location **and** replication (2 replica reads / 2 replica writes) — not of one hop.

**C — migration under load** (4 namespaces, one client thread each doing a write+read mix every ≈ 5 ms; every 4th acknowledged write has a unique key; 2 reps)

| protocol | operations | application errors | stale reads | acknowledged unique writes missing on the new group | copies left on the old group | duration |
|---|---|---|---|---|---|---|
| **4-phase** | 5 272 / 5 373 | **0 / 0** | **0 / 0** | **0 of 660 / 0 of 674** | **0** | 19.0 / 19.2 s |
| naive copy-then-switch | 3 125 / 2 895 | 0 / 0 | 1 / 4 | **22 of 392 / 24 of 364 (≈ 6 %)** | **12 / 12** | 2.4 / 2.4 s |

The naive protocol is 8× faster but silently **loses ≈ 6 % of acknowledged writes** (those landing on the old group after the copy was taken) and leaves orphan copies behind. The 4-phase protocol has zero errors and zero loss, at the price of ≈ 19 s of wall time (3 settle periods per namespace, run sequentially) during which the worst-case operation latency was 56–61 ms (mean operation latency during migration ≈ 11 ms).

**D — failure transparency** (replica r1 down for 5 s of a 12 s run)

| client | operations | errors seen by the application | availability |
|---|---|---|---|
| bound to physical replica r1 | 237 / 237 | 103 / 102 | **56.5 % / 57.0 %** |
| logical layer over 3 replicas | 207 / 207 | **0 / 0** | **100 %** |

(The logical client completes fewer operations in the same time because each one is a quorum operation — that is the overhead from B showing up as throughput.)

## Hypotheses
| Hypothesis | Outcome |
|---|---|
| H1 layer overhead is a small constant, < 3× | **mostly supported**: 1.70× (read) and 1.86× (write) with cached placement; **not** supported with an uncached lookup (3.75×) — placement caching is essential |
| H2 4-phase migration loses nothing; naive loses writes | **supported**: 0 of 1 334 vs 46 of 756 acknowledged writes lost |
| H3 logical client sees ≈ 0 errors during a crash | **supported**: 0 errors, 100 % availability vs 57 % |

## Failure-driven engineering log (week 9)
| Question | Answer |
|---|---|
| What changed? | Logical-resource layer, placement records in the name service, 4-phase migration, quorum I/O. |
| What failed? | **(1) The first migration test reported "0 lost writes" for the naive protocol.** **(2) After making the test honest, the 4-phase run left 12 copies on the old group. (3)** The unique-key writes initially all failed. |
| Why? | (1) the check only compared the *final counter*, and a later overwrite hides an earlier lost write; (2) the replica's `DELETE` simply removed the row, and anti-entropy from a peer that had not yet been cleaned **pushed the key straight back** (a "resurrected delete" — invisible while there were only a few keys to delete, obvious with ≈ 120 keys); (3) the layer treats everything before the last `/` as the namespace, so the key `site/NAI-03/ev/…` named an unknown namespace. |
| How fixed? | (1) every 4th acknowledged write gets its own key and *all* are looked up on the new group afterwards; (2) deletes are now **versioned tombstones** (`replica.py`), listed with `?live=1` for migrations, with a regression test (`test_delete_is_a_tombstone_and_is_not_resurrected_by_anti_entropy`); (3) key format `…/ev-<client>-<n>`. |
| Alternative considered | Lowering the anti-entropy rate or deleting on all replicas at once: both only narrow the race; tombstones remove it. |
| What was learned? | A metric that can be satisfied by a later overwrite measures nothing; "delete" in an eventually-consistent store is a *write*; a bug can stay invisible until the data volume grows. |

## Limits
Loopback; replica failure = process crash; the migration blocks per namespace (sequential) and its duration scales with the number of namespaces; placement TTL (0.5 s) bounds how long a client can act on an old epoch and sets the settle time.
