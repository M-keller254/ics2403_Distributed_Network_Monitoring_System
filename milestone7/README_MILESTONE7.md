# Milestone 7 - Fault Tolerance and Recovery

## Goal
Inject controlled failures into the two subsystems that already recover
automatically (Milestone 4's leader election, Milestone 5's 2PC), and
measure that recovery with availability and MTTR, instead of just
asserting it works.

## Failures injected
| Failure | How | Applies to |
|---|---|---|
| Crash | `docker stop` | aggregator or store-b |
| Omission | `docker pause` (alive, unresponsive) | aggregator or store-b |
| Communication | approximated as `docker pause` on one host - a true partition needs separate hosts | aggregator or store-b |

Byzantine failures (a node returning plausible but wrong data) are not
tested - detecting them needs redundancy and voting beyond a 2-replica
design, and is noted here rather than attempted.

## What recovers automatically, and what does not

**Aggregator tier (Milestone 4):** the standby detects the leader's
missed heartbeats and takes over. Forwarding resumes with no
intervention. This is tested and confirmed working.

**Store tier (Milestone 5):** while store-b is down, every 2PC vote
from it fails, so the coordinator correctly ABORTs every write - no
partial commits are left. Once store-b restarts, new writes commit
successfully again.

**Important limitation, found while testing this milestone:**
restarting store-b does **not** restore the data it held before the
crash. `store/app.py` keeps `committed = {}` purely in memory with no
persistence, so a restarted process starts empty. Recovery here means
**write-path availability is restored for new transactions**, not that
store-b's pre-crash state is reconciled with store-a. Verified directly:
after a crash-and-restart cycle, store-a retained every key it ever
committed, while store-b was missing exactly the keys committed before
its restart and present only those committed after.

This is a textbook gap: true crash recovery of a stateless replica
needs either durable storage (write-ahead log, disk-backed commits) or
an anti-entropy / state-transfer step on rejoin, neither of which this
course-scale demo implements. `fault_test.py` measures availability of
the write path correctly; it does not check or claim full data
reconciliation, and this README says so explicitly rather than
overclaiming.

## Metrics
| Metric | Formula | Where |
|---|---|---|
| MTTR | time from failure to first successful recovery signal | `fault_test.py` |
| Availability | (trial window - downtime) / trial window | `fault_test.py` |

## How to run
```powershell
docker compose up --build
Set-Location .\milestone7
Test-Path .\experiments\fault_test.py
python .\experiments\fault_test.py --component store --fault crash --trials 3
python .\experiments\fault_test.py --component store --fault omission --trials 3
python .\experiments\fault_test.py --component aggregator --fault crash --trials 3

## Deliverables
- [x] Fault-injection framework (`fault_test.py`; crash, omission, communication)
- [x] Recovery mechanism (election + 2PC abort/retry - built in M4/M5, measured here)
- [x] Availability and MTTR analysis
- [x] Failure comparison (crash vs omission, same script, `--fault` flag)
- [x] Limitation identified and documented: stateless-replica recovery restores
      availability, not historical consistency
