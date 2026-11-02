# Milestone 4 - Distributed Algorithms and Coordination

**Theme 2: Distributed Network Monitoring System**

## Goal
Remove the aggregator as a single point of failure using **leader election**.

## Design
Two aggregators run side by side (`docker-compose.yml`).
- Edge nodes send every report to BOTH aggregators, so both hold fresh data.
- Only the **leader** forwards the regional summary to the cloud collector.
- The standby holds data and takes over if the leader dies.

### Election rule (heartbeat-based, Bully-style)
1. Every aggregator sends a heartbeat to its peers every 1 s.
2. A peer is **alive** if its heartbeat arrived within the last 3 s.
3. The **alive aggregator with the highest ID is the leader**.
4. Each aggregator evaluates this rule locally, so all reach the same answer.
5. A 3 s startup grace period lets peers be discovered before anyone forwards.
6. A term counter increases at every leader change.

## Complexity analysis
| Quantity | Value (n aggregators) |
|---|---|
| Heartbeat messages per interval | n(n-1), so O(n^2) (2 msgs/s for n = 2) |
| Edge-to-aggregator messages | n per report (standby cost: doubles at n = 2) |
| Worst-case failover | HB_TIMEOUT (3 s) + FLUSH_INTERVAL (5 s), about 8 s |

## Evaluation metrics
| Metric | How it is measured |
|---|---|
| Coordination overhead | `hb_sent` / `hb_received` counters at `GET :6001/leader`, plus edge fan-out |
| Synchronization delay | Election time and forwarding-resume time from `failover_test.py` |
| Consistency | Both `/leader` endpoints (6001, 6002) must report the same `leader_id` |
| Scalability | Heartbeat count grows as n(n-1) |

## How to run
```powershell
docker compose -f docker-compose.yml up --build
# second terminal:
Invoke-RestMethod http://localhost:6001/leader
Invoke-RestMethod http://localhost:6002/leader     # same leader_id on both
python experiments/failover_test.py --trials 3     # stops the leader, times recovery
```
Live demo of a failure: `docker stop aggregator-2`, watch `aggregator-1` logs print
"leader change", then `docker start aggregator-2` and watch leadership return.

## Hypothesis for the report
*Running two aggregators with heartbeat election keeps data flowing to the cloud after a
leader crash, at the cost of about 2x edge messages and an interruption of up to
HB_TIMEOUT + FLUSH_INTERVAL.*
Independent variable: leader up or crashed. Dependent: election time, forwarding-resume time.
Controlled: workload, hardware, resource limits.

## Known limitations
- Simplified Bully (no ELECTION / OK / COORDINATOR messages); it uses heartbeats instead.
- If only the heartbeat link fails while both stay alive, both may forward (split brain, duplicates).
- A recovered higher-ID aggregator takes leadership back (preemption).
- Not yet implemented: Lamport clocks (a possible extension).

## Deliverables
- [x] Distributed algorithm implementation (`aggregator/app.py`)
- [x] Algorithm and complexity analysis (above)
- [ ] Experimental results (run `failover_test.py` and record your numbers)
