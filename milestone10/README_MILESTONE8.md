# Milestone 8 - Distributed System Models

No new code. This milestone analyses the system as it stands after
Milestone 7 against the course's five reference models, using
measurements already collected rather than a new experiment.

## Model comparison

| Model | Description | Fit for this system |
|---|---|---|
| Host-based | One shared host serves everything | The Milestone 2 baseline: all nodes report straight to one collector |
| Processor pool | Interchangeable processors pick up any job | Poor fit - edge, aggregator and store roles are fixed, not interchangeable |
| Workstation | Independent machines that mostly work alone, cooperating occasionally | Close fit for the edge nodes: they aggregate locally and only occasionally report upward |
| Server-based | Dedicated servers provide one specific service | Matches the aggregator and store/coordinator tiers - each does one job |
| Integrated | A deliberate mix, chosen per component | **What this system is**, as of Milestone 7 |

## Evaluation, using real measurements already taken

| Criterion | Flat (Milestone 2) | Current (Milestones 3-7) |
|---|---|---|
| Latency | Lower per hop; all load hits one collector | One extra hop at the aggregator; bounded by `FLUSH_INTERVAL` |
| Throughput at the collector | Grows with node count (bottleneck) | Roughly constant, since the aggregator sends one summary regardless of edge-node count |
| Reliability | Single collector = single point of failure | No single point of failure for the aggregator tier (M4) or the write path (M5); a restarted store-b still loses its own prior state (M7 finding) |
| Resource cost | Fewest containers | More containers (2 aggregators, 2 stores, 1 coordinator) for the reliability gained |
| Scalability | Poor past a handful of nodes | Better for the collector; the coordinator still talks to every participant (2PC does not scale well past a few replicas) |

## Conclusion
An **integrated model** fits best: edge nodes behave like workstations doing
local work, the aggregator and store/coordinator tiers behave like
dedicated servers, and reliability for the write path and the core tier was
bought deliberately, at a measured cost in extra containers and messages
(see Milestones 4, 5 and 7).

## Deliverables
- [x] Model comparison (table above)
- [x] Experimental results reused from Milestones 2, 5, 7 (no new experiment needed for this milestone)
- [x] Architectural justification (conclusion above)
