# Milestone 3 - Distributed Architecture

**Theme 2: Distributed Network Monitoring System**

## Goal
Turn the flat "all nodes -> one collector" prototype into a tiered
**Edge -> Core -> Cloud** architecture.

## Architecture
```
edge-node-a --\
               >--> aggregator-1 (core tier) --> collector (cloud tier) --> /status
edge-node-b --/                                      ^
core-node ------------------------------------------/
```
| Tier | Service | Responsibility |
|---|---|---|
| Edge | edge-node-a, edge-node-b | Sample and aggregate locally (Milestone 2) |
| Core | aggregator-1 | Receives edge reports, forwards ONE regional summary every 5 s |
| Cloud | collector | Stores latest state and serves `/status` |

Service decomposition: the old collector did receiving, storing and serving.
Now the aggregator does intermediate roll-up and the collector only stores and serves.

## Architecture styles compared
| Style | Fit for this system |
|---|---|
| Client-server (Milestones 1-2) | Simple, but every node hits one server |
| Multi-tier (this milestone) | Adds a roll-up layer; collector load stops growing with edge nodes |
| Service-oriented / microservices | Reached in practice: each tier is its own container with one job |

## Scalability analysis
Messages per second arriving at the collector, with N edge nodes and window W seconds:
- Flat: (N + 1) / W
- Tiered: 1 / F (aggregator flush, F = 5 s) + 1 / W (core-node)

Flat load grows with N; tiered load stays constant. With N = 2 the gain is small
(about 0.6 vs 0.4 messages/s); it grows as edge nodes are added.

## Edge vs core trade-off
- Edge: lowest latency, sees only its own data.
- Core: sees the whole region, but adds one network hop and a flush delay of up to 5 s.
- New single point of failure: the aggregator. Milestone 4 addresses this.

## How to run
```powershell
docker compose -f docker-compose.flat.yml up --build     # baseline (Milestone 2 shape)
# in a second terminal:
python experiments/run_experiment.py --duration 60 --label flat_run1
# Ctrl+C, then:
docker compose -f docker-compose.flat.yml down --remove-orphans

docker compose -f docker-compose.m3.yml up --build       # tiered
python experiments/run_experiment.py --duration 60 --label m3_run1
docker compose -f docker-compose.m3.yml down --remove-orphans
```
Compare `records/s` at the collector in the two summaries (baseline vs proposed).

## Deliverables
- [x] Architectural design (diagram above)
- [x] Deployment architecture (`docker-compose.m3.yml`)
- [x] Service decomposition (aggregator vs collector)
- [x] Scalability analysis (model above; confirm with your measured runs)
- [x] Edge vs core trade-off analysis
