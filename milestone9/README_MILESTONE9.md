# Milestone 9 - Transparency and Reliability

## Goal
Hide the Milestone 5 store replicas' internal detail (which one, whether
one just failed) behind a single client-facing read path.

## Design
`gateway/app.py` exposes `GET /read/<key>`. It tries each known store
replica in turn and returns the first that has the key. Writes still go
through the existing `txn-coordinator` `/write`, since that is where the
2PC guarantee lives - this gateway only adds transparency to reads.

| Transparency type | How it's achieved |
|---|---|
| Access transparency | Client always calls `GET /read/<key>` the same way |
| Location transparency | Client never names a specific replica's host/port |
| Replication transparency | Client cannot tell there are two replicas, not one |
| Failure transparency | If the first replica is down, the gateway retries the next one automatically |
| Migration transparency | (Milestone 10) replica locations resolved via the name server instead of a fixed list |

## Measured (verified locally, real failure injection)
| Scenario | `_served_by` | `_tried` | Overhead |
|---|---|---|---|
| Both replicas healthy | store-a | 1 | 0.0026 s |
| store-a down | store-b (automatic failover) | 2 | 0.0032 s |

The client queried the exact same `/read/temperature` URL both times.

## Known limitation (carried over from Milestone 7)
If store-a crashes, loses data, and is restarted, the gateway will happily
read whatever store-a now has - which may be less than store-b holds (see
Milestone 7's finding). The gateway provides transparency about *which*
replica answered; it does not detect or repair *inconsistency between*
replicas. A real system would need version vectors or read-repair for that.

## How to run
```powershell
docker compose up --build
Invoke-RestMethod -Method Post http://localhost:7000/write -Body (@{key="t";value=21}|ConvertTo-Json) -ContentType "application/json"
Invoke-RestMethod http://localhost:8000/read/t
docker stop store-a
Invoke-RestMethod http://localhost:8000/read/t     # same URL, different backend
```

## Deliverables
- [x] Transparent service mechanism (`gateway/app.py`)
- [x] Replication/failure experiment (store-a down, confirmed automatic failover)
- [x] Reliability evaluation (gateway masks the failure mode replicas introduce)
- [x] Overhead analysis (sub-5ms gateway indirection, measured above)
