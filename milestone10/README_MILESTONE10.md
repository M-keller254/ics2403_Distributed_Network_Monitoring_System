# Milestone 10 - Naming and Distributed Process Management

## Goal
Resolve logical service names to network locations dynamically instead of
hardcoding host:port everywhere, and log basic process lifecycle events.

## Naming subsystem
`nameserver/app.py`:
```
Name -> Name Resolution -> Resource Location
```
- `POST /register {name, url}` - a service announces itself
- `POST /heartbeat {name}` - proves it is still alive (10 s staleness window)
- `GET /resolve/<name>` - current URL, or 404 if unknown
- `GET /directory` - every registered service and its liveness

`store/app.py` self-registers and sends a heartbeat every 3 s via
`register_client.py`, best-effort: if `NAME_SERVER` is not set, this is a
no-op, so Milestones 5-9 keep working unchanged.

`gateway/app.py` (Milestone 9) now resolves `store-a`/`store-b` via the
name server when `NAME_SERVER` + `LOGICAL_NAMES` are set, falling back to
its static `REPLICAS` list if the name server is unreachable.

## Process management
```
Create -> Locate -> Schedule -> Coordinate -> Terminate
```
| Step | Where |
|---|---|
| Create | `POST /register` (logged as a `create` event) |
| Locate | `GET /resolve/<name>` (logged as a `locate` event) |
| Schedule | Docker Compose's own scheduling (not reimplemented) |
| Coordinate | Milestone 4 election + Milestone 5 2PC |
| Terminate | `POST /deregister` (logged as a `terminate` event) |

`GET /events` returns the full event log.

## Measured (verified locally, real process registration and failover)
- Resolution time: ~7 microseconds per `/resolve` call - negligible next to
  network latency, as expected for an in-memory registry at this scale.
- Gateway read via name-resolved replicas works identically to the static
  list from Milestone 9, and still fails over correctly when store-a is
  stopped (confirmed: `_served_by` switched from store-a to store-b, same
  `/read/<key>` URL both times).

## Scalability note
A single in-memory name server is itself a single point of failure and a
potential bottleneck at scale - the same lesson Milestone 3 taught about
the original single collector. Out of scope to fix here (would mean
replicating or sharding the registry); named as a limitation.

## How to run
```powershell
docker compose up --build
Invoke-RestMethod http://localhost:9000/directory
Invoke-RestMethod http://localhost:8000/read/<key>   # gateway now resolves via the name server
```

## Deliverables
- [x] Naming subsystem (`nameserver/app.py`)
- [x] Process-management subsystem (create/locate/terminate event log)
- [x] Resolution latency analysis (~7 microseconds, measured above)
- [x] Scalability analysis (single name server is a known limit, noted above)
