# Milestone 10 - ZooKeeper Naming and Distributed Process Management

## Goal
Resolve logical service names to network locations dynamically instead of
hardcoding host:port everywhere, and log basic process lifecycle events.

## Naming subsystem
Apache ZooKeeper stores the logical-name registry. The HTTP facade in
`nameserver/app.py` preserves the service API used by the stores and gateway.
Each registration is stored as an **ephemeral znode** under `/services`.
The name server refreshes the registration timestamp on each store heartbeat;
the gateway ignores entries whose heartbeat is older than 10 seconds.

Docker Compose DNS still provides container-to-container transport. This
milestone adds a separate application-level registry so the gateway can ask
for a logical replica name and inspect its current URL and heartbeat status.

The REST routes are:
```
Name -> Name Resolution -> Resource Location
```
- `POST /register {name, url}` - a service announces itself
- `POST /heartbeat {name}` - proves it is still alive (10 s staleness window)
- `GET /resolve/<name>` - current URL, or 404 if unknown
- `GET /directory` - every registered service and its liveness

`store/app.py` self-registers every 3 seconds through
`register_client.py`. The registry records that as a heartbeat. Earlier
milestone folders keep their independent Compose definitions and do not use
ZooKeeper.

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

`GET /events` returns lifecycle and lookup events collected by the HTTP facade.

## Demonstrate name resolution

Start the stack from this folder. First stop whichever other milestone's
Compose stack is running because Milestones 7-10 reuse fixed container names.
Run `docker compose down` from that milestone's folder, then start Milestone 10:

```powershell
Set-Location "C:\Users\USER\Desktop\-ics2403-distributed-network-monitoring\milestone10"

# The store image imports requests in register_client.py. Confirm that the
# dependency is present before building (it should be in store/requirements.txt).
Get-Content .\store\requirements.txt

docker compose up --build -d
docker compose ps
Invoke-RestMethod http://localhost:9000/health
Start-Sleep -Seconds 5
Invoke-RestMethod http://localhost:9000/directory | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:9000/resolve/store-a
```

The directory should show `count: 2`, with `store-a` and `store-b` marked
`alive: true`. If it is empty, check the store dependency and startup logs:

```powershell
docker compose logs --tail 40 store-a store-b nameserver
```

Inspect the ZooKeeper registry directly:

```powershell
docker compose exec zookeeper zkCli.sh -server localhost:2181 ls /services
```

Measure the actual ZooKeeper-backed name-resolution latency and save the
observations to CSV:

```powershell
python .\milestone_experiment.py --service store-a --trials 20
```

The script writes `results\zookeeper_resolution.csv`. Its columns include
`http_latency_s` (end-to-end HTTP resolution time) and `registry_latency_s`
(the name server's ZooKeeper lookup time). Report values from your own run,
not an earlier in-memory registry measurement. Import this CSV in Power BI
to compare those two latency columns across trials.

For a failure demonstration, stop a registered store, wait past the 10-second
heartbeat lease, then check its registry status and read through the gateway:

```powershell
# Write first and verify that both replicas commit the value.
$writeResult = Invoke-RestMethod -Method Post http://localhost:7000/write `
  -Body (@{key="temperature";value=21} | ConvertTo-Json) `
  -ContentType "application/json"
$writeResult
if ($writeResult.decision -ne "COMMIT") { throw "Write did not commit; do not stop a store yet." }

# Simulate store-a going offline, then wait beyond the 10-second stale window.
docker stop store-a
Start-Sleep -Seconds 11
Invoke-RestMethod http://localhost:9000/resolve/store-a
Invoke-RestMethod http://localhost:8000/read/temperature

# Restore the store after collecting the evidence.
docker start store-a
```

Expected evidence: the transaction response says `COMMIT` and both participant
votes are `True`; after the wait, `/resolve/store-a` reports `alive: false`;
the gateway read returns the committed value from `http://store-b:7002`.
In the captured run, the gateway returned `temperature = 21` from store-b.

## Evaluation and limitations

ZooKeeper provides the shared registry, while the Flask name-server facade
provides the project’s HTTP interface. Registrations disappear when the
name-server's ZooKeeper session expires; stores re-register on their next
three-second cycle. The Compose stack uses one ZooKeeper server for a local
demonstration, so it demonstrates naming and leases, not a highly available
production ZooKeeper ensemble.

Use only measurements produced by `milestone_experiment.py` for the ZooKeeper
version; the earlier in-memory registry timing is not a ZooKeeper measurement.
The single ZooKeeper container in this Compose file remains a single point of
failure and is intended for a local course demonstration.

## Deliverables
- [x] ZooKeeper-backed naming subsystem (`nameserver/app.py` and `zookeeper` service)
- [x] Process-management subsystem (create/locate/terminate event log)
- [x] Run and save ZooKeeper resolution latency measurements
- [x] Scalability limitation documented (single local ZooKeeper server)
