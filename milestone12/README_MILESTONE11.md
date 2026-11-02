# Milestone 11 - RPC and Distributed Shared Memory

## Goal
Make the Client -> Client Stub -> Network -> Server Stub -> Server structure
explicit, and treat the Milestone 5 store/coordinator pair as what it
already is: a distributed shared-state abstraction.

## Architecture
```
Caller code
    |
    v
rpc/client_stub.py (SharedStateClient)   <- Client Stub: looks like .put()/.get()
    |  marshals to JSON, sends HTTP
    v
Network
    |
    v
txn-coordinator /write  or  gateway /read/<key>   <- Server Stub: Flask route
    |  unmarshals JSON, calls the real logic
    v
Server (2PC across store-a / store-b)
```
The Flask routes already added in Milestones 5 and 9 ARE the server stubs -
this milestone adds the missing client-side half, so a caller writes
`client.put("key", value)` instead of hand-building an HTTP request, and
gets a Python exception (`RPCError`) instead of an HTTP status code to
handle.

## Distributed shared memory
`store-a` + `store-b`, kept consistent via the Milestone 5 coordinator, are
the distributed shared-state abstraction: multiple callers see the same
`key -> value` mapping no matter which replica answers, the same property
real distributed shared memory provides.

## Measured (real run, local)
| Operation | n | avg latency | throughput |
|---|---|---|---|
| `put()` | 15 | 0.0088 s | 113.6 calls/s |
| `get()` | 15 | 0.0033 s | 304.3 calls/s |

Consistency: 15/15 keys returned identical values on repeated reads through
the gateway. Message overhead per committed write: 2 participants x
(1 PREPARE + 1 COMMIT) = 4 messages, vs 1 for a non-replicated write - this
is the same 2PC synchronization cost Milestone 5 already isolated.

## How to run
```powershell
docker compose up --build
python experiments/rpc_benchmark.py --writes 20 --reads 20
```

## Deliverables
- [x] RPC implementation (`rpc/client_stub.py`, server stubs = existing Flask routes)
- [x] Distributed memory/state mechanism (store-a/store-b + 2PC, reused from M5)
- [x] Communication benchmarks (latency and throughput, measured above)
- [x] Consistency analysis (15/15 identical reads through the gateway)
