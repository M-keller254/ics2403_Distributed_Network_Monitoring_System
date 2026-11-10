# Milestone 6 - Concurrency and Deadlock Management

Two shared locks in the collector represent real contention points:
`registry_lock` (known nodes) and `stats_lock` (aggregate counters).
`collector/deadlock_demo.py` implements three strategies against the
same two operations (transfer, reconcile):

| Endpoint | Lock order | Result |
|---|---|---|
| `/demo/unsafe` | opposite order each operation | real, permanent deadlock (Pi -> Rj cycle) |
| `/demo/safe` | same fixed order (registry, then stats) | deadlock **prevention** |
| `/demo/detect` | opposite order, try-acquire with timeout | deadlock **detection + recovery** |

## Run
```powershell
docker compose up --build
python .\experiments/deadlock_test.py
docker compose restart collector
Run order matters: the script calls `/demo/safe` and `/demo/detect`
first (both return normally), and `/demo/unsafe` last, since that
call deliberately and permanently deadlocks the collector process.
Restart the collector (`docker compose restart collector`) afterward.

## Deliverables
- [x] Concurrent distributed implementation (ThreadPoolExecutor + two shared locks)
- [x] Deadlock experiment (`/demo/unsafe` reproduces a real deadlock)
- [x] Detection/recovery mechanism (`/demo/detect`, timeout-based)
- [x] Resource utilization analysis (lock hold/wait times in `deadlock_test.py` output)
