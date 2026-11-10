# Milestone 5 - Distributed Transactions

Adds a Two-Phase Commit coordinator (`txn-coordinator/`) writing to two
storage replicas (`store/`, run twice as store-a and store-b).

Model: **Prepare -> Commit -> Completion**, or **Prepare -> Abort** if
any participant votes NO or is unreachable.

## Run
```powershell
Set-Location .\milestone5
docker compose up --build
python .\experiments/txn_test.py --n 20
```
The script runs normal writes, then puts store-b into `fail_mode` to
force aborts, then checks store-a and store-b hold IDENTICAL
committed data (no partial commits survive a failed participant).

## Deliverables
- [x] Transaction subsystem (`txn-coordinator/`, `store/`)
- [x] Failure experiment (store-b forced down mid-run)
- [x] Recovery mechanism (abort on any NO vote; no partial writes)
- [x] Consistency analysis (`store-a`/`store-b` compared in `txn_test.py`)
