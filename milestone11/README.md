# Milestone11 (Week 11)

This folder is a complete, runnable snapshot of the system as of
Milestone 11: **RPC and Distributed Shared Memory**, with Kafka carrying the
edge monitoring event stream in `monitoring.reports`.

Everything from Milestones 1 through 11 is included and working
together - this is not a diff, it is the full system at this point in
its evolution. See `README_MILESTONE11.md` for this week's specific
goal, design, and measured results. Earlier `README_MILESTONEx.md`
files are kept for weeks 10 and earlier.

## Run it
```powershell
docker compose up --build
```

The edge agents publish to Kafka; both aggregator consumer groups receive the
stream and the elected leader forwards regional summaries to the collector.
See `README_MILESTONE11.md` for Kafka inspection commands and the benchmark.

## Reproducibility and engineering log
See `docs/reproducibility.md` and `docs/engineering-log.md` (updated
every week, as required).
