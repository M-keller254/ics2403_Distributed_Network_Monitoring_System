#!/usr/bin/env bash
# Reproduce every number in docs/ and results/ (about 2.5 hours on a 2-vCPU machine). Run experiments ONE AT A TIME:
# they measure timing, so running them concurrently (or while doing other heavy work) changes the results.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD"
python3 data/generate_dataset.py                       # prints the sha256; must equal the value in docs/reproducibility.md
for m in m01_foundation m02_performance m03_architecture m04_coordination m05_transactions m06_concurrency \
         m07_fault_tolerance m08_models m09_transparency m10_naming_procman m11_rpc_dsm m12_storage; do
  echo "=== $m"; python3 -u -m system.experiments.$m
done
# capstone (resumable: re-running skips trials already in results/m12_capstone_raw.csv; delete that file for a clean run)
python3 -u -m system.experiments.m12_capstone failure_rate bandwidth users nodes cpu memory net_load request_rate data_volume transport
python3 -m system.experiments.m12_analyze
python3 -u -m system.experiments.m12_coordination        # coordination overhead (3.5 min)
python3 -m system.experiments.m12_tables                 # results/m12_tables.md (the tables quoted in docs/)
python3 powerbi/make_preview.py
python3 -u -m system.scripts.demo_failure                # Part E live demo: kill the leader, show detection/recovery/restoration (25 s)
python3 -m pytest tests -q                              # unit + integration tests (about 3 minutes)
