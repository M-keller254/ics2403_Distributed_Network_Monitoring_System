"""A spawned worker process (Milestone 10): anomaly detection for a list of sites. Writes a JSON result file.

  python -m system.procman.worker --proc job1.w3 --sites NAI-01,NAI-02 --out /tmp/x.json
Env: DATASET_CSV, WORK_REPEAT (heavier analytics: repeat the scan N times), SLOW_FACTOR (emulated slower node)
"""
import argparse
import csv
import json
import os
import time

from system.edge import analytics
from system.edge.replayer import DEFAULT_CSV, INT_FIELDS, STR_FIELDS


def load_sites(path, sites):
    want, rows = set(sites), []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if r["site_id"] in want:
                for k in r:
                    r[k] = int(r[k]) if k in INT_FIELDS else (r[k] if k in STR_FIELDS else float(r[k]))
                rows.append(r)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proc", required=True)
    ap.add_argument("--sites", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    t0 = time.time()
    sites = a.sites.split(",")
    rows = load_sites(os.environ.get("DATASET_CSV", DEFAULT_CSV), sites)
    flagged = []
    for _ in range(int(os.environ.get("WORK_REPEAT", "1"))):
        flagged = []
        for s in sites:
            flagged.extend(analytics.detect_site([r for r in rows if r["site_id"] == s]))
    compute = time.time() - t0
    slow = float(os.environ.get("SLOW_FACTOR", "1"))
    if slow > 1:
        time.sleep(compute * (slow - 1))                      # emulated heterogeneity: this node is `slow`x slower
    res = {"proc": a.proc, "sites": sites, "rows": len(rows), "flagged": len(flagged), "flagged_ids": flagged[:50],
           "compute_s": round(compute, 3), "wall_s": round(time.time() - t0, 3), "pid": os.getpid()}
    tmp = a.out + ".tmp"
    json.dump(res, open(tmp, "w"))
    os.replace(tmp, a.out)


if __name__ == "__main__":
    main()
