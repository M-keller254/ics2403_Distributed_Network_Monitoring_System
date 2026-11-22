"""
Distributed analytics workload (Milestone 2): per-site anomaly detection over the dataset.

The task is embarrassingly parallel by site, which is exactly how a monitoring system
would distribute it: each node processes the sites it is responsible for and nobody
needs another node's data. Method: robust rolling z-score on CPU and latency
(median/MAD over a trailing window), flag if both channels deviate.

    detect_site(rows)            -> list of flagged record_ids for one site (ordered by time)
    run_partitioned(rows, k)     -> (flagged_ids, wall_seconds) using k worker processes
    score(flagged, rows)         -> precision / recall / f1 against the is_anomaly label
"""
import statistics
import time
from concurrent.futures import ProcessPoolExecutor

WINDOW = 24            # trailing readings (2 hours at 5-minute sampling)
Z_THRESHOLD = 4.0


def _robust_z(x, window):
    med = statistics.median(window)
    mad = statistics.median([abs(v - med) for v in window]) or 1e-6
    return 0.6745 * (x - med) / mad


def detect_site(rows):
    rows = sorted(rows, key=lambda r: r["timestamp"])
    cpu_w, lat_w, flagged = [], [], []
    for r in rows:
        if len(cpu_w) >= WINDOW // 2:
            if _robust_z(r["cpu_pct"], cpu_w) > Z_THRESHOLD and _robust_z(r["latency_ms"], lat_w) > Z_THRESHOLD:
                flagged.append(r["record_id"])
                continue                       # do not let an anomaly poison the baseline window
        cpu_w.append(r["cpu_pct"])
        lat_w.append(r["latency_ms"])
        del cpu_w[:-WINDOW], lat_w[:-WINDOW]
    return flagged


def _work(site_rows):
    out = []
    for rows in site_rows:
        out.extend(detect_site(rows))
    return out


def group_by_site(rows):
    g = {}
    for r in rows:
        g.setdefault(r["site_id"], []).append(r)
    return list(g.values())


def run_partitioned(rows, k, pool=None):
    """Partition sites round-robin across k workers (processes stand in for nodes).

    Returns (flagged_ids, wall_seconds, rows_per_worker). With a pre-started `pool` the wall time
    excludes process start-up (steady-state cost); without one it is end-to-end including spawn.
    """
    groups = group_by_site(rows)
    parts = [groups[i::k] for i in range(k)]
    t0 = time.perf_counter()
    if k == 1:
        flagged = _work(parts[0])
    elif pool is not None:
        flagged = [i for r in pool.map(_work, parts) for i in r]
    else:
        with ProcessPoolExecutor(max_workers=k) as ex:
            flagged = [i for r in ex.map(_work, parts) for i in r]
    wall = time.perf_counter() - t0
    return flagged, wall, [sum(len(r) for r in p) for p in parts]


_LOCAL = {}


def _init_local(by_site):
    _LOCAL["by_site"] = by_site


def _work_local(site_ids):
    out = []
    for sid in site_ids:
        out.extend(detect_site(_LOCAL["by_site"][sid]))
    return out


def run_partitioned_local(rows, k):
    """Data-local variant: every worker already holds its sites (as an edge node holds its own data),
    so only site ids go out and only flagged ids come back. Pool start-up is excluded from the timing
    (a deployed edge node is long-running); the data hand-over is done once, before timing.
    """
    by_site = {}
    for r in rows:
        by_site.setdefault(r["site_id"], []).append(r)
    ids = sorted(by_site)
    parts = [ids[i::k] for i in range(k)]
    if k == 1:
        _init_local(by_site)
        t0 = time.perf_counter()
        flagged = _work_local(parts[0])
        wall = time.perf_counter() - t0
    else:
        with ProcessPoolExecutor(max_workers=k, initializer=_init_local, initargs=(by_site,)) as ex:
            list(ex.map(len, [[]] * k))               # spawn workers
            t0 = time.perf_counter()
            flagged = [i for r in ex.map(_work_local, parts) for i in r]
            wall = time.perf_counter() - t0
    return flagged, wall, [sum(len(by_site[s]) for s in p) for p in parts]


def score(flagged, rows):
    truth = {r["record_id"] for r in rows if r["is_anomaly"]}
    f = set(flagged)
    tp = len(f & truth)
    p = tp / len(f) if f else 0.0
    rec = tp / len(truth) if truth else 0.0
    return {"flagged": len(f), "truth": len(truth), "tp": tp, "precision": round(p, 4), "recall": round(rec, 4),
            "f1": round(2 * p * rec / (p + rec), 4) if p + rec else 0.0}
