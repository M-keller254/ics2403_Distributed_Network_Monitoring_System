"""
Milestone 2 -- Distributed Processing and Performance.

Hypothesis: spreading the edge work over more nodes raises aggregate throughput until the
single flat collector becomes the bottleneck; latency per request then grows while
throughput flattens. Distributed analytics (per-site anomaly detection) speeds up with
workers until the host's cores run out.

Design: independent variable = number of edge nodes N (1, 2, 4), each streaming its share
of the sites. Controlled: 30,000 dataset rows, chunk=500, same flat collector, same host.
Dependent: throughput (rows/s = completed work / time), latency (T_response - T_request),
jitter (stdev of latency), request loss (failed / attempted), CPU and memory of the
collector, load distribution (Jain index). Each point is repeated REPS times.
Extra run: collector with 5% injected request errors, to show the loss metric measures what
it claims (expect ~5%).
Part B: distributed compute -- the anomaly-detection task over all 120,000 rows with
k = 1, 2, 4 worker processes: speedup, efficiency, detection quality.

Run:  python -m system.experiments.m02_performance
"""
import statistics
import threading
import time

import requests

from system.common.metrics import mean_ci95, summarize
from system.edge import analytics
from system.experiments.common import ResourceSampler, dataset, drive_bulk, jain_index, partition_by_site, save_csv, save_json
from system.scripts.cluster import Cluster, free_ports

ROWS = 30000
REPS = 3


def one_run(n_nodes, rows, inject_error=0.0):
    port = free_ports(1)[0]
    with Cluster() as c:
        c.start("flat", "system.baseline.collector", port, env={"DATA_DIR": f"{c.workdir}/data"})
        if inject_error:
            requests.post(c.url("flat") + "/fault", json={"error_prob": inject_error})
        parts = partition_by_site(rows, n_nodes)
        results, finish = [None] * n_nodes, [0.0] * n_nodes
        from system.experiments.common import drive_bulk as db

        def work(i):
            results[i] = db(c.url("flat"), parts[i])
            finish[i] = time.perf_counter()

        with ResourceSampler([c.procs["flat"].pid]) as rs:
            t0 = time.perf_counter()
            ths = [threading.Thread(target=work, args=(i,)) for i in range(n_nodes)]
            [t.start() for t in ths]
            [t.join() for t in ths]
            wall = time.perf_counter() - t0
        stored = requests.get(c.url("flat") + "/readings/count").json()["rows_in_db"]
        lat = [x for r in results for x in r["latencies_s"]]
        attempted = sum(r["requests"] for r in results)
        failed = sum(r["failed_requests"] for r in results)
        out = {"nodes": n_nodes, "injected_error": inject_error, "wall_s": round(wall, 3),
               "rows_completed": sum(r["ok_rows"] for r in results), "rows_stored": stored,
               "throughput_rows_s": round(sum(r["ok_rows"] for r in results) / wall, 1),
               "lat_mean_ms": summarize(lat, 1000)["mean"], "lat_p95_ms": summarize(lat, 1000)["p95"],
               "jitter_ms": summarize(lat, 1000)["jitter"], "request_loss_pct": round(100.0 * failed / attempted, 2),
               "rows_per_node": [len(p) for p in parts], "jain_rows": jain_index([len(p) for p in parts]),
               "finish_spread_s": round(max(finish) - min(finish), 3)}
        out.update(rs.summary())
        return out


def main():
    rows = dataset(ROWS)
    print(f"Part A: flat collector, {len(rows):,} rows, N edge nodes in {{1,2,4}} x {REPS} reps")
    raw = []
    for n in (1, 2, 4):
        for rep in range(REPS):
            r = one_run(n, rows)
            r["rep"] = rep + 1
            raw.append(r)
            print(f"  N={n} rep={rep + 1}: {r['throughput_rows_s']:>8} rows/s  lat mean {r['lat_mean_ms']} ms p95 {r['lat_p95_ms']} ms  "
                  f"jitter {r['jitter_ms']} ms  collector CPU {r['svc_cpu_pct_mean']}%  loss {r['request_loss_pct']}%")
    for rep in range(REPS):
        r = one_run(2, rows, inject_error=0.05)
        r["rep"] = rep + 1
        raw.append(r)
        print(f"  N=2 +5% injected errors rep={rep + 1}: measured request loss {r['request_loss_pct']}%  stored {r['rows_stored']}/{r['rows_completed']}")
    save_csv("m02_throughput_raw.csv", raw)

    summary = []
    for n in (1, 2, 4):
        g = [r for r in raw if r["nodes"] == n and not r["injected_error"]]
        tp, ci = mean_ci95([r["throughput_rows_s"] for r in g])
        summary.append({"nodes": n, "reps": len(g), "throughput_mean": round(tp, 1), "throughput_ci95": round(ci or 0, 1),
                        "lat_mean_ms": round(statistics.mean(r["lat_mean_ms"] for r in g), 2),
                        "lat_p95_ms": round(statistics.mean(r["lat_p95_ms"] for r in g), 2),
                        "jitter_ms": round(statistics.mean(r["jitter_ms"] for r in g), 2),
                        "collector_cpu_pct": round(statistics.mean(r["svc_cpu_pct_mean"] for r in g), 1),
                        "collector_rss_mb": round(statistics.mean(r["svc_rss_mb_mean"] for r in g), 1),
                        "jain_rows": g[0]["jain_rows"]})
    save_csv("m02_throughput_summary.csv", summary)

    print("\nPart B: distributed anomaly detection over all rows (cold = incl. start-up, warm = pool ready but rows shipped, data-local = workers hold their sites)")
    from concurrent.futures import ProcessPoolExecutor
    allrows = dataset()
    comp = []
    for k in (1, 2, 4):
        pool = ProcessPoolExecutor(max_workers=k) if k > 1 else None
        if pool:
            list(pool.map(abs, range(k)))            # force workers to spawn before timing
        for rep in range(REPS * 2):
            for mode, pl in (("cold", None), ("warm", pool)):
                if mode == "warm" and k == 1 and rep >= REPS:
                    continue
                flagged, wall, per = analytics.run_partitioned(allrows, k, pool=pl)
                q = analytics.score(flagged, allrows)
                comp.append({"workers": k, "mode": mode if k > 1 else "single", "rep": rep + 1, "wall_s": round(wall, 3),
                             "rows_per_worker": per, "jain_rows": jain_index(per), **q})
        if pool:
            pool.shutdown()
        for rep in range(REPS * 2):
            flagged, wall, per = analytics.run_partitioned_local(allrows, k)
            q = analytics.score(flagged, allrows)
            comp.append({"workers": k, "mode": "data-local", "rep": rep + 1, "wall_s": round(wall, 3),
                         "rows_per_worker": per, "jain_rows": jain_index(per), **q})
    base = statistics.mean(x["wall_s"] for x in comp if x["workers"] == 1)
    for x in comp:
        x["speedup"] = round(base / x["wall_s"], 2)
        x["efficiency"] = round(x["speedup"] / x["workers"], 2)
    for k in (1, 2, 4):
        for mode in ("single", "cold", "warm", "data-local"):
            g = [x for x in comp if x["workers"] == k and x["mode"] == mode]
            if g:
                print(f"  k={k} {mode:6}: wall {statistics.mean(x['wall_s'] for x in g):.3f}s speedup {statistics.mean(x['speedup'] for x in g):.2f}x "
                      f"F1 {g[0]['f1']} (P {g[0]['precision']}, R {g[0]['recall']}) site-balance Jain {g[0]['jain_rows']}")
    save_csv("m02_compute_scaling.csv", comp)
    part_b = []
    for k in (1, 2, 4):
        for mode in ("single", "cold", "warm", "data-local"):
            g = [x for x in comp if x["workers"] == k and x["mode"] == mode]
            if g:
                part_b.append({"workers": k, "mode": mode, "wall_s_mean": round(statistics.mean(x["wall_s"] for x in g), 3),
                               "speedup": round(statistics.mean(x["speedup"] for x in g), 2)})
    save_json("m02_summary.json", {"part_a": summary, "part_b": part_b, "host_cores": __import__("os").cpu_count()})


if __name__ == "__main__":
    main()
