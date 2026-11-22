"""
Milestone 3 -- Distributed Architecture: scalability and edge-vs-core trade-off.

A  Flat client-server vs tiered Edge->Core: N monitoring agents post live reports.
   Flat: agents -> one collector.   Tiered: agents -> edge gateway(s) (aggregation BATCH_SIZE=10) -> bus -> core leader.
   Metric: first-tier latency / throughput as N grows, and what the upper tier (collector / core) has to absorb.
B  Edge aggregation trade-off (BATCH_SIZE 1, 5, 20, 50): messages and bytes crossing the edge->core link vs
   staleness of the data at the core (the price of aggregating at the edge).
C  Dataset path message size (ROWS_PER_MSG 10, 100, 500) over the real edge->bus->core path.

Hypotheses:  H1 a single tier saturates first (latency rises with N) while the tiered design keeps the
core's incoming message rate flat;  H2 edge batching cuts core-bound messages ~BATCH_SIZE-fold at the
cost of staleness ~ FLUSH_INTERVAL;  H3 larger dataset messages raise throughput until per-message cost is amortised.

Run:  python -m system.experiments.m03_architecture
"""
import statistics
import threading
import time

import requests

from system.common.metrics import summarize
from system.edge.replayer import run_replay
from system.experiments.common import ResourceSampler, dataset, save_csv, save_json
from system.scripts.cluster import Cluster
from system.scripts.stack import edge_core_cloud, wait_for


def report(node, seq):
    return {"node_id": node, "node_role": "edge", "cpu_percent": 20 + (seq * 7) % 50, "mem_percent": 40 + (seq * 3) % 30,
            "timestamp": time.time(), "anomaly": seq % 37 == 0, "compute_time_ms": 5.0, "report_interval": 1.0}


def agent_load(url, n_agents, reports_each):
    """n_agents threads, each posting reports_each live reports back-to-back (burst = worst case)."""
    lat, fails, lock, errs = [], [0], threading.Lock(), {}

    def one(i):
        s, mine = requests.Session(), []
        for k in range(reports_each):
            t0 = time.perf_counter()
            try:
                r = s.post(url + "/metrics", json=report(f"agent-{i}", k), timeout=10)
                if r.status_code == 200:
                    mine.append(time.perf_counter() - t0)
                else:
                    with lock:
                        fails[0] += 1
            except requests.RequestException as e:
                with lock:
                    fails[0] += 1
                    errs[type(e).__name__] = errs.get(type(e).__name__, 0) + 1
        with lock:
            lat.extend(mine)

    t0 = time.perf_counter()
    ths = [threading.Thread(target=one, args=(i,)) for i in range(n_agents)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    return lat, fails[0], time.perf_counter() - t0, errs


def part_a():
    rows, REPORTS, REPS = [], 25, 3
    for n in (20, 100, 400):
        for design, rep in [(d, r) for r in range(1, REPS + 1) for d in ("flat", "tiered-1gw", "tiered-2gw")]:
            with Cluster() as c:
                if design == "flat":
                    c.start("flat", "system.baseline.collector", 22000, env={"DATA_DIR": f"{c.workdir}/d"})
                    urls, watch = [c.url("flat")], [c.procs["flat"].pid]
                else:
                    names = edge_core_cloud(c, base=22100, transport="kafka", gateway_env={"BATCH_SIZE": 10, "FLUSH_INTERVAL": 1})
                    urls = [c.url("gateway")]
                    if design == "tiered-2gw":
                        c.start("gateway2", "system.edge.gateway", 22150, env={"TRANSPORT": "kafka", "GATEWAY_ID": "edge-gateway-2",
                                "BATCH_SIZE": 10, "FLUSH_INTERVAL": 1, "CORE_URLS": c.specs["gateway"]["env"]["CORE_URLS"]})
                        urls.append(c.url("gateway2"))
                    watch = [c.procs[k].pid for k in c.procs if k.startswith("gateway")]
                results, wall = [], None
                with ResourceSampler(watch) as rs:
                    t0 = time.perf_counter()
                    outs = [None] * len(urls)

                    def go(j):
                        share = n // len(urls)
                        outs[j] = agent_load(urls[j], share, REPORTS)
                    ths = [threading.Thread(target=go, args=(j,)) for j in range(len(urls))]
                    [t.start() for t in ths]
                    [t.join() for t in ths]
                    wall = time.perf_counter() - t0
                lat = [x for o in outs for x in o[0]]
                fails = sum(o[1] for o in outs)
                total = (n // len(urls)) * len(urls) * REPORTS
                row = {"design": design, "rep": rep, "agents": n, "errors": {k: v for o in outs for k, v in o[3].items()},
                       "agents_n": n, "reports": total, "wall_s": round(wall, 2),
                       "throughput_rps": round(len(lat) / wall, 1), "lat_mean_ms": summarize(lat, 1000)["mean"],
                       "lat_p95_ms": summarize(lat, 1000)["p95"], "lat_p99_ms": summarize(lat, 1000)["p99"],
                       "jitter_ms": summarize(lat, 1000)["jitter"], "failed": fails, **rs.summary()}
                if design == "flat":
                    row["upper_tier_msgs"] = total          # collector IS the upper tier: every report lands on it
                else:
                    wait_for(lambda: sum(c.get(x, "/stats")["reports_represented"] for x in names["cores"]) >= total * 0.99 or None,
                             timeout=40, what="core to drain")
                    time.sleep(1.5)
                    row["upper_tier_msgs"] = sum(c.get(x, "/stats")["ingest_messages"] for x in names["cores"])
                    row["upper_tier_reports"] = sum(c.get(x, "/stats")["reports_represented"] for x in names["cores"])
                rows.append(row)
                print(f"  A {design:11} N={n:4} rep{rep}: {row['throughput_rps']:>7} rep/s  lat mean {row['lat_mean_ms']} p95 {row['lat_p95_ms']} ms  "
                      f"upper-tier msgs {row['upper_tier_msgs']}  failed {fails}")
    save_csv("m03_scalability.csv", rows)
    return rows


def part_b():
    rows, N, R = [], 40, 30
    for batch in (1, 5, 20, 50):
        with Cluster() as c:
            names = edge_core_cloud(c, base=22300, transport="kafka", gateway_env={"BATCH_SIZE": batch, "FLUSH_INTERVAL": 1.0})
            # paced load: 40 agents x 30 reports at ~10 reports/s each => ~400/s total for ~3 s
            def agent(i):
                s = requests.Session()
                for k in range(R):
                    s.post(c.url("gateway") + "/metrics", json=report(f"agent-{i}", k), timeout=10)
                    time.sleep(0.1)
            ths = [threading.Thread(target=agent, args=(i,)) for i in range(N)]
            [t.start() for t in ths]
            [t.join() for t in ths]
            total = N * R
            time.sleep(1.5 + 0.2)
            try:
                wait_for(lambda: sum(c.get(x, "/stats")["reports_represented"] for x in names["cores"]) >= total or None, 20, what="drain")
            except TimeoutError:
                pass
            st = [c.get(x, "/stats") for x in names["cores"]]
            gw = c.get("gateway", "/metrics_summary")["forwarding"]
            row = {"batch_size": batch, "reports": total, "reports_at_core": sum(s["reports_represented"] for s in st),
                   "core_messages": sum(s["ingest_messages"] for s in st), "edge_to_core_bytes": gw["bytes"],
                   "bytes_per_report": round(gw["bytes"] / total, 1),
                   "staleness_ms": round(statistics.mean(s["avg_end_to_end_latency_ms"] for s in st if s["avg_end_to_end_latency_ms"]), 1)}
            row["message_reduction_x"] = round(total / row["core_messages"], 1) if row["core_messages"] else None
            rows.append(row)
            print(f"  B batch={batch:2}: {row['core_messages']:4} core msgs ({row['message_reduction_x']}x fewer)  {row['bytes_per_report']} B/report  staleness {row['staleness_ms']} ms")
    save_csv("m03_edge_aggregation.csv", rows)
    return rows


def part_c():
    rows, data = [], dataset(20000)
    for order in ("time", "site"):
        for per in (10, 100, 500):
            with Cluster() as c:
                names = edge_core_cloud(c, base=22500, transport="kafka", gateway_env={"ROWS_PER_MSG": per})
                t0 = time.perf_counter()
                run_replay(c.url("gateway"), rows=data, chunk=2000, quiet=True, order=order)
                wait_for(lambda: sum(c.get(x, "/readings/count")["rows_in_db"] for x in names["cores"]) >= len(data) or None, 120, what="core drain")
                wall = time.perf_counter() - t0
                gw = c.get("gateway", "/metrics_summary")["forwarding"]
                pl = [c.get(x, "/pipeline")["pipeline_latency_s"] for x in names["cores"]]
                pl = [p for p in pl if p.get("n")]
                row = {"upload_order": order, "rows_per_msg": per, "rows": len(data), "end_to_end_s": round(wall, 2),
                       "rows_per_s": round(len(data) / wall, 0), "messages": gw["messages"], "avg_rows_per_message": round(len(data) / gw["messages"], 1),
                       "bytes": gw["bytes"], "bytes_per_row": round(gw["bytes"] / len(data), 1),
                       "pipeline_lat_mean_s": pl[0]["mean"] if pl else None, "pipeline_lat_p95_s": pl[0]["p95"] if pl else None}
                rows.append(row)
                print(f"  C {order:4} rows/msg={per:3}: {row['rows_per_s']:>7.0f} rows/s {row['messages']:5} msgs ({row['avg_rows_per_message']} rows/msg achieved) "
                      f"{row['bytes_per_row']} B/row  pipeline lat mean {row['pipeline_lat_mean_s']} s")
    save_csv("m03_message_size.csv", rows)
    return rows


def main():
    print("Part A: flat vs tiered scalability"); a = part_a()
    print("Part B: edge aggregation trade-off"); b = part_b()
    print("Part C: dataset message size over edge->bus->core"); cc = part_c()
    save_json("m03_summary.json", {"scalability": a, "edge_aggregation": b, "message_size": cc})


if __name__ == "__main__":
    main()
