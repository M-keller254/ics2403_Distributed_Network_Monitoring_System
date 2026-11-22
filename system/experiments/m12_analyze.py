"""Aggregate results/m12_capstone_raw.csv into results/m12_capstone_summary.csv (+ m12_summary.json with the headline comparisons).

Each row of the summary = one (sweep, level, system) cell: n repetitions, mean and sample standard deviation of every metric, and the
half-width of a 95 % confidence interval for the mean (Student t; with n = 2-3 these intervals are WIDE - that is the honest uncertainty).
"""
import csv
import math
import os
import statistics

from system.experiments.common import RESULTS, save_csv, save_json

METRICS = ["complete_s", "send_s", "e2e_lag_s", "e2e_rows_s", "ingest_rows_s", "req_lat_mean_ms", "req_lat_p95_ms", "req_lat_p99_ms", "jitter_sd_ms",
           "jitter_rfc3550_ms", "request_failure_rate_pct", "ingest_availability_pct", "ingest_outages", "mttr_resume_s", "kills", "rows_lost", "duplicates",
           "cpu_pct_mean", "cpu_pct_max", "rss_mb_mean", "host_cpu_pct_mean", "wire_mb", "raw_mb", "wire_mbps_mean", "compression_ratio"]
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571}


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load():
    with open(os.path.join(RESULTS, "m12_capstone_raw.csv"), newline="") as f:
        return list(csv.DictReader(f))


def main():
    raw = load()
    order, cells = {}, {}
    for r in raw:
        sw, lv = r["sweep"], r["level"]
        sysname = r["system"] if sw != "transport" else f"proposed ({r['transport']})"
        order.setdefault(sw, [])
        if lv not in order[sw]:
            order[sw].append(lv)
        cells.setdefault((sw, lv, sysname), []).append(r)
    rows = []
    for (sw, lv, sysname), rs in cells.items():
        good = [r for r in rs if not r.get("error")]
        out = {"sweep": sw, "level": lv, "level_order": order[sw].index(lv) + 1, "system": sysname, "trials": len(rs), "trials_with_error": len(rs) - len(good),
               "reps": len(good)}
        for m in METRICS:
            vals = [num(r.get(m)) for r in good if num(r.get(m)) is not None]
            if vals:
                out[m + "_mean"] = round(statistics.mean(vals), 3)
                out[m + "_sd"] = round(statistics.stdev(vals), 3) if len(vals) > 1 else 0.0
                out[m + "_ci95"] = round(T95.get(len(vals) - 1, 2.0) * statistics.stdev(vals) / math.sqrt(len(vals)), 3) if len(vals) > 1 else None
            else:
                out[m + "_mean"] = None
        if not good:
            out["rows_lost_mean"] = statistics.mean(num(r.get("rows_lost")) or 0 for r in rs)
        rows.append(out)
    rows.sort(key=lambda r: (r["sweep"], r["level_order"], r["system"]))
    save_csv("m12_capstone_summary.csv", rows)

    # ---- headline comparisons: improved vs baseline vs proposed, per sweep level
    head = []
    for sw in order:
        for lv in order[sw]:
            cell = {r["system"]: r for r in rows if r["sweep"] == sw and r["level"] == lv}
            b, p, i = cell.get("baseline-flat"), cell.get("proposed"), cell.get("proposed+improved")
            g = lambda x, k: x.get(k) if x else None
            ratio = lambda a, c: round(a / c, 2) if a and c else None
            head.append({"sweep": sw, "level": lv,
                         "complete_s_baseline": g(b, "complete_s_mean"), "complete_s_proposed": g(p, "complete_s_mean"), "complete_s_improved": g(i, "complete_s_mean"),
                         "speedup_improved_vs_baseline": ratio(g(b, "complete_s_mean"), g(i, "complete_s_mean")),
                         "speedup_improved_vs_proposed": ratio(g(p, "complete_s_mean"), g(i, "complete_s_mean")),
                         "cpu_improved_vs_proposed": ratio(g(i, "cpu_pct_mean_mean"), g(p, "cpu_pct_mean_mean")),
                         # under cross-traffic the proposed systems' link counter also sees the noise bytes while the baseline's counter does not -> not comparable, so left blank
                         "wire_mb_baseline": None if sw == "net_load" else g(b, "wire_mb_mean"), "wire_mb_improved": None if sw == "net_load" else g(i, "wire_mb_mean"),
                         "avail_baseline": g(b, "ingest_availability_pct_mean"), "avail_proposed": g(p, "ingest_availability_pct_mean"),
                         "lost_baseline": g(b, "rows_lost_mean"), "lost_proposed": g(p, "rows_lost_mean"), "lost_improved": g(i, "rows_lost_mean")})
    save_csv("m12_headline.csv", head)
    save_json("m12_summary.json", {"levels": order, "headline": head, "cells": len(rows), "raw_trials": len(raw)})


if __name__ == "__main__":
    main()
