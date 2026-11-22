"""
Milestone 8 -- Distributed System Models: host-based, workstation, server-based, processor pool, integrated.

Method: (1) calibrate the real per-row compute cost of the anomaly detector on this machine; (2) feed it into the
seeded discrete-event simulator (system/models/simulator.py) so all five models run the SAME workload; (3) compare
latency, throughput, utilisation, scalability, reliability and cost; (4) score the models with a weighted decision
matrix and show how sensitive the winner is to the weights; (5) the write-up (docs/milestones/08-*.md) compares the simulator's
claims with the LIVE Milestone-3 measurements (flat collector ~ host-based, tiered gateways ~ integrated); that comparison is
done from results/m03_*.csv, not re-run here.

Hypotheses
  H1 at the reference load all models serve the offered load; they differ in latency and in how much hardware they need.
  H2 under growing load the host saturates first; the pool and integrated models degrade last.
  H3 with node failures only models that can re-route (pool, integrated) keep latency low; host-based stalls.
  H4 integrated cuts WAN traffic ~10x and keeps latency near the workstation model at a fraction of its utilisation waste.

Run:  python -m system.experiments.m08_models
"""
import statistics
import time

from system.edge import analytics
from system.experiments.common import dataset, save_csv, save_json
from system.models.simulator import simulate

KINDS = ["host-based", "workstation", "server-based", "processor-pool", "integrated"]
SEEDS = [2403 + i for i in range(5)]
PINNED_US_PER_ROW = 4.0   # measured 4.04-4.27 us/row on the reference machine; pinned so that every run of this experiment is bit-for-bit reproducible
HEAVY = 40.0          # assumption: the production detector is ~40x heavier than the robust z-score measured here


def calibrate():
    rows = dataset(30000)
    t0 = time.perf_counter()
    analytics.run_partitioned(rows, 1)
    s = time.perf_counter() - t0
    return s / len(rows)


def avg(runs, key):
    vals = [r[key] for r in runs if r[key] is not None]
    return round(statistics.mean(vals), 3) if vals else None


def ci(runs, key):
    vals = [r[key] for r in runs if r[key] is not None]
    return round(1.96 * statistics.stdev(vals) / len(vals) ** 0.5, 3) if len(vals) > 1 else 0


def sweep(cpu_row, label, configs):
    rows = []
    for cfg in configs:
        for kind in KINDS:
            runs = [simulate(kind, seed=s, cpu_per_row=cpu_row, **cfg["args"]) for s in SEEDS]
            row = {"experiment": label, **cfg["tag"], "model": kind}
            for k in ("throughput_jobs_s", "completion_pct", "lat_mean_s", "lat_p95_s", "lat_p99_s", "compute_cores", "total_cores", "compute_utilization", "wan_gb", "cost_units", "jobs_interrupted"):
                row[k] = avg(runs, k)
            row["lat_mean_ci95"] = ci(runs, "lat_mean_s")
            rows.append(row)
    return rows


def main():
    measured = calibrate()
    per_row = PINNED_US_PER_ROW / 1e6                  # pinned: the failure simulations are chaotic, so timing noise (4.04-4.27 us across runs) would change the results
    if abs(measured * 1e6 - PINNED_US_PER_ROW) / PINNED_US_PER_ROW > 0.25:
        print(f"  WARNING: this machine measures {measured * 1e6:.2f} us/row, more than 25 % away from the pinned {PINNED_US_PER_ROW} us/row; results below use the pinned value")
    cpu_row = per_row * HEAVY
    print(f"calibration: {measured * 1e6:.2f} us/row measured, {per_row * 1e6:.1f} us used (pinned) -> x{HEAVY:.0f} production detector = {cpu_row * 1e6:.1f} us/row, {cpu_row * 1000 * 1000:.0f} ms of CPU per 1000-row job")
    out = {"calibration": {"measured_us_per_row": round(measured * 1e6, 3), "used_us_per_row": PINNED_US_PER_ROW, "heavy_factor": HEAVY, "cpu_ms_per_job": round(cpu_row * 1000 * 1000, 1)}}

    base = sweep(cpu_row, "reference", [{"tag": {"load": 1.0}, "args": {"load": 1.0, "horizon": 600}}])
    save_csv("m08_reference.csv", base)
    for r in base:
        print(f"  ref {r['model']:15} thr {r['throughput_jobs_s']:>6}/s  lat {r['lat_mean_s']:>7}s (p95 {r['lat_p95_s']:>7})  util {r['compute_utilization']:.2f}  cores {r['total_cores']:>4.0f}  WAN {r['wan_gb']} GB  cost {r['cost_units']}")

    load = sweep(cpu_row, "load", [{"tag": {"load": l}, "args": {"load": l, "horizon": 600}} for l in (0.5, 0.8, 1.0, 1.2, 1.5)])
    save_csv("m08_load_sweep.csv", load)

    scal = sweep(cpu_row, "scalability", [{"tag": {"sites": n}, "args": {"sites": n, "load": 1.0, "horizon": 600, "params": {
        "host_cores": 4, "servers": max(2, n // 12), "server_cores": 2, "pool": max(5, round(5 * n / 24))}}} for n in (12, 24, 48, 96, 192)])
    save_csv("m08_scalability.csv", scal)

    rel = sweep(cpu_row, "reliability", [{"tag": {"mtbf_s": m}, "args": {"load": 1.0, "horizon": 1800, "params": {"mtbf": m, "mttr": 60.0}}} for m in (None, 600, 300)])
    save_csv("m08_reliability.csv", rel)
    for r in rel:
        print(f"  reliability MTBF={r['mtbf_s']}: {r['model']:15} completion {r['completion_pct']}%  lat mean {r['lat_mean_s']}s  p99 {r['lat_p99_s']}s")

    # ---- decision matrix: reference + scalability(96 sites) + reliability(MTBF 600)
    def get(rows, model, **flt):
        return next(r for r in rows if r["model"] == model and all(r.get(k) == v for k, v in flt.items()))
    crit = {}
    for kind in KINDS:
        ref, s96, rl = get(base, kind), get(scal, kind, sites=96), get(rel, kind, mtbf_s=600)
        crit[kind] = {"latency": ref["lat_p95_s"], "throughput": get(load, kind, load=1.5)["throughput_jobs_s"], "utilization": ref["compute_utilization"],
                      "scalability": s96["lat_p95_s"], "reliability": rl["lat_p99_s"], "cost": ref["cost_units"]}                    # cost_units already prices WAN traffic (0.09/GB); adding GB again double-counted it
        # cost model B: hardware that the sites already own is free (workstation = all cores, integrated = the 24 edge cores); dedicated servers are not
        owned = ref["total_cores"] if kind == "workstation" else (ref["total_cores"] - ref["compute_cores"] if kind == "integrated" else 0)
        crit[kind]["cost_B"] = round(ref["cost_units"] - owned * 1.0 * 600 / 3600.0, 3)

    def score(vals, higher_better):
        lo, hi = min(vals.values()), max(vals.values())
        return {k: (1.0 if hi == lo else ((v - lo) / (hi - lo) if higher_better else (hi - v) / (hi - lo))) for k, v in vals.items()}
    scores = {}
    for c, hb in (("latency", False), ("throughput", True), ("utilization", True), ("scalability", False), ("reliability", False), ("cost", False), ("cost_B", False)):
        sc = score({k: crit[k][c] for k in KINDS}, hb)
        for k in KINDS:
            scores.setdefault(k, {})[c] = round(sc[k], 3)
    weightsets = {"balanced": dict(latency=.2, throughput=.15, utilization=.1, scalability=.2, reliability=.2, cost=.15),
                  "cost-driven": dict(latency=.1, throughput=.1, utilization=.2, scalability=.1, reliability=.1, cost=.4),
                  "latency-driven": dict(latency=.4, throughput=.1, utilization=.05, scalability=.15, reliability=.2, cost=.1),
                  "reliability-driven": dict(latency=.1, throughput=.1, utilization=.05, scalability=.2, reliability=.45, cost=.1)}
    matrix = []
    for cost_model, ckey in (("A: every core is priced", "cost"), ("B: site-owned hardware is free", "cost_B")):
        for wname, w in weightsets.items():
            tot = {k: round(sum(scores[k][ckey if c == "cost" else c] * w[c] for c in w), 3) for k in KINDS}
            winner = max(tot, key=tot.get)
            ranked = sorted(tot, key=tot.get, reverse=True)
            matrix.append({"cost_model": cost_model, "weights": wname, **tot, "winner": winner, "runner_up": ranked[1]})
            print(f"  decision [{cost_model[:1]} / {wname}]: " + "  ".join(f"{k}={v}" for k, v in tot.items()) + f"  -> {winner} (then {ranked[1]})")
    save_csv("m08_decision_matrix.csv", matrix)
    save_csv("m08_criteria_raw.csv", [{"model": k, **crit[k]} for k in KINDS])
    save_csv("m08_criteria_scores.csv", [{"model": k, **scores[k]} for k in KINDS])
    out.update(reference=base, load=load, scalability=scal, reliability=rel, criteria=crit, scores=scores, decision=matrix)
    save_json("m08_summary.json", out)


if __name__ == "__main__":
    main()
