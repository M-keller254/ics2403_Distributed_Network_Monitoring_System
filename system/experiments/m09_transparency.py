"""
Milestone 9 -- Transparency and Reliability.

Logical resource  = "site/<ID>/<attribute>" (e.g. site/NAI-01/threshold).   Physical resource = a row on one replica host.

A  Access + location transparency: identical application code against a replicated-KV namespace and a file-backed one.
B  Overhead: direct physical access vs the transparency layer (cached placement, cold placement, writes via quorum).
C  Migration transparency: move namespaces between replica groups UNDER LOAD with the 4-phase protocol, versus a naive
   copy-then-switch. Counted: application errors, lost writes, stale reads, worst latency, final consistency.
D  Failure transparency + reliability: a replica crashes for 5 s. Application-visible errors and availability for a client
   bound to the physical replica versus a client using the logical layer.
E  Replication transparency: one logical write -> how many physical copies exist (and agree) after anti-entropy.

Hypotheses
  H1 the layer adds a small constant latency (one cached directory lookup + quorum fan-out), < 3x a direct access.
  H2 4-phase migration: zero application errors, zero lost writes; naive migration loses writes made during the copy (counted with a unique key for every 4th acknowledged write, so a later overwrite cannot hide a loss).
  H3 during a replica crash the logical client sees ~0 errors; the physical client is unavailable for the whole outage.

Run:  python -m system.experiments.m09_transparency
"""
import json
import os
import statistics
import threading
import time

import requests

from system.common.metrics import summarize
from system.experiments.common import save_csv, save_json
from system.scripts.cluster import Cluster
from system.transparency.layer import Directory, LogicalStore, migrate

BASE = 29000
SITES = [f"NAI-{i:02d}" for i in range(1, 9)]


def build(c):
    c.start("naming", "system.naming.server", BASE, env={"NODE_NAME": "dir", "ZONE": ""})
    urls = {f"r{i}": f"http://127.0.0.1:{BASE + i}" for i in range(1, 7)}
    for gid, members in (("G1", ["r1", "r2", "r3"]), ("G2", ["r4", "r5", "r6"])):
        peers = ",".join(f"{m}={urls[m]}" for m in members)
        for m in members:
            c.start(m, "system.replication.replica", int(urls[m].rsplit(":", 1)[1]), env={"REPLICA_ID": m, "DATA_DIR": f"{c.workdir}/db", "PEERS": peers, "ANTI_ENTROPY_S": 0.5})
    d = Directory(c.url("naming"))
    d.set_group("G1", {m: urls[m] for m in ("r1", "r2", "r3")})
    d.set_group("G2", {m: urls[m] for m in ("r4", "r5", "r6")})
    for s in SITES:
        d.set_placement(f"site/{s}", {"groups": ["G1"], "read_from": "G1", "epoch": 0, "backend": "kv"})
    os.makedirs(f"{c.workdir}/files", exist_ok=True)
    d.set_placement("site/FILE-01", {"groups": [], "read_from": None, "epoch": 0, "backend": "file", "file": "site-file-01.json"})
    return urls, d


def app_workload(layer, ns):
    """The 'application': identical code for every backend/location."""
    layer.write(f"{ns}/threshold", {"cpu": 85, "latency_ms": 120})
    layer.write(f"{ns}/state", "healthy")
    layer.write(f"{ns}/state", "degraded")
    return layer.read(f"{ns}/threshold"), layer.read(f"{ns}/state")


def part_a(c, urls, d):
    layer = LogicalStore(c.url("naming"), "app", files_dir=f"{c.workdir}/files")
    rows = []
    for ns, where in (("site/NAI-01", "replicated KV, group G1 (3 hosts)"), ("site/FILE-01", "local JSON file (1 host)")):
        t, s = app_workload(layer, ns)
        rows.append({"logical_resource": ns, "physical_backend": where, "threshold_read": json.dumps(t), "state_read": s, "same_application_code": True})
    migrate(d, "site/NAI-01", "G1", "G2")
    t, s = app_workload(layer, "site/NAI-01")
    rows.append({"logical_resource": "site/NAI-01", "physical_backend": "replicated KV, group G2 (moved, app unchanged)", "threshold_read": json.dumps(t), "state_read": s, "same_application_code": True})
    for r in rows:
        print("  A", r)
    save_csv("m09_access_location.csv", rows)
    return rows


def part_b(c, urls, d):
    layer = LogicalStore(c.url("naming"), "bench", files_dir=f"{c.workdir}/files")
    cold = LogicalStore(c.url("naming"), "bench-cold")
    cold.dir.resolver.ttl = 0
    for i in range(50):
        layer.write(f"site/NAI-02/k{i}", i)
    time.sleep(1)
    s = requests.Session()
    n = 400

    def timed(fn):
        out = []
        for i in range(n):
            t0 = time.perf_counter()
            fn(i)
            out.append(time.perf_counter() - t0)
        return out
    cases = {
        "direct physical READ (1 replica, no layer)": lambda i: s.get(f"{urls['r1']}/kv/site/NAI-02/k{i % 50}", timeout=2),
        "logical READ (layer, cached placement, R=2)": lambda i: layer.read(f"site/NAI-02/k{i % 50}"),
        "logical READ (layer, placement looked up every call)": lambda i: (cold.dir.resolver.cache.clear(), cold.read(f"site/NAI-02/k{i % 50}")),
        "direct physical WRITE (1 replica, no layer)": lambda i: s.put(f"{urls['r1']}/kv/site/NAI-02/w{i % 50}", json={"value": i, "version": int(time.time() * 1e6), "writer": "x"}, timeout=2),
        "logical WRITE (layer, W=2 of 3 replicas)": lambda i: layer.write(f"site/NAI-02/w{i % 50}", i),
    }
    rows, base = [], None
    for name, fn in cases.items():
        lat = timed(fn)
        sm = summarize(lat, 1000)
        wall = sum(lat)
        row = {"operation": name, "mean_ms": sm["mean"], "p95_ms": sm["p95"], "jitter_ms": sm["jitter"], "ops_per_s_1thread": round(n / wall, 1)}
        rows.append(row)
    rd, wd = rows[0]["mean_ms"], rows[3]["mean_ms"]
    for r in rows:
        r["overhead_x_vs_direct"] = round(r["mean_ms"] / (rd if "READ" in r["operation"] else wd), 2)
        print("  B", r)
    save_csv("m09_overhead.csv", rows)
    return rows


def migration_run(c, urls, d, mode, seconds=12.0, migrate_at=3.0, namespaces=("site/NAI-03", "site/NAI-04", "site/NAI-05", "site/NAI-06")):
    layer_stats, stop = [], threading.Event()
    last_ack, lock = {}, threading.Lock()
    acked_keys = []                                               # unique key per write: a write lost during migration cannot be hidden by a later overwrite
    errors, stale, lat_events = [0], [0], []
    t_start = time.perf_counter()

    def client(ns, cid):
        layer = LogicalStore(c.url("naming"), f"c{cid}")
        i = 0
        while not stop.is_set():
            key = f"{ns}/counter"
            t0 = time.perf_counter()
            try:
                if i % 2 == 0:
                    layer.write(key, i)
                    ukey = f"{ns}/ev-{cid}-{i}"
                    if i % 8 == 0:                                  # every 4th write also gets a unique key (bounds the data volume to be migrated)
                        layer.write(ukey, i)
                    with lock:
                        last_ack[key] = i
                        if i % 8 == 0:
                            acked_keys.append((ukey, i))
                else:
                    v = layer.read(key)
                    with lock:
                        if v is not None and key in last_ack and v < last_ack[key]:
                            stale[0] += 1
            except Exception as e:
                with lock:
                    errors[0] += 1
                    if os.environ.get('M9_DEBUG') and errors[0] < 3:
                        import traceback; traceback.print_exc()
            lat_events.append((t0 - t_start, time.perf_counter() - t0))
            i += 1
            time.sleep(0.005)
        layer_stats.append(layer.stats)
    ths = [threading.Thread(target=client, args=(ns, k)) for k, ns in enumerate(namespaces)]
    for ns in namespaces:                                         # seed
        d.set_placement(ns, {"groups": ["G1"], "read_from": "G1", "epoch": 0, "backend": "kv"})
    [t.start() for t in ths]
    time.sleep(migrate_at)
    t0 = time.time()
    timings = {}
    for ns in namespaces:
        if mode == "4-phase":
            timings[ns] = migrate(d, ns, "G1", "G2")
        else:                                                     # naive: copy once, then switch placement
            src = d.group("G1", fresh=True)
            dst = d.group("G2", fresh=True)
            for url in src.values():
                ents = [e for e in requests.get(url + "/dump", params={"live": 1}, timeout=5).json()["entries"] if e["key"].startswith(ns + "/")]
                for du in dst.values():
                    requests.post(du + "/load", json={"entries": ents}, timeout=5)
            time.sleep(0.4)                                       # window in which clients still write to G1
            d.set_placement(ns, {"groups": ["G2"], "read_from": "G2", "epoch": 9, "backend": "kv"})
    mig_s = time.time() - t0
    time.sleep(max(0, seconds - migrate_at - mig_s))
    stop.set()
    [t.join() for t in ths]
    time.sleep(1.5)
    # lost writes: the last acknowledged write must be what the owning group now holds
    lost = 0
    for ns in namespaces:
        key = f"{ns}/counter"
        got = None
        for url in list(d.group("G2", fresh=True).values()):
            r = requests.get(f"{url}/kv/{key}", timeout=2)
            if r.status_code == 200:
                got = max(got or -1, r.json()["value"])
        if got is None or got < last_ack.get(key, -1):
            lost += 1
    g2 = list(d.group("G2", fresh=True).values())
    missing = 0
    for ukey, val in acked_keys:
        if not any(requests.get(f"{u}/kv/{ukey}", timeout=2).status_code == 200 for u in g2):
            missing += 1
    left_in_g1 = sum(1 for url in d.group("G1", fresh=True).values() for ns in namespaces if requests.get(f"{url}/kv/{ns}/counter", timeout=2).status_code == 200)
    ops = len(lat_events)
    worst = max(l for _, l in lat_events)
    in_mig = [l for t, l in lat_events if migrate_at <= t <= migrate_at + mig_s]
    return {"migration": mode, "operations": ops, "app_errors": errors[0], "stale_reads": stale[0], "namespaces_with_lost_last_write": lost, "acked_unique_writes": len(acked_keys), "acked_writes_missing_on_new_group": missing, "namespaces": len(namespaces),
            "migration_duration_s": round(mig_s, 2), "worst_latency_ms": round(worst * 1000, 1),
            "mean_latency_ms_during_migration": round(1000 * statistics.mean(in_mig), 2) if in_mig else None, "copies_left_on_source_group": left_in_g1,
            "reresolutions": sum(s["reresolutions"] for s in layer_stats)}


def part_c():
    rows = []
    for mode in ("4-phase", "naive copy-then-switch"):
        for rep in range(2):
            with Cluster() as c:
                urls, d = build(c)
                r = migration_run(c, urls, d, mode)
                r["rep"] = rep + 1
                rows.append(r)
                print("  C", r)
    save_csv("m09_migration.csv", rows)
    return rows


def part_d():
    rows = []
    for rep in range(2):
        with Cluster() as c:
            urls, d = build(c)
            layer = LogicalStore(c.url("naming"), "app")
            for i in range(20):
                layer.write(f"site/NAI-07/k{i}", i)
            time.sleep(1)
            res = {"physical": [], "logical": []}
            stop = threading.Event()
            t_start = time.perf_counter()

            def physical():
                s = requests.Session()
                i = 0
                while not stop.is_set():
                    t0 = time.perf_counter()
                    try:
                        ok = s.get(f"{urls['r1']}/kv/site/NAI-07/k{i % 20}", timeout=0.5).status_code == 200
                    except requests.RequestException:
                        ok = False
                    res["physical"].append((t0 - t_start, ok))
                    i += 1
                    time.sleep(0.05)

            def logical():
                i = 0
                while not stop.is_set():
                    t0 = time.perf_counter()
                    try:
                        ok = layer.read(f"site/NAI-07/k{i % 20}") == i % 20
                        layer.write(f"site/NAI-07/k{i % 20}", i % 20)
                    except Exception:
                        ok = False
                    res["logical"].append((t0 - t_start, ok))
                    i += 1
                    time.sleep(0.05)
            ths = [threading.Thread(target=physical), threading.Thread(target=logical)]
            [t.start() for t in ths]
            time.sleep(3)
            c.kill("r1")
            time.sleep(5)
            c.restart("r1")
            time.sleep(4)
            stop.set()
            [t.join() for t in ths]
            for kind in ("physical", "logical"):
                ev = res[kind]
                n_ok = sum(ok for _, ok in ev)
                rows.append({"client": "bound to physical replica r1" if kind == "physical" else "logical layer (3 replicas)", "rep": rep + 1, "ops": len(ev),
                             "errors_seen_by_application": len(ev) - n_ok, "availability_pct": round(100 * n_ok / len(ev), 2)})
                print("  D", rows[-1])
    save_csv("m09_failure_transparency.csv", rows)
    return rows


def part_e(c, urls, d):
    layer = LogicalStore(c.url("naming"), "app")
    layer.write("site/NAI-08/threshold", {"cpu": 90})
    time.sleep(1.5)
    holders = {}
    for rid in ("r1", "r2", "r3"):
        r = requests.get(f"{urls[rid]}/kv/site/NAI-08/threshold", timeout=2)
        holders[rid] = r.json()["value"] if r.status_code == 200 else None
    out = [{"logical_write": "site/NAI-08/threshold = {cpu: 90}", "physical_copies": sum(v is not None for v in holders.values()), "copies_agree": len({json.dumps(v) for v in holders.values()}) == 1, "per_replica": json.dumps(holders)}]
    print("  E", out[0])
    save_csv("m09_replication_transparency.csv", out)
    return out


def main():
    with Cluster() as c:
        urls, d = build(c)
        print("A"); a = part_a(c, urls, d)
        print("B"); b = part_b(c, urls, d)
        print("E"); e = part_e(c, urls, d)
    print("C migration under load"); cc = part_c()
    print("D failure transparency"); dd = part_d()
    save_json("m09_summary.json", {"access_location": a, "overhead": b, "migration": cc, "failure": dd, "replication": e})


if __name__ == "__main__":
    main()
