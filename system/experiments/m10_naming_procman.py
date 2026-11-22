"""
Milestone 10 -- Naming and Distributed Process Management.

NAMING  (Name -> Name Resolution -> Resource Location)
  A  resolution latency and messages per lookup: static map, broadcast, central name server, hierarchical prefix resolver
     (cold / with prefix table / with answer cache).
  B  scalability: broadcast cost vs number of nodes; central vs hierarchical under concurrent clients; central server vs number of names.
  C  consistency: how long clients keep returning the OLD location after a resource moves (static map vs cache TTL).
  D  failure: lookup success when the central server / the root / one regional server is down.
PROCESS MANAGEMENT  (Create -> Locate -> Schedule -> Coordinate -> Terminate)
  E  the same 24-site anomaly job on 4 node agents under three scheduling policies with one slow node; then a node is
     killed mid-job and the controller re-schedules.

Hypotheses
  H1 broadcast costs N messages per lookup and its latency grows with N; central = 1 message; prefix = 1 after warm-up.
  H2 the central server saturates before the hierarchy (load is spread over regional servers).
  H3 staleness after a move is bounded by the cache TTL; a static map is stale forever.
  H4 least-loaded scheduling beats round-robin/random when a node is slower; the controller completes the job after a node dies.

Run:  python -m system.experiments.m10_naming_procman
"""
import random
import statistics
import threading
import time

import requests

from system.common.metrics import summarize
from system.experiments.common import save_csv, save_json
from system.naming.resolver import BroadcastResolver, NameServerResolver, PrefixResolver, StaticResolver
from system.procman.controller import Controller
from system.scripts.cluster import Cluster

BASE = 30000
REGIONS = {"nai": "NAI", "mom": "MOM", "kis": "KIS", "nak": "NAK", "eld": "ELD", "nye": "NYE"}
SITES = [f"{p}-{i:02d}" for p in REGIONS.values() for i in range(1, 5)]


def fqdn(site):
    return f"ke.{site[:3].lower()}.{site}.agent"


def reg(url, name, loc, ttl=30):
    requests.post(url + "/register", json={"name": name, "location": loc, "ttl": ttl}, timeout=3)


def build_naming(c, extra_nodes=0):
    """root('') -> ke -> six regional zone servers (each holds its sites' records); one flat central server holds everything."""
    c.start("root", "system.naming.server", BASE, env={"NODE_NAME": "root", "ZONE": ""})
    c.start("ke", "system.naming.server", BASE + 1, env={"NODE_NAME": "ke", "ZONE": "ke"})
    requests.post(c.url("root") + "/delegate", json={"zone": "ke", "server": c.url("ke")})
    c.start("central", "system.naming.server", BASE + 2, env={"NODE_NAME": "central", "ZONE": ""})
    reg_nodes = []
    for i, (z, pre) in enumerate(REGIONS.items()):
        n = f"zone-{z}"
        c.start(n, "system.naming.server", BASE + 10 + i, env={"NODE_NAME": n, "ZONE": f"ke.{z}"})
        requests.post(c.url("ke") + "/delegate", json={"zone": f"ke.{z}", "server": c.url(n)})
        reg_nodes.append(n)
    for s in SITES:
        loc = f"http://10.0.{SITES.index(s)}.1:8080"
        reg(c.url(f"zone-{s[:3].lower()}"), fqdn(s), loc)
        reg(c.url("central"), fqdn(s), loc)
    for i in range(extra_nodes):
        c.start(f"extra{i}", "system.naming.server", BASE + 40 + i, env={"NODE_NAME": f"extra{i}", "ZONE": ""})
    return reg_nodes


def lookups(resolver, names, n, seed=1):
    rng = random.Random(seed)
    lat = []
    for _ in range(n):
        nm = rng.choice(names)
        t0 = time.perf_counter()
        resolver.resolve(nm)
        lat.append(time.perf_counter() - t0)
    return lat


def part_a_b():
    names = [fqdn(s) for s in SITES]
    table = {fqdn(s): f"http://10.0.{SITES.index(s)}.1:8080" for s in SITES}
    rows = []
    with Cluster() as c:
        zones = build_naming(c, extra_nodes=2)              # 6 zone servers + 2 empty = 8 broadcast nodes
        nodes8 = [c.url(z) for z in zones] + [c.url("extra0"), c.url("extra1")]
        schemes = {
            "static map (local table)": StaticResolver(table),
            "broadcast to 8 nodes": BroadcastResolver(nodes8),
            "central name server": NameServerResolver(c.url("central")),
            "prefix resolver, cold (no cache, iterative from root)": PrefixResolver(c.url("root"), use_cache=False),
        }
        pr_table = PrefixResolver(c.url("root"), use_cache=True)
        pr_table.resolve(names[0]); pr_table.cache.clear()
        for nm in names:                                    # learn all delegations, then drop the answers: prefix table only
            pr_table.resolve(nm)
        pr_table.cache.clear()
        schemes["prefix resolver, prefix table (answers not cached)"] = pr_table
        schemes["prefix resolver, prefix table + answer cache (TTL 30 s)"] = PrefixResolver(c.url("root"), use_cache=True)
        for label, r in schemes.items():
            if "answer cache" in label:
                lookups(r, names, len(names) * 2)           # warm
            before = dict(r.stats)
            if "(answers not cached)" in label:
                r.cache.clear()
                _orig = r.cache
                class NoCache(dict):
                    def __setitem__(self, k, v): pass
                r.cache = NoCache()
            lat = lookups(r, names, 1500)
            n = r.stats["lookups"] - before["lookups"]
            sm = summarize(lat, 1000, 3)
            rows.append({"scheme": label, "nodes": 8, "lookups": n, "mean_ms": sm["mean"], "p95_ms": sm["p95"], "jitter_ms": sm["jitter"],
                         "messages_per_lookup": round((r.stats["messages"] - before["messages"]) / n, 2), "cache_hits_pct": round(100 * (r.stats["cache_hits"] - before["cache_hits"]) / n, 1),
                         "failures": r.stats["failures"] - before["failures"]})
            print("  A", rows[-1])
        save_csv("m10_resolution_latency.csv", rows)

        # ---- B1: central vs hierarchy under concurrent clients
        load_rows = []
        for clients in (1, 4, 16, 32):
            for label, mk in (("central name server", lambda: NameServerResolver(c.url("central"))),
                              ("hierarchy (prefix table)", lambda: PrefixResolver(c.url("root"), use_cache=True))):
                lat_all, stop = [], threading.Event()

                def worker(k):
                    r = mk()
                    r.cache = type("NC", (dict,), {"__setitem__": lambda s, k_, v: None})() if True else r.cache
                    if "hierarchy" in label:
                        for nm in names:
                            r.resolve(nm)
                    lat = []
                    rng = random.Random(k)
                    while not stop.is_set():
                        t0 = time.perf_counter()
                        r.resolve(rng.choice(names))
                        lat.append(time.perf_counter() - t0)
                    lat_all.extend(lat)
                ths = [threading.Thread(target=worker, args=(k,)) for k in range(clients)]
                t0 = time.perf_counter()
                [t.start() for t in ths]
                time.sleep(3)
                stop.set()
                [t.join() for t in ths]
                wall = time.perf_counter() - t0
                sm = summarize(lat_all, 1000)
                load_rows.append({"scheme": label, "clients": clients, "lookups_per_s": round(len(lat_all) / wall, 0), "mean_ms": sm["mean"], "p95_ms": sm["p95"]})
                print("  B1", load_rows[-1])
        save_csv("m10_resolution_load.csv", load_rows)

        # ---- B3: central server vs number of names
        size_rows = []
        for n_names in (1000, 10000, 100000):
            recs = [{"name": f"ke.syn.S{i}.agent", "location": f"http://10.9.{i % 250}.{i % 200}:80", "ttl": 30} for i in range(n_names)]
            t0 = time.perf_counter()
            for i in range(0, n_names, 5000):
                requests.post(c.url("central") + "/register", json={"records": recs[i:i + 5000]}, timeout=30)
            reg_s = time.perf_counter() - t0
            r = NameServerResolver(c.url("central"))
            lat = []
            rng = random.Random(3)
            for _ in range(800):
                t = time.perf_counter()
                r.resolve(f"ke.syn.S{rng.randrange(n_names)}.agent")
                lat.append(time.perf_counter() - t)
            sm = summarize(lat, 1000)
            size_rows.append({"names_registered": n_names, "bulk_register_s": round(reg_s, 2), "lookup_mean_ms": sm["mean"], "lookup_p95_ms": sm["p95"]})
            print("  B3", size_rows[-1])
        save_csv("m10_resolution_vs_names.csv", size_rows)

    # ---- B2: broadcast vs number of nodes
    bc_rows = []
    for n_nodes in (4, 8, 16, 24):
        with Cluster() as c:
            urls = []
            for i in range(n_nodes):
                c.start(f"n{i}", "system.naming.server", BASE + i, env={"NODE_NAME": f"n{i}", "ZONE": ""})
                urls.append(c.url(f"n{i}"))
            c.start("central", "system.naming.server", BASE + 90, env={"NODE_NAME": "central", "ZONE": ""})
            for k, s in enumerate(SITES):
                loc = f"http://10.0.{k}.1:8080"
                reg(urls[k % n_nodes], fqdn(s), loc)
                reg(c.url("central"), fqdn(s), loc)
            for label, r in (("broadcast", BroadcastResolver(urls)), ("central", NameServerResolver(c.url("central")))):
                before = dict(r.stats)
                lat = lookups(r, [fqdn(s) for s in SITES], 600)
                sm = summarize(lat, 1000)
                bc_rows.append({"scheme": label, "nodes": n_nodes, "mean_ms": sm["mean"], "p95_ms": sm["p95"], "messages_per_lookup": round((r.stats["messages"] - before["messages"]) / 600, 1)})
                print("  B2", bc_rows[-1])
    save_csv("m10_broadcast_vs_nodes.csv", bc_rows)
    return rows, load_rows, size_rows, bc_rows


def part_c():
    """A resource moves at t=0. How long do clients keep seeing the old location?"""
    rows = []
    with Cluster() as c:
        build_naming(c)
        name = fqdn("NAI-01")
        zone = c.url("zone-nai")
        old = "http://10.0.0.1:8080"
        new = "http://10.99.99.99:8080"
        clients = {"static map": StaticResolver({name: old})}
        for ttl in (0, 1, 5):
            clients[f"name server, cache TTL {ttl}s"] = NameServerResolver(c.url("central"), ttl=ttl)
            pr = PrefixResolver(c.url("root"))
            clients[f"prefix resolver, record TTL {ttl}s"] = pr
        for k, r in clients.items():
            r.resolve(name)
        reg(c.url("central"), name, new, ttl=0)
        for ttl in (0, 1, 5):
            pass
        # per-client record TTL for the prefix resolver is the record's TTL: register it with that TTL per client run
        for label, r in clients.items():
            if label.startswith("prefix resolver"):
                ttl = int(label.split("TTL ")[1].rstrip("s"))
                r.cache.clear()
                reg(zone, name, old, ttl=ttl)
                r.resolve(name)                              # caches the old answer with its TTL
                reg(zone, name, new, ttl=ttl)
            elif label.startswith("name server"):
                ttl = int(label.split("TTL ")[1].rstrip("s"))
                r.cache.clear()
                reg(c.url("central"), name, old, ttl=0)
                r.resolve(name)
                reg(c.url("central"), name, new, ttl=0)
        t0 = time.perf_counter()
        first_fresh = {k: None for k in clients}
        stale_polls = {k: 0 for k in clients}
        polls = {k: 0 for k in clients}
        while time.perf_counter() - t0 < 7:
            for label, r in clients.items():
                v = r.resolve(name)
                polls[label] += 1
                if v == new:
                    if first_fresh[label] is None:
                        first_fresh[label] = time.perf_counter() - t0
                else:
                    stale_polls[label] += 1
            time.sleep(0.05)
        for label in clients:
            rows.append({"client": label, "time_until_new_location_s": round(first_fresh[label], 2) if first_fresh[label] is not None else "never (>7 s)",
                         "stale_answers_pct": round(100 * stale_polls[label] / polls[label], 1), "lookups_in_7s": polls[label],
                         "messages": r.stats["messages"] if False else clients[label].stats["messages"]})
            print("  C", rows[-1])
    save_csv("m10_staleness.csv", rows)
    return rows


def part_d():
    rows = []
    names = [fqdn(s) for s in SITES]
    scenarios = {"no failure": None, "central name server down": ["central"], "root server down": ["root"], "regional server zone-nai down": ["zone-nai"]}
    for label, down in scenarios.items():
        with Cluster() as c:
            zones = build_naming(c, extra_nodes=0)
            nodes = [c.url(z) for z in zones]
            warm = PrefixResolver(c.url("root"))
            for nm in names:
                warm.resolve(nm)                              # warm prefix table (answers cached too)
            warm.cache.clear()                                # keep only the learned delegations (prefix table)
            cold = PrefixResolver(c.url("root"))
            static = StaticResolver({fqdn(s): f"http://10.0.{SITES.index(s)}.1:8080" for s in SITES})
            for d in down or []:
                c.kill(d)
            clients = {"static": static, "broadcast": BroadcastResolver(nodes), "central": NameServerResolver(c.url("central")),
                       "prefix cold": cold, "prefix warm (table)": warm}
            for k, r in clients.items():
                ok = sum(1 for nm in names if r.resolve(nm))
                rows.append({"failure": label, "resolver": k, "names_resolved": ok, "of": len(names), "success_pct": round(100 * ok / len(names), 1)})
                print("  D", rows[-1])
    save_csv("m10_failure.csv", rows)
    return rows


def _tasks():
    return [[s] for s in SITES]


def part_e():
    rows = []
    tasks = _tasks()
    for policy in ("round-robin", "random", "least-loaded"):
        for rep in range(3):
            with Cluster() as c:
                c.start("naming", "system.naming.server", BASE + 100, env={"NODE_NAME": "procs", "ZONE": ""})
                nodes = {}
                for i in range(4):
                    slow = 3.0 if i == 3 else 1.0
                    c.start(f"node{i}", "system.procman.node", BASE + 110 + i, env={"NODE_NAME": f"node{i}", "NAMING_URL": c.url("naming"), "SLOW_FACTOR": slow,
                                                                                      "WORK_REPEAT": 8, "OUT_DIR": f"{c.workdir}/out{i}", "DATASET_CSV": "data/telemetry_120k.csv"})
                    nodes[f"node{i}"] = c.url(f"node{i}")
                ctl = Controller(nodes, c.url("naming"), policy=policy, cap=1, seed=rep)
                res = ctl.run_job(f"job{rep}", tasks, timeout=180)
                sched = summarize(ctl.stats["schedule_ms"]) if ctl.stats["schedule_ms"] else {}
                rows.append({"policy": policy, "rep": rep + 1, "makespan_s": res["wall_s"], "workers": res["workers"], "flagged": res["flagged"], "rows": res["rows"],
                             "tasks_on_slow_node3": res["per_node"]["node3"], "tasks_per_node": "/".join(str(res["per_node"][n]) for n in sorted(nodes)),
                             "mean_schedule_ms": sched.get("mean"), "locate_calls": ctl.stats["locate_calls"]})
                print("  E", rows[-1])
    save_csv("m10_scheduling.csv", rows)
    # ---- failure: kill node1 mid-job
    fr = part_e_failure()
    return rows, fr


def part_e_failure():
    """Kill node1 two seconds into a 24-task job. Controls without a kill, and the kill with and without cache invalidation (the original bug)."""
    tasks = _tasks()
    fr = []
    for policy, kill, inval in (("least-loaded", False, True), ("least-loaded", True, False), ("least-loaded", True, True),
                                ("round-robin", False, True), ("round-robin", True, False), ("round-robin", True, True)):
        with Cluster() as c:
            c.start("naming", "system.naming.server", BASE + 100, env={"NODE_NAME": "procs", "ZONE": ""})
            nodes = {}
            for i in range(4):
                c.start(f"node{i}", "system.procman.node", BASE + 110 + i, env={"NODE_NAME": f"node{i}", "NAMING_URL": c.url("naming"), "WORK_REPEAT": 8,
                                                                                  "OUT_DIR": f"{c.workdir}/out{i}", "DATASET_CSV": "data/telemetry_120k.csv"})
                nodes[f"node{i}"] = c.url(f"node{i}")
            ctl = Controller(nodes, c.url("naming"), policy=policy, cap=1, invalidate_stale=inval)
            if kill:
                threading.Timer(2.0, lambda: c.kill("node1")).start()
            res = ctl.run_job("fjob", tasks, timeout=240)
            fr.append({"policy": policy, "node_killed_at_s": 2.0 if kill else None, "stale_name_cache_invalidated": inval if kill else None, "makespan_s": res["wall_s"],
                       "workers_completed": res["workers"], "of": len(tasks), "tasks_rescheduled": ctl.stats["rescheduled"], "flagged": res["flagged"], "result_complete": res["workers"] == len(tasks)})
            print("  E-fail", fr[-1])
    save_csv("m10_process_failure.csv", fr)
    return fr


def main(parts="abcde"):
    out = {}
    if "a" in parts:
        print("A/B resolution"); out["resolution"] = part_a_b()
    if "c" in parts:
        print("C staleness"); out["staleness"] = part_c()
    if "d" in parts:
        print("D failure"); out["failure"] = part_d()
    if "e" in parts:
        print("E process management"); out["procman"] = part_e()
    if "f" in parts and "e" not in parts:
        print("F process failure only"); out["procman_failure"] = part_e_failure()
    import json
    import os
    from system.experiments.common import RESULTS
    p = os.path.join(RESULTS, "m10_summary.json")
    prev = json.load(open(p)) if os.path.exists(p) else {}
    prev.update(out)
    save_json("m10_summary.json", prev)


if __name__ == "__main__":
    import sys
    main("".join(sys.argv[1:]) if len(sys.argv) > 1 else "acde")
