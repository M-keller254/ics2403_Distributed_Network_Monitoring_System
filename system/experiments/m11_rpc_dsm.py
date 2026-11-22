"""
Milestone 11 -- RPC and Distributed Shared Memory.

RPC  (Client -> Client stub -> Network -> Server stub -> Server)
  A  three implementations of the same monitoring service: REST/JSON, our RPC framework (JSON), gRPC/protobuf.
     Latency, throughput (8 concurrent clients), bytes on the wire per call, for small / medium / large payloads and streaming.
  B  call semantics under reply loss (30 % of replies lost): maybe, at-least-once, at-most-once.
DSM  (directory-based shared memory, 4 nodes)
  C  strong (write-invalidate) vs eventual (write-update) consistency: operation cost, message overhead per op, staleness.
  D  synchronisation: lost updates with plain read-modify-write vs distributed lock vs atomic add at the home node.

Hypotheses
  H1 gRPC/protobuf messages are several times smaller than JSON and faster for large payloads; for tiny calls the HTTP/1.1 vs
     HTTP/2 stack dominates and the gap is small.
  H2 at-least-once double-executes under reply loss; at-most-once executes exactly once; maybe loses calls.
  H3 strong DSM: 0 stale reads, write cost grows with the number of sharers (2 messages per invalidation); eventual DSM: cheap
     writes but a measurable fraction of stale reads inside a short window.
  H4 plain read-modify-write loses updates even on a coherent DSM; lock or atomic-add gives the exact count; atomic-add is cheaper.

Run:  python -m system.experiments.m11_rpc_dsm
"""
import statistics
import threading
import time

import grpc
import requests

from system.common.metrics import summarize
from system.experiments.common import save_csv, save_json
from system.rpc import monitor_pb2 as pb
from system.rpc import monitor_pb2_grpc as pbg
from system.rpc.framework import RpcClient
from system.rpc.rest_server import sample
from system.scripts.cluster import Cluster

BASE = 31000


def timed(fn, n):
    lat = []
    for i in range(n):
        t0 = time.perf_counter()
        fn(i)
        lat.append(time.perf_counter() - t0)
    return lat


def throughput(fn, clients=8, seconds=3.0):
    stop, counts = threading.Event(), []

    def w(k):
        n = 0
        while not stop.is_set():
            fn(k)
            n += 1
        counts.append(n)
    ths = [threading.Thread(target=w, args=(k,)) for k in range(clients)]
    t0 = time.perf_counter()
    [t.start() for t in ths]
    time.sleep(seconds)
    stop.set()
    [t.join() for t in ths]
    return sum(counts) / (time.perf_counter() - t0)


def part_a():
    rows = []
    with Cluster() as c:
        c.start("rest", "system.rpc.rest_server", BASE)
        c.start("grpc", "system.rpc.grpc_server", BASE + 2, health="/health", wait=False)
        time.sleep(1.5)
        rest = c.url("rest")
        rpcs = RpcClient(rest, "at-most-once", timeout=5)
        ch = grpc.insecure_channel(f"127.0.0.1:{BASE + 2}", options=[("grpc.max_send_message_length", 64 << 20), ("grpc.max_receive_message_length", 64 << 20)])
        stub = pbg.MonitorStub(ch)
        grpc.channel_ready_future(ch).result(timeout=10)
        sess = requests.Session()
        sizes = {"small: get_site": 0, "medium: put 100 readings": 100, "large: put 5000 readings": 5000}
        for label, n in sizes.items():
            reps = 400 if n == 0 else (200 if n == 100 else 25)
            readings = [sample(i) for i in range(n)]
            pb_batch = pb.Batch(readings=[pb.Reading(record_id=r["record_id"], site=r["site"], cpu_pct=r["cpu_pct"], mem_pct=r["mem_pct"], rx_mbps=r["rx_mbps"], tx_mbps=r["tx_mbps"],
                                                     latency_ms=r["latency_ms"], jitter_ms=r["jitter_ms"], packet_loss_pct=r["packet_loss_pct"], active_users=r["active_users"],
                                                     is_anomaly=r["is_anomaly"]) for r in readings])
            if n == 0:
                impls = {
                    "REST/JSON": (lambda i: sess.get(f"{rest}/rest/site/NAI-01", timeout=5), None),
                    "RPC framework (JSON)": (lambda i: rpcs.get_site(site="NAI-01"), None),
                    "gRPC/protobuf": (lambda i: stub.GetSite(pb.SiteReq(site="NAI-01")), pb.SiteReq(site="NAI-01")),
                }
                req_bytes = {"REST/JSON": len("GET /rest/site/NAI-01"), "RPC framework (JSON)": len('{"id":"x","method":"get_site","params":{"site":"NAI-01"}}') + 32,
                             "gRPC/protobuf": len(pb.SiteReq(site="NAI-01").SerializeToString()) + 5}
                resp_bytes = {"REST/JSON": len(sess.get(f"{rest}/rest/site/NAI-01").content), "RPC framework (JSON)": len(requests.post(rest + "/rpc", json={"id": "x", "method": "get_site", "params": {"site": "NAI-01"}}).content),
                              "gRPC/protobuf": len(stub.GetSite(pb.SiteReq(site="NAI-01")).SerializeToString()) + 5}
            else:
                impls = {
                    "REST/JSON": (lambda i, r=readings: sess.post(f"{rest}/rest/readings", json={"readings": r}, timeout=30), None),
                    "RPC framework (JSON)": (lambda i, r=readings: rpcs.put_readings(readings=r), None),
                    "gRPC/protobuf": (lambda i, b=pb_batch: stub.PutReadings(b), None),
                }
                import json as _j
                req_bytes = {"REST/JSON": len(_j.dumps({"readings": readings})), "RPC framework (JSON)": len(_j.dumps({"id": "x" * 32, "method": "put_readings", "params": {"readings": readings}})),
                             "gRPC/protobuf": pb_batch.ByteSize() + 5}
                resp_bytes = {"REST/JSON": 17, "RPC framework (JSON)": 60, "gRPC/protobuf": 7}
            for name, (fn, _) in impls.items():
                lat = timed(fn, reps)
                tp = throughput(fn, 8, 2.5)
                sm = summarize(lat, 1000)
                rows.append({"payload": label, "implementation": name, "mean_ms": sm["mean"], "p95_ms": sm["p95"], "jitter_ms": sm["jitter"],
                             "throughput_calls_s_8_clients": round(tp, 0), "request_bytes": req_bytes[name], "response_bytes": resp_bytes[name]})
                print("  A", rows[-1])
        # ---- streaming 10,000 readings
        N = 10000
        def rest_stream():
            r = sess.get(f"{rest}/rest/stream?count={N}", stream=True, timeout=60)
            return sum(1 for _ in r.iter_lines())
        def grpc_stream():
            return sum(1 for _ in stub.StreamReadings(pb.StreamReq(count=N)))
        def rpc_all():
            return len(rpcs.stream_readings(count=N))
        for name, fn in (("REST/JSON (NDJSON stream)", rest_stream), ("RPC framework (one JSON array)", rpc_all), ("gRPC/protobuf (server stream)", grpc_stream)):
            ts = []
            for _ in range(5):
                t0 = time.perf_counter(); n_got = fn(); ts.append(time.perf_counter() - t0)
            rows.append({"payload": f"stream {N} readings", "implementation": name, "mean_ms": round(1000 * statistics.mean(ts), 1), "p95_ms": round(1000 * max(ts), 1), "jitter_ms": round(1000 * statistics.pstdev(ts), 1),
                         "throughput_calls_s_8_clients": round(N / statistics.mean(ts), 0), "request_bytes": None, "response_bytes": None, "readings_received": n_got})
            print("  A", rows[-1])
    save_csv("m11_rpc_benchmark.csv", rows)
    return rows


def part_b():
    rows = []
    for sem in ("maybe", "at-least-once", "at-most-once"):
        with Cluster() as c:
            c.start("srv", "system.rpc.rest_server", BASE + 10, env={"SEMANTICS": sem})
            requests.post(c.url("srv") + "/rpc_admin", json={"drop_reply_prob": 0.3, "semantics": sem})
            cl = RpcClient(c.url("srv"), sem, timeout=0.4, retries=4)
            ok = fail = 0
            for _ in range(200):
                try:
                    cl.increment(counter="c")
                    ok += 1
                except TimeoutError:
                    fail += 1
            st = requests.get(c.url("srv") + "/state").json()
            rows.append({"semantics": sem, "calls_issued": 200, "calls_returned_ok": ok, "calls_failed": fail, "server_executions": st["counter"],
                         "duplicate_executions": max(0, st["counter"] - 200), "lost_executions": max(0, ok - st["counter"]) if sem == "maybe" else 0,
                         "retries_sent": cl.stats["retries"], "replies_from_dedup_cache": st["rpc"]["duplicates_answered_from_cache"],
                         "outcome": {"maybe": "calls can be lost (client gives up)", "at-least-once": "counter over-counts (non-idempotent op executed again)", "at-most-once": "exactly 200 executions"}[sem]})
            print("  B", rows[-1])
    save_csv("m11_call_semantics.csv", rows)
    return rows


def start_dsm(c, mode, n=4):
    peers = ",".join(f"{i}=http://127.0.0.1:{BASE + 20 + i}" for i in range(n))
    for i in range(n):
        c.start(f"d{i}", "system.dsm.node", BASE + 20 + i, env={"NODE_INDEX": i, "PEERS": peers, "MODE": mode})
    return [c.url(f"d{i}") for i in range(n)]


def stats_sum(urls):
    tot = {"msgs": 0, "inv": 0, "updates": 0}
    for u in urls:
        s = requests.get(u + "/stats").json()
        tot["msgs"] += s["net_msgs"]
        tot["inv"] += s["ops"]["invalidations_sent"]
        tot["updates"] += s["ops"]["updates_sent"]
    return tot


def part_c():
    rows = []
    for mode in ("strong", "eventual"):
        with Cluster() as c:
            u = start_dsm(c, mode)
            s = requests.Session()
            home = None

            def op(node, kind, key, value=None):
                t0 = time.perf_counter()
                r = s.post(f"{u[node]}/{kind}", json={"key": key, "value": value}, timeout=10).json()
                return time.perf_counter() - t0, r

            def measure(label, fn):
                b = stats_sum(u)
                lat = [fn(i) for i in range(30)]
                a = stats_sum(u)
                rows.append({"mode": mode, "operation": label, "mean_ms": round(1000 * statistics.mean(lat), 2), "p95_ms": round(1000 * sorted(lat)[int(0.95 * len(lat))], 2),
                             "messages_per_op": round((a["msgs"] - b["msgs"]) / 30, 2), "invalidations_per_op": round((a["inv"] - b["inv"]) / 30, 2), "updates_per_op": round((a["updates"] - b["updates"]) / 30, 2)})
                print("  C", rows[-1])
            measure("read, local hit", lambda i: (op(0, "write", f"hit{i}", 1), op(0, "read", f"hit{i}"))[1][0])
            measure("read, miss (fetch via home)", lambda i: (op(1, "write", f"miss{i}", 1), op(2, "read", f"miss{i}"))[1][0])
            for sharers in (0, 1, 2, 3):
                def w_with_sharers(i, sh=sharers):
                    key = f"sh{sh}-{i}"
                    op(0, "write", key, 0)
                    for r in range(1, 1 + sh):
                        op(r, "read", key)
                    return op(0, "write", key, 1)[0]
                measure(f"write with {sharers} remote sharer(s)", w_with_sharers)
            # ---- staleness: node0 writes i; immediately node1 and node2 read
            stale = [0, 0]
            lags = []
            for i in range(1, 301):
                op(0, "write", "stalekey", i)
                for r in (1, 2):
                    v = op(r, "read", "stalekey")[1]["value"]
                    stale[0] += int(v is not None and v < i)
                    stale[1] += 1
            rows.append({"mode": mode, "operation": "visibility test: 300 writes, 600 immediate remote reads", "mean_ms": None, "p95_ms": None,
                         "messages_per_op": None, "invalidations_per_op": None, "updates_per_op": None, "stale_reads": stale[0], "reads": stale[1], "stale_pct": round(100 * stale[0] / stale[1], 1)})
            print("  C", rows[-1])
            # ---- staleness window (eventual): time until all nodes see the new value
            win = []
            for i in range(20):
                key = f"win{i}"
                op(0, "write", key, 0)
                for r in (1, 2, 3):
                    op(r, "read", key)
                op(0, "write", key, 1)
                t0 = time.perf_counter()
                while any(op(r, "read", key)[1]["value"] != 1 for r in (1, 2, 3)):
                    pass
                win.append(time.perf_counter() - t0)
            rows.append({"mode": mode, "operation": "time until every node sees a new value (20 trials)", "mean_ms": round(1000 * statistics.mean(win), 2), "p95_ms": round(1000 * max(win), 2)})
            print("  C", rows[-1])
            # ---- workload throughput
            for label, wfrac in (("95% read / 5% write", 0.05), ("50% read / 50% write", 0.5)):
                import random
                stop, cnt, lats = threading.Event(), [], []
                b = stats_sum(u)

                def worker(k):
                    rng, ss, n = random.Random(k), requests.Session(), 0
                    while not stop.is_set():
                        key = f"w{rng.randrange(20)}"
                        t0 = time.perf_counter()
                        if rng.random() < wfrac:
                            ss.post(f"{u[k]}/write", json={"key": key, "value": n}, timeout=10)
                        else:
                            ss.post(f"{u[k]}/read", json={"key": key}, timeout=10)
                        lats.append(time.perf_counter() - t0)
                        n += 1
                    cnt.append(n)
                ths = [threading.Thread(target=worker, args=(k,)) for k in range(4)]
                t0 = time.perf_counter()
                [t.start() for t in ths]
                time.sleep(3)
                stop.set()
                [t.join() for t in ths]
                wall = time.perf_counter() - t0
                a = stats_sum(u)
                total = sum(cnt)
                rows.append({"mode": mode, "operation": f"workload {label}, 4 clients", "mean_ms": round(1000 * statistics.mean(lats), 2), "p95_ms": round(1000 * sorted(lats)[int(0.95 * len(lats))], 2),
                             "messages_per_op": round((a["msgs"] - b["msgs"]) / total, 2), "throughput_ops_s": round(total / wall, 0)})
                print("  C", rows[-1])
    save_csv("m11_dsm_consistency.csv", rows)
    return rows


def part_d():
    rows = []
    with Cluster() as c:
        u = start_dsm(c, "strong")
        for style in ("plain read-modify-write", "distributed lock", "atomic add at home node"):
            requests.post(u[0] + "/write", json={"key": "ctr", "value": 0})
            for x in u:
                requests.post(x + "/reset")
            requests.post(u[0] + "/write", json={"key": "ctr", "value": 0})
            b = stats_sum(u)

            def worker(k):
                s = requests.Session()
                for _ in range(40):
                    if style == "plain read-modify-write":
                        v = s.post(f"{u[k]}/read", json={"key": "ctr"}).json()["value"] or 0
                        s.post(f"{u[k]}/write", json={"key": "ctr", "value": v + 1})
                    elif style == "distributed lock":
                        s.post(f"{u[k]}/lock", json={"key": "ctr", "timeout": 20})
                        v = s.post(f"{u[k]}/read", json={"key": "ctr"}).json()["value"] or 0
                        s.post(f"{u[k]}/write", json={"key": "ctr", "value": v + 1})
                        s.post(f"{u[k]}/unlock", json={"key": "ctr"})
                    else:
                        s.post(f"{u[k]}/add", json={"key": "ctr", "delta": 1})
            ths = [threading.Thread(target=worker, args=(k,)) for k in range(4)]
            t0 = time.perf_counter()
            [t.start() for t in ths]
            [t.join() for t in ths]
            wall = time.perf_counter() - t0
            a = stats_sum(u)
            final = requests.post(u[1] + "/read", json={"key": "ctr"}).json()["value"]
            rows.append({"update style": style, "increments_issued": 160, "final_counter": final, "lost_updates": 160 - final, "wall_s": round(wall, 2),
                         "messages": a["msgs"] - b["msgs"], "messages_per_increment": round((a["msgs"] - b["msgs"]) / 160, 1)})
            print("  D", rows[-1])
    save_csv("m11_dsm_synchronization.csv", rows)
    return rows


def main(parts="abcd"):
    out = {}
    if "a" in parts:
        print("A RPC benchmark"); out["benchmark"] = part_a()
    if "b" in parts:
        print("B call semantics"); out["semantics"] = part_b()
    if "c" in parts:
        print("C DSM consistency"); out["dsm"] = part_c()
    if "d" in parts:
        print("D DSM synchronisation"); out["sync"] = part_d()
    save_json("m11_summary.json", out)


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else "abcd")
