"""Integration tests: each starts real service processes (system.scripts.cluster) and checks end-to-end behaviour.

They use the same builders as the experiments (system.scripts.stack, system.experiments.*), so a green run means the
experiment harness itself works. Total run time is a few minutes; use `pytest -m "not slow"`-style selection with -k if needed.
"""
import glob
import sqlite3
import threading
import time

import requests

from system.edge.replayer import load_rows, run_replay
from system.scripts.cluster import Cluster
from system.scripts.stack import edge_core_cloud, wait_for


def _distinct_ids(c):
    ids = set()
    for p in glob.glob(f"{c.workdir}/core-data/core-*.sqlite3"):
        con = sqlite3.connect(p, timeout=30)
        ids.update(r[0] for r in con.execute("SELECT record_id FROM readings"))
        con.close()
    return len(ids)


# ---------------------------------------------------------------- pipeline: edge -> bus -> core (leader) -> bus -> archiver -> storage
def test_pipeline_end_to_end_with_archive(cluster):
    c = cluster
    edge_core_cloud(c, base=23000, transport="kafka", core_env={"HEARTBEAT_INTERVAL": 0.5, "ARCHIVE_TOPIC": "core.processed"})
    c.start("archiver", "system.cloud.archiver", 23050, env={"STORAGE": "object", "ARCHIVE_TOPIC": "core.processed"})
    rows = load_rows(limit=20000)
    run_replay(c.url("gateway"), rows=rows, chunk=1000, quiet=True)
    wait_for(lambda: _distinct_ids(c) >= 20000 or None, 90, what="core stored all rows")
    leaders = [n for n in ("core1", "core2", "core3") if c.get(n, "/leader")["is_leader"]]
    assert leaders == ["core3"], "Bully: the highest id must be the single leader"
    wait_for(lambda: requests.get(c.url("archiver") + "/verify", timeout=30).json()["object"]["distinct_record_ids"] >= 20000 or None, 90, what="archive")
    assert requests.get(c.url("archiver") + "/verify", timeout=30).json()["object"]["distinct_record_ids"] == 20000


def test_leader_crash_loses_no_rows(cluster):
    c = cluster
    edge_core_cloud(c, base=23100, transport="kafka", core_env={"HEARTBEAT_INTERVAL": 0.4})
    rows = load_rows(limit=30000)
    t = threading.Thread(target=lambda: run_replay(c.url("gateway"), rows=rows, chunk=1000, quiet=True))
    t.start()
    wait_for(lambda: _distinct_ids(c) >= 6000 or None, 60, what="progress before the crash")
    c.kill("core3")
    t.join()
    wait_for(lambda: _distinct_ids(c) >= 30000 or None, 120, what="all rows stored after failover")
    assert _distinct_ids(c) == 30000
    assert wait_for(lambda: c.get("core2", "/leader")["is_leader"] or None, 30, what="core2 takes over")


# ---------------------------------------------------------------- transactions
def test_2pc_commit_abort_and_invariant(cluster):
    from system.experiments.m05_transactions import start_txn_cluster, total_quota, transfer_ops
    c = cluster
    parts = start_txn_cluster(c, 3)
    before, _ = total_quota(c, parts)
    ok = requests.post(c.url("coord") + "/txn", json={"ops": transfer_ops(parts, "acct-0"), "protocol": "2pc"}, timeout=10).json()
    assert ok["outcome"] == "COMMIT"
    bad = requests.post(c.url("coord") + "/txn", json={"ops": [{"participant": "p1", "account": "low", "delta": -999},
                                                                  {"participant": "p2", "account": "low", "delta": 999}], "protocol": "2pc"}, timeout=10).json()
    assert bad["outcome"] == "ABORT"
    assert total_quota(c, parts)[0] == before, "money is neither created nor destroyed (atomicity)"


def test_2pc_blocks_but_3pc_terminates_after_coordinator_crash():
    from system.experiments.m05_transactions import indoubt_count, start_txn_cluster, transfer_ops
    res = {}
    for proto in ("2pc", "3pc"):
        with Cluster() as c:
            parts = start_txn_cluster(c, 3, indoubt=1.5)
            requests.post(c.url("coord") + "/fault_point", json={"crash_at": "after_prepare", "count": 1}, timeout=5)
            try:
                requests.post(c.url("coord") + "/txn", json={"ops": transfer_ops(parts, "acct-1"), "protocol": proto}, timeout=10)
            except requests.RequestException:
                pass
            time.sleep(6)                                     # participants try to terminate on their own meanwhile
            res[proto] = indoubt_count(c, parts)
    assert res["2pc"][0] > 0 and res["2pc"][1] > 0, f"2PC participants must stay blocked holding locks: {res}"
    assert res["3pc"][0] == 0, f"3PC participants must resolve without the coordinator: {res}"


# ---------------------------------------------------------------- replication / fault tolerance
def test_quorum_store_survives_crash_and_byzantine_with_four_replicas(cluster):
    from system.experiments.m07_fault_tolerance import start_replicas
    from system.replication.client import ReplicatedStore
    c = cluster
    urls = start_replicas(c, 4)
    s = ReplicatedStore(urls, w=3, r=3, vote=True, name="t")
    s.put("k", "v1")
    c.kill("r1")                                              # one crash ...
    s.put("k", "v2")
    assert s.get("k") == "v2"
    requests.post(urls["r2"] + "/fault", json={"byzantine": True})      # ... plus one Byzantine replica (N >= 3f+1 is satisfied for f=1 only for ONE of them at a time)
    c.restart("r1")
    wait_for(lambda: requests.get(urls["r1"] + "/kv/k", timeout=2).json().get("value") == "v2" or None, 15, what="anti-entropy catch-up")
    assert s.get("k") == "v2"


def test_delete_is_a_tombstone_and_is_not_resurrected_by_anti_entropy(cluster):
    """Regression (M9): a plain row delete was undone by anti-entropy from a peer that had not yet been cleaned."""
    from system.experiments.m07_fault_tolerance import start_replicas
    c = cluster
    urls = start_replicas(c, 3)
    v = int(time.time() * 1e6)
    for u in urls.values():
        requests.put(u + "/kv/ns/k", json={"value": 1, "version": v, "writer": "t"}, timeout=2)
    requests.delete(urls["r1"] + "/kv/ns/k", timeout=2)               # only r1 is cleaned; r2 and r3 still hold the key
    time.sleep(3.0)                                                  # several anti-entropy rounds
    assert all(requests.get(u + "/kv/ns/k", timeout=2).status_code == 404 for u in urls.values())
    live = requests.get(urls["r2"] + "/dump", params={"live": 1}, timeout=2).json()["entries"]
    assert not [e for e in live if e["key"] == "ns/k"]


# ---------------------------------------------------------------- DFS
def test_dfs_replication_failover_rereplication_and_checksum(cluster):
    from system.experiments.m12_storage import start_dfs
    c = cluster
    dfs = start_dfs(c, 5, 3, dead_after=3.0, base=23200)
    dfs.chunk_size = 64 * 1024
    data = bytes(range(256)) * 2000
    dfs.put("/a/file.bin", data)
    assert dfs.get("/a/file.bin") == data
    victim = requests.get(c.url("nn") + "/locate", params={"path": "/a/file.bin"}).json()["chunks"][0]["replicas"][0]["name"]
    c.kill(victim)
    assert dfs.get("/a/file.bin") == data                    # reads fail over immediately
    wait_for(lambda: (lambda s: s["under_replicated"] == 0 and not s["datanodes"][victim]["alive"])(requests.get(c.url("nn") + "/status").json()) or None, 40, what="re-replication")
    loc = requests.get(c.url("nn") + "/locate", params={"path": "/a/file.bin"}).json()["chunks"][0]
    requests.post(loc["replicas"][0]["url"] + f"/corrupt/{loc['chunk_id']}")
    assert dfs.get("/a/file.bin") == data and dfs.stats["failovers"] >= 1       # bit-rot detected by checksum, other replica used


def test_dfs_chunks_under_construction_are_not_garbage_collected(cluster):
    """Regression for a real bug found in Milestone 12: a heartbeat between chunk write and commit made the namenode order the
    datanode to delete 'orphan' chunks that were simply not committed yet."""
    from system.experiments.m12_storage import start_dfs
    c = cluster
    dfs = start_dfs(c, 3, 1, dead_after=10.0, base=23250)
    alloc = requests.post(c.url("nn") + "/allocate", json={"path": "/slow", "n_chunks": 1}, timeout=5).json()["chunks"][0]
    dn = alloc["datanodes"][0]
    requests.put(dn["url"] + f"/chunk/{alloc['chunk_id']}", data=b"x" * 1000, headers={"X-Forward": ""}, timeout=5)
    time.sleep(3.5)                                           # >= 3 heartbeats before the commit
    assert requests.get(dn["url"] + f"/chunk/{alloc['chunk_id']}", timeout=5).status_code == 200


# ---------------------------------------------------------------- naming
def test_hierarchical_naming_with_referrals_and_prefix_table(cluster):
    from system.naming.resolver import PrefixResolver
    c = cluster
    c.start("root", "system.naming.server", 23300, env={"NODE_NAME": "root", "ZONE": ""})
    c.start("zone", "system.naming.server", 23301, env={"NODE_NAME": "z", "ZONE": "ke.nai"})
    requests.post(c.url("root") + "/delegate", json={"zone": "ke.nai", "server": c.url("zone")})
    requests.post(c.url("zone") + "/register", json={"name": "ke.nai.NAI-01.agent", "location": "http://h1:80", "ttl": 0})
    cold = PrefixResolver(c.url("root"), use_cache=False)
    assert cold.resolve("ke.nai.NAI-01.agent") == "http://h1:80"
    assert cold.stats["hops"] >= 2                            # root answers with a referral, then the zone server answers
    warm = PrefixResolver(c.url("root"))
    warm.resolve("ke.nai.NAI-01.agent")
    h = warm.stats["hops"]
    warm.cache.clear()                                        # keep the learned prefix table, drop answer cache
    warm.resolve("ke.nai.NAI-01.agent")
    assert warm.stats["hops"] - h == 1, "a learned prefix table goes straight to the zone server"
    assert PrefixResolver(c.url("root")).resolve("ke.zzz.none") is None


# ---------------------------------------------------------------- RPC call semantics
def test_rpc_at_most_once_vs_at_least_once():
    from system.rpc.framework import RpcClient
    got = {}
    for sem in ("at-least-once", "at-most-once"):
        with Cluster() as c:
            c.start("srv", "system.rpc.rest_server", 23400, env={"SEMANTICS": sem})
            requests.post(c.url("srv") + "/rpc_admin", json={"drop_reply_prob": 0.4, "semantics": sem})
            cl = RpcClient(c.url("srv"), sem, timeout=1.0, retries=8)
            for _ in range(60):
                cl.increment(counter="x")
            got[sem] = requests.get(c.url("srv") + "/state", timeout=5).json()["counter"]
    assert got["at-most-once"] == 60
    assert got["at-least-once"] > 60


# ---------------------------------------------------------------- DSM
def test_dsm_strong_coherence_invalidation_and_atomic_add(cluster):
    from system.experiments.m11_rpc_dsm import start_dsm
    c = cluster
    u = start_dsm(c, "strong")
    requests.post(u[0] + "/write", json={"key": "x", "value": 1}, timeout=5)
    assert requests.post(u[1] + "/read", json={"key": "x"}, timeout=5).json()["value"] == 1
    requests.post(u[2] + "/write", json={"key": "x", "value": 2}, timeout=5)
    assert requests.get(u[1] + "/state/x", timeout=5).json()["state"] == "I", "writer must have invalidated the reader's copy"
    assert requests.post(u[1] + "/read", json={"key": "x"}, timeout=5).json()["value"] == 2
    for k in range(4):
        for _ in range(10):
            requests.post(u[k] + "/add", json={"key": "n", "delta": 1}, timeout=5)
    assert requests.post(u[0] + "/read", json={"key": "n"}, timeout=5).json()["value"] == 40


# ---------------------------------------------------------------- process management
def test_process_manager_reschedules_work_of_a_killed_node(cluster):
    from system.procman.controller import Controller
    c = cluster
    c.start("naming", "system.naming.server", 23500, env={"NODE_NAME": "procs", "ZONE": ""})
    nodes = {}
    for i in range(3):
        c.start(f"n{i}", "system.procman.node", 23510 + i, env={"NODE_NAME": f"n{i}", "NAMING_URL": c.url("naming"), "WORK_REPEAT": 8})
        nodes[f"n{i}"] = c.url(f"n{i}")
    ctl = Controller(nodes, c.url("naming"), policy="least-loaded")
    threading.Timer(1.5, lambda: c.kill("n1")).start()
    sites = ["NAI-01", "NAI-02", "MOM-01", "MOM-02", "KIS-01", "KIS-02"]
    res = ctl.run_job("t", [[s] for s in sites], timeout=150)
    assert res["workers"] == 6 and res["rows"] > 0
