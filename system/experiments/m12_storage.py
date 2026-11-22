"""
Milestone 12 (part 1): the distributed-storage tier of the final architecture.

    Users/Devices -> Edge -> Telecom Core -> Cloud -> DISTRIBUTED STORAGE      (this file measures the last two arrows)

Parts
  A  HDFS-style file system: write/read throughput vs chunk size and replication factor
  B  datanode failures: reads keep working, namenode re-replicates, how long until fully replicated again; when does data get lost
  C  bit-rot: a corrupted replica is detected by its checksum and bypassed
  D  end to end: the full 120,000-row dataset goes edge -> Kafka -> core -> Kafka(core.processed) -> archiver -> DFS + object store,
     then the archiver is crashed mid-stream and restarted; storage must still contain exactly 120,000 distinct record_ids.

Hypotheses
  H1 write throughput falls roughly as 1/replication (the pipeline writes every byte R times); reads do not.
  H2 with R=3 and 5 datanodes, one datanode failure loses nothing, is invisible to readers, and heals in ~DEAD_AFTER_S + copy time;
     two simultaneous failures lose data only if both held the same chunk's only remaining replicas (R=2 is not enough, R=3 is).
  H3 every corrupted replica is detected (0 corrupt bytes returned to the application).
  H4 the archive path is at-least-once, so an archiver crash creates duplicate *rows* across overlapping objects but never loses a row; after
     de-duplication on record_id the stored set equals the dataset.
"""
import hashlib
import os
import statistics
import threading
import time

import requests

from system.edge.replayer import run_replay
from system.experiments.common import dataset, save_csv, save_json
from system.scripts.cluster import Cluster
from system.scripts.stack import edge_core_cloud, wait_for
from system.storage.dfs_client import DfsClient

BASE = 27000


def start_dfs(c, n_dn=5, replication=3, dead_after=3.0, base=BASE):
    c.start("nn", "system.storage.namenode", base, env={"REPLICATION": replication, "META_DIR": f"{c.workdir}/nn", "DEAD_AFTER_S": dead_after})
    for i in range(1, n_dn + 1):
        c.start(f"dn{i}", "system.storage.datanode", base + i,
                env={"DN_NAME": f"dn{i}", "DATA_DIR": f"{c.workdir}/dn{i}", "NAMENODE_URL": c.url("nn"), "PUBLIC_URL": f"http://127.0.0.1:{base + i}"})
    wait_for(lambda: len([d for d in requests.get(c.url("nn") + "/status").json()["datanodes"].values() if d["alive"]]) >= n_dn, 20, what="datanodes registered")
    return DfsClient(c.url("nn"))


def part_a(reps=3, size_mb=24):
    out = []
    data = os.urandom(size_mb << 20)
    for chunk_kb in (256, 1024, 4096):
        for repl in (1, 2, 3):
            ws, rs = [], []
            for rep in range(1, reps + 1):
                with Cluster() as c:
                    start_dfs(c, 5, repl, dead_after=10.0)      # generous: heartbeats compete with bulk transfers for 2 CPUs
                    dfs = DfsClient(c.url("nn"), chunk_size=chunk_kb << 10)
                    t0 = time.perf_counter()
                    dfs.put("/bench/file.bin", data)
                    w = time.perf_counter() - t0
                    t0 = time.perf_counter()
                    back = dfs.get("/bench/file.bin")
                    r = time.perf_counter() - t0
                    assert hashlib.sha256(back).digest() == hashlib.sha256(data).digest()
                    ws.append(size_mb / w)
                    rs.append(size_mb / r)
            row = {"chunk_kb": chunk_kb, "replication": repl, "size_mb": size_mb, "write_mb_s": round(statistics.mean(ws), 1), "write_sd": round(statistics.pstdev(ws), 1),
                   "read_mb_s": round(statistics.mean(rs), 1), "read_sd": round(statistics.pstdev(rs), 1), "reps": reps,
                   "physical_mb_written": size_mb * repl}
            out.append(row)
            print(f"  A chunk {chunk_kb:5} KB  R={repl}: write {row['write_mb_s']:>6} MB/s (±{row['write_sd']})  read {row['read_mb_s']:>6} MB/s (±{row['read_sd']})")
    save_csv("m12_dfs_throughput.csv", out)
    return out


def heal_time(c, victims, timeout=60):
    """Returns (detect_s, heal_s, status): time until the namenode declares every victim dead, then until no chunk is under-replicated.
    Both are measured from the moment this function is called (= right after the last kill)."""
    t0 = time.time()
    detect = None
    while time.time() - t0 < timeout:
        s = requests.get(c.url("nn") + "/status").json()
        if detect is None and all(not s["datanodes"][v]["alive"] for v in victims):
            detect = round(time.time() - t0, 2)
        if detect is not None and s["under_replicated"] == 0:
            return detect, round(time.time() - t0, 2), s
        time.sleep(0.2)
    return detect, None, requests.get(c.url("nn") + "/status").json()


def part_b(reps=2):
    out = []
    data = os.urandom(8 << 20)
    for repl in (2, 3):
        for kills in (1, 2):
            for rep in range(1, reps + 1):
                with Cluster() as c:
                    dfs = start_dfs(c, 5, repl, dead_after=3.0)
                    dfs.chunk_size = 512 << 10
                    dfs.put("/f", data)
                    n_chunks = len(requests.get(c.url("nn") + "/locate", params={"path": "/f"}).json()["chunks"])
                    victims = ["dn1", "dn2"][:kills]
                    heal = []
                    read_ok = 0
                    errors = 0
                    t_first_kill = time.time()
                    for v in victims:                      # sequential failures, each given time to be detected and healed (the realistic case)
                        c.kill(v)
                        try:
                            ok = dfs.get("/f") == data
                        except IOError:
                            ok = False
                        read_ok += int(ok)
                        errors += int(not ok)
                        if kills == 2:
                            time.sleep(0.0)              # second failure comes immediately: before any healing -> worst case
                    det, h, st = heal_time(c, victims, 45)
                    try:
                        final_ok = dfs.get("/f") == data
                    except IOError:
                        final_ok = False
                    row = {"replication": repl, "datanodes_killed_back_to_back": kills, "rep": rep, "chunks": n_chunks, "reads_after_failure_ok": read_ok,
                           "reads_after_failure_failed": errors, "chunks_lost": st["lost"], "re_replications": st["re_replications"], "seconds_until_namenode_detects": det, "seconds_until_fully_replicated": h,
                           "final_read_correct": final_ok, "client_failovers": dfs.stats["failovers"]}
                    out.append(row)
                    print(f"  B R={repl} kill {kills} back-to-back rep{rep}: lost chunks {st['lost']}, re-replicated {st['re_replications']}, detected after {det}s, healed after {h}s, final read ok={final_ok}")
    save_csv("m12_dfs_failures.csv", out)
    return out


def part_c(trials=20):
    with Cluster() as c:
        dfs = start_dfs(c, 5, 3)
        dfs.chunk_size = 256 << 10
        data = os.urandom(2 << 20)
        dfs.put("/rot", data)
        loc = requests.get(c.url("nn") + "/locate", params={"path": "/rot"}).json()
        wrong = detected = 0
        rows = []
        for i in range(trials):
            ch = loc["chunks"][i % len(loc["chunks"])]
            rep = ch["replicas"][0]
            requests.post(rep["url"] + f"/corrupt/{ch['chunk_id']}")
            before = dfs.stats["failovers"]
            got = dfs.get("/rot")
            if got != data:
                wrong += 1
            detected += int(dfs.stats["failovers"] > before)
        rows.append({"trials": trials, "corruptions_injected": trials, "reads_that_detected_and_bypassed": detected, "reads_returning_corrupt_data": wrong})
        print(f"  C {trials} corrupted replicas: detected+bypassed {detected}, corrupt data returned {wrong}")
    save_csv("m12_dfs_checksum.csv", rows)
    return rows


def part_d(reps=1):
    rows = []
    data = dataset()
    for scenario in ("archiver runs to completion", "archiver crashed at ~50% and restarted"):
        for rep in range(1, reps + 1):
            with Cluster() as c:
                dfs = start_dfs(c, 4, 3, base=BASE + 100)
                names = edge_core_cloud(c, base=BASE + 200, transport="kafka", core_env={"HEARTBEAT_INTERVAL": 0.5, "ARCHIVE_TOPIC": "core.processed"})
                arch_env = {"STORAGE": "both", "NAMENODE_URL": c.url("nn"), "ARCHIVE_TOPIC": "core.processed"}
                c.start("archiver", "system.cloud.archiver", BASE + 290, env=arch_env)
                t0 = time.perf_counter()
                crashed_at = None
                if scenario.startswith("archiver crashed"):
                    def chaos():
                        nonlocal crashed_at
                        while True:
                            try:
                                if requests.get(c.url("archiver") + "/stats", timeout=1).json()["rows"] >= len(data) // 2:
                                    c.kill("archiver")
                                    crashed_at = time.perf_counter() - t0
                                    time.sleep(1.0)
                                    c.restart("archiver")
                                    return
                            except Exception:
                                pass
                            time.sleep(0.05)
                    threading.Thread(target=chaos, daemon=True).start()
                run_replay(c.url("gateway"), rows=data, chunk=2000, quiet=True)
                sent = time.perf_counter() - t0
                wait_for(lambda: sum(c.get(n, "/readings/count")["rows_in_db"] for n in names["cores"] if c.alive(n)) >= len(data) or None, 240, what="core drain")
                core_done = time.perf_counter() - t0

                def archived():
                    try:
                        v = requests.get(c.url("archiver") + "/verify", timeout=60).json()
                        return v["object"]["distinct_record_ids"] >= len(data) and v["dfs"]["distinct_record_ids"] >= len(data)
                    except Exception:
                        return False
                wait_for(archived, 300, interval=2, what="archive complete")
                done = time.perf_counter() - t0
                v = requests.get(c.url("archiver") + "/verify", timeout=120).json()
                st = requests.get(c.url("archiver") + "/stats").json()
                stat = requests.get(c.url("nn") + "/status").json()
                row = {"scenario": scenario, "rep": rep, "rows_in": len(data), "distinct_in_object_store": v["object"]["distinct_record_ids"], "distinct_in_dfs": v["dfs"]["distinct_record_ids"],
                       "objects_object_store": v["object"]["objects"], "objects_dfs": v["dfs"]["objects"], "rows_lost": len(data) - min(v["object"]["distinct_record_ids"], v["dfs"]["distinct_record_ids"]),
                       "sender_s": round(sent, 1), "core_complete_s": round(core_done, 1), "archive_complete_s": round(done, 1), "archive_lag_after_core_s": round(done - core_done, 1),
                       "archiver_crashed_at_s": round(crashed_at, 1) if crashed_at else None, "dfs_chunks": stat["chunks"], "dfs_under_replicated": stat["under_replicated"],
                       "archiver_rows_processed_last_incarnation": st["rows"], "write_errors": st["write_errors"]}
                rows.append(row)
                print(f"  D {scenario:42}: object store {row['distinct_in_object_store']}/{len(data)}  DFS {row['distinct_in_dfs']}/{len(data)}  lost {row['rows_lost']}  "
                      f"archive done {row['archive_complete_s']}s (lag {row['archive_lag_after_core_s']}s)  crash at {row['archiver_crashed_at_s']}")
    save_csv("m12_archive_end_to_end.csv", rows)
    return rows


def main(parts="abcd"):
    res = {}
    if "a" in parts:
        print("Part A: DFS throughput")
        res["A"] = part_a()
    if "b" in parts:
        print("Part B: datanode failures")
        res["B"] = part_b()
    if "c" in parts:
        print("Part C: checksums")
        res["C"] = part_c()
    if "d" in parts:
        print("Part D: end-to-end archive")
        res["D"] = part_d()
    prev = {}
    import json
    p = os.path.join(os.path.dirname(__file__), "..", "..", "results", "m12_storage_summary.json")
    if os.path.exists(p):
        prev = json.load(open(p))
    prev.update(res)
    save_json("m12_storage_summary.json", prev)


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else "abcd")
