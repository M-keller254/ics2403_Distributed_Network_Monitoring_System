"""
Milestone 5 -- Distributed Transactions: 2PC vs 3PC, ACID, failure behaviour, recovery.

Workload: "reallocate capacity": move Mbps quota between sites that live on different resource managers
(participants). A transaction is zero-sum, so  TOTAL QUOTA ACROSS ALL PARTICIPANTS  is the consistency invariant.

A  Latency / throughput vs protocol (2PC, 3PC), participants (2,3,4) and concurrency (1,4,16).
B  Atomicity & isolation: (i) no-transaction baseline when one resource manager is down (money is created/lost),
   (ii) the same failure with 2PC (clean abort), (iii) 16 clients hammering ONE hot account: no lost updates.
C  Coordinator crash at each protocol step, participants kept up: does the system resolve without the coordinator
   (non-blocking?) and how long are locks held? Then the coordinator restarts and recovery completes the transaction.
D  Participant crash while in-doubt: restart, recover locks, finish the transaction.
E  Availability timeline: a participant is down for 4 s of a 12 s run; share of transactions committed per second.

Hypotheses
  H1 3PC costs one extra round trip: latency ~ +1 RTT (+25-60 %) and lower throughput than 2PC when un-contended.
  H2 2PC blocks (locks held until the coordinator returns) when it crashes after collecting votes; 3PC resolves by itself.
  H3 With 2PC (and 3PC) the invariant holds in every failure scenario; without a protocol it breaks.
  H4 Neither protocol can commit while a participant is down (atomic commit needs everyone): availability dips equally.

Run:  python -m system.experiments.m05_transactions
"""
import statistics
import threading
import time

import requests

from system.common.metrics import summarize
from system.experiments.common import save_csv, save_json
from system.scripts.cluster import Cluster
from system.scripts.stack import wait_for

BASE = 26000
N_ACCTS = 16
INITIAL = 100000


def start_txn_cluster(c, n_parts=3, accounts=None, indoubt=1.5, prepare_delay=0):
    accounts = accounts or ",".join(f"acct-{i}:{INITIAL}" for i in range(N_ACCTS)) + f",hot:{INITIAL},low:10"
    urls = {f"p{i}": f"http://127.0.0.1:{BASE + i}" for i in range(1, n_parts + 1)}
    coord = BASE + 90
    for pid in urls:
        peers = ",".join(f"{k}={v}" for k, v in urls.items() if k != pid)
        c.start(pid, "system.txn.participant", int(urls[pid].rsplit(":", 1)[1]), env={
            "PART_ID": pid, "DATA_DIR": f"{c.workdir}/db", "ACCOUNTS": accounts, "COORD_URL": f"http://127.0.0.1:{coord}",
            "PEERS": peers, "INDOUBT_AFTER_S": indoubt, "LOCK_TIMEOUT_S": 1.0, "PREPARE_DELAY_MS": prepare_delay})
    c.start("coord", "system.txn.coordinator", coord, env={"DATA_DIR": f"{c.workdir}/coord", "PARTICIPANTS": ",".join(f"{k}={v}" for k, v in urls.items()),
                                                            "PHASE_TIMEOUT_S": 2.0})
    return list(urls)


def transfer_ops(parts, acct, amount=5):
    """p1 pays, every other participant receives `amount` (zero-sum)."""
    ops = [{"participant": parts[0], "account": acct, "delta": -amount * (len(parts) - 1)}]
    ops += [{"participant": p, "account": acct, "delta": amount} for p in parts[1:]]
    return ops


def total_quota(c, parts):
    tot, per = 0, {}
    for p in parts:
        if c.alive(p):
            b = c.get(p, "/balance")
            per[p] = b["total"]
            tot += b["total"]
    return tot, per


def run_load(c, parts, protocol, clients, per_client, hot=False, amount=5, prepare_mode=None):
    lat, outcomes, lock = [], {"COMMIT": 0, "ABORT": 0, "error": 0}, threading.Lock()
    committed_by_acct = {}

    def worker(i):
        s = requests.Session()
        acct = "hot" if hot else f"acct-{i % N_ACCTS}"
        for _ in range(per_client):
            t0 = time.perf_counter()
            try:
                r = s.post(c.url("coord") + "/txn", json={"ops": transfer_ops(parts, acct, amount), "protocol": protocol, "prepare_mode": prepare_mode}, timeout=15).json()
                dt = time.perf_counter() - t0
                with lock:
                    outcomes[r["outcome"]] += 1
                    if r["outcome"] == "COMMIT":
                        lat.append(dt)
                        committed_by_acct[acct] = committed_by_acct.get(acct, 0) + 1
            except Exception:
                with lock:
                    outcomes["error"] += 1
    t0 = time.perf_counter()
    ths = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    return lat, outcomes, time.perf_counter() - t0, committed_by_acct


def part_a():
    rows = []
    for protocol in ("2pc", "3pc"):
        for n_parts in (2, 3, 4):
            for clients in (1, 4, 16):
                reps = []
                for rep in range(3):
                    with Cluster() as c:
                        parts = start_txn_cluster(c, n_parts)
                        before, _ = total_quota(c, parts)
                        lat, out, wall, _ = run_load(c, parts, protocol, clients, 120 // clients if clients > 1 else 60)
                        after, _ = total_quota(c, parts)
                        reps.append({"tps": len(lat) / wall, "lat": lat, "aborts": out["ABORT"] + out["error"], "ok": before == after})
                allat = [x for r in reps for x in r["lat"]]
                s = summarize(allat, 1000)
                row = {"protocol": protocol, "participants": n_parts, "clients": clients,
                       "throughput_tps": round(statistics.mean(r["tps"] for r in reps), 1),
                       "lat_mean_ms": s["mean"], "lat_p95_ms": s["p95"], "jitter_ms": s["jitter"],
                       "aborted_or_failed": sum(r["aborts"] for r in reps), "invariant_held": all(r["ok"] for r in reps)}
                rows.append(row)
                print(f"  A {protocol} parts={n_parts} clients={clients:2}: {row['throughput_tps']:>6} tx/s  lat mean {row['lat_mean_ms']} ms p95 {row['lat_p95_ms']} ms  aborts {row['aborted_or_failed']}  invariant {row['invariant_held']}")
    save_csv("m05_latency_throughput.csv", rows)
    return rows


def part_b():
    out = []
    # (i) no protocol: independent writes, participant p2 is down when the credit is sent
    with Cluster() as c:
        parts = start_txn_cluster(c, 2)
        before, _ = total_quota(c, parts)
        c.kill("p2")
        for k in range(20):                               # naive client: debit p1, then credit p2, no coordination
            tid = f"naive-{k}"
            requests.post(c.url("p1") + "/prepare", json={"txid": tid, "ops": [{"account": "acct-0", "delta": -10}], "protocol": "naive"})
            requests.post(c.url("p1") + "/commit", json={"txid": tid})
            try:
                requests.post(c.url("p2") + "/prepare", json={"txid": tid, "ops": [{"account": "acct-0", "delta": 10}]}, timeout=0.3)
            except requests.RequestException:
                pass                                       # credit lost
        c.restart("p2")
        after, _ = total_quota(c, ["p1", "p2"])
        out.append({"case": "no transaction protocol, p2 down", "ops": 20, "total_before": before, "total_after": after, "quota_lost": before - after, "invariant_held": before == after})
    # (ii) same failure under 2PC
    for protocol in ("2pc", "3pc"):
        with Cluster() as c:
            parts = start_txn_cluster(c, 2)
            before, _ = total_quota(c, parts)
            c.kill("p2")
            res = [requests.post(c.url("coord") + "/txn", json={"ops": transfer_ops(parts, "acct-0"), "protocol": protocol}, timeout=15).json() for _ in range(20)]
            c.restart("p2")
            time.sleep(3)
            after, per = total_quota(c, parts)
            st = c.get("p1", "/state")
            out.append({"case": f"{protocol}, p2 down", "ops": 20, "committed": sum(r["outcome"] == "COMMIT" for r in res), "aborted": sum(r["outcome"] == "ABORT" for r in res),
                        "total_before": before, "total_after": after, "quota_lost": before - after, "invariant_held": before == after, "locks_left": len(st["locks_held"])})
    # (iii) isolation: 16 clients, one hot account, final balance must equal initial minus committed deltas.
    #       parallel prepare -> cross-participant lock-order inversion = distributed deadlock broken only by lock timeouts;
    #       ordered prepare  -> a global lock order prevents it.
    for protocol, mode in (("2pc", "parallel"), ("2pc", "ordered"), ("3pc", "ordered")):
        with Cluster() as c:
            parts = start_txn_cluster(c, 2)
            lat, o, wall, comm = run_load(c, parts, protocol, 16, 15, hot=True, amount=7, prepare_mode=mode)
            time.sleep(1)
            b1, b2 = c.get("p1", "/balance")["accounts"]["hot"], c.get("p2", "/balance")["accounts"]["hot"]
            n = o["COMMIT"]
            out.append({"case": f"{protocol}, {mode} prepare, 16 clients on one hot account", "ops": 16 * 15, "committed": n, "aborted_lock_timeout": o["ABORT"],
                        "p1_expected": INITIAL - 7 * n, "p1_actual": b1, "p2_expected": INITIAL + 7 * n, "p2_actual": b2,
                        "invariant_held": (b1 == INITIAL - 7 * n and b2 == INITIAL + 7 * n), "throughput_tps": round(n / wall, 1),
                        "lat_mean_ms": summarize(lat, 1000).get("mean"), "lock_timeouts": sum(c.get(p, "/state")["stats"]["lock_timeouts"] for p in parts)})
    for r in out:
        print("  B", {k: r[k] for k in r})
    save_csv("m05_atomicity_isolation.csv", out)
    return out


def indoubt_count(c, parts):
    n, locks = 0, 0
    for p in parts:
        if c.alive(p):
            s = c.get(p, "/state")
            n += len(s["in_doubt"])
            locks += len(s["locks_held"])
    return n, locks


def part_c():
    out = []
    scenarios = [("2pc", "after_prepare"), ("2pc", "after_decision_log"), ("3pc", "after_prepare"), ("3pc", "mid_precommit"),
                 ("3pc", "after_precommit"), ("3pc", "after_decision_log")]
    for protocol, point in scenarios:
        for rep in range(2):
            with Cluster() as c:
                parts = start_txn_cluster(c, 3)
                before, _ = total_quota(c, parts)
                requests.post(c.url("coord") + "/fault_point", json={"crash_at": point})
                tx_res = {}

                def go():
                    try:
                        tx_res["r"] = requests.post(c.url("coord") + "/txn", json={"ops": transfer_ops(parts, "acct-1"), "protocol": protocol}, timeout=10).json()
                    except Exception as e:
                        tx_res["err"] = type(e).__name__
                th = threading.Thread(target=go)
                th.start()
                th.join()
                t_crash = time.perf_counter()
                # coordinator is dead. do the participants resolve by themselves?
                resolved_alone, blocked_s, locks_peak = False, None, 0
                while time.perf_counter() - t_crash < 9:
                    n, locks = indoubt_count(c, parts)
                    locks_peak = max(locks_peak, locks)
                    if n == 0:
                        resolved_alone, blocked_s = True, time.perf_counter() - t_crash
                        break
                    time.sleep(0.1)
                states_alone = [c.get(p, "/state")["log_counts"] for p in parts]
                # restart coordinator -> recovery
                t_r = time.perf_counter()
                c.restart("coord")
                rec_s = None
                while time.perf_counter() - t_r < 15:
                    n, locks = indoubt_count(c, parts)
                    if n == 0:
                        rec_s = time.perf_counter() - t_r
                        break
                    time.sleep(0.1)
                time.sleep(0.5)
                after, per = total_quota(c, parts)
                st = [c.get(p, "/txstate/x")["state"] for p in parts]      # placeholder to ensure alive
                # outcome per participant for this tx
                outcomes = set()
                for p in parts:
                    cnt = c.get(p, "/state")["log_counts"]
                    outcomes.add(("C" if cnt.get("COMMITTED") else "") + ("A" if cnt.get("ABORTED") else ""))
                row = {"protocol": protocol, "coordinator_crash_point": point, "rep": rep + 1,
                       "resolved_without_coordinator": resolved_alone, "blocked_s_while_coordinator_down": round(blocked_s, 2) if blocked_s is not None else ">9 (still blocked)",
                       "locks_held_peak": locks_peak, "recovery_after_restart_s": round(rec_s, 2) if rec_s is not None else None,
                       "participants_agree": len(outcomes) == 1, "final_outcome": "/".join(sorted(outcomes)),
                       "invariant_held": before == after}
                out.append(row)
                print("  C", {k: row[k] for k in row})
    save_csv("m05_coordinator_crash.csv", out)
    return out


def part_d():
    """Participant p2 misses the decision (omission) and then crashes while in doubt.
       (i) coordinator alive  -> its redelivery loop heals p2 after the restart;
       (ii) coordinator ALSO crashed -> p2 must recover from its own log: locks are re-taken, then the peers (who committed) tell it."""
    out = []
    for scenario in ("p2 crash, coordinator alive", "p2 AND coordinator crash"):
        for protocol in ("2pc", "3pc"):
            with Cluster() as c:
                parts = start_txn_cluster(c, 3)
                before, _ = total_quota(c, parts)
                requests.post(c.url("coord") + "/fault_point", json={"skip_phase2_to": ["p2"], "count": 1})    # p2 never hears the decision
                r = requests.post(c.url("coord") + "/txn", json={"ops": transfer_ops(parts, "acct-2"), "protocol": protocol}, timeout=10).json()
                c.kill("p2")
                if scenario.endswith("coordinator crash"):
                    c.kill("coord")
                time.sleep(1.0)
                t0 = time.perf_counter()
                c.restart("p2")
                st0 = c.get("p2", "/state")
                time.sleep(1.2)                                       # let recover_locks() run (1 s after start)
                st_mid = c.get("p2", "/state")
                wait_for(lambda: not c.get("p2", "/state")["in_doubt"], timeout=25, what="p2 recovery")
                rec = time.perf_counter() - t0
                st = c.get("p2", "/state")
                after, per = total_quota(c, parts)
                out.append({"scenario": scenario, "protocol": protocol, "decision": r["outcome"], "in_doubt_after_restart": len(st0["in_doubt"]),
                            "locks_restored_after_restart": len(st_mid["locks_held"]), "recovery_s": round(rec, 2),
                            "resolved_by_coordinator": st["stats"]["resolved_by_coordinator"], "resolved_by_peers": st["stats"]["resolved_by_peers"],
                            "p2_state_final": "COMMITTED" if st["log_counts"].get("COMMITTED") else "other", "invariant_held": before == after})
                print("  D", out[-1])
    save_csv("m05_participant_recovery.csv", out)
    return out


def part_e():
    out = []
    for protocol in ("2pc", "3pc"):
        with Cluster() as c:
            parts = start_txn_cluster(c, 3)
            stop, events = threading.Event(), []

            def client(i):
                s = requests.Session()
                while not stop.is_set():
                    t = time.perf_counter()
                    try:
                        r = s.post(c.url("coord") + "/txn", json={"ops": transfer_ops(parts, f"acct-{i}"), "protocol": protocol}, timeout=8).json()
                        events.append((t, r["outcome"] == "COMMIT"))
                    except Exception:
                        events.append((t, False))
                    time.sleep(0.01)
            t_start = time.perf_counter()
            ths = [threading.Thread(target=client, args=(i,)) for i in range(4)]
            [t.start() for t in ths]
            time.sleep(4); c.kill("p3"); t_down = time.perf_counter() - t_start
            time.sleep(4); c.restart("p3"); t_up = time.perf_counter() - t_start
            time.sleep(4); stop.set(); [t.join() for t in ths]
            for sec in range(12):
                w = [ok for t, ok in events if sec <= t - t_start < sec + 1]
                out.append({"protocol": protocol, "second": sec, "attempts": len(w), "committed": sum(w), "success_rate": round(sum(w) / len(w), 2) if w else None,
                            "p3_down": t_down <= sec + 0.5 < t_up})
            up = [e for e in events if not (t_down <= e[0] - t_start < t_up)]
            dn = [e for e in events if (t_down <= e[0] - t_start < t_up)]
            print(f"  E {protocol}: success while all up {100 * sum(ok for _, ok in up) / len(up):.1f}% | while p3 down {100 * sum(ok for _, ok in dn) / max(len(dn), 1):.1f}%  "
                  f"(down {t_down:.1f}s..{t_up:.1f}s, run availability {100 * sum(ok for _, ok in events) / len(events):.1f}%)")
    save_csv("m05_availability_timeline.csv", out)
    return out


def main(parts="abcde"):
    import json as _j, os as _o
    from system.experiments.common import RESULTS
    path = _o.path.join(RESULTS, "m05_summary.json")
    summary = _j.load(open(path)) if _o.path.exists(path) else {}
    if "a" in parts:
        print("A latency/throughput"); summary["latency_throughput"] = part_a()
    if "b" in parts:
        print("B atomicity & isolation"); summary["atomicity_isolation"] = part_b()
    if "c" in parts:
        print("C coordinator crash points"); summary["coordinator_crash"] = part_c()
    if "d" in parts:
        print("D participant crash + recovery"); summary["participant_recovery"] = part_d()
    if "e" in parts:
        print("E availability timeline"); part_e()
    save_json("m05_summary.json", summary)


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else "abcde")
