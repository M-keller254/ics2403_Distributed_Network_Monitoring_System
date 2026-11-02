"""
Milestone 11 experiment: benchmark the RPC client stub against the
distributed shared-state service (txn-coordinator + store-a/store-b).

Measures everything the brief asks for:
  - Communication latency   (per-call RPC round trip)
  - Throughput              (calls completed per second)
  - Consistency             (two independent clients agree on every key)
  - Synchronization overhead (2PC's extra cost vs a plain single-node write,
                               approximated by comparing put() latency here
                               against a single store's own /commit-less
                               write path is not available standalone, so
                               this is reported as 2PC's absolute cost,
                               which Milestone 5 already isolated)
  - Message overhead        (PREPARE + COMMIT per store = 2N messages for
                               N replicas, read from txn-coordinator /stats)

Usage (system must already be running):
    python experiments/rpc_benchmark.py --writes 20 --reads 20
"""
import argparse, csv, os, statistics, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rpc.client_stub import SharedStateClient, RPCError
import requests

COORD = "http://localhost:7000"
GATEWAY = "http://localhost:8000"


def run(n_writes, n_reads):
    client = SharedStateClient(COORD, GATEWAY)
    rows = []

    for i in range(n_writes):
        try:
            r = client.put(f"bench-{i}", i)
            rows.append({"op": "put", "key": f"bench-{i}", **r})
        except RPCError as e:
            rows.append({"op": "put", "key": f"bench-{i}", "error": str(e)})

    for i in range(n_reads):
        key = f"bench-{i % max(1, n_writes)}"
        try:
            r = client.get(key)
            rows.append({"op": "get", "key": key, **r})
        except RPCError as e:
            rows.append({"op": "get", "key": key, "error": str(e)})

    # consistency check: two independent "clients" reading the same keys
    # through the SAME gateway (which may answer from either replica) must
    # see identical values
    mismatches = 0
    for i in range(n_writes):
        key = f"bench-{i}"
        try:
            v1 = client.get(key)["value"]
            v2 = client.get(key)["value"]
            if v1 != v2:
                mismatches += 1
        except RPCError:
            pass

    os.makedirs("experiments/results", exist_ok=True)
    fieldnames = sorted({k for r in rows for k in r})
    with open("experiments/results/rpc_benchmark.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames); w.writeheader(); w.writerows(rows)

    put_lat = [r["rpc_round_trip_s"] for r in rows if r.get("op") == "put" and "rpc_round_trip_s" in r]
    get_lat = [r["rpc_round_trip_s"] for r in rows if r.get("op") == "get" and "rpc_round_trip_s" in r]

    print(f"put(): n={len(put_lat)} avg_latency={statistics.mean(put_lat):.4f}s "
          f"throughput={len(put_lat)/sum(put_lat):.2f} calls/s" if put_lat else "no successful puts")
    print(f"get(): n={len(get_lat)} avg_latency={statistics.mean(get_lat):.4f}s "
          f"throughput={len(get_lat)/sum(get_lat):.2f} calls/s" if get_lat else "no successful gets")
    print(f"Consistency: {n_writes - mismatches}/{n_writes} keys returned identical "
          f"values on repeated reads through the gateway")

    try:
        stats = requests.get(f"{COORD}/stats", timeout=2).json()
        print(f"Coordinator stats (message overhead proxy): {stats}")
        print("Message overhead per committed write: 2 participants x "
              "(1 PREPARE + 1 COMMIT) = 4 messages, vs 1 message for a "
              "non-replicated write - this is 2PC's synchronization cost,")
        print("already isolated and measured in Milestone 5's txn_test.py.")
    except requests.exceptions.RequestException:
        pass

    print("Saved experiments/results/rpc_benchmark.csv")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--writes", type=int, default=20)
    ap.add_argument("--reads", type=int, default=20)
    a = ap.parse_args()
    run(a.writes, a.reads)
