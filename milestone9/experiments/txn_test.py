"""
Milestone 5 experiment: transaction latency, throughput and failure
behaviour under Two-Phase Commit.

Runs N writes through the coordinator, then puts store-b into
fail_mode to force aborts, and finally checks that store-a and
store-b hold IDENTICAL committed data (no partial commits survived).

Usage: python experiments/txn_test.py --n 20
"""
import argparse, time, csv, os, requests

COORD = "http://localhost:7000"
STORE_A = "http://localhost:7001"
STORE_B = "http://localhost:7002"


def run(n):
    rows = []
    t0 = time.time()
    for i in range(n):
        r = requests.post(f"{COORD}/write", json={"key": f"k{i}", "value": i}, timeout=5)
        rows.append({"txn": i, "decision": r.json()["decision"], "latency_s": r.json()["latency_s"],
                     "phase": "normal"})
    normal_throughput = n / (time.time() - t0)

    print(f"Normal phase: {n} txns, throughput={normal_throughput:.2f} txn/s")

    print("Simulating store-b failure (omission failure) ...")
    requests.post(f"{STORE_B}/fail_mode", json={"down": True})
    t0 = time.time()
    for i in range(n, n + 5):
        r = requests.post(f"{COORD}/write", json={"key": f"k{i}", "value": i}, timeout=5)
        rows.append({"txn": i, "decision": r.json()["decision"], "latency_s": r.json()["latency_s"],
                     "phase": "store_b_down"})
    requests.post(f"{STORE_B}/fail_mode", json={"down": False})

    os.makedirs("experiments/results", exist_ok=True)
    with open("experiments/results/txn_test.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["txn", "decision", "latency_s", "phase"])
        w.writeheader(); w.writerows(rows)

    a = requests.get(f"{STORE_A}/data").json()["committed"]
    b = requests.get(f"{STORE_B}/data").json()["committed"]
    consistent = a == b
    print(f"Consistency check: store-a and store-b identical = {consistent} "
          f"({len(a)} vs {len(b)} committed keys)")
    aborted = sum(1 for r in rows if r["decision"] == "ABORT")
    print(f"Aborted transactions (expected during store_b_down phase): {aborted}")
    print("Saved experiments/results/txn_test.csv")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    run(ap.parse_args().n)
