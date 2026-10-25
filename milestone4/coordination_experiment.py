import csv, os, time
import requests

NODES = {
    "core-node": "http://127.0.0.1:5000",
    "edge-node-a": "http://127.0.0.1:5001",
    "edge-node-b": "http://127.0.0.1:5002",
}
ROUNDS = 20

os.makedirs("results", exist_ok=True)
rows = []
messages = 0
lamport_violations = 0
wall_violations = 0
start = time.time()

for rnd in range(1, ROUNDS + 1):
    for sender, sbase in NODES.items():
        s = requests.get(sbase + "/status").json()
        messages += 1
        sent_clock, sent_ts = s["lamport_clock"], s["timestamp"]
        for receiver, rbase in NODES.items():
            if receiver == sender:
                continue
            t0 = time.time()
            r = requests.post(rbase + "/sync", json={"lamport_clock": sent_clock}).json()
            delay = time.time() - t0
            messages += 1
            lamport_ok = r["lamport_clock"] > sent_clock
            wall_ok = r["timestamp"] >= sent_ts
            lamport_violations += 0 if lamport_ok else 1
            wall_violations += 0 if wall_ok else 1
            rows.append({
                "round": rnd, "sender": sender, "receiver": receiver,
                "sent_clock": sent_clock, "receiver_clock_after": r["lamport_clock"],
                "sync_delay_s": round(delay, 5),
                "lamport_order_ok": int(lamport_ok),
                "wallclock_order_ok": int(wall_ok),
            })
    print(f"Round {rnd:2d} done")

total = time.time() - start
delays = [x["sync_delay_s"] for x in rows]
with open("results/coordination.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)

n = len(NODES)
deliveries = len(rows)
print("\n--- Coordination Summary ---")
print(f"Nodes: {n}   Rounds: {ROUNDS}")
print(f"Total messages: {messages} (expected {ROUNDS * n * n} = rounds x n^2)")
print(f"Messages per round: {messages / ROUNDS:.0f}")
print(f"Total time: {total:.2f}s   Avg round time: {total / ROUNDS:.3f}s")
print(f"Avg sync delay: {sum(delays) / len(delays):.4f}s   Max: {max(delays):.4f}s")
print(f"Lamport ordering violations:    {lamport_violations} of {deliveries}")
print(f"Wall-clock ordering violations: {wall_violations} of {deliveries}")
print("Wrote results/coordination.csv")