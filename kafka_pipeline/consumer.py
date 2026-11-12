import argparse
import csv
import json
import os
import statistics
import time

from kafka import KafkaConsumer

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RESULTS = os.path.join(ROOT, "results")

FIELDS = ["row_id", "timestamp", "node_id", "cpu_percent", "memory_percent",
          "latency_ms", "packet_loss_percent", "bytes_in", "bytes_out", "status",
          "sender", "producer_lamport", "core_lamport", "kafka_delay_ms"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="network-metrics")
    ap.add_argument("--bootstrap", default="127.0.0.1:9092")
    ap.add_argument("--group", default="core-node")
    ap.add_argument("--idle", type=int, default=15)
    args = ap.parse_args()

    os.makedirs(RESULTS, exist_ok=True)
    out_path = os.path.join(RESULTS, "kafka_consumed.csv")

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=args.idle * 1000,
    )

    clock = 0
    count = 0
    delays = []
    first = last = None
    print(f"Consumer waiting on topic '{args.topic}' (stops after {args.idle}s idle) ...")
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for msg in consumer:
            m = msg.value
            now = time.time()
            first = first or now
            last = now
            clock = max(clock, int(m["lamport_clock"])) + 1
            delay_ms = max(0.0, (now - float(m["sent_ts"])) * 1000)
            delays.append(delay_ms)
            w.writerow({
                "row_id": m["row_id"], "timestamp": m["timestamp"], "node_id": m["node_id"],
                "cpu_percent": m["cpu_percent"], "memory_percent": m["memory_percent"],
                "latency_ms": m["latency_ms"], "packet_loss_percent": m["packet_loss_percent"],
                "bytes_in": m["bytes_in"], "bytes_out": m["bytes_out"], "status": m["status"],
                "sender": m["sender"], "producer_lamport": m["lamport_clock"],
                "core_lamport": clock, "kafka_delay_ms": round(delay_ms, 2),
            })
            count += 1
            if count % 10_000 == 0:
                print(f"  consumed {count:,} rows ...")

    if count == 0:
        print("No messages received. Is the producer finished and the topic name right?")
        return
    span = max(last - first, 1e-9)
    jitter = statistics.pstdev(delays) if len(delays) > 1 else 0.0
    summary = (f"rows_consumed={count}\n"
               f"throughput_msg_per_s={count / span:.0f}\n"
               f"mean_kafka_delay_ms={statistics.mean(delays):.2f}\n"
               f"jitter_ms(stdev)={jitter:.2f}\n"
               f"max_kafka_delay_ms={max(delays):.2f}\n"
               f"final_core_lamport_clock={clock}\n")
    with open(os.path.join(RESULTS, "kafka_summary.txt"), "w") as f:
        f.write(summary)
    print("\n" + summary + f"Saved {out_path}")


if __name__ == "__main__":
    main()