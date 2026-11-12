import argparse
import csv
import json
import os
import time

from kafka import KafkaProducer

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DEFAULT_FILE = os.path.join(ROOT, "dataset", "network_metrics.csv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=DEFAULT_FILE)
    ap.add_argument("--topic", default="network-metrics")
    ap.add_argument("--bootstrap", default="127.0.0.1:9092")
    ap.add_argument("--node", default="edge-node-a")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=20,
        batch_size=64 * 1024,
        acks="all",
    )

    clock = 0
    sent = 0
    start = time.time()
    with open(args.file, newline="") as f:
        for row in csv.DictReader(f):
            clock += 1
            row["sent_ts"] = time.time()
            row["lamport_clock"] = clock
            row["sender"] = args.node
            producer.send(args.topic, key=row["node_id"], value=row)
            sent += 1
            if sent % 10_000 == 0:
                print(f"  sent {sent:,} rows ...")
            if args.limit and sent >= args.limit:
                break
    producer.flush()
    elapsed = time.time() - start
    print(f"Done. Sent {sent:,} messages to topic '{args.topic}' in {elapsed:.1f}s "
          f"({sent / elapsed:,.0f} msg/s). Final Lamport clock = {clock}")


if __name__ == "__main__":
    main()