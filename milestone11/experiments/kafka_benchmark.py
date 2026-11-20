"""Measure Kafka broker acknowledgements and event delivery for Milestone 11.

Run while the M11 Compose stack is up:
    python experiments/kafka_benchmark.py --messages 100

The script creates a run-specific temporary topic so earlier benchmark
records cannot be mistaken for this run's observations.
"""
import argparse
import csv
import json
import statistics
import time
import uuid
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer


def run(messages, bootstrap):
    run_id = uuid.uuid4().hex[:12]
    topic = f"m11-benchmark-{run_id}"
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )
    sent = {}
    send_started = time.perf_counter()
    producer_started_wall = time.time()
    try:
        for seq in range(messages):
            event_id = f"{run_id}-{seq}"
            produced_at = time.time()
            payload = {"run_id": run_id, "event_id": event_id,
                       "sequence": seq, "produced_at": produced_at}
            ack_started = time.perf_counter()
            record = producer.send(topic, key=run_id, value=payload).get(timeout=20)
            sent[event_id] = {
                "sequence": seq,
                "produced_at": produced_at,
                "broker_ack_s": time.perf_counter() - ack_started,
                "partition": record.partition,
                "offset": record.offset,
            }
    finally:
        producer.close(timeout=10)
    publish_elapsed_s = time.perf_counter() - send_started

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=f"m11-benchmark-{run_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    consumed = {}
    deadline = time.monotonic() + 45
    try:
        while len(consumed) < messages and time.monotonic() < deadline:
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    payload = record.value
                    if payload.get("run_id") == run_id:
                        consumed[payload["event_id"]] = {
                            "consumer_delivery_s": max(0.0, time.time() - payload["produced_at"]),
                            "consumer_partition": record.partition,
                            "consumer_offset": record.offset,
                        }
    finally:
        consumer.close()

    rows = []
    for event_id, sent_row in sent.items():
        rows.append({"run_id": run_id, "topic": topic, "event_id": event_id,
                     **sent_row, **consumed.get(event_id, {}),
                     "consumed": event_id in consumed})

    output = Path(__file__).resolve().parent / "results" / "kafka_benchmark.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    ack_latencies = [row["broker_ack_s"] for row in rows]
    delivery_latencies = [row["consumer_delivery_s"] for row in rows if row.get("consumed")]
    publish_throughput = len(rows) / publish_elapsed_s if publish_elapsed_s else 0
    print(f"Topic: {topic}")
    print(f"Published and acknowledged: {len(rows)}/{messages}")
    print(f"Broker-ack mean latency: {statistics.mean(ack_latencies):.6f} s")
    print(f"Publish throughput: {publish_throughput:.2f} events/s")
    print(f"Consumed: {len(consumed)}/{messages}")
    if delivery_latencies:
        print(f"Mean producer-to-consumer delay: {statistics.mean(delivery_latencies):.6f} s")
    print(f"Saved: {output}")
    if len(consumed) != messages:
        raise SystemExit("Not all benchmark events were consumed before the timeout.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", type=int, default=100)
    parser.add_argument("--bootstrap", default="localhost:9094")
    args = parser.parse_args()
    if args.messages < 1:
        parser.error("--messages must be at least 1")
    run(args.messages, args.bootstrap)
