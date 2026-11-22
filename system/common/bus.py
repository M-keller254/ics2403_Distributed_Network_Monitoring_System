"""Message bus between the edge tier and the core tier.

The lecturer asked for Kafka between an edge node and the core. Production path:

    BUS=kafka  KAFKA_BOOTSTRAP=kafka:9092      -> Apache Kafka via confluent-kafka (librdkafka)

For development on a machine with no broker (and for the unit tests) there is a
file-backed stand-in with the SAME interface and the same semantics we rely on
(partitioned append-only log, keyed partitioning, consumer-group committed offsets,
at-least-once delivery):

    BUS=local  LOCAL_BUS_DIR=/some/dir          -> NOT Kafka; a development stand-in only

Interface (both implementations):
    bus = get_bus()
    p = bus.producer();            p.send(topic, key, value_dict); p.flush(); p.close()
    c = bus.consumer(topic, group) c.poll(max_records, timeout_s) -> [Record]; c.commit(); c.close()
    bus.ensure_topic(topic, partitions); bus.lag(topic, group)
Delivery guarantee used by the core: poll -> process -> commit  (at-least-once; the
core de-duplicates by record_id so replays after a failover are harmless).
"""
import fcntl
import json
import os
import threading
import time
import zlib
from collections import namedtuple

Record = namedtuple("Record", "partition offset key value")


def _partition_for(key, n):
    return zlib.crc32((key or "").encode()) % n


# ======================================================================= Kafka
class KafkaProducerWrapper:
    def __init__(self, bootstrap, **extra):
        from confluent_kafka import Producer
        conf = {
            "bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True,
            "linger.ms": 20, "compression.type": "lz4", "message.timeout.ms": 30000,
        }
        conf.update(extra)
        self._p = Producer(conf)
        self.delivered = 0
        self.failed = 0
        self.last_error = None

    def _cb(self, err, msg):
        if err is not None:
            self.failed += 1
            self.last_error = str(err)
        else:
            self.delivered += 1

    def send(self, topic, key, value):
        data = json.dumps(value).encode()
        while True:
            try:
                self._p.produce(topic, key=(key or "").encode(), value=data, on_delivery=self._cb)
                break
            except BufferError:          # local queue full: let librdkafka drain
                self._p.poll(0.2)
        self._p.poll(0)

    def flush(self, timeout=30):
        return self._p.flush(timeout)

    def close(self):
        self.flush(10)


class KafkaConsumerWrapper:
    def __init__(self, bootstrap, topic, group, **extra):
        from confluent_kafka import Consumer
        conf = {
            "bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False,
            "auto.offset.reset": "earliest", "session.timeout.ms": 10000,
        }
        conf.update(extra)
        self._c = Consumer(conf)
        self._c.subscribe([topic])

    def poll(self, max_records=500, timeout=1.0):
        out = []
        for m in self._c.consume(num_messages=max_records, timeout=timeout):
            if m.error():
                continue                 # partition EOF / transient broker errors: next poll retries
            out.append(Record(m.partition(), m.offset(), (m.key() or b"").decode(), json.loads(m.value())))
        return out

    def commit(self):
        from confluent_kafka import KafkaException
        try:
            self._c.commit(asynchronous=False)
        except KafkaException as e:      # nothing consumed yet -> nothing to commit
            if "NO_OFFSET" not in str(e):
                raise

    def close(self):
        self._c.close()


class KafkaBus:
    kind = "kafka"

    def __init__(self, bootstrap):
        self.bootstrap = bootstrap

    def producer(self, **kw):
        return KafkaProducerWrapper(self.bootstrap, **kw)

    def consumer(self, topic, group, **kw):
        return KafkaConsumerWrapper(self.bootstrap, topic, group, **kw)

    def ensure_topic(self, topic, partitions=3, replication=1, wait_s=30):
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        deadline = time.time() + wait_s
        while True:
            try:
                futs = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication)])
                for f in futs.values():
                    try:
                        f.result(timeout=10)
                    except Exception as e:
                        if "TOPIC_ALREADY_EXISTS" not in str(e):
                            raise
                return
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(1)

    def lag(self, topic, group):
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": group, "enable.auto.commit": False})
        try:
            md = c.list_topics(topic, timeout=10).topics[topic]
            tps = [TopicPartition(topic, p) for p in md.partitions]
            total = 0
            for tp in c.committed(tps, timeout=10):
                lo, hi = c.get_watermark_offsets(tp, timeout=10)
                total += hi - (tp.offset if tp.offset >= 0 else lo)
            return total
        finally:
            c.close()


# ======================================================================= local stand-in
class LocalProducer:
    def __init__(self, bus):
        self.bus = bus
        self.delivered = 0
        self.failed = 0
        self.last_error = None

    def send(self, topic, key, value):
        n = self.bus.partitions(topic)
        path = self.bus._plog(topic, _partition_for(key, n))
        line = (json.dumps({"k": key, "v": value}) + "\n").encode()
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            os.write(fd, line)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        self.delivered += 1

    def flush(self, timeout=0):
        return 0

    def close(self):
        pass


class LocalConsumer:
    def __init__(self, bus, topic, group):
        self.bus, self.topic, self.group = bus, topic, group
        self.n = bus.partitions(topic)
        self.pos = bus._read_offsets(topic, group, self.n)      # committed position per partition (byte offset)
        self.uncommitted = dict(self.pos)

    def poll(self, max_records=500, timeout=1.0):
        deadline = time.time() + timeout
        out = []
        while True:
            for p in range(self.n):
                if len(out) >= max_records:
                    break
                path = self.bus._plog(self.topic, p)
                if not os.path.exists(path):
                    continue
                with open(path, "rb") as f:
                    f.seek(self.uncommitted[p])
                    while len(out) < max_records:
                        start = f.tell()
                        line = f.readline()
                        if not line or not line.endswith(b"\n"):
                            break
                        rec = json.loads(line)
                        out.append(Record(p, start, rec["k"], rec["v"]))
                        self.uncommitted[p] = f.tell()
            if out or time.time() >= deadline:
                return out
            time.sleep(0.05)

    def commit(self):
        self.bus._write_offsets(self.topic, self.group, self.uncommitted)
        self.pos = dict(self.uncommitted)

    def close(self):
        pass


class LocalBus:
    kind = "local"

    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)
        self._lock = threading.Lock()

    def _tdir(self, topic):
        d = os.path.join(self.root, topic)
        os.makedirs(d, exist_ok=True)
        return d

    def _plog(self, topic, p):
        return os.path.join(self._tdir(topic), f"p{p}.log")

    def partitions(self, topic):
        meta = os.path.join(self._tdir(topic), "meta.json")
        if not os.path.exists(meta):
            self.ensure_topic(topic, 3)
        return json.load(open(meta))["partitions"]

    def ensure_topic(self, topic, partitions=3, replication=1, wait_s=0):
        meta = os.path.join(self._tdir(topic), "meta.json")
        if not os.path.exists(meta):
            tmp = meta + f".{os.getpid()}"
            json.dump({"partitions": partitions}, open(tmp, "w"))
            os.replace(tmp, meta)

    def _ofile(self, topic, group):
        return os.path.join(self._tdir(topic), f"offsets-{group}.json")

    def _read_offsets(self, topic, group, n):
        f = self._ofile(topic, group)
        d = json.load(open(f)) if os.path.exists(f) else {}
        return {p: int(d.get(str(p), 0)) for p in range(n)}

    def _write_offsets(self, topic, group, pos):
        f = self._ofile(topic, group)
        tmp = f + f".{os.getpid()}"
        json.dump({str(p): o for p, o in pos.items()}, open(tmp, "w"))
        os.replace(tmp, f)

    def producer(self, **kw):
        return LocalProducer(self)

    def consumer(self, topic, group, **kw):
        return LocalConsumer(self, topic, group)

    def lag(self, topic, group):
        n = self.partitions(topic)
        committed = self._read_offsets(topic, group, n)
        total = 0
        for p in range(n):
            path = self._plog(topic, p)
            if os.path.exists(path):
                with open(path, "rb") as f:
                    f.seek(committed[p])
                    total += sum(1 for _ in f)
        return total


def get_bus():
    kind = os.environ.get("BUS", "kafka").lower()
    if kind == "kafka":
        return KafkaBus(os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    return LocalBus(os.environ.get("LOCAL_BUS_DIR", "/tmp/local-bus"))
