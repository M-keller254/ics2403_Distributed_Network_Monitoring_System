"""Fast unit tests: dataset, message bus semantics, logical clocks, lock manager, simulator."""
import csv
import hashlib
import os
import threading
import time

import pytest

from system.common.bus import LocalBus
from system.common.lamport import LamportClock
from system.concurrency.lockmgr import Deadlock, OrderViolation, ResourceManager
from system.models.simulator import simulate

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV = os.path.join(ROOT, "data", "telemetry_120k.csv")


# ---------------------------------------------------------------- dataset
def test_dataset_has_more_than_100k_rows_and_schema():
    with open(CSV, newline="") as f:
        rd = csv.DictReader(f)
        assert rd.fieldnames[:3] == ["record_id", "timestamp", "site_id"]
        n = sum(1 for _ in rd)
    assert n == 120000


def test_dataset_is_reproducible_and_seed_sensitive():
    from data.generate_dataset import generate
    a, b, c = generate(60, 2403), generate(60, 2403), generate(60, 7)
    assert a == b, "same seed must give identical rows"
    assert a != c, "different seed must give different rows"
    assert len(a) == 60 * 24
    assert [r[0] for r in a] == list(range(1, len(a) + 1))


def test_dataset_sha256_matches_published():
    # the first 8 hex digits are recorded in docs/reproducibility.md
    h = hashlib.sha256(open(CSV, "rb").read()).hexdigest()
    assert h.startswith("7e81ca81")


# ---------------------------------------------------------------- bus
def test_bus_keyed_partitioning_and_commit(tmp_path):
    bus = LocalBus(str(tmp_path))
    bus.ensure_topic("t", 3)
    p = bus.producer()
    for i in range(30):
        p.send("t", f"k{i % 5}", {"i": i})
    p.flush()
    c = bus.consumer("t", "g")
    got = []
    while True:
        recs = c.poll(100, 0.1)
        if not recs:
            break
        got += recs
    assert sorted(r.value["i"] for r in got) == list(range(30))
    parts = {}
    for r in got:
        parts.setdefault(r.key, set()).add(r.partition)
    assert all(len(v) == 1 for v in parts.values()), "one key must always map to one partition"


def test_bus_at_least_once_redelivery_without_commit(tmp_path):
    bus = LocalBus(str(tmp_path))
    bus.ensure_topic("t", 1)
    p = bus.producer()
    for i in range(5):
        p.send("t", "k", {"i": i})
    p.flush()
    c1 = bus.consumer("t", "g")
    assert len(c1.poll(10, 0.1)) == 5
    c1.close()                                     # crashed before commit
    c2 = bus.consumer("t", "g")
    assert len(c2.poll(10, 0.1)) == 5              # everything is delivered again
    c2.commit()
    c2.close()
    c3 = bus.consumer("t", "g")
    assert c3.poll(10, 0.1) == []                  # committed offsets are respected


# ---------------------------------------------------------------- Lamport
def test_lamport_happens_before():
    a, b = LamportClock(), LamportClock()
    t1 = a.tick()
    t2 = b.update(t1)
    assert t2 > t1
    assert b.tick() > t2


# ---------------------------------------------------------------- lock manager
def test_cycle_detection_finds_four_cycle():
    edges = [("P0", "P1"), ("P1", "P2"), ("P2", "P3"), ("P3", "P0")]
    cyc = ResourceManager.find_cycle(edges)
    assert cyc and set(cyc) == {"P0", "P1", "P2", "P3"}
    assert ResourceManager.find_cycle([("A", "B"), ("B", "C")]) is None


def _ring(rm, n=2):
    errs = []

    def w(i):
        try:
            rm.begin(f"P{i}")
            rm.acquire(f"P{i}", f"R{i}", timeout=3)
            time.sleep(0.05)
            rm.acquire(f"P{i}", f"R{(i + 1) % n}", timeout=3)
            rm.release_all(f"P{i}")
        except Deadlock:
            errs.append("deadlock")
            rm.release_all(f"P{i}")
        except TimeoutError:
            errs.append("timeout")
            rm.release_all(f"P{i}")
    ths = [threading.Thread(target=w, args=(i,)) for i in range(n)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    return errs


def test_deadlock_without_handling_times_out():
    rm = ResourceManager({"R0": 1, "R1": 1}, "none")
    assert "timeout" in _ring(rm)


def test_deadlock_detected_and_victim_aborted():
    rm = ResourceManager({"R0": 1, "R1": 1}, "detect", detect_interval=0.05)
    errs = _ring(rm)
    assert errs.count("deadlock") == 1 and "timeout" not in errs
    assert rm.stats["deadlocks_detected"] == 1
    rm.close()


def test_wait_die_never_deadlocks():
    rm = ResourceManager({"R0": 1, "R1": 1}, "wait-die")
    errs = _ring(rm)
    assert "timeout" not in errs


def test_ordered_policy_rejects_out_of_order_request():
    rm = ResourceManager({"A": 1, "B": 1}, "ordered")
    rm.acquire("p", "B")
    with pytest.raises(OrderViolation):
        rm.acquire("p", "A")


def test_fifo_prevents_starvation_of_large_request():
    rm = ResourceManager({"pool": 4}, "none", fair=True)
    for i in range(4):
        rm.acquire(f"s{i}", "pool", 1)
    got = []
    t = threading.Thread(target=lambda: (rm.acquire("big", "pool", 4, timeout=3), got.append(1)))
    t.start()
    time.sleep(0.1)
    late_result = []

    def late_req():
        try:
            rm.acquire("late", "pool", 1, timeout=0.5)
            late_result.append("granted")
        except TimeoutError:
            late_result.append("timeout")
    late = threading.Thread(target=late_req)         # arrives after the big request, so FIFO must serve it AFTER the big one
    late.start()
    time.sleep(0.1)
    for i in range(4):
        rm.release(f"s{i}", "pool")
    t.join()
    assert got == [1], "the 4-unit request must be granted once the pool drains (no starvation by later 1-unit requests)"
    late.join()
    assert late_result == ["timeout"], "the late small request must not barge ahead of the older large one"


# ---------------------------------------------------------------- simulator
def test_simulator_is_deterministic():
    a = simulate("processor-pool", seed=5, horizon=60)
    b = simulate("processor-pool", seed=5, horizon=60)
    assert a == b


def test_integrated_model_reduces_wan_traffic():
    base = simulate("processor-pool", seed=5, horizon=60)
    integ = simulate("integrated", seed=5, horizon=60)
    assert integ["wan_gb"] < 0.2 * base["wan_gb"]
