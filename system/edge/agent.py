"""
Edge agent (Milestones 1, 2 and 4). Ported unchanged in behaviour from the Week 4 code;
the Lamport clock class now lives in system/common/lamport.py.
Agent process: runs on every monitored node (edge-node-a, edge-node-b, core-node).

Builds on Week 1's agent unchanged in spirit -- it still reads local resource
stats and pushes them to the collector -- but now adds two things Week 2 asks
for explicitly:

1. Distributed computation (Process model, extended):
   Every cycle, each node independently performs a small CPU-bound task
   (counting primes up to WORK_UNITS) *locally*. This is genuine computation
   being distributed across nodes, not just monitoring data being forwarded.
   WORK_UNITS is configurable per node (see docker-compose.yml), so nodes can
   be given different workloads on purpose -- that's what lets us measure and
   discuss workload distribution rather than assuming it's automatically even.

2. Local analytics (a real, lightweight distributed-processing task in its
   own right): each node keeps a short rolling window of its own past CPU/mem
   readings and flags when the *current* reading is an outlier relative to
   its own recent history. This computation runs independently on every node
   -- nobody aggregates it centrally -- which is exactly what "distributed"
   means here.
"""

import os
import threading
import time
import socket
from collections import deque

import psutil
import requests

from system.common.lamport import LamportClock

NODE_ID = os.environ.get("NODE_ID", socket.gethostname())
NODE_ROLE = os.environ.get("NODE_ROLE", "edge")  # "edge" or "core"
COLLECTOR_URL = os.environ.get("COLLECTOR_URL", "http://localhost:5000/metrics")   # the edge gateway
REPORT_INTERVAL = float(os.environ.get("REPORT_INTERVAL", "2"))

# Week 2: size of the local computational workload this node performs every
# cycle. Different nodes can be given different values in docker-compose.yml
# to deliberately create an uneven workload distribution to measure.
WORK_UNITS = int(os.environ.get("WORK_UNITS", "20000"))

# Week 4: deliberately skew this node's wall clock, to show what physical
# clocks get wrong and what Lamport clocks get right. 0 in normal operation.
SKEW_SECONDS = float(os.environ.get("SKEW_SECONDS", "0"))


clock = LamportClock()

# Week 2: how many past readings each node keeps for its own rolling-average
# analytics. This state lives only on this node -- it is not shared.
ROLLING_WINDOW_SIZE = 10
ANOMALY_THRESHOLD_PCT = 20.0  # flag if current cpu is this many points above its own rolling average

cpu_history = deque(maxlen=ROLLING_WINDOW_SIZE)
mem_history = deque(maxlen=ROLLING_WINDOW_SIZE)


def count_primes_up_to(n: int) -> int:
    """The distributed computation itself: a plain CPU-bound task (trial-division
    prime counting) with no shortcuts, so its cost scales predictably with n and
    is easy to reason about when measuring compute time across nodes."""
    count = 0
    for candidate in range(2, n):
        is_prime = True
        for divisor in range(2, int(candidate ** 0.5) + 1):
            if candidate % divisor == 0:
                is_prime = False
                break
        if is_prime:
            count += 1
    return count


def read_local_stats() -> dict:
    """Resource model: CPU %, memory %, network bytes sent/received."""
    net = psutil.net_io_counters()
    return {
        "cpu_percent": psutil.cpu_percent(interval=None),
        "mem_percent": psutil.virtual_memory().percent,
        "net_bytes_sent": net.bytes_sent,
        "net_bytes_recv": net.bytes_recv,
    }


def compute_local_analytics(cpu_percent: float, mem_percent: float) -> dict:
    """Week 2 distributed processing: rolling average + outlier flag, computed
    entirely from this node's own history. No other node's data is involved."""
    cpu_history.append(cpu_percent)
    mem_history.append(mem_percent)

    cpu_avg = sum(cpu_history) / len(cpu_history)
    mem_avg = sum(mem_history) / len(mem_history)
    is_anomalous = (cpu_percent - cpu_avg) > ANOMALY_THRESHOLD_PCT

    return {
        "cpu_rolling_avg": round(cpu_avg, 2),
        "mem_rolling_avg": round(mem_avg, 2),
        "anomaly": is_anomalous,
    }


def main() -> None:
    print(
        f"[agent] starting on node={NODE_ID} role={NODE_ROLE}, "
        f"work_units={WORK_UNITS}, reporting to {COLLECTOR_URL}"
    )
    psutil.cpu_percent(interval=None)  # warm-up call, see Week 1 note
    time.sleep(0.5)

    while True:
        # 1. Do this cycle's distributed computation, and time it.
        compute_start = time.time()
        primes_found = count_primes_up_to(WORK_UNITS)
        compute_time_ms = (time.time() - compute_start) * 1000

        # 2. Read this node's own resource stats (after the compute load,
        #    so CPU% reflects the work we just did).
        stats = read_local_stats()

        # 3. Run this node's local analytics on its own history.
        analytics = compute_local_analytics(stats["cpu_percent"], stats["mem_percent"])

        report = {
            "node_id": NODE_ID,
            "node_role": NODE_ROLE,
            "timestamp": time.time() + SKEW_SECONDS,
            "lamport": clock.tick(),
            **stats,
            **analytics,
            "work_units": WORK_UNITS,
            "primes_found": primes_found,
            "compute_time_ms": round(compute_time_ms, 2),
            "report_interval": REPORT_INTERVAL,
        }

        try:
            resp = requests.post(COLLECTOR_URL, json=report, timeout=2)
            try:
                clock.update(resp.json().get("lamport", 0))  # receive event
            except ValueError:
                pass
            flag = " [ANOMALY]" if analytics["anomaly"] else ""
            print(
                f"[agent:{NODE_ID}] cpu={stats['cpu_percent']:.1f}% "
                f"mem={stats['mem_percent']:.1f}% compute={compute_time_ms:.1f}ms "
                f"primes={primes_found}{flag} -> collector {resp.status_code}"
            )
        except requests.exceptions.RequestException as exc:
            print(f"[agent:{NODE_ID}] FAILED to reach collector: {exc}")

        time.sleep(REPORT_INTERVAL)


if __name__ == "__main__":
    main()
