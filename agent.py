"""
Week 1 - Distributed OS Foundation
Agent process: runs on every monitored node (edge-node-a, edge-node-b, core-node).

Responsibility (Process model, Week 1):
    - Read local resource stats (CPU, memory, network I/O) every REPORT_INTERVAL seconds.
    - Push them to the collector over HTTP.
    - Log delivery success/failure so the console output doubles as a live health check.

Configuration is via environment variables so the SAME image can run as any node
(see docker-compose.yml) -- this keeps the "one system, many nodes" model honest,
rather than hand-writing a separate script per node.
"""

import os
import time
import socket

import psutil
import requests

NODE_ID = os.environ.get("NODE_ID", socket.gethostname())
NODE_ROLE = os.environ.get("NODE_ROLE", "edge")  # "edge" or "core"
COLLECTOR_URL = os.environ.get("COLLECTOR_URL", "http://localhost:5000/metrics")
REPORT_INTERVAL = float(os.environ.get("REPORT_INTERVAL", "2"))


def read_local_stats() -> dict:
    """Resource model, Week 1: CPU %, memory %, network bytes sent/received."""
    net = psutil.net_io_counters()
    return {
        "node_id": NODE_ID,
        "node_role": NODE_ROLE,
        "timestamp": time.time(),
        "cpu_percent": psutil.cpu_percent(interval=None),
        "mem_percent": psutil.virtual_memory().percent,
        "net_bytes_sent": net.bytes_sent,
        "net_bytes_recv": net.bytes_recv,
    }


def main() -> None:
    print(f"[agent] starting on node={NODE_ID} role={NODE_ROLE}, reporting to {COLLECTOR_URL}")
    # psutil's first cpu_percent() call is a baseline reading (always 0.0) -- warm it up.
    psutil.cpu_percent(interval=None)
    time.sleep(0.5)

    while True:
        stats = read_local_stats()
        try:
            resp = requests.post(COLLECTOR_URL, json=stats, timeout=2)
            print(
                f"[agent:{NODE_ID}] sent report "
                f"(cpu={stats['cpu_percent']:.1f}% mem={stats['mem_percent']:.1f}%) "
                f"-> collector responded {resp.status_code}"
            )
        except requests.exceptions.RequestException as exc:
           
           
            print(f"[agent:{NODE_ID}] FAILED to reach collector: {exc}")

        time.sleep(REPORT_INTERVAL)


if __name__ == "__main__":
    main()
