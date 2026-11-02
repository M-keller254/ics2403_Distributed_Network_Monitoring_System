"""Periodic node agent; sends host/container process metrics to the collector."""
import os
import time
from datetime import datetime, timezone
from threading import Thread

import psutil
import requests
from werkzeug.serving import make_server
from agent.service import AgentState


def select_interval(metrics, base_interval=2.0, adaptive=False, quiet_interval=5.0,
                    busy_interval=1.0, cpu_threshold=75.0, memory_threshold=80.0):
    """Choose the next delay; adaptive mode samples more often under high load."""
    if not adaptive:
        return max(1.0, float(base_interval))
    hot = (metrics["cpu_percent"] >= cpu_threshold
           or metrics["memory_percent"] >= memory_threshold)
    return max(0.5, float(busy_interval)) if hot else max(
        float(base_interval), float(quiet_interval))


def read_metrics(node_id):
    counters = psutil.net_io_counters()
    return {
        "node_id": node_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cpu_percent": psutil.cpu_percent(interval=None),
        "memory_percent": psutil.virtual_memory().percent,
        "bytes_sent": counters.bytes_sent,
        "bytes_recv": counters.bytes_recv,
    }


def start_control_server(node_id, port):
    state = AgentState(node_id, lambda: read_metrics(node_id))
    server = make_server("0.0.0.0", port, state.create_app(), threaded=True)
    Thread(target=server.serve_forever, daemon=True).start()
    return server


def run():
    node_id = os.getenv("NODE_ID", "unnamed-node")
    endpoint = os.getenv("COLLECTOR_URL", "http://localhost:5000").rstrip("/") + "/ingest"
    agent_port = int(os.getenv("AGENT_PORT", "5001"))
    start_control_server(node_id, agent_port)
    interval = max(1.0, float(os.getenv("REPORT_INTERVAL", "2")))
    adaptive = os.getenv("ADAPTIVE_REPORTING", "false").lower() in {"1", "true", "yes"}
    quiet_interval = max(interval, float(os.getenv("QUIET_INTERVAL", "5")))
    busy_interval = max(0.5, float(os.getenv("BUSY_INTERVAL", "1")))
    cpu_threshold = float(os.getenv("HOT_CPU_THRESHOLD", "75"))
    memory_threshold = float(os.getenv("HOT_MEMORY_THRESHOLD", "80"))
    session = requests.Session()
    psutil.cpu_percent(interval=None)  # prime the non-blocking CPU sampler
    while True:
        try:
            metrics = read_metrics(node_id)
            response = session.post(endpoint, json=metrics, timeout=3)
            response.raise_for_status()
            next_interval = select_interval(
                metrics, interval, adaptive, quiet_interval, busy_interval,
                cpu_threshold, memory_threshold)
        except requests.RequestException as exc:
            print(f"report failed for {node_id}: {exc}", flush=True)
            next_interval = busy_interval if adaptive else interval
        time.sleep(next_interval)


if __name__ == "__main__":
    run()
