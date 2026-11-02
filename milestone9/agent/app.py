"""
Agent service — Week 2 version.

Each node no longer just forwards one raw reading. It collects a
small local WINDOW of CPU/memory samples, aggregates them itself
(min/avg/max), and sends the SUMMARY to the collector. This is the
"distribute computation across nodes" requirement for Milestone 2:
each node does its own local processing instead of the collector
doing all the work centrally.

Configuration via environment variables (set in docker-compose.yml):
    NODE_NAME        - e.g. "edge-node-a"
    NODE_ROLE        - e.g. "edge" or "core"
    COLLECTOR_URL    - e.g. "http://collector:5000/report"
    REPORT_INTERVAL  - seconds between local samples (default 1)
    WINDOW_SIZE      - how many samples to aggregate before reporting (default 5)
"""

import os
import time
import statistics
import psutil
import requests

NODE_NAME = os.environ.get("NODE_NAME", "unknown-node")
NODE_ROLE = os.environ.get("NODE_ROLE", "unknown")
COLLECTOR_URL = os.environ.get("COLLECTOR_URL", "http://collector:5000/report")
TARGETS = [u.strip() for u in COLLECTOR_URL.split(",") if u.strip()]
REPORT_INTERVAL = float(os.environ.get("REPORT_INTERVAL", "1"))
WINDOW_SIZE = int(os.environ.get("WINDOW_SIZE", "5"))

CGROUP_MEM_CURRENT = "/sys/fs/cgroup/memory.current"
CGROUP_MEM_MAX = "/sys/fs/cgroup/memory.max"


def get_container_memory_percent():
    try:
        with open(CGROUP_MEM_CURRENT) as f:
            used = int(f.read().strip())
        with open(CGROUP_MEM_MAX) as f:
            raw_max = f.read().strip()
        if raw_max == "max":
            return psutil.virtual_memory().percent
        limit = int(raw_max)
        return round((used / limit) * 100, 1)
    except (FileNotFoundError, ValueError, ZeroDivisionError):
        return psutil.virtual_memory().percent


def get_cpu_percent():
    return psutil.cpu_percent(interval=1)


def send_report(summary, attempt_send_time):
    """
    Sends the aggregated summary to EVERY target URL (one target in
    Milestones 1-2, several aggregators in Milestone 4). Latency is
    timed per target in this process: T_response - T_request.
    Returns True if at least one target accepted the report.
    """
    payload = {
        "node": NODE_NAME, "role": NODE_ROLE,
        "cpu": summary["cpu_avg"], "mem": summary["mem_avg"],
        "cpu_min": summary["cpu_min"], "cpu_max": summary["cpu_max"],
        "mem_min": summary["mem_min"], "mem_max": summary["mem_max"],
        "window_size": summary["window_size"],
    }
    any_ok = False
    for url in TARGETS:
        t_request = time.time()
        try:
            resp = requests.post(url, json={**payload, "sent_at": t_request}, timeout=3)
            latency = time.time() - t_request
            any_ok = any_ok or resp.status_code == 200
            print(f"[agent:{NODE_NAME}] window={summary['window_size']} "
                  f"cpu_avg={summary['cpu_avg']}% mem_avg={summary['mem_avg']}% "
                  f"-> {url} responded {resp.status_code} latency={latency:.3f}s",
                  flush=True)
        except requests.exceptions.RequestException as e:
            print(f"[agent:{NODE_NAME}] FAILED to reach {url}: {e}", flush=True)
    return any_ok


def collect_window():
    """Collects WINDOW_SIZE samples, one per REPORT_INTERVAL-ish tick,
    and returns an aggregated summary dict. This IS the local distributed
    processing: each node reduces its own raw samples before anything
    is sent over the network."""
    cpu_samples = []
    mem_samples = []
    for _ in range(WINDOW_SIZE):
        cpu_samples.append(get_cpu_percent())      # blocks ~1s internally
        mem_samples.append(get_container_memory_percent())

    return {
        "cpu_avg": round(statistics.mean(cpu_samples), 2),
        "cpu_min": round(min(cpu_samples), 2),
        "cpu_max": round(max(cpu_samples), 2),
        "mem_avg": round(statistics.mean(mem_samples), 2),
        "mem_min": round(min(mem_samples), 2),
        "mem_max": round(max(mem_samples), 2),
        "window_size": WINDOW_SIZE,
    }


def main():
    print(
        f"[agent:{NODE_NAME}] starting, role={NODE_ROLE}, "
        f"window_size={WINDOW_SIZE}, reporting to {COLLECTOR_URL}",
        flush=True,
    )
    while True:
        summary = collect_window()
        send_report(summary, time.time())


if __name__ == "__main__":
    main()
