"""Stop and restart one Compose node to measure stale detection and recovery."""
import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone

import requests


def node_state(base_url, node_id):
    response = requests.get(f"{base_url.rstrip('/')}/nodes/{node_id}", timeout=3)
    if response.status_code == 404:
        return "unknown"
    response.raise_for_status()
    return response.json()["state"]


def wait_for_state(base_url, node_id, expected, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if node_state(base_url, node_id) == expected:
                return time.monotonic()
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise TimeoutError(f"node {node_id} did not reach state {expected}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", default="edge-node-a", choices=[
        "edge-node-a", "edge-node-b", "core-node"])
    parser.add_argument("--url", default="http://127.0.0.1:5000")
    parser.add_argument("--timeout", type=float, default=45)
    parser.add_argument("--execute", action="store_true",
                        help="actually stop/restart the named local Compose service")
    args = parser.parse_args()
    if not args.execute:
        print(f"Dry run only. Re-run with --execute to stop and restart {args.node}.")
        return
    subprocess.run(["docker", "compose", "stop", args.node], check=True)
    stopped = time.monotonic()
    failure_time = datetime.now(timezone.utc).isoformat()
    try:
        stale_at = wait_for_state(args.url, args.node, "stale", args.timeout)
    finally:
        subprocess.run(["docker", "compose", "start", args.node], check=True)
    restarted = time.monotonic()
    healthy_at = wait_for_state(args.url, args.node, "healthy", args.timeout)
    result = {
        "node": args.node, "failure_injected_at": failure_time,
        "stale_detection_seconds": round(stale_at - stopped, 3),
        "recovery_seconds": round(healthy_at - restarted, 3),
        "total_unhealthy_seconds": round(healthy_at - stopped, 3),
        "stale_threshold_seconds": os.getenv("STALE_AFTER_SECONDS", "10"),
    }
    os.makedirs("results", exist_ok=True)
    filename = f"results/fault-{args.node}-{int(time.time())}.json"
    with open(filename, "w", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
    print(result)
    print(f"Saved fault-injection evidence to {filename}")


if __name__ == "__main__":
    main()
