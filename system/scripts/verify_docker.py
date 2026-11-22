"""End-to-end check of the full docker-compose stack (docker-compose.system.yml).

    docker compose -f docker-compose.system.yml up --build -d
    python -m system.scripts.verify_docker --expect 120000

Waits until the archiver (published on localhost:5003) reports that BOTH distributed storage back ends (the HDFS-style DFS and the MinIO
object store) contain every record id, then prints the gateway's link statistics and the cloud digest. Exit code 0 = pipeline verified.
"""
import argparse
import sys
import time

import requests


def get(url, timeout=60):
    return requests.get(url, timeout=timeout).json()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expect", type=int, default=120000)
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--gateway", default="http://localhost:5000")
    ap.add_argument("--archiver", default="http://localhost:5003")
    ap.add_argument("--cloud", default="http://localhost:5002")
    a = ap.parse_args()
    t0, last = time.time(), None
    while time.time() - t0 < a.timeout:
        try:
            v = get(a.archiver + "/verify")
            last = {k: x["distinct_record_ids"] for k, x in v.items()}
            print(f"[{int(time.time() - t0):4}s] distinct record ids in storage: {last}", flush=True)
            if last and all(n >= a.expect for n in last.values()):
                break
        except requests.RequestException as e:
            print(f"[{int(time.time() - t0):4}s] waiting for the archiver ({type(e).__name__})", flush=True)
        time.sleep(10)
    else:
        print(f"FAILED: not all {a.expect} rows reached storage within {a.timeout}s (last: {last})")
        return 1
    g = get(a.gateway + "/metrics_summary")
    print("gateway link bytes  :", g["link"])
    print("gateway producer    :", g["producer"])
    print("archiver stats      :", get(a.archiver + "/stats"))
    try:
        print("cloud analytics     :", get(a.cloud + "/analytics"))
    except Exception as e:                       # the cloud summary path is informational only
        print("cloud digest        : unavailable", type(e).__name__)
    print(f"OK: {a.expect} rows verified in {', '.join(last)} storage")
    return 0


if __name__ == "__main__":
    sys.exit(main())
