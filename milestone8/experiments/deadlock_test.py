"""
Milestone 6 experiment.

Order matters: /demo/unsafe reproduces a REAL, permanent deadlock in
the collector process (two threads each hold one lock and wait
forever for the other). So this script runs safe, then detect, then
unsafe LAST, and tells you to restart the collector afterward.

Usage: python experiments/deadlock_test.py
"""
import time
import requests

COLLECTOR = "http://localhost:5000"


def call(path, client_timeout):
    t0 = time.time()
    try:
        r = requests.post(f"{COLLECTOR}{path}", timeout=client_timeout)
        return r.status_code, r.json(), time.time() - t0
    except requests.exceptions.ReadTimeout:
        return None, "TIMED OUT (client gave up waiting)", time.time() - t0


if __name__ == "__main__":
    print("1) /demo/safe  - same two locks, SAME fixed order (prevention)")
    print("   ", call("/demo/safe", 6))

    print("\n2) /demo/detect - opposite lock order, but with try-acquire + timeout (detect+recover)")
    print("   ", call("/demo/detect", 6))

    print("\n3) /demo/unsafe - opposite lock order, NO timeout (this WILL deadlock the collector)")
    print("   ", call("/demo/unsafe?timeout=3", 4))
    print("\nThe collector is now deadlocked on purpose. Restart it before")
    print("running any further experiments: docker compose restart collector")
