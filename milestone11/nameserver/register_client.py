"""
Tiny helper: best-effort self-registration with the Milestone 10 name
server. Safe to import even when NAME_SERVER is not set - every call
becomes a no-op so earlier milestones' services keep working unchanged.
"""
import os, threading, time
import requests

NAME_SERVER = os.environ.get("NAME_SERVER")


def register_and_heartbeat(logical_name, own_url, interval=3):
    if not NAME_SERVER:
        return
    def loop():
        while True:
            try:
                requests.post(f"{NAME_SERVER}/register",
                              json={"name": logical_name, "url": own_url}, timeout=1)
            except requests.exceptions.RequestException:
                pass
            time.sleep(interval)
    threading.Thread(target=loop, daemon=True).start()
