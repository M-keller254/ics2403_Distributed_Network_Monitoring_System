"""Archive a sample monitoring record in the project's MinIO object store."""
import sys

import requests


BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000"
payload = {
    "object_name": "milestone12/sample-reading.json",
    "data": {"node": "edge-node-a", "cpu_percent": 25.0,
             "memory_percent": 37.0, "milestone": 12},
}

try:
    response = requests.post(
        BASE_URL.rstrip("/") + "/storage/objects", json=payload, timeout=10
    )
    response.raise_for_status()
    print("Object stored in MinIO:", response.json())
    listing = requests.get(
        BASE_URL.rstrip("/") + "/storage/objects",
        params={"prefix": "milestone12/"}, timeout=10,
    )
    listing.raise_for_status()
    print("Objects in milestone12/:", listing.json()["objects"])
except requests.RequestException as exc:
    print("Could not reach MinIO-backed collector API.")
    print("Start the complete project from its root folder with: docker compose up --build")
    print("Details:", exc)
    raise SystemExit(1)
