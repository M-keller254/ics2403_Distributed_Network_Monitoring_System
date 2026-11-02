import json, os, time
import requests

CORE = "http://127.0.0.1:5000/aggregate"
os.makedirs("results", exist_ok=True)
print("Cloud aggregator running. Press Ctrl+C to stop.")
while True:
    try:
        response = requests.get(CORE, timeout=3)
        response.raise_for_status()
        data = response.json()
        data["cloud_received_at"] = time.time()
        with open("results/cloud_log.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(data) + "\n")
        print("Stored one record from core")
    except requests.RequestException as exc:
        print("Core unreachable:", exc)
    time.sleep(5)
