import json, time
import requests

CORE = "http://127.0.0.1:5000/aggregate"

print("Cloud aggregator running. Press Ctrl+C to stop.")
while True:
    try:
        data = requests.get(CORE, timeout=3).json()
        data["cloud_received_at"] = time.time()
        with open("results/cloud_log.jsonl", "a") as f:
            f.write(json.dumps(data) + "\n")
        print("Stored one record from core")
    except requests.RequestException as e:
        print("Core unreachable:", e)
    time.sleep(5)