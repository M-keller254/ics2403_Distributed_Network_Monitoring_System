"""Small RPC-style client for the monitoring collector's HTTP/JSON API."""
import requests

class CollectorClient:
    def __init__(self, base_url, timeout=3):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def report(self, metrics):
        response = requests.post(self.base_url + "/ingest", json=metrics, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def status(self):
        response = requests.get(self.base_url + "/status", timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def history(self, node_id, limit=100):
        response = requests.get(
            f"{self.base_url}/nodes/{node_id}/history",
            params={"limit": limit}, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
