"""
Milestone 11 - explicit RPC client stub.

The system already talks HTTP/JSON between services (Milestones 1-10).
What was missing is the explicit Client -> Client Stub -> Network ->
Server Stub -> Server structure the brief asks for: a stub that makes
a remote call LOOK like a local function call, hiding the HTTP details
from the caller.

This stub wraps the Milestone 5 distributed shared-state service
(txn-coordinator + store-a/store-b) as plain Python methods:

    from rpc.client_stub import SharedStateClient
    client = SharedStateClient("http://localhost:7000", gateway="http://localhost:8000")
    client.put("temperature", 21)       # looks local; is a 2PC write underneath
    client.get("temperature")           # looks local; is a transparent read underneath

Every call also returns timing, so communication latency, throughput
and overhead can be measured without instrumenting every caller by hand.
"""
import time
import requests


class RPCError(Exception):
    pass


class SharedStateClient:
    """Client Stub. Marshals a method call into an HTTP/JSON request,
    sends it over the network to the matching Server Stub, and
    unmarshals the response - the caller never sees any of that."""

    def __init__(self, coordinator_url, gateway_url, timeout=5):
        self.coordinator_url = coordinator_url.rstrip("/")
        self.gateway_url = gateway_url.rstrip("/")
        self.timeout = timeout

    def put(self, key, value):
        """Remote procedure: PUT(key, value) -> {decision, latency_s}.
        Marshalled as a POST /write to the txn-coordinator's server stub."""
        t0 = time.time()
        try:
            r = requests.post(f"{self.coordinator_url}/write",
                               json={"key": key, "value": value}, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            raise RPCError(f"put({key!r}) failed: {e}")
        body = r.json()
        return {"decision": body.get("decision"),
                "server_latency_s": body.get("latency_s"),
                "rpc_round_trip_s": round(time.time() - t0, 4)}

    def get(self, key):
        """Remote procedure: GET(key) -> value, via the Milestone 9
        gateway's server stub (/read/<key>), transparent to which
        replica actually answers."""
        t0 = time.time()
        try:
            r = requests.get(f"{self.gateway_url}/read/{key}", timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            raise RPCError(f"get({key!r}) failed: {e}")
        if r.status_code != 200:
            raise RPCError(f"get({key!r}) not found: {r.json()}")
        body = r.json()
        return {"value": body["value"], "served_by": body.get("_served_by"),
                "rpc_round_trip_s": round(time.time() - t0, 4)}
