"""
A small RPC framework over HTTP/JSON (Milestone 11).

    Client -> Client stub -> Network -> Server stub -> Server

  client stub    marshals the call into {id, method, params}, sends it, waits, unmarshals the reply, raises RpcError on a
                 remote exception; method calls look local:   stub.increment(name="x")
  server stub    unmarshals, dispatches to the registered function, marshals the result
  semantics      the three classic call semantics, selectable per client / per server:
                   maybe          one attempt, no retry            (may execute 0 or 1 times)
                   at-least-once  retry until a reply arrives      (may execute > 1 times: fine for idempotent calls only)
                   at-most-once   retry + the server keeps a reply cache keyed by request id, so a retried request is NOT
                                  executed again but answered from the cache (executes 0 or 1 times)
  accounting     messages, bytes sent/received, retries, per-call latency.
"""
import json
import threading
import time
import uuid
from collections import OrderedDict

import requests


class RpcError(Exception):
    pass


class RpcServer:
    def __init__(self, app, semantics="at-most-once", cache_size=100000, path="/rpc"):
        self.methods, self.semantics, self.cache, self.cache_size = {}, semantics, OrderedDict(), cache_size
        self.lock = threading.Lock()
        self.executions = {}
        self.drop_reply_prob = 0.0
        self.stats = {"requests": 0, "executed": 0, "duplicates_answered_from_cache": 0, "replies_dropped": 0}
        import random
        self.rng = random.Random(7)

        @app.route(path, methods=["POST"])
        def _rpc():
            from flask import jsonify, request
            req = request.get_json(force=True)
            self.stats["requests"] += 1
            rid = req["id"]
            if self.semantics == "at-most-once":
                with self.lock:
                    if rid in self.cache:
                        self.stats["duplicates_answered_from_cache"] += 1
                        reply = self.cache[rid]
                        return jsonify(reply)
            fn = self.methods.get(req["method"])
            if fn is None:
                reply = {"id": rid, "error": f"no such method {req['method']}"}
            else:
                try:
                    reply = {"id": rid, "result": fn(**req.get("params", {}))}
                    self.stats["executed"] += 1
                    self.executions[req["method"]] = self.executions.get(req["method"], 0) + 1
                except Exception as e:
                    reply = {"id": rid, "error": f"{type(e).__name__}: {e}"}
            if self.semantics == "at-most-once":
                with self.lock:
                    self.cache[rid] = reply
                    if len(self.cache) > self.cache_size:
                        self.cache.popitem(last=False)
            if self.drop_reply_prob and self.rng.random() < self.drop_reply_prob:       # the reply is lost on the way back
                self.stats["replies_dropped"] += 1
                time.sleep(2.0)
            return jsonify(reply)

        @app.route("/rpc_admin", methods=["POST", "GET"])
        def _admin():
            from flask import jsonify, request
            if request.method == "POST":
                d = request.get_json(force=True)
                if "drop_reply_prob" in d:
                    self.drop_reply_prob = float(d["drop_reply_prob"])
                if "semantics" in d:
                    self.semantics = d["semantics"]
                if d.get("reset"):
                    self.cache.clear(); self.executions.clear()
                    self.stats.update({k: 0 for k in self.stats})
            return jsonify({"semantics": self.semantics, "drop_reply_prob": self.drop_reply_prob, "stats": self.stats, "executions": self.executions})

    def register(self, name=None):
        def deco(fn):
            self.methods[name or fn.__name__] = fn
            return fn
        return deco


class RpcClient:
    def __init__(self, url, semantics="at-most-once", timeout=0.5, retries=4, path="/rpc"):
        self.url, self.semantics, self.timeout, self.path = url.rstrip("/"), semantics, timeout, path
        self.retries = 0 if semantics == "maybe" else retries
        self.s = requests.Session()
        self.stats = {"calls": 0, "messages": 0, "bytes_out": 0, "bytes_in": 0, "retries": 0, "failures": 0, "lat": []}

    def call(self, method, **params):
        req = {"id": uuid.uuid4().hex, "method": method, "params": params}
        body = json.dumps(req)
        self.stats["calls"] += 1
        t0 = time.perf_counter()
        for attempt in range(self.retries + 1):
            self.stats["messages"] += 1
            self.stats["bytes_out"] += len(body)
            if attempt:
                self.stats["retries"] += 1
            try:
                r = self.s.post(self.url + self.path, data=body, headers={"Content-Type": "application/json"}, timeout=self.timeout)
                self.stats["bytes_in"] += len(r.content)
                rep = r.json()
                self.stats["lat"].append(time.perf_counter() - t0)
                if "error" in rep:
                    raise RpcError(rep["error"])
                return rep["result"]
            except (requests.Timeout, requests.ConnectionError):
                continue
        self.stats["failures"] += 1
        raise TimeoutError(f"{method}: no reply after {self.retries + 1} attempts")

    def __getattr__(self, method):                            # stub: client.increment(name="x")
        if method.startswith("_"):
            raise AttributeError(method)
        return lambda **params: self.call(method, **params)
