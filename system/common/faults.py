"""Fault-injection framework (Milestone 7), built into every service.

Every service calls  FaultInjector(name).install(app).  That adds:
  * a before_request hook that applies the active fault to every request, and
  * GET/POST/DELETE /fault to inspect, set, and clear faults at runtime.

Failure model covered (brief, milestone 7):
  crash          crash_in_s=N        process exits (os._exit) after N seconds
  node/service   down=true           process alive but every request gets 503 (service failure)
  omission       omit_prob=p         a request is accepted but never answered (client times out)
  communication  delay_ms, jitter_ms extra latency; blocked_from=[names] drops traffic from
                                     those callers (a network partition seen from this node)
  application    error_prob=p        request answered with HTTP 500
  byzantine      byzantine=true      the service returns plausible but WRONG data
                                     (services call corrupt() on values they serve)

POST /fault  {"omit_prob": 0.3}              merge settings
POST /fault  {"clear": true}                 remove all faults
GET  /fault                                  show current settings
"""
import os
import random
import threading
import time

from flask import jsonify, request

HANG_SECONDS = float(os.environ.get("FAULT_HANG_SECONDS", "6"))


class FaultInjector:
    def __init__(self, name):
        self.name = name
        self.cfg = {}
        self.lock = threading.Lock()
        self.hits = {"dropped": 0, "errored": 0, "delayed": 0, "blocked": 0, "down": 0}

    # ---- state ----
    def set(self, **kw):
        with self.lock:
            self.cfg.update(kw)
        if kw.get("crash_in_s") is not None:
            threading.Timer(float(kw["crash_in_s"]), lambda: os._exit(137)).start()

    def clear(self):
        with self.lock:
            self.cfg = {}

    def get(self, key, default=None):
        with self.lock:
            return self.cfg.get(key, default)

    # ---- byzantine ----
    def corrupt(self, value):
        """Return a plausible-but-wrong version of value if the byzantine fault is active."""
        if not self.get("byzantine"):
            return value
        if isinstance(value, bool):
            return not value
        if isinstance(value, (int, float)):
            return value + 1000003
        if isinstance(value, str):
            return value[::-1] + "~"
        if isinstance(value, dict):
            return {k: self.corrupt(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.corrupt(v) for v in value]
        return value

    # ---- request hook ----
    def _before(self):
        if request.path == "/fault":
            return None
        with self.lock:
            cfg = dict(self.cfg)
        if not cfg:
            return None
        if cfg.get("down"):
            self.hits["down"] += 1
            return jsonify({"error": "service down (injected)"}), 503
        caller = request.headers.get("X-From", "")
        if caller and caller in (cfg.get("blocked_from") or []):
            self.hits["blocked"] += 1
            time.sleep(min(HANG_SECONDS, float(cfg.get("hang_s", HANG_SECONDS))))
            return jsonify({"error": "partitioned (injected)"}), 503
        if cfg.get("omit_prob") and random.random() < float(cfg["omit_prob"]):
            self.hits["dropped"] += 1
            time.sleep(min(HANG_SECONDS, float(cfg.get("hang_s", HANG_SECONDS))))
            return jsonify({"error": "omitted (injected)"}), 503
        if cfg.get("delay_ms") or cfg.get("jitter_ms"):
            self.hits["delayed"] += 1
            d = float(cfg.get("delay_ms", 0)) + random.uniform(0, float(cfg.get("jitter_ms", 0)))
            time.sleep(d / 1000.0)
        if cfg.get("error_prob") and random.random() < float(cfg["error_prob"]):
            self.hits["errored"] += 1
            return jsonify({"error": "internal error (injected)"}), 500
        return None

    def install(self, app):
        app.before_request(self._before)

        @app.route("/fault", methods=["GET", "POST", "DELETE"])
        def _fault():
            if request.method == "GET":
                with self.lock:
                    return jsonify({"service": self.name, "faults": dict(self.cfg), "hits": dict(self.hits)})
            if request.method == "DELETE":
                self.clear()
                return jsonify({"service": self.name, "faults": {}})
            body = request.get_json(silent=True) or {}
            if body.pop("clear", False):
                self.clear()
            if body:
                self.set(**body)
            with self.lock:
                return jsonify({"service": self.name, "faults": dict(self.cfg)})

        return self
