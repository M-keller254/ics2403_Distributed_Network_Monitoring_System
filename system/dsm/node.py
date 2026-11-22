"""
Distributed shared memory (Milestone 11): a directory-based, page(object)-granularity DSM.

Every node exposes the SAME shared address space (named objects) through read(key) / write(key, value). Under the hood:

  home node      each key has a HOME = hash(key) mod N that keeps the directory entry: owner (the node holding the only
                 writable copy), sharers (nodes holding read-only copies) and the last written-back value.
  local cache    per node, state per key:   I invalid | S shared (read-only copy) | M modified (exclusive, writable)

  MODE=strong    write-INVALIDATE protocol (sequentially consistent):
      read  hit (S or M): local, 0 messages.   miss: ask the home; if some node owns it (M) the home pulls the value back
            and downgrades that owner to S, then returns it; the reader becomes a sharer.
      write hit in M: local, 0 messages.   otherwise ask the home; the home INVALIDATES every sharer (and fetches from the
            previous owner), waits for the acknowledgements, then grants M to the writer.
  MODE=eventual  write-UPDATE, no invalidation round: a write is applied locally and pushed asynchronously to all peers
      (last-writer-wins by (timestamp, node)); reads are always local -> fast, but may be stale for a short window.

  Synchronisation: lock(key)/unlock(key) (distributed mutex held at the home) and add(key, delta) (atomic fetch-and-add
  executed at the home) give correct read-modify-write; plain read+write is NOT atomic (lost-update demo).

Endpoints (client facing):  POST /read /write /add /lock /unlock    GET /stats  POST /reset  GET /state/<key>
Endpoints (node to node):   /home_read /home_write /inv /fetch /update /home_add /home_lock /home_unlock
Env: NODE_INDEX, PEERS "0=url0,1=url1,..." (all nodes incl. self), MODE
"""
import threading
import time

from flask import Flask, jsonify, request

from system.common.config import env_int, env_str, parse_peers
from system.common.faults import FaultInjector
from system.common.netcall import Net

app = Flask(__name__)
IDX = env_int("NODE_INDEX", 0)
PORT = env_int("PORT", 7600)
PEERS = parse_peers(env_str("PEERS", f"0=http://127.0.0.1:{PORT}"))
N = len(PEERS)
MODE = env_str("MODE", "strong")
faults = FaultInjector(f"dsm{IDX}").install(app)
net = Net(f"dsm{IDX}", default_timeout=3.0)

cache = {}                                                   # key -> {"state","value","version"}
directory = {}                                               # key -> {"owner","sharers","value","version"}   (keys homed here)
dirlocks, cache_lock, dl = {}, threading.Lock(), threading.Lock()
locks = {}                                                   # distributed mutex state for keys homed here: key -> holder
lock_cv = threading.Condition()
ops = {"read_hits": 0, "read_misses": 0, "write_hits": 0, "write_misses": 0, "invalidations_sent": 0, "invalidations_received": 0,
       "fetches": 0, "updates_sent": 0, "updates_received": 0, "lock_waits": 0, "adds": 0}


def home_of(key):
    return sum(ord(c) * (i + 1) for i, c in enumerate(key)) % N


def dirlock(key):
    with dl:
        return dirlocks.setdefault(key, threading.RLock())


def call(idx, path, payload, timeout=None):
    if idx == IDX:                                           # a node talking to itself needs no network hop
        with app.test_request_context(path, method="POST", json=payload):
            return app.view_functions[path.strip("/").replace("/", "_")]().get_json()
    r = net.post(PEERS[idx] + path, payload, timeout=timeout, tag=path.strip("/"))
    return r.json()


def entry(key):
    return directory.setdefault(key, {"owner": None, "sharers": set(), "value": None, "version": 0})


# ------------------------------------------------------------------ home side
@app.route("/home_read", methods=["POST"])
def home_read():
    d = request.get_json(force=True)
    key, who = d["key"], d["node"]
    with dirlock(key):
        e = entry(key)
        if e["owner"] is not None and e["owner"] != who:
            ops["fetches"] += 1
            r = call(e["owner"], "/fetch", {"key": key, "invalidate": False})
            e["value"], e["version"] = r["value"], r["version"]
            e["sharers"].add(e["owner"])
            e["owner"] = None
        e["sharers"].add(who)
        return jsonify({"value": e["value"], "version": e["version"]})


@app.route("/home_write", methods=["POST"])
def home_write():
    d = request.get_json(force=True)
    key, who = d["key"], d["node"]
    with dirlock(key):
        e = entry(key)
        if e["owner"] is not None and e["owner"] != who:
            ops["fetches"] += 1
            r = call(e["owner"], "/fetch", {"key": key, "invalidate": True})
            e["value"], e["version"] = r["value"], r["version"]
        for s in [s for s in e["sharers"] if s != who]:      # invalidate every other copy and wait for the acks
            ops["invalidations_sent"] += 1
            call(s, "/inv", {"key": key})
        e["sharers"] = set()
        e["owner"] = who
        return jsonify({"value": e["value"], "version": e["version"]})


@app.route("/home_add", methods=["POST"])
def home_add():
    """Atomic fetch-and-add executed at the home: correct read-modify-write without a client-side lock."""
    d = request.get_json(force=True)
    key, delta, who = d["key"], d["delta"], d["node"]
    with dirlock(key):
        e = entry(key)
        if e["owner"] is not None:
            r = call(e["owner"], "/fetch", {"key": key, "invalidate": True})
            e["value"], e["version"] = r["value"], r["version"]
            e["owner"] = None
        for s in list(e["sharers"]):
            ops["invalidations_sent"] += 1
            call(s, "/inv", {"key": key})
        e["sharers"] = set()
        e["value"] = (e["value"] or 0) + delta
        e["version"] += 1
        ops["adds"] += 1
        return jsonify({"value": e["value"], "version": e["version"]})


@app.route("/home_lock", methods=["POST"])
def home_lock():
    d = request.get_json(force=True)
    key, who = d["key"], d["node"]
    deadline = time.time() + d.get("timeout", 10)
    with lock_cv:
        while key in locks and locks[key] != who:
            ops["lock_waits"] += 1
            if not lock_cv.wait(max(0.01, deadline - time.time())) and time.time() >= deadline:
                return jsonify({"granted": False})
        locks[key] = who
    return jsonify({"granted": True})


@app.route("/home_unlock", methods=["POST"])
def home_unlock():
    d = request.get_json(force=True)
    with lock_cv:
        if locks.get(d["key"]) == d["node"]:
            del locks[d["key"]]
            lock_cv.notify_all()
    return jsonify({"ok": True})


# ------------------------------------------------------------------ cache side
@app.route("/inv", methods=["POST"])
def inv():
    key = request.get_json(force=True)["key"]
    ops["invalidations_received"] += 1
    with cache_lock:
        cache.pop(key, None)
    return jsonify({"ok": True})


@app.route("/fetch", methods=["POST"])
def fetch():
    d = request.get_json(force=True)
    with cache_lock:
        c = cache.get(d["key"])
        if c is None:
            return jsonify({"value": None, "version": 0})
        out = {"value": c["value"], "version": c["version"]}
        if d.get("invalidate"):
            del cache[d["key"]]
        else:
            c["state"] = "S"
    return jsonify(out)


@app.route("/update", methods=["POST"])
def update():
    d = request.get_json(force=True)
    ops["updates_received"] += 1
    with cache_lock:
        c = cache.get(d["key"])
        if c is None or (d["version"], d["origin"]) > (c["version"], c.get("origin", -1)):
            cache[d["key"]] = {"state": "S", "value": d["value"], "version": d["version"], "origin": d["origin"]}
    return jsonify({"ok": True})


# ------------------------------------------------------------------ client facing
@app.route("/read", methods=["POST"])
def read():
    key = request.get_json(force=True)["key"]
    with cache_lock:
        c = cache.get(key)
        if c is not None:
            ops["read_hits"] += 1
            return jsonify({"value": c["value"], "state": c["state"], "hit": True})
    ops["read_misses"] += 1
    r = call(home_of(key), "/home_read", {"key": key, "node": IDX})
    with cache_lock:
        cache[key] = {"state": "S", "value": r["value"], "version": r["version"]}
    return jsonify({"value": r["value"], "state": "S", "hit": False})


@app.route("/write", methods=["POST"])
def write():
    d = request.get_json(force=True)
    key, value = d["key"], d["value"]
    if MODE == "eventual":
        v = time.time_ns()
        with cache_lock:
            cache[key] = {"state": "S", "value": value, "version": v, "origin": IDX}
        for i in PEERS:
            if i != IDX:
                ops["updates_sent"] += 1
                threading.Thread(target=lambda i=i: _safe_update(i, key, value, v), daemon=True).start()
        return jsonify({"ok": True, "hit": True})
    with cache_lock:
        c = cache.get(key)
        if c is not None and c["state"] == "M":
            c["value"] = value
            ops["write_hits"] += 1
            return jsonify({"ok": True, "hit": True})
    ops["write_misses"] += 1
    r = call(home_of(key), "/home_write", {"key": key, "node": IDX})
    with cache_lock:
        cache[key] = {"state": "M", "value": value, "version": r["version"] + 1}
    return jsonify({"ok": True, "hit": False})


def _safe_update(i, key, value, v):
    try:
        net.post(PEERS[i] + "/update", {"key": key, "value": value, "version": v, "origin": IDX}, timeout=2, tag="update")
    except Exception:
        pass


@app.route("/add", methods=["POST"])
def add():
    d = request.get_json(force=True)
    r = call(home_of(d["key"]), "/home_add", {"key": d["key"], "delta": d.get("delta", 1), "node": IDX})
    with cache_lock:
        cache.pop(d["key"], None)
    return jsonify(r)


@app.route("/lock", methods=["POST"])
def lock_():
    d = request.get_json(force=True)
    return jsonify(call(home_of(d["key"]), "/home_lock", {"key": d["key"], "node": IDX, "timeout": d.get("timeout", 10)}, timeout=d.get("timeout", 10) + 2))


@app.route("/unlock", methods=["POST"])
def unlock():
    d = request.get_json(force=True)
    return jsonify(call(home_of(d["key"]), "/home_unlock", {"key": d["key"], "node": IDX}))


@app.route("/state/<key>")
def state(key):
    with cache_lock:
        return jsonify(cache.get(key) or {"state": "I"})


@app.route("/stats")
def stats():
    return jsonify({"node": IDX, "mode": MODE, "ops": dict(ops), "net_msgs": net.counters.get("msgs"), "net_bytes": net.counters.get("bytes"),
                    "msgs_by_kind": {k[5:]: v for k, v in net.counters.snapshot().items() if k.startswith("msgs:")}})


@app.route("/reset", methods=["POST"])
def reset():
    with cache_lock:
        cache.clear()
    directory.clear()
    for k in ops:
        ops[k] = 0
    net.counters = type(net.counters)()
    return jsonify({"ok": True})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "node": IDX})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
