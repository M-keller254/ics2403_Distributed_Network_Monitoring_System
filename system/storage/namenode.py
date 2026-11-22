"""
NameNode of a small HDFS-style distributed file system (Milestone 12).

Holds METADATA only:  file -> ordered chunks -> the datanodes that store each chunk. Data never flows through it.
  - chunks: fixed-size (CHUNK_SIZE), identified by id, each stored on REPLICATION datanodes
  - placement: the least-used live datanodes
  - liveness: datanodes heartbeat with a block report; silent for DEAD_AFTER_S => dead
  - self-healing: a monitor loop finds under-replicated chunks and orders a surviving replica holder's peer to copy it
  POST /allocate {path, n_chunks, replication?} -> [{chunk_id, datanodes:[{name,url}]}]
  POST /commit   {path, chunks:[{chunk_id,size,checksum}]}      GET /locate?path=  GET /ls?prefix=   DELETE /file?path=
  POST /heartbeat {name,url,used,chunks:[...]}     GET /status
Env: PORT, REPLICATION (3), DEAD_AFTER_S (4), META_DIR
"""
import json
import os
import threading
import time
import uuid

import requests
from flask import Flask, jsonify, request

from system.common.config import env_float, env_int, env_str

app = Flask(__name__)
PORT = env_int("PORT", 9000)
REPLICATION = env_int("REPLICATION", 3)
DEAD_AFTER_S = env_float("DEAD_AFTER_S", 4.0)
META_DIR = env_str("META_DIR", "/tmp/namenode")
os.makedirs(META_DIR, exist_ok=True)
META = os.path.join(META_DIR, "fsimage.json")
lock = threading.RLock()
pending = {}      # chunk_id -> allocation time: allocated but not yet committed ("under construction"; must not be garbage-collected as orphans)
PENDING_TTL_S = 300.0
files, chunks, datanodes = {}, {}, {}      # path -> {chunks:[ids],size,created} ; chunk_id -> {size,checksum,holders:set} ; name -> {url,last,used}
stats = {"allocations": 0, "commits": 0, "re_replications": 0, "chunks_lost": 0, "deletes": 0}


def persist():
    with lock:
        tmp = META + ".tmp"
        json.dump({"files": files, "chunks": {k: {**v, "holders": sorted(v["holders"])} for k, v in chunks.items()}}, open(tmp, "w"))
        os.replace(tmp, META)


def load():
    if os.path.exists(META):
        d = json.load(open(META))
        files.update(d["files"])
        for k, v in d["chunks"].items():
            chunks[k] = {**v, "holders": set()}          # holders are rebuilt from the datanodes' block reports


def alive():
    now = time.time()
    return {n: d for n, d in datanodes.items() if now - d["last"] <= DEAD_AFTER_S}


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    d = request.get_json(force=True)
    with lock:
        datanodes[d["name"]] = {"url": d["url"].rstrip("/"), "last": time.time(), "used": d.get("used", 0), "n_chunks": len(d.get("chunks", []))}
        for cid in d.get("chunks", []):
            if cid in chunks:
                chunks[cid]["holders"].add(d["name"])
        now = time.time()
        orphans = [c for c in d.get("chunks", []) if c not in chunks and now - pending.get(c, 0) > PENDING_TTL_S]
    return jsonify({"ok": True, "delete": orphans})


@app.route("/allocate", methods=["POST"])
def allocate():
    d = request.get_json(force=True)
    rep = d.get("replication", REPLICATION)
    with lock:
        live = alive()
        if len(live) < 1:
            return jsonify({"error": "no live datanodes"}), 503
        rep = min(rep, len(live))
        out = []
        for _ in range(d["n_chunks"]):
            cid = uuid.uuid4().hex
            pending[cid] = time.time()
            targets = sorted(live, key=lambda n: (live[n]["used"] + live[n]["n_chunks"], n))[:rep]
            for n in targets:                              # account for what we are about to place so the next chunk spreads out
                live[n]["n_chunks"] += 1
            out.append({"chunk_id": cid, "datanodes": [{"name": n, "url": live[n]["url"]} for n in targets]})
    stats["allocations"] += d["n_chunks"]
    return jsonify({"chunks": out})


@app.route("/commit", methods=["POST"])
def commit():
    d = request.get_json(force=True)
    with lock:
        ids = []
        for c in d["chunks"]:
            chunks[c["chunk_id"]] = {"size": c["size"], "checksum": c["checksum"], "holders": set(c.get("holders", [])), "want": c.get("want", REPLICATION)}
            ids.append(c["chunk_id"])
            pending.pop(c["chunk_id"], None)
        files[d["path"]] = {"chunks": ids, "size": sum(c["size"] for c in d["chunks"]), "created": time.time()}
        stats["commits"] += 1
    persist()
    return jsonify({"ok": True})


@app.route("/locate")
def locate():
    p = request.args["path"]
    with lock:
        f = files.get(p)
        if f is None:
            return jsonify({"error": "no such file"}), 404
        live = alive()
        out = [{"chunk_id": cid, "size": chunks[cid]["size"], "checksum": chunks[cid]["checksum"],
                "replicas": [{"name": n, "url": live[n]["url"]} for n in sorted(chunks[cid]["holders"]) if n in live]} for cid in f["chunks"]]
    return jsonify({"path": p, "size": f["size"], "chunks": out})


@app.route("/ls")
def ls():
    prefix = request.args.get("prefix", "")
    with lock:
        return jsonify({"files": [{"path": p, "size": f["size"], "chunks": len(f["chunks"])} for p, f in sorted(files.items()) if p.startswith(prefix)]})


@app.route("/file", methods=["DELETE"])
def rm():
    p = request.args["path"]
    with lock:
        f = files.pop(p, None)
        for cid in (f or {}).get("chunks", []):
            chunks.pop(cid, None)
    stats["deletes"] += 1
    persist()
    return jsonify({"ok": f is not None})


@app.route("/status")
def status():
    with lock:
        live = alive()
        under = [cid for cid, c in chunks.items() if len([h for h in c["holders"] if h in live]) < min(c.get("want", REPLICATION), max(len(live), 1))]
        lost = [cid for cid, c in chunks.items() if not [h for h in c["holders"] if h in live]]
        return jsonify({"datanodes": {n: {"alive": n in live, "chunks": d["n_chunks"], "used_bytes": d["used"], "silent_s": round(time.time() - d["last"], 1)} for n, d in datanodes.items()},
                        "files": len(files), "chunks": len(chunks), "under_replicated": len(under), "lost": len(lost), **stats})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


def monitor_loop():
    """Self-healing: keep every chunk on `want` live datanodes."""
    while True:
        time.sleep(1.0)
        with lock:
            live = alive()
            todo = []
            for cid, c in chunks.items():
                have = [h for h in c["holders"] if h in live]
                want = min(c.get("want", REPLICATION), len(live))
                if have and len(have) < want:
                    cands = sorted([n for n in live if n not in c["holders"]], key=lambda n: live[n]["n_chunks"])
                    if cands:
                        todo.append((cid, live[have[0]]["url"], cands[0], live[cands[0]]["url"]))
                        live[cands[0]]["n_chunks"] += 1
                elif not have and c["holders"]:
                    stats["chunks_lost"] = max(stats["chunks_lost"], 1)
        for cid, src, dst_name, dst_url in todo:
            try:
                r = requests.post(dst_url + "/replicate_from", json={"chunk_id": cid, "source": src}, timeout=5)
                if r.status_code == 200:
                    with lock:
                        chunks[cid]["holders"].add(dst_name)
                    stats["re_replications"] += 1
            except requests.RequestException:
                pass


if __name__ == "__main__":
    load()
    threading.Thread(target=monitor_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
