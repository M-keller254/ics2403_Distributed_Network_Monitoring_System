"""Client for the HDFS-style file system: put / get / ls / rm with chunking, pipelined replication and replica failover."""
import hashlib

import requests


class DfsClient:
    def __init__(self, namenode_url, chunk_size=1 << 20, replication=None):
        self.nn, self.chunk_size, self.replication = namenode_url.rstrip("/"), chunk_size, replication
        self.s = requests.Session()
        self.stats = {"bytes_written": 0, "bytes_read": 0, "failovers": 0, "checksum_errors": 0, "chunk_writes": 0}

    def put(self, path, data):
        parts = [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)] or [b""]
        body = {"path": path, "n_chunks": len(parts)}
        if self.replication:
            body["replication"] = self.replication
        alloc = self.s.post(self.nn + "/allocate", json=body, timeout=5)
        alloc.raise_for_status()
        meta = []
        for part, a in zip(parts, alloc.json()["chunks"]):
            dns = a["datanodes"]
            sha = hashlib.sha256(part).hexdigest()
            r = self.s.put(dns[0]["url"] + f"/chunk/{a['chunk_id']}", data=part, timeout=15,
                           headers={"X-Checksum": sha, "X-Forward": ",".join(d["url"] for d in dns[1:])})
            r.raise_for_status()
            self.stats["chunk_writes"] += 1
            self.stats["bytes_written"] += len(part)
            meta.append({"chunk_id": a["chunk_id"], "size": len(part), "checksum": sha, "holders": [d["name"] for d in dns], "want": len(dns)})
        self.s.post(self.nn + "/commit", json={"path": path, "chunks": meta}, timeout=5).raise_for_status()
        return len(parts)

    def get(self, path):
        loc = self.s.get(self.nn + "/locate", params={"path": path}, timeout=5)
        loc.raise_for_status()
        out = []
        for c in loc.json()["chunks"]:
            got = None
            for i, rep in enumerate(c["replicas"]):
                try:
                    r = self.s.get(rep["url"] + f"/chunk/{c['chunk_id']}", timeout=5)
                except requests.RequestException:
                    self.stats["failovers"] += 1
                    continue
                if r.status_code == 200 and hashlib.sha256(r.content).hexdigest() == c["checksum"]:
                    got = r.content
                    break
                self.stats["checksum_errors"] += int(r.status_code == 409 or r.status_code == 200)
                self.stats["failovers"] += 1
            if got is None:
                raise IOError(f"chunk {c['chunk_id']} of {path} unavailable on all replicas")
            self.stats["bytes_read"] += len(got)
            out.append(got)
        return b"".join(out)

    def ls(self, prefix=""):
        return self.s.get(self.nn + "/ls", params={"prefix": prefix}, timeout=5).json()["files"]

    def rm(self, path):
        return self.s.delete(self.nn + "/file", params={"path": path}, timeout=5).json()["ok"]

    def status(self):
        return self.s.get(self.nn + "/status", timeout=5).json()
