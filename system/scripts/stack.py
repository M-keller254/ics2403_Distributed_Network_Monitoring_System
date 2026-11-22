"""Reusable stack builders for experiments/tests (local processes, mirrors docker-compose).

edge_core_cloud(c, base)  -> cloud + N core replicas (Bully) + edge gateway
Each returns a dict of service names -> urls. Ports are base+offset so runs never collide.
"""
import time

import requests


def wait_for(fn, timeout=30, interval=0.2, what="condition"):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {what}")


def edge_core_cloud(c, base=21000, n_cores=3, transport="kafka", gateway_env=None, core_env=None, cloud_env=None, with_cloud=True, core_cpus=None, core_mem_mb=None):
    names = {"gateway": "gateway", "cores": [f"core{i}" for i in range(1, n_cores + 1)], "cloud": "cloud"}
    ports = {f"core{i}": base + 10 + i for i in range(1, n_cores + 1)}
    peers = ",".join(f"{i}=http://127.0.0.1:{ports[f'core{i}']}" for i in range(1, n_cores + 1))
    if with_cloud:
        c.start("cloud", "system.cloud.service", base + 2, env=dict(cloud_env or {}))
    for i in range(1, n_cores + 1):
        env = {"NODE_ID": i, "CORE_ID": f"core-{i}", "PEERS": peers, "TRANSPORT": transport,
               "CLOUD_URL": f"http://127.0.0.1:{base + 2}/digest", "DATA_DIR": f"{c.workdir}/core-data", "STARTUP_DELAY": 1.5,
               "DIGEST_INTERVAL": 2}
        env.update(core_env or {})
        c.start(f"core{i}", "system.core.service", ports[f"core{i}"], env=env, cpus=core_cpus, mem_mb=core_mem_mb)
    genv = {"TRANSPORT": transport, "CORE_URLS": ",".join(f"http://127.0.0.1:{p}" for p in ports.values())}
    genv.update(gateway_env or {})
    c.start("gateway", "system.edge.gateway", base, env=genv)
    wait_for(lambda: any(c.get(n, "/leader")["is_leader"] for n in names["cores"]), 30, what="a Bully leader")
    return names


def current_leader(c, names):
    for n in names["cores"]:
        if not c.alive(n):
            continue
        try:
            if c.get(n, "/leader", timeout=1)["is_leader"]:
                return n
        except Exception:
            continue
    return None


def rows_in_db(c, names):
    """Distinct rows persisted across all live cores (each core keeps its own SQLite file)."""
    total = 0
    for n in names["cores"]:
        if c.alive(n):
            try:
                total += c.get(n, "/readings/count", timeout=3)["rows_in_db"]
            except Exception:
                pass
    return total
