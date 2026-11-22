"""Shared experiment utilities: result files, load driver, resource sampler, dataset cache."""
import csv
import json
import os
import statistics
import threading
import time

import psutil
import requests

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RESULTS = os.path.join(REPO, "results")
os.makedirs(RESULTS, exist_ok=True)
_DATASET = {}


def dataset(limit=None):
    from system.edge.replayer import load_rows
    key = limit
    if key not in _DATASET:
        _DATASET[key] = load_rows(limit=limit)
    return _DATASET[key]


def save_csv(name, rows):
    if not rows:
        return
    path = os.path.join(RESULTS, name)
    fields = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"  saved {os.path.relpath(path, REPO)} ({len(rows)} rows)")


def save_json(name, obj):
    path = os.path.join(RESULTS, name)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)
    print(f"  saved {os.path.relpath(path, REPO)}")


class ResourceSampler:
    """Samples CPU% and RSS of a set of pids (and whole-host CPU) every `interval` seconds."""

    def __init__(self, pids, interval=0.25):
        self.procs = []
        for p in pids:
            try:
                pr = psutil.Process(p)
                pr.cpu_percent(None)
                self.procs.append(pr)
            except psutil.Error:
                pass
        self.interval, self.cpu, self.rss, self.host, self._stop = interval, [], [], [], threading.Event()
        psutil.cpu_percent(None)
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.wait(self.interval):
            c = m = 0.0
            for pr in self.procs:
                try:
                    c += pr.cpu_percent(None)
                    m += pr.memory_info().rss
                except psutil.Error:
                    pass
            self.cpu.append(c)
            self.rss.append(m / 1e6)
            self.host.append(psutil.cpu_percent(None))

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *a):
        self._stop.set()
        self._t.join()

    def summary(self):
        f = lambda v: round(statistics.mean(v), 1) if v else None
        return {"svc_cpu_pct_mean": f(self.cpu), "svc_cpu_pct_max": round(max(self.cpu), 1) if self.cpu else None,
                "svc_rss_mb_mean": f(self.rss), "host_cpu_pct_mean": f(self.host)}


def drive_bulk(url, rows, chunk=500, retries=0, timeout=15):
    """POST rows to url+'/bulk' in chunks. Returns dict(latencies_s, ok_rows, failed_requests, requests)."""
    sess = requests.Session()
    lat, ok_rows, failed, n = [], 0, 0, 0
    for i in range(0, len(rows), chunk):
        part = rows[i:i + chunk]
        for attempt in range(retries + 1):
            n += 1
            t0 = time.perf_counter()
            try:
                r = sess.post(url + "/bulk", json={"rows": part}, timeout=timeout)
                dt = time.perf_counter() - t0
                if r.status_code == 200:
                    lat.append(dt)
                    ok_rows += len(part)
                    break
                failed += 1
            except requests.RequestException:
                failed += 1
    return {"latencies_s": lat, "ok_rows": ok_rows, "failed_requests": failed, "requests": n}


def jain_index(xs):
    """Jain's fairness index: 1.0 = perfectly even load distribution, 1/n = all load on one node."""
    s = sum(xs)
    return round(s * s / (len(xs) * sum(x * x for x in xs)), 4) if xs and s else None


def partition_by_site(rows, n):
    sites = sorted({r["site_id"] for r in rows})
    owner = {s: i % n for i, s in enumerate(sites)}
    parts = [[] for _ in range(n)]
    for r in rows:
        parts[owner[r["site_id"]]].append(r)
    return parts
