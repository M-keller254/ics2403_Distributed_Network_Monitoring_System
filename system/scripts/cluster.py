"""Local process cluster for experiments and tests (no Docker needed).

Starts any service module as a subprocess with its own env/port, waits for /health,
and can kill (crash), stop, restart and fault-inject it. Mirrors docker-compose, so an
experiment written against this runs against the real stack by changing only base URLs
(see docs/run_with_docker.md).

    with Cluster("/tmp/run1") as c:
        c.start("core1", "system.core.service", port=5001, env={...})
        c.kill("core1")           # SIGKILL = crash failure
        c.restart("core1")
"""
import os
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import requests

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


class Cluster:
    def __init__(self, workdir=None, clean=True):
        self.workdir = workdir or tempfile.mkdtemp(prefix="cluster-")
        if clean and os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir, ignore_errors=True)
        os.makedirs(self.workdir, exist_ok=True)
        self.procs = {}
        self.specs = {}
        self.base_env = {"BUS": "local", "LOCAL_BUS_DIR": os.path.join(self.workdir, "bus"),
                         "OBJECT_STORE": "local", "OBJECT_STORE_DIR": os.path.join(self.workdir, "objects"),
                         "PYTHONPATH": REPO_ROOT, "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1"}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop_all()

    def url(self, name):
        return f"http://127.0.0.1:{self.specs[name]['port']}"

    def start(self, name, module, port, env=None, wait=True, health="/health", timeout=30, cpus=None, mem_mb=None):
        """cpus: list of CPU ids the process may run on (CPU capacity); mem_mb: address-space cap via RLIMIT_AS (memory)."""
        env = dict(env or {})
        env["PORT"] = str(port)
        self.specs[name] = {"module": module, "port": port, "env": env, "health": health, "cpus": cpus, "mem_mb": mem_mb}
        return self._spawn(name, wait, timeout)

    def _spawn(self, name, wait=True, timeout=30):
        s = self.specs[name]
        e = dict(os.environ)
        e.update(self.base_env)
        e.update({k: str(v) for k, v in s["env"].items()})
        log = open(os.path.join(self.workdir, f"{name}.log"), "ab")
        cpus, mem_mb = s.get("cpus"), s.get("mem_mb")

        def limits():
            if cpus:
                os.sched_setaffinity(0, set(cpus))
            if mem_mb:
                resource.setrlimit(resource.RLIMIT_AS, (int(mem_mb) << 20, int(mem_mb) << 20))
        p = subprocess.Popen([sys.executable, "-m", s["module"]], env=e, cwd=REPO_ROOT, stdout=log, stderr=subprocess.STDOUT,
                             preexec_fn=limits if (cpus or mem_mb) else None)
        self.procs[name] = p
        if wait and s["health"]:
            self.wait_healthy(name, timeout)
        return p

    def wait_healthy(self, name, timeout=30):
        s, end = self.specs[name], time.time() + timeout
        while time.time() < end:
            if self.procs[name].poll() is not None:
                raise RuntimeError(f"{name} exited early (see {self.workdir}/{name}.log)")
            try:
                if requests.get(self.url(name) + s["health"], timeout=1).status_code == 200:
                    return True
            except requests.RequestException:
                pass
            time.sleep(0.15)
        raise TimeoutError(f"{name} not healthy after {timeout}s (see {self.workdir}/{name}.log)")

    def alive(self, name):
        p = self.procs.get(name)
        return p is not None and p.poll() is None

    def kill(self, name):
        """SIGKILL: a crash failure (no cleanup, no goodbye)."""
        p = self.procs.get(name)
        if p and p.poll() is None:
            p.kill()
            p.wait(5)

    def stop(self, name):
        p = self.procs.get(name)
        if p and p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                p.kill()

    def pause(self, name):
        self.procs[name].send_signal(signal.SIGSTOP)

    def resume(self, name):
        self.procs[name].send_signal(signal.SIGCONT)

    def restart(self, name, wait=True, timeout=30):
        self.kill(name)
        return self._spawn(name, wait, timeout)

    def stop_all(self):
        for n in list(self.procs):
            p = self.procs[n]
            if p.poll() is None:
                try:
                    p.send_signal(signal.SIGCONT)
                except Exception:
                    pass
                self.kill(n)

    def get(self, name, path, timeout=3):
        return requests.get(self.url(name) + path, timeout=timeout).json()

    def post(self, name, path, payload=None, timeout=5):
        r = requests.post(self.url(name) + path, json=payload or {}, timeout=timeout)
        return r.status_code, r.json()

    def log_tail(self, name, n=15):
        try:
            return "".join(open(os.path.join(self.workdir, f"{name}.log"), errors="ignore").readlines()[-n:])
        except FileNotFoundError:
            return ""


def free_ports(n, start=20000):
    """Pick n free TCP ports (so concurrent experiments never collide)."""
    import socket
    out, p = [], start
    while len(out) < n:
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", p))
                out.append(p)
            except OSError:
                pass
        p += 1
    return out
