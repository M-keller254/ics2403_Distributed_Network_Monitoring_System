"""Measurement helpers shared by services and experiments.

Definitions used throughout the project (brief, milestone 2):
  throughput = completed work / time
  latency    = T_response - T_request
  jitter     = standard deviation of latency (and of inter-arrival gaps)
"""
import math
import statistics
import threading


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] if lo == hi else s[lo] + (s[hi] - s[lo]) * (k - lo)


def summarize(values, unit_scale=1.0, ndigits=3):
    """Summary of a list of latency samples: n, mean, p50, p95, p99, min, max, jitter(stdev)."""
    v = [x * unit_scale for x in values]
    if not v:
        return {"n": 0}
    r = lambda x: round(x, ndigits)
    return {
        "n": len(v), "mean": r(statistics.mean(v)), "p50": r(percentile(v, 50)),
        "p95": r(percentile(v, 95)), "p99": r(percentile(v, 99)),
        "min": r(min(v)), "max": r(max(v)),
        "jitter": r(statistics.pstdev(v)) if len(v) > 1 else 0.0,
    }


def mean_ci95(values):
    """(mean, half-width of the 95% CI) using the normal approximation (t=1.96; use n>=10)."""
    if len(values) < 2:
        return (values[0] if values else None, None)
    m = statistics.mean(values)
    return m, 1.96 * statistics.stdev(values) / math.sqrt(len(values))


class Counters:
    """Thread-safe named counters, e.g. messages sent per type (message complexity)."""

    def __init__(self):
        self._d = {}
        self._lock = threading.Lock()

    def inc(self, name, n=1):
        with self._lock:
            self._d[name] = self._d.get(name, 0) + n

    def get(self, name):
        with self._lock:
            return self._d.get(name, 0)

    def snapshot(self):
        with self._lock:
            return dict(self._d)
