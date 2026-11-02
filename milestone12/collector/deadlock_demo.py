"""
Milestone 6 - Concurrency and Deadlock Management.

Two shared resources represent real contention points in the
monitoring system:
    R1 = node_registry_lock   (the set of known nodes)
    R2 = stats_lock           (aggregate counters)

Three endpoint pairs demonstrate three different strategies:

  /unsafe/*   - two operations acquire R1 and R2 in OPPOSITE order.
                Under concurrent load this reproduces a genuine
                thread deadlock (Pi -> Rj cycle).
  /safe/*     - both operations acquire R1 THEN R2 (a fixed global
                order). This is deadlock PREVENTION: a cycle can
                never form because every path is the same order.
  /detect/*   - both operations use try-acquire with a timeout. If a
                lock can't be obtained in time, the operation releases
                whatever it already holds and reports a detected
                deadlock instead of hanging forever. This is deadlock
                DETECTION + RECOVERY.

This module is imported by collector/app.py, which exposes the
endpoints; it is written as its own file so the three strategies are
easy to point to individually in the report.
"""
import threading
import time

registry_lock = threading.Lock()
stats_lock = threading.Lock()

WORK_TIME = 0.4      # time held between acquiring the two locks - makes
                      # the deadlock window wide enough to reproduce reliably


def unsafe_transfer():
    with registry_lock:
        time.sleep(WORK_TIME)
        with stats_lock:
            return "transfer ok (unsafe order: registry -> stats)"


def unsafe_reconcile():
    with stats_lock:
        time.sleep(WORK_TIME)
        with registry_lock:
            return "reconcile ok (unsafe order: stats -> registry)"


def safe_transfer():
    with registry_lock:
        time.sleep(WORK_TIME)
        with stats_lock:
            return "transfer ok (safe order: registry -> stats)"


def safe_reconcile():
    # Same fixed order as safe_transfer (registry -> stats): prevention
    # by lock ordering means a cycle can never form.
    with registry_lock:
        time.sleep(WORK_TIME)
        with stats_lock:
            return "reconcile ok (safe order: registry -> stats)"


def detect_transfer(timeout=0.5):
    got_r = registry_lock.acquire(timeout=timeout)
    if not got_r:
        return "ABORTED: could not acquire registry_lock (deadlock avoided)"
    try:
        time.sleep(WORK_TIME)
        got_s = stats_lock.acquire(timeout=timeout)
        if not got_s:
            return "DEADLOCK DETECTED: releasing registry_lock and aborting transfer"
        try:
            return "transfer ok (detect/recover mode)"
        finally:
            stats_lock.release()
    finally:
        registry_lock.release()


def detect_reconcile(timeout=0.5):
    got_s = stats_lock.acquire(timeout=timeout)
    if not got_s:
        return "ABORTED: could not acquire stats_lock (deadlock avoided)"
    try:
        time.sleep(WORK_TIME)
        got_r = registry_lock.acquire(timeout=timeout)
        if not got_r:
            return "DEADLOCK DETECTED: releasing stats_lock and aborting reconcile"
        try:
            return "reconcile ok (detect/recover mode)"
        finally:
            registry_lock.release()
    finally:
        stats_lock.release()
