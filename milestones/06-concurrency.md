# Milestone 6 — Concurrency and deadlock

Agents serve threaded Flask endpoints, protect their Lamport/transaction state with locks, and the collector can poll them in parallel. POST /deadlock/check detects cycles in a supplied wait-for graph. experiments/compare_models.py records serial versus parallel live polling time. This demonstrates concurrency and deadlock reasoning; it is not a general distributed lock manager.
