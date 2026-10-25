# Milestone 6 — Concurrency and deadlocks

## Run the separate milestone exercise

From this folder, run:

~~~powershell
python milestone_experiment.py
~~~

Expected output:

- Circular wait detected: True means the first wait-for graph contains a cycle.
- Cycle-free graph detected: False means the second graph has no cycle.
- Ordered lock operation result: completed safely shows the example lock pair uses a consistent order.

## Check the integrated collector endpoint

With the root Compose stack running, send a wait-for graph to the collector:

~~~powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:5000/deadlock/check -ContentType application/json -Body '{"wait_for_graph":{"p1":["p2"],"p2":["p1"]}}'
~~~

Expected response: deadlock_detected is True.

## What this demonstrates and limits

The graph algorithm detects a cycle in a supplied graph; it does not discover or resolve arbitrary operating-system deadlocks. The ordered-lock example is a local teaching demonstration, not a distributed lock manager.
