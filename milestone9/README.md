# Milestone 9 — Transparency and node health

## Run the separate milestone exercise

From this folder, run:

~~~powershell
python milestone_experiment.py
~~~

Expected output: one report is healthy, the old report is stale, and the node that has never reported is unknown.

## Check live node health

With the root Compose stack running, run:

~~~powershell
Invoke-RestMethod http://127.0.0.1:5000/nodes/edge-node-a
~~~

The response should include a health state, report age, and CPU/memory readings. Open the Grafana dashboard at http://127.0.0.1:3000 for live CPU, memory, and accepted-report plots.

## What this demonstrates and limits

The API hides storage details and presents a stable node-health view. Its state is based on the last received report and configured stale threshold; it is not a guarantee that the physical host or every service is healthy.
