# Milestone 3 — Edge–core–cloud architecture

## Purpose and files

Two edge agents expose /status; the core polls them through HTTP and returns a combined /aggregate response; the cloud aggregator polls the core every five seconds and appends JSON records to results/cloud_log.jsonl.

## Run

The local topology uses host ports 5000–5002. If the root Docker Compose stack is running, stop it first with docker compose down from the project root; this keeps its data volumes while freeing port 5000. Open four PowerShell terminals. In each, change to this folder and run one command:

~~~powershell
cd .\milestone3
python -m pip install flask psutil requests
python edge_node.py edge-node-a 5001
~~~

~~~powershell
cd .\milestone3
python edge_node.py edge-node-b 5002
~~~

~~~powershell
cd .\milestone3
python core_node.py
~~~

~~~powershell
cd .\milestone3
python cloud_aggregator.py
~~~

In a fifth terminal, inspect the core response:

~~~powershell
Invoke-RestMethod http://127.0.0.1:5000/aggregate | ConvertTo-Json -Depth 5
~~~

## Expected observation

The aggregate JSON should include reports for edge-node-a and edge-node-b. The cloud terminal should print “Stored one record from core” every five seconds, and results/cloud_log.jsonl should grow by one JSON line each interval.

## What this demonstrates and limits

This shows an edge–core–cloud message path and aggregation. It runs on one computer, uses synchronous HTTP polling, and has no durable cloud replication. Stop each process with Ctrl+C when finished.
