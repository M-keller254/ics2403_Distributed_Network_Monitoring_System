# Milestone 4 — Coordination and logical clocks

## Purpose and files

The edge and core services maintain Lamport logical clocks. /status returns the current clock; /sync receives a remote clock and advances the local clock. coordination_experiment.py sends synchronization messages among all three nodes for 20 rounds and writes results/coordination.csv.

## Run the nodes

The local topology uses host ports 5000–5002. Stop the root Compose stack first with docker compose down from the project root so port 5000 is free; this preserves its data volumes. Open three PowerShell terminals and run:

~~~powershell
cd .\milestone4
python -m pip install flask psutil requests
python core_node.py
~~~

~~~powershell
cd .\milestone4
python edge_node.py edge-node-a 5001 0
~~~

~~~powershell
cd .\milestone4
python edge_node.py edge-node-b 5002 2
~~~

The final number is the simulated wall-clock skew in seconds. Open a fourth terminal:

~~~powershell
cd .\milestone4
python coordination_experiment.py
~~~

## Expected observation

The experiment runs 20 rounds and should report 180 total messages (three node status requests plus six inter-node sync messages per round). It prints synchronization delay and Lamport/wall-clock ordering violations, then saves the raw per-message observations to results/coordination.csv. Lamport ordering violations should be zero; wall-clock ordering can disagree because one edge has a simulated two-second clock skew.

## What this demonstrates and limits

Lamport clocks represent causal ordering; they do not synchronize physical clocks or prove that messages arrive on time. This is a single-machine classroom experiment. Stop each service with Ctrl+C when finished.
