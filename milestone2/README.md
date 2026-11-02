# Milestone 2 — Distributed processing and performance

## Purpose and files

Three independent Flask processes expose /status with CPU and memory readings. experiments/run_experiment.py polls them once per second and records HTTP latency, jitter, successful polls, request-failure rate, CPU, memory, and throughput. It writes CSV files under experiments/results/.

## Run the three nodes

Milestone 2 uses host ports 5000–5002. Stop the root Docker stack first so its collector releases port 5000. From the project root, run docker compose down (this preserves named data volumes). Then open three PowerShell terminals and run one node in each:

~~~powershell
cd .\milestone2
python -m pip install flask psutil requests
python core_node.py
~~~

~~~powershell
cd .\milestone2
python edge_node_a.py
~~~

~~~powershell
cd .\milestone2
python edge_node_b.py
~~~

## Run the performance measurement

Open a fourth terminal:

~~~powershell
cd .\milestone2\experiments
python run_experiment.py --duration 60 --label m2-run1
python run_experiment.py --duration 60 --label m2-run2
~~~

Each run creates milestone2/experiments/results/<label>.csv. The nodes must remain running for both runs.

## Expected observation

The script prints a summary for each node and an overall throughput value. With all nodes available, success should be near 100% and request-failure rate near 0%. The exact latency, jitter, and CPU values depend on the computer and workload. Compare the two fresh CSV files; do not assume older sample results in PERFORMANCE.md will match your machine.

The reported “loss” is failed HTTP polls, not packet loss measured by a network packet capture.

## Evidence and limitations

Keep both CSV files and record the machine, Python version, duration, and poll interval. All three processes run on one computer, so these results do not represent latency across a real wide-area network. CPU and memory readings are system-wide host readings.
