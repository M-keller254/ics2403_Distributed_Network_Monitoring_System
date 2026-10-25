# Distributed Network Monitoring System

Theme 2 coursework project for ICS 2403. This project evolves an edge–core monitoring topology through twelve milestones. The root Docker Compose stack runs the integrated version; the milestone folders contain focused instructions and experiments for each stage.

## Start the integrated system

Requirements: Docker Desktop with Compose v2 and PowerShell.

From this project folder, create a local environment file only if one does not already exist, then pull the images and start the system:

~~~powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose pull
docker compose up --build
~~~

Leave that terminal running. In another terminal in this folder, confirm the services and reports:

~~~powershell
docker compose ps
Invoke-RestMethod http://127.0.0.1:5000/status
Invoke-RestMethod http://127.0.0.1:5000/aggregate | ConvertTo-Json -Depth 5
~~~

Expected services: collector, edge-node-a, edge-node-b, core-node, MinIO, Prometheus, and Grafana. The status response should list recent reports from all three monitoring nodes.

## Live pages

- Collector status: http://127.0.0.1:5000/status
- Live aggregate: http://127.0.0.1:5000/aggregate
- Prometheus: http://127.0.0.1:9090
- Grafana dashboard: http://127.0.0.1:3000
- MinIO Console: http://127.0.0.1:9001

Grafana username is admin; use GRAFANA_PASSWORD from .env. MinIO credentials are MINIO_ROOT_USER and MINIO_ROOT_PASSWORD from .env. The dashboard refreshes every five seconds. Services bind to localhost for this coursework setup.

To stop the stack, press Ctrl+C in the terminal running Compose. Named volumes preserve collector, node, Prometheus, Grafana, and MinIO data. docker compose down removes the containers but preserves these volumes; do not use docker compose down -v unless you intentionally want to delete the stored data.

## Milestone folders and commands

Each milestone has its own README with the run command, expected observation, evidence to retain, and limitations.

| Milestone | Folder | Main topic |
|---|---|---|
| 1 | milestone1/README.md | Distributed OS foundation and live container topology |
| 2 | milestone2/README.md | Distributed processing and performance measurements |
| 3 | milestone3/README.md | Edge–core–cloud architecture and aggregation |
| 4 | milestone4/README.md | Coordination and Lamport clocks |
| 5 | milestone5/README.md | Distributed transactions and 2PC |
| 6 | milestone6/README.md | Concurrency and deadlock detection |
| 7 | milestone7/README.md | Fault tolerance and recovery |
| 8 | milestone8/README.md | Distributed models and event ordering |
| 9 | milestone9/README.md | Transparency and node health |
| 10 | milestone10/README.md | Naming and service discovery |
| 11 | milestone11/README.md | RPC and versioned distributed state |
| 12 | milestone12/README.md | MinIO object storage and capstone archive |

Milestones 2–4 use standalone local processes on ports 5000–5002, so stop the Compose stack before running them. Milestones 5–10 have standalone concept exercises; their READMEs distinguish those demonstrations from the integrated API checks. Milestones 11 and 12 use the running integrated stack.

## Python requirements and unit tests

To run project tests locally, create a virtual environment if desired, then install the pinned requirements and use unittest:

~~~powershell
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
~~~

## Scope and limitations

This is a classroom prototype running on one computer. The collector and MinIO server are single instances, not replicated high-availability services. Some milestone folders intentionally isolate concepts with simulations; a successful simulation demonstrates that example only. Use the experiments and ENGINEERING_LOG_TEMPLATE.md to record repeatable performance and failure-recovery evidence for the report.
