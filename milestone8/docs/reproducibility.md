# Reproducibility Record

Fill this in once, then update it whenever a tool version changes.
A second researcher should be able to reproduce your results using
only this file plus the source code.

## Hardware
- Machine: <e.g. HP laptop, model>
- CPU: <run `wmic cpu get name` on Windows>
- RAM: <total system RAM>

## Operating System
- Host OS: Windows <version> (check: Settings → System → About)
- WSL2 distro: Ubuntu <version> (check: `wsl --list --verbose`)

## Container Platform
- Docker Desktop version: <run `docker --version`>
- Docker Compose version: <run `docker compose version`>

## Languages / Runtimes
- Python version (host): <run `python --version`>
- Python version (containers): 3.13-slim (pinned in Dockerfiles)

## Frameworks / Libraries (pinned versions)
Collector (`collector/requirements.txt`):
- flask==3.0.3

Agent (`agent/requirements.txt`):
- psutil==6.0.0
- requests==2.32.3

## Network Configuration
- Docker network: `monitor-net` (bridge driver, created by Compose)
- Collector exposed on host port: 5000
- Inter-container communication: internal Docker DNS
  (e.g. agents reach the collector at `http://collector:5000`)

## Resource Limits (per container)
- edge-node-a: 0.5 CPU, 128 MB memory
- edge-node-b: 0.5 CPU, 128 MB memory
- core-node: 1.0 CPU, 256 MB memory

## Experimental Parameters
- Report interval: 5 seconds (set via REPORT_INTERVAL env var)
- Run duration for each experiment: <fill in per experiment>
- Random seeds: <fill in once you add any randomized load generation>

## How to Reproduce
```powershell
git clone <your-repo-url>
cd monitoring-system
docker compose up --build
```
Then in a separate terminal:
```powershell
Invoke-RestMethod http://localhost:5000/status
```
