# Milestone 1 — Distributed OS foundation

## Purpose

Start the edge–core–collector system as separate processes in Docker containers. Each agent samples CPU, memory, and network counters and sends a report to the collector over HTTP. Compose provides separate container processes and service-name networking.

The implementation is shared with the integrated project at the parent folder: docker-compose.yml, agent/agent.py, agent/service.py, and collector/app.py. This folder is for the milestone instructions and evidence; it does not duplicate that code.

## Run

Open PowerShell in the project root (the folder containing docker-compose.yml and requirements.txt). Create .env from .env.example only if .env does not already exist. Then run:

~~~powershell
docker compose up --build
~~~

Leave that terminal running. In a second terminal, also in the project root, check the services:

~~~powershell
docker compose ps
Invoke-RestMethod http://127.0.0.1:5000/status
~~~

## Expected observation

Seven services should be listed as running: collector, edge-node-a, edge-node-b, core-node, MinIO, Prometheus, and Grafana. The collector status should show recent reports for the three monitoring nodes. The Grafana dashboard is at http://127.0.0.1:3000.

## What this demonstrates

The agents are separate processes communicating with a central service by messages over the container network. They share the host operating system and physical machine, so this is a distributed-process demonstration, not a multi-computer or production cluster.

## Evidence to save

Save a screenshot of docker compose ps with the services running and a screenshot or output of /status listing all three nodes. Record the host OS, Docker version, Python version, and date.
