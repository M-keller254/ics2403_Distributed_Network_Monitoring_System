# Milestone 3 — Architecture

The project uses an agent/collector architecture: producers push JSON over HTTP, the collector validates and persists it, and Prometheus scrapes the collector. Grafana connects to Prometheus. Docker Compose supplies deployment and DNS-based addressing. The API boundary is the JSON contract in the root README.
