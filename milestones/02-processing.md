# Milestone 2 — Distributed processing and performance

Reports carry a source timestamp plus collector receipt time. The Prometheus endpoint exports each node's latest CPU and memory values and accepted report count. Explore REPORT_INTERVAL and Prometheus scrape interval independently. Compare report volume and freshness; remember CPU values are sampled inside the container and may not equal host-wide utilization.
