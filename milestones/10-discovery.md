# Milestone 10 — Naming, discovery, process management

Compose provides service-name DNS and supervises node processes. The collector also exposes a SQLite-backed lease registry at GET/POST /registry, so services can register and resolve logical names. Compose start/stop/restart commands provide the process lifecycle controls in this lab. The registry is centralized and not replicated.
