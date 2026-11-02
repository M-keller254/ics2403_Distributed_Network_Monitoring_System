# Milestone 1 — Distributed OS foundation

Run three independent Linux containers on a Docker bridge. Each agent samples metrics locally and sends them to the collector using HTTP. The services share no process memory; service DNS and message passing are the communication mechanisms.

Explore: stop one agent and observe that the remaining agents still report. The collector marks old readings stale after STALE_AFTER_SECONDS.
