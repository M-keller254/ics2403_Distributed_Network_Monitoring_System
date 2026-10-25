# Milestone 10 — Naming and service discovery

## Run the lease exercise

From this folder, run:

~~~powershell
python milestone_experiment.py
~~~

Expected output: the collector URL resolves at time 29 and returns None at time 30 after its lease expires.

## Check the integrated registry

With the root Compose stack running, run:

~~~powershell
Invoke-RestMethod http://127.0.0.1:5000/registry
~~~

The response should list the configured edge and core services. The integrated collector also supports POST registration with an endpoint and a lease. Its registry data is held in the collector SQLite database.

## What this demonstrates and limits

The folder exercise is an in-memory lease demonstration. The integrated registry exposes service names and endpoints through one collector; it is not a replicated service-discovery system.
