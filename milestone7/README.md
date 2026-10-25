# Milestone 7 — Fault tolerance and recovery

## Run the retry exercise

From this folder, run:

~~~powershell
python milestone_experiment.py
~~~

Expected output: a simulated operation recovers on attempt 3 after two temporary failures. The helper uses bounded retries; it does not provide exactly-once delivery or durable queuing.

## Inject a failure into the running project

With the root Compose stack running, open a second terminal in the project root and run:

~~~powershell
python experiments\fault_injection.py --execute
~~~

This command actually stops edge-node-a, measures how long the collector takes to mark it stale, restarts it, and measures recovery. It writes a JSON result under results/. This is an intentional failure test; keep the Compose terminal open and let the experiment finish.

## Expected observation and limits

The local exercise should recover on attempt 3. The live failure test should observe stale then healthy states and report elapsed times. Results depend on the configured stale threshold and machine. The collector and MinIO setup are single instances, so this does not prove zero downtime or replicated fault tolerance.
