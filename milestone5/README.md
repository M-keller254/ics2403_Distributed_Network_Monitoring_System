# Milestone 5 — Distributed transactions

## Run the separate milestone exercise

From this folder, run:

~~~powershell
python milestone_experiment.py
~~~

Expected output: the first case commits after both simulated participants prepare. The second forces edge-node-b to vote no and aborts. This small folder exercise is a deterministic teaching simulation; it does not contact the Docker agents.

## Run the integrated 2PC transaction

With the root Docker Compose stack running, run this command from the project root in a second terminal:

~~~powershell
python experiments\transaction_experiment.py
~~~

The live experiment contacts all three agents, runs one commit and one forced abort, prints their decisions and latency, and saves results/transactions.csv. You can also run one live commit directly:

~~~powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:5000/transactions/two-phase -ContentType application/json -Body '{"value":{"alert_threshold":75}}' | ConvertTo-Json -Depth 5
~~~

To exercise the abort path, run:

~~~powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:5000/transactions/two-phase -ContentType application/json -Body '{"value":{"alert_threshold":80},"fail_prepare_node":"edge-node-b"}' | ConvertTo-Json -Depth 5
~~~

Expected response: edge-node-b votes no and the coordinator reports decision abort.

## Expected observation and limits

For a successful transaction the agents vote yes and the coordinator reports decision commit. With the forced no vote, it reports abort. Check recovery_pending and the prepared/completed participant lists. The integrated coordinator and agents persist transaction state in SQLite, but this classroom 2PC protocol can block during network partitions and is not a consensus system.
