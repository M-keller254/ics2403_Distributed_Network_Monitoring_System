# Milestone 11 — RPC and distributed state

## Call the live collector

With the root Compose stack running, open a terminal in this folder and run:

~~~powershell
python milestone_experiment.py
~~~

The client calls the collector HTTP status endpoint and prints current node readings. Expected output includes edge-node-a, edge-node-b, and core-node.

## Exercise the JSON-RPC and versioned state APIs

From a second terminal, run:

~~~powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:5000/rpc -ContentType application/json -Body '{"method":"status"}' | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Put -Uri http://127.0.0.1:5000/state/alert-threshold -ContentType application/json -Body '{"value":75,"expected_version":0}'
Invoke-RestMethod http://127.0.0.1:5000/state/alert-threshold
~~~

Expected result: RPC returns node status; the first state write creates version 1; the GET returns value 75 and version 1.

Run the PUT example once on a fresh database. If you run it again, use the current expected_version returned by the GET (version 1 after the first write), or the server correctly returns a version-conflict response.

## What this demonstrates and limits

The client uses HTTP/JSON as the RPC transport. The state endpoint uses a version check to reject stale writes. State is stored by one collector in SQLite; it is not replicated consensus state.
