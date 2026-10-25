# Milestone 8 — Distributed models and event ordering

## Run the vector-clock exercise

From this folder, run:

~~~powershell
python milestone_experiment.py
~~~

Expected output: Before vs after is before; Independent events is concurrent; and the merged clock contains both edge counters. This demonstrates causal ordering and concurrent events in a vector-clock model.

## Compare integrated serial and parallel polling

With the root Compose stack running, open a second terminal in the project root and run:

~~~powershell
python experiments\compare_models.py --trials 20
~~~

The experiment requests /aggregate in serial and parallel modes and saves raw trials to results/model_comparison.csv. Compare the mean elapsed times and failed-node counts printed at the end.

## What this demonstrates and limits

The CSV compares two polling strategies as model proxies. It is not a full simulation of every distributed-system model or a multi-host network benchmark. Record the trial count, hardware, and date when reporting results.
