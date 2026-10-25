# Milestone 12 — Storage and capstone archive

## Purpose

The experiment sends a sample monitoring record to the integrated collector, which stores the JSON object in the S3-compatible MinIO bucket and lists it back.

## Run

If the project stack is not running, open a PowerShell terminal in this folder and start Compose from the parent project folder:

~~~powershell
Set-Location ..
docker compose up --build
~~~

Leave that terminal running. In another terminal in this folder, run:

~~~powershell
python -m pip install requests
python milestone_experiment.py
~~~

## Expected observation

The experiment prints bucket telemetry-archive, object name milestone12/sample-reading.json, its size/etag, and a listing containing the object. To view it in the MinIO console, open http://127.0.0.1:9001, sign in with MINIO_ROOT_USER and MINIO_ROOT_PASSWORD from the project root .env, then browse to the object in the bucket.

## What this demonstrates and limits

This demonstrates writing, listing, and retrieving an S3-compatible object. The Compose configuration runs one MinIO server with a persistent local volume; it does not provide distributed replication or high availability. Keep experiment evidence and discuss this limitation in the capstone report.
