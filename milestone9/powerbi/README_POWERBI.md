# Milestone 9 Power BI view

The experiment writes a Power BI-ready CSV from actual gateway reads. It records
the same `GET /read/<key>` route with both stores available and with `store-a`
stopped. It does not invent or reuse example measurements.

## Generate the CSV

Start the Milestone 9 Compose stack, then open a second PowerShell terminal:

```powershell
Set-Location "C:\Users\USER\Desktop\-ics2403-distributed-network-monitoring\milestone9"
python .\experiments\gateway_transparency_test.py --trials 3
```

The output is `experiments/results/powerbi_gateway_transparency.csv`. Each trial
contains one healthy-read row and one store-failure row. The script restores
`store-a` even if the failed-read request raises an error.

## Build the report in Power BI Desktop

1. Choose **Get data → Text/CSV** and select
   `milestone9/experiments/results/powerbi_gateway_transparency.csv`.
2. Confirm `trial`, `status_code`, and `replicas_tried` are numeric columns;
   keep `scenario`, `served_by`, and `failover_succeeded` as text/boolean.
3. Add a table with `trial`, `scenario`, `served_by`, `replicas_tried`,
   `gateway_overhead_s`, and `failover_succeeded`.
4. Add a bar chart with `scenario` on the axis and average
   `gateway_overhead_s` as the value.
5. Add a card showing the count of `store_a_stopped` rows where
   `failover_succeeded` is `True`, and a second card showing the total number
   of failure trials.
6. Save the report as `Milestone9_Transparency.pbix`.

This report is an analysis of the generated experiment CSV, not a live monitor.
Generate a new CSV and refresh/reselect it in Power BI to show a later run.
