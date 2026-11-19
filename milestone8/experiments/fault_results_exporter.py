"""Expose saved Milestone 7 fault-test CSV summaries to Prometheus.

This reads existing experiment evidence; it does not rerun or change tests.
"""
import csv
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

RESULTS = Path(os.environ.get("RESULTS_DIR", "/results"))


def scrape():
    rows = []
    for path in sorted(RESULTS.glob("fault_*.csv")):
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                component = row.get("component") or path.stem.removeprefix("fault_")
                fault = row.get("fault") or "unknown"
                try:
                    mttr = float(row["mttr_s"])
                except (KeyError, TypeError, ValueError):
                    mttr = None
                try:
                    availability = float(row["availability"])
                except (KeyError, TypeError, ValueError):
                    availability = None
                rows.append((component, fault, mttr, availability))

    groups = {}
    for component, fault, mttr, availability in rows:
        group = groups.setdefault((component, fault), {"mttr": [], "availability": [], "trials": 0})
        group["trials"] += 1
        if mttr is not None:
            group["mttr"].append(mttr)
        if availability is not None:
            group["availability"].append(availability)

    lines = [
        "# HELP dnms_fault_mean_mttr_seconds Mean recovery time from saved Milestone 7 CSV trials.",
        "# TYPE dnms_fault_mean_mttr_seconds gauge",
        "# HELP dnms_fault_mean_availability_ratio Mean availability from saved Milestone 7 CSV trials.",
        "# TYPE dnms_fault_mean_availability_ratio gauge",
        "# HELP dnms_fault_trials_total Number of saved trials in the CSV for this fault scenario.",
        "# TYPE dnms_fault_trials_total gauge",
        "# HELP dnms_fault_recovered_trials_total Trials with a recorded recovery time.",
        "# TYPE dnms_fault_recovered_trials_total gauge",
    ]
    for (component, fault), data in sorted(groups.items()):
        labels = f'component="{component}",fault="{fault}"'
        if data["mttr"]:
            lines.append(f'dnms_fault_mean_mttr_seconds{{{labels}}} {sum(data["mttr"]) / len(data["mttr"])}')
        if data["availability"]:
            lines.append(f'dnms_fault_mean_availability_ratio{{{labels}}} {sum(data["availability"]) / len(data["availability"])}')
        lines.append(f'dnms_fault_trials_total{{{labels}}} {data["trials"]}')
        lines.append(f'dnms_fault_recovered_trials_total{{{labels}}} {len(data["mttr"])}')
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_error(404)
            return
        payload = scrape().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        print("[fault-results] " + format % args, flush=True)


if __name__ == "__main__":
    print(f"Reading saved CSVs from {RESULTS}", flush=True)
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
