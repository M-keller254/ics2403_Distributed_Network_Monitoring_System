"""Archive experiment CSV data in MinIO through the collector API."""
import argparse
import csv
import datetime
import requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_file")
    parser.add_argument("--url", default="http://127.0.0.1:5000")
    parser.add_argument("--object-name")
    args = parser.parse_args()
    with open(args.csv_file, newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    payload = {
        "object_name": args.object_name,
        "data": {"source_file": args.csv_file, "archived_at": datetime.datetime.now(
            datetime.timezone.utc).isoformat(), "rows": rows},
    }
    response = requests.post(args.url.rstrip("/") + "/storage/objects",
                             json=payload, timeout=15)
    response.raise_for_status()
    print(response.json())


if __name__ == "__main__":
    main()
