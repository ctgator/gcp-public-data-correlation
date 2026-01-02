#!/usr/bin/env python3

import argparse
import gzip
import json
import logging
import re
import requests
from datetime import datetime, timezone
from io import TextIOWrapper

from google.cloud import bigquery


FILENAME_RE = re.compile(
    r"pageviews-(?P<date>\d{8})-(?P<hour>\d{2})0000\.gz"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest Wikimedia hourly pageviews using a manifest file"
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help="Path to manifest JSON file"
    )
    parser.add_argument(
        "--project_id",
        required=True,
        default="pub-data-correlation-dev",
        help="GCP project ID"
    )
    parser.add_argument(
        "--dataset",
        required=True,
        default="raw",
        help="BigQuery dataset name"
    )
    parser.add_argument(
        "--table",
        default="raw_pageviews",
        help="BigQuery table name (default: raw_pageviews)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and parse files but do not insert into BigQuery"
    )
    return parser.parse_args()


def extract_date_hour(filename: str):
    match = FILENAME_RE.match(filename)
    if not match:
        raise ValueError(f"Invalid pageviews filename: {filename}")

    date_str = match.group("date")
    hour = int(match.group("hour"))

    date = datetime.strptime(date_str, "%Y%m%d").date()
    return date, hour


def stream_pageviews(url, date, hour):
    """
    Generator yielding parsed pageview rows.
    """
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    ingested_at = datetime.now(timezone.utc)

    with gzip.GzipFile(fileobj=response.raw) as gz:
        wrapper = TextIOWrapper(gz, encoding="utf-8", errors="replace")
        for line in wrapper:
            parts = line.strip().split(" ")
            parts = line.strip().split(" ")
            if len(parts) < 3:
                continue
            
            project = parts[0]

            if project.startswith("en"):
                continue

            article = parts[1]
            
            try:
                views = int(parts[2])
            except ValueError:
                continue
            
            yield {
                "project": project,
                "article": article,
                "views": views,
                "date": date.isoformat(),
                "hour": hour,
                "ingested_at": ingested_at.isoformat()
            }


def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    with open(args.manifest) as f:
        manifest = json.load(f)

    base_url = manifest["base_url"]
    files = manifest["files"]

    client = None
    if not args.dry_run:
        client = bigquery.Client(project=args.project_id)
        table_id = f"{args.project_id}.{args.dataset}.{args.table}"

    total_rows = 0

    for filename in files:
        date, hour = extract_date_hour(filename)
        url = f"{base_url}{filename}"

        logging.info(
            "Processing %s (date=%s hour=%02d)",
            filename, date, hour
        )

        rows = []
        for row in stream_pageviews(url, date, hour):
            rows.append(row)

            # Flush in batches to control memory usage
            if len(rows) >= 10_000:
                if not args.dry_run and client is not None:
                    errors = client.insert_rows_json(table_id, rows) # type: ignore
                    if errors:
                        raise RuntimeError(errors)
                total_rows += len(rows)
                rows.clear()

        if rows:
            if not args.dry_run and client is not None:
                errors = client.insert_rows_json(table_id, rows) # type: ignore
                if errors:
                    raise RuntimeError(errors)
            total_rows += len(rows)

    logging.info("Completed ingestion: %d rows processed", total_rows)


if __name__ == "__main__":
    main()

