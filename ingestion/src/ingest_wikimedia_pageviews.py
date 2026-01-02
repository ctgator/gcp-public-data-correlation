#!/usr/bin/env python3

import json
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from google.cloud import bigquery
from urllib.parse import quote



# -----------------------------
# Configuration
# -----------------------------

PROJECT_ID = "pub-data-correlation-dev"
DATASET_ID = "raw"
TABLE_ID = "wikimedia_pageviews"

BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"

PROJECT = "en.wikipedia"
ACCESS = "all-access"
AGENT = "user"
GRANULARITY = "hourly"

REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 0.2  # polite rate limiting

HEADERS = {
    "User-Agent": (
        "pub-data-correlation/1.0 "
        "(https://pysynapse.com; mailto:kris.kokomoor@gmail.com)"
    )
}


# -----------------------------
# Helpers
# -----------------------------

def utc_now():
    return datetime.now(timezone.utc)


def ts(dt):
    return dt.isoformat() if dt else None


def safe_end_hour(now: datetime) -> datetime:
    # Wikimedia pageviews lag ~1–2 hours
    return (now.replace(minute=0, second=0, microsecond=0)
            - timedelta(hours=2))


def build_url(article: str, start: str, end: str) -> str:
    encoded_article = quote(article, safe="")
    return (
        f"{BASE_URL}/{PROJECT}/{ACCESS}/{AGENT}/"
        f"{encoded_article}/{GRANULARITY}/{start}/{end}"
    )


def fetch_pageviews(article: str, start: str, end: str) -> Dict:
    url = build_url(article, start, end)
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def parse_items(
    article: str,
    payload: Dict,
    ingest_time: datetime,
    request_params: Dict,
) -> List[Dict]:

    rows = []

    for item in payload.get("items", []):
        hour_ts = datetime.strptime(
            item["timestamp"], "%Y%m%d%H"
        ).replace(tzinfo=timezone.utc)

        rows.append(
            {
                "project": PROJECT,
                "article": article,
                "hour_ts": ts(hour_ts),
                "views": item.get("views"),
                "agent": AGENT,
                "access": ACCESS,
                "ingest_time": ts(ingest_time),
                "request_params": json.dumps(request_params),
                "response_complete": True,
                "raw_payload": json.dumps(item),
            }
        )

    return rows


def insert_rows(rows: List[Dict]):
    if not rows:
        print("No rows to insert.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    errors = client.insert_rows_json(
        table_ref,
        rows,
        row_ids=[None] * len(rows),
    )

    if errors:
        raise RuntimeError(f"Insert errors: {errors}")

    print(f"Inserted {len(rows)} rows into {table_ref}")


# -----------------------------
# Main
# -----------------------------

def main():
    ingest_time = utc_now()
    print(f"Wikimedia ingest started at {ingest_time.isoformat()}")

    # Example seed articles — we will automate this later
    articles = [
        "California",
        "Alaska",
        "Japan",
    ]

    # Last 24 hours
    safe_end = safe_end_hour(ingest_time)
    safe_start = safe_end - timedelta(hours=24)

    end = safe_end.strftime("%Y%m%d%H")
    start = safe_start.strftime("%Y%m%d%H")


    all_rows = []

    for article in articles:
        print(f"Fetching pageviews for: {article}")

        payload = fetch_pageviews(article, start, end)

        rows = parse_items(
            article=article,
            payload=payload,
            ingest_time=ingest_time,
            request_params={
                "start": start,
                "end": end,
                "article": article,
            },
        )

        all_rows.extend(rows)
        time.sleep(SLEEP_SECONDS)

    insert_rows(all_rows)
    print("Wikimedia ingest completed.")


if __name__ == "__main__":
    from datetime import timedelta
    main()

