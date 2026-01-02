#!/usr/bin/env python3

import json
import requests
from datetime import datetime, timezone
from typing import List, Dict

from google.cloud import bigquery


# -----------------------------
# Configuration
# -----------------------------

PROJECT_ID = "pub-data-correlation-dev"
DATASET_ID = "raw"
TABLE_ID = "usgs_seismic_events"

USGS_FEED_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
)

REQUEST_TIMEOUT_SECONDS = 30


# -----------------------------
# Helpers
# -----------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def fetch_usgs_geojson(url: str) -> Dict:
    resp = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()


def ts(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def parse_features(
    geojson: Dict, ingest_time: datetime
) -> List[Dict]:
    rows = []

    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [])

        # Defensive parsing
        longitude = coords[0] if len(coords) > 0 else None
        latitude = coords[1] if len(coords) > 1 else None
        depth_km = coords[2] if len(coords) > 2 else None

        event_time_ms = props.get("time")
        updated_time_ms = props.get("updated")


        row = {
            "event_id": feature.get("id"),
            "event_time": ts(
                datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
                if event_time_ms
                else None
            ),
            "updated_time": ts(
                datetime.fromtimestamp(updated_time_ms / 1000, tz=timezone.utc)
                if updated_time_ms
                else None
            ),
            "ingest_time": ts(ingest_time),
            "latitude": latitude,
            "longitude": longitude,
            "depth_km": depth_km,
            "magnitude": props.get("mag"),
            "magnitude_type": props.get("magType"),
            "place_description": props.get("place"),
            "status": props.get("status"),
            "raw_payload": json.dumps(feature),
        }

        rows.append(row)

    return rows


def insert_rows_bq(rows: List[Dict]) -> None:
    if not rows:
        print("No rows to insert.")
        return

    client = bigquery.Client(project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = client.insert_rows_json(
        table_ref,
        rows,
        row_ids=[None] * len(rows),  # allow duplicates (append-only)
    )

    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    print(f"Inserted {len(rows)} rows into {table_ref}")


# -----------------------------
# Main
# -----------------------------

def main():
    ingest_time = utc_now()
    print(f"Ingest started at {ingest_time.isoformat()}")

    geojson = fetch_usgs_geojson(USGS_FEED_URL)
    rows = parse_features(geojson, ingest_time)

    insert_rows_bq(rows)

    print("Ingest completed successfully.")


if __name__ == "__main__":
    main()

