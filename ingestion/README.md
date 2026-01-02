# Ingestion

This directory contains the raw ingestion pipeline for Wikimedia
hourly pageview data.

## Design principles

- Explicit temporal scoping via manifests
- Streaming processing (no intermediate file storage)
- Append-only raw tables
- Reproducibility over convenience

## Components

- `manifests/`
  JSON files defining the exact hourly pageview dumps to ingest.

- `src/ingest_pageviews_manifest.py`
  Streams, parses, filters, and loads pageviews into BigQuery.

- `sql/create_raw_pageviews_table.sql`
  Raw table schema (partitioned and clustered).

- `Makefile`
  Table lifecycle management (create / drop / reset).

## Scope

To reduce noise and cost, ingestion is currently restricted to
English Wikipedia pageviews. The pipeline supports broader scopes
but is intentionally constrained for rapid iteration.
