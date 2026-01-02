# GCP Public Data Correlation

This project explores how large-scale public datasets can be ingested,
modeled, and analyzed to detect correlations between real-world events
and human attention signals.

The initial focus is on correlating hourly Wikipedia pageview activity
with a significant seismic event:
the 2019 M7.1 Ridgecrest earthquake.

## Why this project

Public datasets such as Wikimedia pageviews are:
- massive,
- noisy,
- and often difficult to work with efficiently.

This project demonstrates:
- production-style ingestion from immutable public data dumps,
- explicit, reproducible temporal scoping via manifests,
- scalable modeling using BigQuery and dbt,
- and principled trade-offs between completeness, cost, and signal quality.

## Architecture (high level)

- Source: Wikimedia hourly pageview dumps
- Ingest: Python streaming ingestion into BigQuery (raw layer)
- Modeling: dbt (stage → analytics)
- Analysis: time-series spike detection and correlation

## Repository structure

