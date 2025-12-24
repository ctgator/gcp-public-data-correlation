# Event-Driven Public Data Correlation on Google Cloud Platform

## Overview

This project explores how **publicly observable real-world events** may (or may not) produce detectable signals in **public digital behavior**, using a small, well-bounded reference architecture on Google Cloud Platform (GCP).

The goal is not to prove a novel scientific claim, but to demonstrate a **repeatable, cost-conscious data engineering pattern** for ingesting, modeling, and analyzing independent public datasets on GCP using modern analytics engineering practices.

This work is intended as:

* A learning and experimentation platform
* A reusable reference for event-driven analytics pipelines
* A concrete demonstration of analytics engineering on GCP

---

## Guiding Question

> **Do externally observable real-world events produce measurable signals in public digital behavior?**

As an initial case study, the project examines:

* **Seismic events** (earthquakes)
* **Wikipedia page view activity**

Both datasets are public, time-indexed, and independently maintained, making them well-suited for transparent analysis.

A “null result” (no meaningful correlation) is a valid and expected outcome.

---

## Data Sources

### Seismic Activity

* **Source:** United States Geological Survey (USGS)
* **Data:** Public earthquake event feeds
* **Characteristics:**

  * Timestamped
  * Geospatial
  * Event-driven

### Public Digital Attention

* **Source:** Wikimedia Pageviews API
* **Data:** Hourly/daily page view counts
* **Characteristics:**

  * Time-series
  * Aggregated, anonymized
  * No authentication required

---

## Architecture (High-Level)

This project intentionally favors **simplicity and transparency** over real-time complexity.

### Ingestion

* Cloud Scheduler triggers lightweight Cloud Functions
* Each function retrieves public data snapshots at a fixed cadence
* Raw data is stored immutably in Cloud Storage

### Storage

* Raw data is loaded into **BigQuery** as partitioned tables
* Schema reflects source structure with minimal transformation

### Transformation

* **dbt** models normalize timestamps, apply aggregation, and align datasets to a common temporal grain
* Transformations emphasize traceability and testability over cleverness

### Analysis

* SQL-based correlation and lag analysis
* Explicit assumptions and limitations documented
* No predictive modeling or machine learning in the initial scope

### Visualization

* Results explored via BigQuery queries and lightweight dashboards
* Visuals support reasoning, not storytelling

---

## Design Principles

This project is guided by a small set of explicit principles:

* **Bounded scope**
  One pipeline, two datasets, one analytical question.

* **Cost awareness**
  Designed to run comfortably within free tiers and small credits.

* **Reproducibility**
  Infrastructure and transformations are declarative and version-controlled.

* **Transparency**
  Raw data is preserved; transformations are explicit.

* **Extensibility**
  The architecture supports future datasets without redesign.

---

## Non-Goals

To avoid scope creep, the following are explicitly out of scope:

* Real-time streaming pipelines
* Predictive or generative modeling
* User-facing applications
* Authentication-heavy APIs
* Production SLAs or high availability guarantees

This is a **reference lab**, not a product.

---

## Why Google Cloud Platform

GCP provides a strong fit for this type of work due to:

* **BigQuery** for cost-efficient, scalable time-series analytics
* **Cloud Functions** for simple event-driven ingestion
* **Cloud Storage** for durable, low-cost raw data retention
* Native integration with modern analytics engineering tools

The project is intentionally designed to showcase **idiomatic GCP usage** without requiring complex infrastructure.

---

## Intended Audience

This work may be useful to:

* Data engineers and analytics engineers
* Architects exploring event-driven analytics patterns
* Practitioners evaluating GCP for public-data workloads
* Interviewers seeking concrete examples of analytics system design

---

## Future Extensions (Not Implemented)

Potential follow-on work could include:

* Additional public event sources (weather alerts, public health notices)
* Streaming ingestion via Pub/Sub
* Automated anomaly detection
* Metric observability dashboards
* Documentation focused on instructional use

None of these are required to achieve the current project’s goals.

---

## About the Author

This project is developed by an independent data engineering practitioner with extensive experience in healthcare data platforms, analytics engineering, and data quality frameworks. It reflects a practical, production-informed approach to building analytics systems that prioritize correctness, clarity, and trust.

