# 🌍 Air Pollution Data Engineering Pipeline

## Overview
This project is an end-to-end **data engineering pipeline** that ingests real-time air pollution data, processes it using **BigQuery**, and orchestrates workflows using **Apache Airflow (Dockerized)**.

The goal is to model air-quality data in an analytics-ready format (fact and dimension tables) and enable downstream use cases such as pollution ranking, health risk analysis, and trend monitoring.

---

## Architecture
WAQI API
↓
Python Extract
↓
Google Cloud Storage (GCS)
↓
BigQuery (Staging)
↓
BigQuery (Fact & Dimension Tables)
↓
Analytics / Use Cases


Airflow (running in Docker) orchestrates the pipeline.

---

## Tech Stack
- Python
- Apache Airflow (Docker)
- Google Cloud Storage
- BigQuery
- SQL
- Docker & Docker Compose

---

## Data Modeling
- **Staging Table**
  - Structured air-quality data loaded from GCS
- **Fact Table (`fact_pollution`)**
  - Grain: **1 city × 1 timestamp**
  - Metrics: AQI, PM2.5, PM10, O₃, NO₂, CO, SO₂
- **Dimension Table (`dim_city`)**
  - City metadata
  - Deterministic surrogate keys using hashing

---

## Orchestration
The Airflow DAG performs:
1. Loading data from **GCS → BigQuery (staging)**
2. Creating and refreshing **fact & dimension tables**
3. Ensuring idempotent and repeatable pipeline runs

---

## Example Use Cases
- Identify **most polluted cities** by PM2.5
- Classify **daily health risk levels**
- Analyze **long-term PM2.5 exposure trends**
- Enable alerts for **unhealthy air days**

---

## Key Engineering Decisions
- BigQuery used as an **analytics warehouse**, not a raw dump
- No enforced primary keys; **deterministic keys** used instead
- Clear separation between **staging** and **analytics**
- Dockerized Airflow for reproducibility and portability

---

## How to Run (High Level)
1. Authenticate with Google Cloud:
   ```bash
   gcloud auth application-default login
   docker compose up -d 
   ```
2. Enable and trigger the DAG from the Airflow UI.
