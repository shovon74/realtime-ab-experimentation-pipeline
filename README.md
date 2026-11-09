# Real-Time A/B Experimentation Event Pipeline

A local-first, Docker Compose-based streaming and batch data pipeline that simulates how an A/B experimentation platform processes user exposure and conversion events.

> **Disclaimer:** All events and metrics in this repository are **100% synthetic** and generated deterministically via seeded hashing for reproducible testing and demonstration.

## Architecture

```text
Python Producer -> Apache Kafka (KRaft: exposures, conversions)
                         |
              Spark 3.5 Structured Streaming
       (dedup, watermarks, stream-stream left join, orphans)
                         |
            Partitioned Parquet Data Lake (./lake)
                         |
          Airflow Batch Load -> Warehouse (BigQuery / Postgres)
                         |
              dbt Core (staging -> intermediate -> marts + 15+ tests)
                         |
       Statistical Engine (Two-Proportion Z-Test, 95% CI, SRM Check)
                         |
                 Metabase / Power BI Dashboard
```

## Tech Stack

- **Language:** Python 3.11
- **Message Broker:** Apache Kafka 3.7.1 (KRaft mode, no ZooKeeper)
- **Stream Processing:** Apache Spark 3.5.1 Structured Streaming (`spark-sql-kafka-0-10_2.12:3.5.1`)
- **Data Lake:** Partitioned Parquet (`event_date=YYYY-MM-DD/experiment_id=<id>/`)
- **Warehouse:** Google BigQuery (`dbt-bigquery` primary) with local PostgreSQL (`dbt-postgres`) switch
- **Transformation & Testing:** dbt Core (`staging`, `intermediate`, `marts`)
- **Orchestration & Stats:** Apache Airflow 2.x (TaskFlow API), `scipy`, `statsmodels`, `pandas`
- **BI Dashboard:** Metabase / Power BI

## Quick Start

```bash
# 1. Copy environment variables
cp .env.example .env

# 2. Start Kafka (KRaft), topic initialization, synthetic producer, and Spark streaming
docker compose up -d

# 3. Run unit and data lake tests locally
python -m pytest tests/ -v
```

## Project Status (Build Phases)

- [x] **Phase 1 — Streaming Core:** Docker Compose with Kafka (KRaft), seeded synthetic event producer with fault injection (duplicates, late events, orphans, 55/45 SRM, and planted conversion lift), and Spark Structured Streaming consumer.
- [x] **Phase 2 — Data Lake:** Event-time watermarks (10m exposures, 30m conversions), `event_id` deduplication, left stream-stream attribution join, orphan conversion routing, and Hive-partitioned Parquet lake output.
- [ ] **Phase 3 — Warehouse & dbt:** Idempotent partition loader, `staging -> intermediate -> marts` models, and 15+ automated data quality tests.
- [ ] **Phase 4 — Stats & Orchestration:** Two-proportion z-test, 95% CI, chi-square SRM detector, and Airflow DAG with quality gate.
- [ ] **Phase 5 — Dashboard & Docs:** Metabase dashboard, exploratory notebook, architecture visuals, and full documentation.
