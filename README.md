# Mini-MFS Data Platform

A hands-on data engineering project built around a realistic **Mobile Financial Services (MFS)** domain — similar to bKash or Nagad. It covers the open-source data engineering core: **Python · Kafka · Spark · Airflow · dbt · PostgreSQL**, running entirely on a single laptop with Docker Compose.

All data is **100% synthetic**. No real phone numbers, no real customers, no real money.

> **Version 2 roadmap** — incremental fact table, dimensional models (`dim_wallet`, `dim_agent`), agent liquidity mart, R3 pass-through fraud detection (batch), and pytest unit tests are planned but not yet implemented. See [spec/mini-mfs-data-platform-spec-v2.md](spec/mini-mfs-data-platform-spec-v2.md).

---

## Architecture

```
┌─────────────────────────┐
│  Transaction Generator  │  Python · ~20 events/sec · injects dirty data
└───────────┬─────────────┘
            │ JSON  key = sender_wallet
            ▼
┌─────────────────────────┐
│  Kafka 4.3.2 (KRaft)    │  topic: mfs.transactions  (3 partitions)
│                         │  topic: mfs.fraud_alerts  (1 partition)
└───────────┬─────────────┘
            ▼
┌──────────────────────────────────────────────────────┐
│  Spark 3.5.3 Structured Streaming  (continuous)      │
│   Query A : raw events  ──► BRONZE Parquet files     │
│   Query B1: R1_VELOCITY ──► Kafka mfs.fraud_alerts   │
│   Query B2: R2_LARGE_CASHOUT ──► Kafka + Bronze      │
└───────────┬──────────────────────────────────────────┘
            ▼
┌──────────────────────────────────────────────────────┐
│  Airflow DAG: mfs_daily_pipeline  (daily 01:00 UTC)  │
│   1. Sensor: wait for bronze partition               │
│   2. Spark batch: bronze ──► SILVER (clean, deduped) │
│   3. SQL upsert: silver ──► PostgreSQL               │
│   4. dbt build: silver ──► GOLD                      │
│   5. Publish audit summary                           │
└───────────┬──────────────────────────────────────────┘
            ▼
┌─────────────────────────┐
│  PostgreSQL Warehouse   │  schemas: silver · staging · gold
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│  Apache Superset        │  dashboards on gold tables (http://localhost:8088)
└─────────────────────────┘
```

**Two paths through the system:**
- **Hot path (seconds):** generator → Kafka → Spark streaming → fraud alerts
- **Cold path (daily):** bronze → silver → gold, orchestrated by Airflow

---

## Data Layers (Medallion Architecture)

| Layer | Location | Format | Rule |
|---|---|---|---|
| **Bronze** | `data/lake/bronze/transactions/ingest_date=YYYY-MM-DD/` | Parquet | Raw JSON as-is + Kafka metadata. Never modified. |
| **Silver** | `data/lake/silver/transactions/event_date=YYYY-MM-DD/` + PostgreSQL `silver` schema | Parquet + table | Typed, validated, deduplicated. One row per `txn_id`. |
| **Quarantine** | `data/lake/quarantine/transactions/ingest_date=YYYY-MM-DD/` | Parquet | Records that failed validation, with `reject_reason`. |
| **Gold** | PostgreSQL `gold` schema | Tables | Aggregations and marts built and tested by dbt. |

---

## Repository Structure

```
mfs-data-platform/
├── docker-compose.yml          # all services
├── Makefile                    # convenience targets
├── .env.example                # generator config template
├── spec/
│   ├── mini-mfs-data-platform-spec.md    # v1 specification
│   ├── mini-mfs-data-platform-spec-v2.md # v2 roadmap
│   └── kafka-basics-reference.md
├── generator/
│   ├── producer.py             # Kafka producer
│   ├── reference_data.py       # wallets, agents, merchants
│   └── scenarios.py            # duplicate/late/fraud injection
├── spark/jobs/
│   ├── schemas.py              # shared PySpark schema
│   ├── stream_ingest.py        # streaming job (bronze + fraud alerts)
│   └── bronze_to_silver.py     # daily batch job
├── sql/
│   └── init_dw.sql             # PostgreSQL DDL (runs on first container start)
├── airflow/
│   ├── Dockerfile              # Airflow + Java 17 + PySpark + dbt
│   └── dags/
│       └── mfs_daily_pipeline.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_transactions.sql
│       │   └── schema.yml
│       └── gold/
│           ├── daily_txn_summary.sql
│           ├── wallet_activity.sql
│           ├── district_heatmap.sql
│           └── schema.yml
├── superset/
│   └── superset_config.py
└── data/                       # git-ignored — created by 'make init'
    ├── lake/{bronze,silver,quarantine}/
    └── checkpoints/
```

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | latest | must be running |
| Python | 3.11+ | for the local generator |
| make | any | Git Bash on Windows; optional — all targets are plain commands |

---

## Services

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8090 | — |
| Spark Master UI | http://localhost:8082 | — |
| Spark Worker UI | http://localhost:8083 | — |
| Airflow UI | http://localhost:8085 | admin / admin123 |
| Superset UI | http://localhost:8088 | admin / admin123 |
| PostgreSQL DW | `localhost:5432` | mfs / mfs (db: `mfs_dw`) |

---

## Quick Start

### Step 1 — Create data directories

```bash
make init
```

### Step 2 — Build the custom Airflow image

```bash
make airflow-build
```

Builds the custom image (Airflow + Java 17 + PySpark + dbt) for all Airflow services. Takes ~5 minutes on first run. Re-run only when [airflow/Dockerfile](airflow/Dockerfile) changes.

### Step 3 — Start all services

```bash
make up
```

Starts Kafka, Spark, PostgreSQL, Airflow, and Superset. Kafka topics and the PostgreSQL schema are created automatically.

### Step 4 — Install generator dependencies

```bash
python -m venv .venv
.venv\Scripts\pip install -r generator\requirements.txt   # Windows
# source .venv/bin/activate && pip install -r generator/requirements.txt  # Mac/Linux
```

### Step 5 — Configure

```bash
copy .env.example .env    # Windows
# cp .env.example .env   # Mac/Linux
```

Defaults work out of the box. See [Configuration](#configuration) to adjust.

### Step 6 — Start the transaction generator (Terminal 1)

```bash
# Sanity check — 100 messages then stop
MAX_MESSAGES=100 .venv/Scripts/python generator/producer.py

# Run continuously
.venv/Scripts/python generator/producer.py
```

Verify messages in Kafka UI → **http://localhost:8090** → Topics → `mfs.transactions`.

### Step 7 — Start the Spark streaming job (Terminal 2)

```bash
make stream
```

First run downloads the Kafka connector JAR (~10 MB, cached after that). After ~30 seconds the first micro-batch runs and Parquet files appear in `data/lake/bronze/`.

### Step 8 — Verify bronze files

```bash
# Check partition folders exist
ls data/lake/bronze/transactions/

# Count rows in PySpark shell
docker compose exec spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077
```

```python
df = spark.read.parquet("/data/lake/bronze/transactions")
df.groupBy("ingest_date").count().show()
```

### Step 9 — Run the batch job manually

After bronze files exist for a date, run the bronze → silver batch job:

```bash
make batch D=2026-11-09
```

Verify results in PostgreSQL (DBeaver, psql, or any SQL client at `localhost:5432`):

```sql
SELECT COUNT(*) FROM silver.transactions;
SELECT * FROM silver.batch_audit ORDER BY started_at DESC LIMIT 5;

-- Idempotency check: re-run the batch and confirm count does not change
SELECT txn_id, COUNT(*) FROM silver.transactions GROUP BY txn_id HAVING COUNT(*) > 1;
```

### Step 10 — Airflow pipeline

Open **http://localhost:8085**, log in with `admin` / `admin123`.

Enable the `mfs_daily_pipeline` DAG (toggle the pause switch). It runs daily at 01:00 UTC and processes each day's bronze data through 5 tasks automatically:

```
wait_for_bronze → bronze_to_silver → load_silver → dbt_build → publish_audit
```

Manual trigger for a specific date:

```bash
make trigger D=2026-11-09
```

After the DAG completes, the gold tables are populated and ready for Superset.

### Step 11 — Superset dashboards

Open **http://localhost:8088**, log in with `admin` / `admin123`.

The `MFS Data Warehouse` PostgreSQL connection is pre-registered. Build charts from the gold tables:

| Chart | Table | Type |
|---|---|---|
| Transaction volume over time | `gold.daily_txn_summary` | Line chart: `total_amount` by `event_date`, colored by `txn_type` |
| Transaction count by type | `gold.daily_txn_summary` | Bar chart: `txn_count` grouped by `txn_type` |
| Top wallets by volume | `gold.wallet_activity` | Table: top 20 by `total_sent` DESC |
| District activity | `gold.district_heatmap` | Bar chart: `txn_count` by `district` |

Create a **Dashboards** → `+ Dashboard`, drag all 4 charts in, and save as "MFS Operations".

---

## dbt Models

dbt runs as `dbt_build` (Task 4) inside the Airflow pipeline. It can also be run independently:

```bash
make dbt-run      # build staging views + gold tables
make dbt-test     # run all schema tests
make dbt-docs     # generate lineage docs (output: dbt/target/index.html)
```

| Model | Schema | Type | Description |
|---|---|---|---|
| `stg_transactions` | `staging` | View | Thin wrapper over `silver.transactions` |
| `daily_txn_summary` | `gold` | Table | Volume by day and transaction type |
| `wallet_activity` | `gold` | Table | Lifetime stats per sender wallet |
| `district_heatmap` | `gold` | Table | Volume by district, date, and type |

Verify gold tables:

```sql
SELECT * FROM gold.daily_txn_summary  ORDER BY event_date DESC LIMIT 10;
SELECT * FROM gold.wallet_activity    ORDER BY total_sent   DESC LIMIT 10;
SELECT * FROM gold.district_heatmap   ORDER BY txn_count    DESC LIMIT 10;
```

---

## Streaming Queries

The streaming job runs three continuous micro-batch queries (trigger: every 30 seconds):

| Query | Input | Output |
|---|---|---|
| **A — Bronze** | `mfs.transactions` | `data/lake/bronze/transactions/` Parquet |
| **B1 — R1_VELOCITY** | parsed transactions | `mfs.fraud_alerts` Kafka + bronze fraud_alerts Parquet |
| **B2 — R2_LARGE_CASHOUT** | parsed transactions | `mfs.fraud_alerts` Kafka + bronze fraud_alerts Parquet |

**Fraud detection rules:**

| Rule | Logic |
|---|---|
| `R1_VELOCITY` | >5 transactions from the same `sender_wallet` in a 1-minute tumbling window |
| `R2_LARGE_CASHOUT` | `txn_type = CASH_OUT` and `amount > ৳25,000` |

**Checkpoint behaviour:** Spark saves Kafka offsets to `data/checkpoints/` on every trigger. Restart the job and it resumes from where it stopped — no gaps, no reprocessed messages.

---

## Configuration

Copy `.env.example` to `.env` and edit. All variables have working defaults.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `TOPIC` | `mfs.transactions` | Target topic |
| `EVENTS_PER_SECOND` | `20` | Target produce rate |
| `DUPLICATE_RATE` | `0.02` | Fraction of messages re-sent 0–5 seconds later |
| `LATE_RATE` | `0.03` | Fraction of events with `event_time` 1–48 hours in the past |
| `MALFORMED_RATE` | `0.01` | Fraction of records corrupted (null amount, bad type, missing ID) |
| `FRAUD_ENABLED` | `true` | Enable velocity bursts and large cash-out patterns |
| `SEED` | `42` | RNG seed for reproducibility |
| `MAX_MESSAGES` | `0` | Stop after N messages (`0` = run forever) |
| `LOG_EVERY` | `500` | Print a stats line every N messages |

---

## Kafka Topics

| Topic | Partitions | Retention | Key |
|---|---|---|---|
| `mfs.transactions` | 3 | 7 days | `sender_wallet` |
| `mfs.fraud_alerts` | 1 | 7 days | `sender_wallet` |

`sender_wallet` is the message key so all transactions from one wallet land on the same partition, preserving per-wallet order that the streaming fraud rules depend on.

---

## Injected Data Problems

The generator deliberately injects problems so each pipeline layer has real work to do.

| Problem | Rate | Handled by |
|---|---|---|
| Duplicates | 2% | `ROW_NUMBER` dedupe in Spark batch + `ON CONFLICT DO NOTHING` in PostgreSQL |
| Late events | 3% | Event-time partitioning in silver (late row lands in correct `event_date` partition) |
| Malformed records | 1% | Validation + quarantine in `bronze_to_silver.py` |
| Velocity fraud | ~0.3% wallets | Spark streaming R1: >5 txns from one wallet in 1 minute |
| Large cash-out | ~0.1% | Spark streaming R2: CASH_OUT > ৳25,000 |
| Pass-through (mule) | ~0.1% | Generator injects pattern; batch R3 detection planned in v2 |

Set `FRAUD_ENABLED=false` to disable all fraud patterns.

---

## Transaction Schema

```json
{
  "txn_id":          "TXN-5c1e9a7e-3f0b-4b8e-9d2a-1a6f0c7b2e11",
  "txn_type":        "CASH_OUT",
  "sender_wallet":   "01711234567",
  "receiver_wallet": null,
  "agent_id":        "AG-0042",
  "merchant_id":     null,
  "biller":          null,
  "amount":          3000.00,
  "fee":             55.50,
  "channel":         "USSD",
  "district":        "Rajshahi",
  "event_time":      "2026-11-02T10:00:00Z"
}
```

**Traffic mix:** SEND_MONEY 35% · CASH_OUT 25% · CASH_IN 20% · MERCHANT_PAY 12% · BILL_PAY 8%

---

## Makefile Targets

Requires `make` (Git Bash on Windows). All targets are plain `docker compose` / Python commands if `make` is not available.

```bash
make init                 # create data/ and airflow/logs directories (run once)
make airflow-build        # build custom Airflow image for all services
make up                   # docker compose up -d
make down                 # stop containers (data volumes kept)
make topics               # describe all Kafka topics
make logs                 # tail Kafka container logs
make gen                  # run generator with .env settings
make gen-load             # 500 events/sec, 5000 messages, seed=0
make stream               # submit the Spark streaming job
make batch D=YYYY-MM-DD   # run bronze→silver batch job for one date
make trigger D=YYYY-MM-DD # manually trigger mfs_daily_pipeline for a date
make dbt-run              # run all dbt models
make dbt-test             # run all dbt schema tests
make dbt-docs             # generate dbt documentation
make reset                # wipe all volumes and bronze/silver/quarantine files (destructive)
```

---

## References

- [Project Specification v1](spec/mini-mfs-data-platform-spec.md) — component specs, schema definitions, acceptance criteria
- [Project Specification v2](spec/mini-mfs-data-platform-spec-v2.md) — planned enhancements (not yet implemented)
- [Kafka Basics Reference](spec/kafka-basics-reference.md) — topics, partitions, offsets, consumer groups with Python examples
