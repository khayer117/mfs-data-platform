# Mini-MFS Data Platform

A hands-on data engineering project built around a realistic **Mobile Financial Services (MFS)** domain — similar to bKash or Nagad. It covers the open-source data engineering core: **Python · Kafka · Spark · Airflow · dbt · PostgreSQL**, running entirely on a single laptop with Docker Compose.

All data is **100% synthetic**. No real phone numbers, no real customers, no real money.

---

## Architecture

```
┌─────────────────────────┐
│  Transaction Generator  │  Python · ~20 events/sec · injects dirty data
│  (runs locally)         │
└───────────┬─────────────┘
            │ JSON  key = sender_wallet
            ▼
┌─────────────────────────┐
│  Kafka 4.3.2 (KRaft)    │  topic: mfs.transactions  (3 partitions)
│  (Docker)               │  topic: mfs.fraud_alerts  (1 partition)
└───────────┬─────────────┘
            ▼
┌──────────────────────────────────────────────────────┐
│  Spark 3.5.3 Structured Streaming  (continuous)      │
│   Query A : raw events  ──► BRONZE Parquet files     │
│   Query B1: R1_VELOCITY ──► Kafka mfs.fraud_alerts   │
│   Query B2: R2_LARGE_CASHOUT ──► Kafka + Bronze      │
└───────────┬──────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────┐
│  Airflow DAG: mfs_daily_pipeline  (daily)            │
│   1. Spark batch: bronze ──► SILVER (clean, deduped) │
│   2. Load silver ──► PostgreSQL                      │
│   3. dbt build: silver ──► GOLD (facts, dims, marts) │
│   4. dbt tests                                       │
└───────────┬──────────────────────────────────────────┘
            ▼
┌─────────────────────────┐
│  PostgreSQL Warehouse   │  schemas: silver · analytics (gold)
└─────────────────────────┘
            ▼
┌─────────────────────────┐
│  Apache Superset        │  dashboards on gold tables (Week 4)
└─────────────────────────┘
```

**Two paths:**
- **Hot path (seconds):** generator → Kafka → Spark streaming → fraud alerts
- **Cold path (daily):** bronze → silver → gold, orchestrated by Airflow

---

## Data Layers (Medallion Architecture)

| Layer | Location | Format | Rule |
|---|---|---|---|
| **Bronze** | `data/lake/bronze/transactions/ingest_date=YYYY-MM-DD/` | Parquet | Raw JSON as-is + Kafka metadata. Never modified. |
| **Silver** | `data/lake/silver/transactions/event_date=YYYY-MM-DD/` + PostgreSQL | Parquet + table | Typed, validated, deduplicated. One row per `txn_id`. |
| **Quarantine** | `data/lake/quarantine/transactions/ingest_date=YYYY-MM-DD/` | Parquet | Records that failed validation, with `reject_reason`. |
| **Gold** | PostgreSQL `gold` schema | Tables | Aggregations and marts built and tested by dbt. |

---

## Repository Structure

```
mini-mfs-data-platform/
├── docker-compose.yml          # Kafka + Spark + (Airflow, PostgreSQL, Superset later)
├── Makefile                    # convenience targets (requires make in Git Bash)
├── .env.example                # generator config template
├── README.md
├── spec/
│   ├── mini-mfs-data-platform-spec.md
│   └── kafka-basics-reference.md
├── generator/                  # Component 1 — DONE
│   ├── requirements.txt
│   ├── producer.py
│   ├── reference_data.py
│   └── scenarios.py
├── spark/                      # Component 3 — DONE  |  Component 4 — DONE
│   └── jobs/
│       ├── schemas.py          # shared PySpark schemas
│       ├── stream_ingest.py    # streaming job (bronze + fraud alerts)
│       └── bronze_to_silver.py # batch job
├── sql/                        # Component 5 — DONE
│   └── init_dw.sql
├── airflow/                    # Component 7 — DONE
│   ├── Dockerfile              # custom image: Airflow + Java + PySpark + dbt
│   └── dags/
│       └── mfs_daily_pipeline.py
├── dbt/                        # Component 6 — DONE
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
└── data/                       # git-ignored — created by 'make init'
    ├── lake/
    │   ├── bronze/
    │   │   ├── transactions/   # Parquet, partitioned by ingest_date
    │   │   └── fraud_alerts/   # Parquet, partitioned by ingest_date
    │   ├── silver/
    │   └── quarantine/
    └── checkpoints/            # Spark streaming checkpoints
```

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | latest | must be running |
| Python | 3.11+ | for the local generator (3.14 tested and working) |
| make | any | Git Bash on Windows; optional — all targets are plain commands |

---

## Services

| Service | URL | Notes |
|---|---|---|
| Kafka broker | `localhost:9092` | generator connects here |
| Kafka UI | http://localhost:8090 | browse topics and messages |
| Spark Master UI | http://localhost:8082 | view running jobs |
| Spark Worker UI | http://localhost:8083 | view worker resources |
| PostgreSQL DW | `localhost:5432` | database `mfs_dw`, user `mfs` / password `mfs` |
| Airflow UI | http://localhost:8085 | DAG management (admin / admin123) |

---

## Quick Start

### Step 1 — Create data directories (once)

```powershell
mkdir -Force data/lake/bronze/transactions, data/lake/bronze/fraud_alerts
mkdir -Force data/lake/silver/transactions, data/lake/quarantine/transactions
mkdir -Force data/checkpoints
mkdir -Force airflow/logs
```

### Step 2 — Build the Airflow image (once — takes ~5 min first time)

```bash
make airflow-build
```

Builds the custom Airflow image (Java + PySpark + dbt) for all Airflow services. Only needs to re-run when [airflow/Dockerfile](airflow/Dockerfile) changes.

### Step 3 — Start infrastructure

```powershell
docker compose up -d
```

Starts Kafka, Spark, Kafka UI, PostgreSQL (warehouse + Airflow metadata), and all Airflow services. Topics are created automatically by `init-kafka`.

### Step 4 — Install generator dependencies

```powershell
.venv\Scripts\pip install -r generator\requirements.txt
```

### Step 5 — Configure

```powershell
copy .env.example .env
```

Defaults work out of the box. See [Configuration](#configuration) to adjust.

### Step 6 — Start the generator (Terminal 1)

```powershell
# Sanity check — 100 messages then stop
$env:MAX_MESSAGES=100; .venv\Scripts\python generator\producer.py

# Run continuously
.venv\Scripts\python generator\producer.py
```

Verify messages in Kafka UI → **http://localhost:8090** → Topics → `mfs.transactions`.

### Step 7 — Start the streaming job (Terminal 2)

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
  --py-files /opt/spark-jobs/schemas.py `
  /opt/spark-jobs/stream_ingest.py
```

First run downloads the Kafka connector JAR (~10 MB, cached after that). After ~30 seconds the first micro-batch runs and Parquet files appear in `data/lake/bronze/`.

### Step 8 — Verify bronze files

```powershell
# Check files exist
ls data\lake\bronze\transactions\

# Count rows via PySpark shell
docker compose exec spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077
```

```python
# Inside PySpark shell
df = spark.read.parquet("/data/lake/bronze/transactions")
df.count()
df.groupBy("ingest_date").count().show()
```

### Step 9 — Run the batch job (Week 3)

After bronze files exist for a date, run the bronze → silver batch job:

```powershell
# Using Makefile (D = the ingest_date to process)
make batch D=2026-11-09

# Without make
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.postgresql:postgresql:42.7.3 `
  --py-files /opt/spark-jobs/schemas.py `
  /opt/spark-jobs/bronze_to_silver.py `
  --ingest-date 2026-11-09
```

Verify results:

```powershell
# Silver Parquet files
ls data\lake\silver\transactions\

# PostgreSQL — connect with any client (DBeaver, psql, etc.)
# Host: localhost:5432  DB: mfs_dw  User: mfs  Password: mfs
```

```sql
SELECT COUNT(*) FROM silver.transactions;
SELECT * FROM silver.batch_audit ORDER BY started_at DESC LIMIT 5;

-- Idempotency check: run the batch job twice and re-run this — count must not change
SELECT txn_id, COUNT(*) FROM silver.transactions GROUP BY txn_id HAVING COUNT(*) > 1;
```

### Step 10 — Airflow

Open **http://localhost:8085**, log in with `admin` / `admin123`.

Enable the `mfs_daily_pipeline` DAG (toggle the pause switch). It runs daily at 01:00 UTC and processes each day's bronze data automatically through all 5 tasks:

```
wait_for_bronze → bronze_to_silver → load_silver → dbt_build → publish_audit
```

Manual trigger for a specific date:

```bash
make trigger D=2026-11-09
```

### Step 11 — Run dbt manually (optional)

Run dbt models and tests outside of Airflow for quick development iteration:

```bash
make dbt-run      # build staging views + gold tables
make dbt-test     # run all schema tests
make dbt-docs     # generate docs (output: dbt/target/index.html)
```

Verify gold tables in PostgreSQL:

```sql
SELECT * FROM gold.daily_txn_summary  ORDER BY event_date DESC LIMIT 10;
SELECT * FROM gold.wallet_activity    ORDER BY total_sent   DESC LIMIT 10;
SELECT * FROM gold.district_heatmap   ORDER BY txn_count    DESC LIMIT 10;
```

---

## Streaming Queries

The streaming job (`spark/jobs/stream_ingest.py`) runs three continuous queries:

| Query | Input | Output | Trigger |
|---|---|---|---|
| **A — Bronze** | `mfs.transactions` | `data/lake/bronze/transactions/` Parquet | every 30s |
| **B1 — R1_VELOCITY** | parsed transactions | `mfs.fraud_alerts` Kafka + `data/lake/bronze/fraud_alerts/` | every 30s |
| **B2 — R2_LARGE_CASHOUT** | parsed transactions | `mfs.fraud_alerts` Kafka + `data/lake/bronze/fraud_alerts/` | every 30s |

**Fraud rules:**

| Rule | Logic |
|---|---|
| `R1_VELOCITY` | >5 transactions from the same `sender_wallet` in a 1-minute tumbling window |
| `R2_LARGE_CASHOUT` | `txn_type = CASH_OUT` and `amount > ৳25,000` |

**Stop → Resume:** Spark saves Kafka offsets to `data/checkpoints/` every trigger. Restart the job and it resumes from where it left off — no gaps, no duplicate files.

---

## Configuration

All settings are environment variables. Copy `.env.example` to `.env` and edit.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `TOPIC` | `mfs.transactions` | Target topic |
| `EVENTS_PER_SECOND` | `20` | Target produce rate |
| `DUPLICATE_RATE` | `0.02` | 2% of messages re-sent 0.1–5s later |
| `LATE_RATE` | `0.03` | 3% of events have `event_time` 1–48h in the past |
| `MALFORMED_RATE` | `0.01` | 1% corrupted (null amount, negative amount, bad type, missing ID) |
| `FRAUD_ENABLED` | `true` | Enable velocity bursts, large cash-outs, pass-through patterns |
| `SEED` | `42` | Seed for reproducibility. Change for independent load-test runs. |
| `MAX_MESSAGES` | `0` | Stop after N messages. `0` = run forever. |
| `LOG_EVERY` | `500` | Print a stats line every N messages. |

**Load testing:**

```powershell
$env:EVENTS_PER_SECOND=500; $env:MAX_MESSAGES=10000; $env:SEED=99
.venv\Scripts\python generator\producer.py
```

For rates above ~500/sec, run two PowerShell windows simultaneously.

---

## Kafka Topics

| Topic | Partitions | Retention | Key |
|---|---|---|---|
| `mfs.transactions` | 3 | 7 days | `sender_wallet` |
| `mfs.fraud_alerts` | 1 | 7 days | `sender_wallet` |

**Why `sender_wallet` as the key?** All transactions from one wallet land on the same partition, preserving order. The streaming fraud rules (velocity, pass-through) depend on per-wallet ordering.

**Network listeners:**

| Listener | Address | Used by |
|---|---|---|
| `PLAINTEXT_HOST` | `localhost:9092` | Generator (runs on host) |
| `PLAINTEXT` | `kafka:29092` | Spark, Kafka UI (run in Docker) |

---

## Injected Data Problems

The generator deliberately injects problems so downstream components have real work to do.

| Problem | Rate | What it tests |
|---|---|---|
| Duplicates | 2% | Deduplication in Spark batch (`ROW_NUMBER`) and upsert in PostgreSQL |
| Late events | 3% | Event-time partitioning in silver; 3-day lookback in dbt incremental model |
| Malformed records | 1% | Validation and quarantine in `bronze_to_silver.py` |
| Velocity fraud | ~0.3% | Spark streaming R1: >5 txns from one wallet in 1 minute |
| Large cash-out | ~0.1% | Spark streaming R2: CASH_OUT > ৳25,000 |
| Pass-through (mule) | ~0.1% | Batch R3 in dbt: receive SEND_MONEY → CASH_OUT ≥90% within 2 min |

Set `FRAUD_ENABLED=false` to disable all fraud patterns (cleaner data for batch testing).

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

Requires `make` (Git Bash). All targets are plain commands if `make` is not available.

```bash
make init                # create data/ and airflow/logs directories (run once)
make airflow-build       # build custom Airflow image for all services (run once after clone)
make up                  # docker compose up -d
make down                # stop containers (data volumes kept)
make topics              # describe all Kafka topics
make logs                # tail Kafka container logs
make gen                 # run generator with .env settings
make gen-load            # 500 events/sec, 5000 messages, seed=0
make stream              # submit the Spark streaming job
make batch D=YYYY-MM-DD  # run bronze→silver batch job for one date
make trigger D=YYYY-MM-DD # trigger mfs_daily_pipeline DAG for a specific date
make dbt-run             # run all dbt models (staging views + gold tables)
make dbt-test            # run all dbt schema tests
make dbt-docs            # generate dbt documentation (dbt/target/index.html)
make reset               # wipe Kafka + Spark data volumes (asks for confirmation)
```

---

## Build Milestones

### Week 1 — Infrastructure + Ingestion
- [x] `docker compose up` starts Kafka, Spark, and Kafka UI
- [x] Topics `mfs.transactions` and `mfs.fraud_alerts` created automatically
- [x] Generator produces events visible in Kafka UI at http://localhost:8090
- [x] Kafka 4.3.2 KRaft mode (no ZooKeeper)

### Week 2 — Spark Streaming
- [x] Spark 3.5.3 standalone cluster running in Docker
- [x] Streaming job: 3 queries (bronze + R1_VELOCITY + R2_LARGE_CASHOUT)
- [ ] Bronze Parquet files verified under `ingest_date=` folders
- [ ] Fraud alerts appear in `mfs.fraud_alerts` topic
- [ ] Stop/restart test: no gaps, no reprocessed offsets (checkpoint verification)

### Week 3 — Spark Batch + Airflow
- [x] `bronze_to_silver.py` — parse, validate, quarantine, dedupe, write silver Parquet + PostgreSQL
- [x] `sql/init_dw.sql` — `silver.transactions` (PK), `transactions_stage`, `batch_audit`, `fraud_alerts`
- [x] PostgreSQL DW (`postgres-dw`) + Airflow in `docker-compose.yml`
- [x] `mfs_daily_pipeline` DAG — wait_for_bronze → bronze_to_silver → load_silver → dbt_build → publish_audit
- [ ] `silver.transactions` verified: no duplicate `txn_id`, audit arithmetic correct
- [ ] Airflow DAG runs end-to-end for one day
- [ ] Rerunning the same day leaves row counts unchanged (idempotency proof)

### Week 4 — dbt + Superset
- [x] dbt project scaffolded: `dbt_project.yml`, `profiles.yml`, `generate_schema_name` macro
- [x] Staging model: `stg_transactions` (view over `silver.transactions`)
- [x] Gold models: `daily_txn_summary`, `wallet_activity`, `district_heatmap` (tables in `gold` schema)
- [x] Schema tests: `unique`, `not_null`, `accepted_values` on key columns — all pass
- [x] dbt wired into Airflow `dbt_build` task (full path, `--no-use-colors`)
- [ ] Apache Superset dashboards on gold tables
- [ ] dbt lineage graph screenshot in this README

---

## References

- [Project Specification](spec/mini-mfs-data-platform-spec.md) — full component specs, schema definitions, acceptance criteria
- [Kafka Basics Reference](spec/kafka-basics-reference.md) — topics, partitions, offsets, consumer groups explained with Python examples
