# Mini-MFS Transaction Data Platform — Project Specification

**Version:** 1.0 (MVP — implemented)
**Next version:** see [mini-mfs-data-platform-spec-v2.md](mini-mfs-data-platform-spec-v2.md)
**Purpose:** A hands-on learning project that covers the open-source data engineering core: **SQL, Python, Kafka, Spark, Airflow, dbt**. It is built around a realistic Mobile Financial Services (MFS) domain, similar to bKash or Nagad.
**Runs on:** a single laptop with Docker Compose (16 GB RAM recommended).

---

## Table of contents

1. [Goals and non-goals](#1-goals-and-non-goals)
2. [Glossary: new terms explained](#2-glossary-new-terms-explained)
3. [Architecture overview](#3-architecture-overview)
4. [The medallion layers (bronze, silver, gold)](#4-the-medallion-layers-bronze-silver-gold)
5. [Component 1: Transaction generator (Python)](#5-component-1-transaction-generator-python)
6. [Component 2: Kafka topics](#6-component-2-kafka-topics)
7. [Component 3: Spark streaming job](#7-component-3-spark-streaming-job)
8. [Component 4: Spark batch job (bronze → silver)](#8-component-4-spark-batch-job-bronze--silver)
9. [Component 5: PostgreSQL warehouse](#9-component-5-postgresql-warehouse)
10. [Component 6: dbt project (silver → gold)](#10-component-6-dbt-project-silver--gold)
11. [Component 7: Airflow DAG](#11-component-7-airflow-dag)
12. [Component 8: Dashboard (optional)](#12-component-8-dashboard-optional)
13. [Infrastructure (Docker Compose)](#13-infrastructure-docker-compose)
14. [Repository structure](#14-repository-structure)
15. [Milestones and acceptance criteria](#15-milestones-and-acceptance-criteria)
16. [Testing and verification](#16-testing-and-verification)
17. [Stretch goals](#17-stretch-goals)
18. [What you can say in an interview](#18-what-you-can-say-in-an-interview)

---

## 1. Goals and non-goals

### Goals

- Simulate a stream of wallet transactions: cash-in, cash-out, send money, merchant payment, bill pay.
- Process the stream **in real time** to detect suspicious activity.
- Process the data **in daily batches** to produce clean, trustworthy analytics tables.
- Practise the problems real pipelines face: **duplicates, late data, bad records, reruns**.
- End with a working, documented project that can go on GitHub as a portfolio piece.

### Non-goals (for the MVP)

- Production-grade security, high availability or multi-node clusters.
- Real money, real phone numbers, or any real customer data. Everything is synthetic.
- Machine-learning fraud models. Simple rules are enough.

---

## 2. Glossary: new terms explained

Read this section first. Every term below appears later in the spec.

### Storage and file formats

| Term | Plain explanation | Analogy from your world |
|---|---|---|
| **Parquet** | A file format for storing tables. Unlike CSV, it stores data **column by column**, compressed, and with the schema (column names and types) inside the file. Reading "only the `amount` column" is very fast because the other columns are never read. | A CSV file stores rows; a Parquet file is like a SQL table exported in a compact, typed, column-oriented binary form. |
| **Row vs columnar storage** | Row storage (PostgreSQL tables, CSV) keeps each record together, which is good for "get one transaction". Columnar storage keeps each column together, which is good for "sum all amounts for October". | OLTP database vs reporting/analytics database. |
| **Data lake** | A folder (local disk, or cloud storage like S3) full of files, usually Parquet, holding raw and processed data. Cheap, and it can hold huge volumes. | A big shared drive, organised by folders. |
| **Partitioning (files)** | Splitting a dataset into sub-folders by a column value, e.g. `ingest_date=2026-11-02/`. A query for one day reads only that folder. | Like a SQL table partitioned by date. |
| **Data warehouse** | A database optimised for analytics and reporting. In this project it is PostgreSQL. | Your reporting database. |
| **Schema** | The structure of the data: column names and types. "Schema enforcement" means rejecting data that doesn't match. | A C# class or a SQL `CREATE TABLE`. |

### Data layers and modelling

| Term | Plain explanation |
|---|---|
| **Medallion architecture** | A convention for organising data in three quality levels: **bronze → silver → gold**. Data gets cleaner and more business-ready at each step. See section 4. |
| **Bronze** | Raw data, stored exactly as it arrived (plus a few technical columns such as arrival time). Never edited. If something goes wrong later, you can rebuild everything from bronze. |
| **Silver** | Cleaned data: duplicates removed, types fixed, bad records separated out, one row per real transaction. Trustworthy, but not yet shaped for reports. |
| **Gold** | Business-ready tables built for specific questions, e.g. "daily volume by transaction type" or "agent liquidity". Dashboards read from gold. |
| **Dimensional model** | A way of designing analytics tables with **fact** tables (events/measurements, e.g. transactions) and **dimension** tables (descriptive context, e.g. wallet, agent, date). Also called a **star schema**, because the fact table sits in the middle and the dimensions surround it. |
| **Fact table** | One row per event, with numbers you add up (amount, fee) and keys pointing to dimensions. Example: `fct_transactions`. |
| **Dimension table** | One row per "thing", with descriptive attributes. Example: `dim_agent` (agent id, district, first-seen date). |
| **Mart** | A gold table built to answer one family of business questions. Example: `mart_agent_liquidity`. |
| **Grain** | What one row in a table represents. The grain of `fct_transactions` is "one row per transaction". Always define the grain before building a table. |

### Data quality problems

| Term | Plain explanation |
|---|---|
| **Dedupe (deduplication)** | Removing duplicate copies of the same record. Duplicates happen because systems retry: a producer resends a message it isn't sure was delivered, or a job is rerun. In this project, two rows with the same `txn_id` are the same transaction, so we keep only one. |
| **Late-arriving data** | An event that arrives long after it happened. Example: a USSD transaction made at 23:58 yesterday arrives at 00:30 today. Pipelines must decide which day it belongs to. |
| **Event time vs processing time** | **Event time** is when the transaction happened (`event_time` field). **Processing/ingestion time** is when our system received it (`ingest_time`). They differ whenever data is late. Reports should use event time. |
| **Quarantine (dead-letter)** | A separate place for records that fail validation (e.g. null amount). They are kept for investigation, not silently dropped, and not allowed into silver. |
| **Idempotent** | An operation that gives the same result no matter how many times you run it. If the daily job runs twice for the same day, the warehouse must not end up with double rows. This is the most important property of a batch pipeline. |
| **Upsert** | "Insert, or update if it already exists." In PostgreSQL: `INSERT ... ON CONFLICT ... DO UPDATE` (or `DO NOTHING`). Used to make loads idempotent. |
| **Backfill** | Running a pipeline for past dates, e.g. to reprocess last week after fixing a bug. Airflow supports this directly. |

### Streaming terms

| Term | Plain explanation |
|---|---|
| **Kafka topic** | A named, append-only log of messages. Producers write to it; consumers read from it. Messages stay for a configured time (retention) even after being read, so they can be replayed. |
| **Partition (Kafka)** | A topic is split into partitions so it can be read in parallel. Messages with the **same key** always go to the same partition, which keeps their order. We key by `sender_wallet`, so all transactions from one wallet stay in order. |
| **Offset** | The position of a message in a partition (0, 1, 2…). A consumer remembers which offset it has reached. |
| **Consumer group** | A set of consumers sharing the work of reading a topic. Each partition is read by one member of the group. |
| **At-least-once delivery** | The guarantee that every message is delivered, but possibly more than once. This is why downstream dedupe is needed. |
| **Structured Streaming** | Spark's streaming engine. You write code that looks like a normal DataFrame query, and Spark runs it continuously on new data in small **micro-batches** (e.g. every 10 seconds). |
| **Checkpoint** | A folder where a streaming job saves its progress (which Kafka offsets it has processed, plus any in-memory state). If the job crashes and restarts, it continues from the checkpoint instead of starting over. |
| **Window** | A time bucket used for streaming aggregations, e.g. "count transactions per wallet per 1-minute window". |
| **Watermark** | A rule that tells Spark how long to wait for late events, e.g. "accept events up to 10 minutes late". After that, the window is finalised and its state is freed from memory. Without a watermark, state grows forever. |
| **State** | Data a streaming job must remember between micro-batches, e.g. running counts per wallet per window. |

### Orchestration and transformation

| Term | Plain explanation |
|---|---|
| **DAG** | "Directed Acyclic Graph": a set of tasks with dependencies and no cycles. In Airflow, a DAG is a Python file describing the pipeline steps and their order. |
| **Task / Operator** | One step in an Airflow DAG. An operator is the type of step (run a bash command, run SQL, submit a Spark job). |
| **Logical date (`ds`)** | The date an Airflow run is *for*, not the date it runs. The run for `2026-11-01` executes after that day ends and processes that day's data. |
| **dbt model** | A `.sql` file containing a `SELECT`. dbt turns it into a table or view. |
| **Materialization** | How dbt builds a model: `view`, `table` (rebuilt every time) or `incremental` (only new/changed rows are added). |
| **dbt source** | A raw table not built by dbt (here: `silver.transactions`), declared in YAML so models can reference it. |
| **dbt test** | An assertion about data, e.g. "`txn_id` is unique". `dbt build` fails if a test fails. |
| **Lineage** | The map of which table is built from which. dbt draws it automatically. |
| **CDC (Change Data Capture)** | Streaming every insert/update/delete from a database's transaction log into Kafka. Used in the stretch goals. |

---

## 3. Architecture overview

```
┌──────────────────────┐
│ Transaction generator│  Python, ~20 events/sec, injects dirty data
│ (producer)           │
└──────────┬───────────┘
           │ JSON messages, key = sender_wallet
           ▼
┌──────────────────────┐
│ Kafka                │  topic: mfs.transactions (3 partitions)
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────────────────────┐
│ Spark Structured Streaming (runs continuously)       │
│  Query A: raw events  ──► BRONZE Parquet files       │
│  Query B: fraud rules ──► Kafka mfs.fraud_alerts     │
│                       └─► BRONZE fraud_alerts files  │
└──────────┬───────────────────────────────────────────┘
           │  (files land in data/lake/bronze/...)
           ▼
┌──────────────────────────────────────────────────────┐
│ Airflow DAG: mfs_daily_pipeline (runs daily)         │
│  1. check bronze data exists for the day             │
│  2. Spark batch: bronze ──► SILVER (clean, deduped)  │
│     bad records ──► QUARANTINE                       │
│  3. load silver ──► PostgreSQL (idempotent upsert)   │
│  4. dbt build: silver ──► GOLD (facts, dims, marts)  │
│  5. dbt tests (fail the run if data is wrong)        │
└──────────┬───────────────────────────────────────────┘
           ▼
┌──────────────────────┐
│ PostgreSQL warehouse │  schemas: silver, analytics (gold)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Metabase dashboard   │  optional
└──────────────────────┘
```

**Two paths through the system:**

- **Hot path (seconds):** generator → Kafka → Spark streaming → fraud alerts. Answers "is something suspicious happening right now?"
- **Cold path (daily):** bronze → silver → gold, orchestrated by Airflow. Answers "what exactly happened yesterday, correctly counted?"

This split (fast but approximate vs slow but correct) is a pattern you'll see in almost every real data platform.

---

## 4. The medallion layers (bronze, silver, gold)

| Layer | Where it lives | Format | Who writes it | Rules |
|---|---|---|---|---|
| **Bronze** | `data/lake/bronze/transactions/ingest_date=YYYY-MM-DD/` | Parquet | Spark streaming | Append-only. Raw JSON kept as-is, plus Kafka metadata. Never modified. |
| **Silver** | `data/lake/silver/transactions/event_date=YYYY-MM-DD/` and PostgreSQL `silver.transactions` | Parquet + table | Spark batch | Typed, validated, deduplicated. One row per `txn_id`. |
| **Quarantine** | `data/lake/quarantine/transactions/ingest_date=YYYY-MM-DD/` | Parquet | Spark batch | Records that failed validation, with a `reject_reason` column. |
| **Gold** | PostgreSQL `analytics` schema | Tables | dbt | Dimensional model and marts. Tested. |

**Why bronze is partitioned by `ingest_date` but silver by `event_date`:**
Bronze records *when data arrived*, so a daily job can simply say "process everything that arrived on 2026-11-01". Silver is organised by *when the transaction happened*, because that is what business reports need. A late transaction made on 31 Oct that arrived on 1 Nov sits in bronze `ingest_date=2026-11-01` but silver `event_date=2026-10-31`.

**A single transaction's journey:**

1. Generator creates `TXN-001`, cash-out ৳3,000, at 10:00:00.
2. Due to a simulated retry, it is sent to Kafka **twice**.
3. Spark streaming writes **both copies** to bronze. Bronze stores the truth of what arrived, duplicates included.
4. The daily Spark batch job sees two rows with `txn_id = TXN-001` and keeps one (dedupe).
5. The silver row is upserted into `silver.transactions`. If the job reruns, the upsert does nothing new (idempotent).
6. dbt builds `fct_transactions`, which has exactly one row for `TXN-001`, and `mart_agent_liquidity`, which counts ৳3,000 of cash-out for that agent exactly once.

---

## 5. Component 1: Transaction generator (Python)

**File:** `generator/producer.py`
**Library:** `confluent-kafka` (or `kafka-python`) and `faker`

### 5.1 Reference data (created once at start-up)

- **5,000 customer wallets:** Bangladeshi-style 11-digit numbers starting with `013`–`019` (synthetic).
- **200 agents**, each with an `agent_id` (`AG-0001`…) and a district.
- **300 merchants** (`MR-0001`…).
- **10 districts:** Dhaka, Chattogram, Rajshahi, Khulna, Sylhet, Barishal, Rangpur, Mymensingh, Cumilla, Gazipur.

### 5.2 Event schema

| Field | Type | Notes |
|---|---|---|
| `txn_id` | string | `TXN-` + UUID. Unique per real transaction. |
| `txn_type` | string | `CASH_IN`, `CASH_OUT`, `SEND_MONEY`, `MERCHANT_PAY`, `BILL_PAY` |
| `sender_wallet` | string | Customer wallet (for CASH_IN this is the receiving customer; see below) |
| `receiver_wallet` | string, nullable | Set for SEND_MONEY |
| `agent_id` | string, nullable | Set for CASH_IN and CASH_OUT |
| `merchant_id` | string, nullable | Set for MERCHANT_PAY |
| `biller` | string, nullable | Set for BILL_PAY (e.g. `DESCO`, `TITAS`, `WASA`) |
| `amount` | decimal(12,2) | ৳10 – ৳25,000 typical |
| `fee` | decimal(10,2) | e.g. 1.85% of amount for CASH_OUT, 0 for others (simplified) |
| `channel` | string | `APP` (80%) or `USSD` (20%) |
| `district` | string | |
| `event_time` | ISO-8601 UTC timestamp | When the transaction happened |

**Rule for CASH_IN:** `sender_wallet` holds the customer wallet that receives the money, and `agent_id` is the agent who received the cash. Keeping the customer in `sender_wallet` for every type makes the per-wallet fraud rules simpler.

**Example message:**

```json
{
  "txn_id": "TXN-5c1e9a7e-3f0b-4b8e-9d2a-1a6f0c7b2e11",
  "txn_type": "CASH_OUT",
  "sender_wallet": "01711234567",
  "receiver_wallet": null,
  "agent_id": "AG-0042",
  "merchant_id": null,
  "biller": null,
  "amount": 3000.00,
  "fee": 55.50,
  "channel": "USSD",
  "district": "Rajshahi",
  "event_time": "2026-11-02T10:00:00Z"
}
```

### 5.3 Normal traffic mix

| Type | Share |
|---|---|
| SEND_MONEY | 35% |
| CASH_OUT | 25% |
| CASH_IN | 20% |
| MERCHANT_PAY | 12% |
| BILL_PAY | 8% |

### 5.4 Deliberately injected problems (configurable)

These give the downstream components real work to do.

| Problem | Rate | How |
|---|---|---|
| **Duplicates** | 2% | Send the exact same message again 0–5 seconds later. |
| **Late events** | 3% | Set `event_time` between 1 and 48 hours in the past. |
| **Malformed records** | 1% | Null `amount`, negative `amount`, unknown `txn_type`, or missing `txn_id`. |
| **Fraud: velocity** | ~0.3% of wallets | Burst of 8–12 SEND_MONEY from one wallet within 60 seconds. |
| **Fraud: large cash-out** | ~0.1% | Single CASH_OUT of ৳25,001–৳60,000. |
| **Fraud: pass-through (mule)** | ~0.1% | Wallet receives SEND_MONEY, then CASH_OUT of ≥90% of it within 2 minutes. |

### 5.5 Configuration (environment variables)

```
KAFKA_BOOTSTRAP=kafka:9092
TOPIC=mfs.transactions
EVENTS_PER_SECOND=20
DUPLICATE_RATE=0.02
LATE_RATE=0.03
MALFORMED_RATE=0.01
FRAUD_ENABLED=true
SEED=42            # makes runs reproducible
```

### 5.6 Producer settings worth learning

- `key = sender_wallet`: keeps one wallet's events in order on one partition.
- `acks=all`: wait until Kafka has safely stored the message.
- `enable.idempotence=true`: stops the *producer itself* creating duplicates on network retries. (Our injected duplicates are separate, on purpose.)

---

## 6. Component 2: Kafka topics

| Topic | Partitions | Retention | Key | Producer | Consumer |
|---|---|---|---|---|---|
| `mfs.transactions` | 3 | 7 days | `sender_wallet` | Generator | Spark streaming |
| `mfs.fraud_alerts` | 1 | 7 days | `sender_wallet` | Spark streaming | Kafka UI / any consumer |

Run Kafka in **KRaft mode** (no ZooKeeper), single broker, replication factor 1. That is fine for local work.

**Experiments to try (week 1):**

- Start two console consumers in the same consumer group and watch them split the partitions.
- Stop a consumer, produce more messages, restart it, and watch it resume from its offset.
- Reset a consumer group's offsets to the beginning and replay the whole topic.

---

## 7. Component 3: Spark streaming job

**File:** `spark/jobs/stream_ingest.py`
**Run mode:** long-running, submitted with `spark-submit` to the Spark container.
**Needs:** the Spark–Kafka connector package matching your Spark and Scala version (`org.apache.spark:spark-sql-kafka-0-10_<scala>:<spark-version>`).

### 7.1 Query A: Kafka → bronze

Steps:

1. Read from `mfs.transactions` with `startingOffsets = earliest`.
2. Keep the raw message as text, and add technical columns:

| Column | Source |
|---|---|
| `raw_value` | Kafka message value (JSON string) |
| `kafka_key` | Kafka key |
| `kafka_partition`, `kafka_offset` | Kafka metadata. Together they uniquely identify a message. |
| `kafka_timestamp` | When Kafka received it |
| `ingest_time` | `current_timestamp()` |
| `ingest_date` | `to_date(ingest_time)`, used for partitioning |

3. Write Parquet to `data/lake/bronze/transactions/`, partitioned by `ingest_date`.
4. Trigger every **30 seconds**.
5. Checkpoint at `data/checkpoints/bronze_transactions/`.

**Why keep raw JSON in bronze instead of parsing?** If a message is malformed, parsing could fail or silently drop fields. Keeping the original text means nothing is ever lost, and silver can always be rebuilt with better parsing logic.

Sketch:

```python
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "mfs.transactions")
       .option("startingOffsets", "earliest")
       .load())

bronze = (raw.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("key").cast("string").alias("kafka_key"),
            "partition", "offset", "timestamp")
          .withColumnRenamed("partition", "kafka_partition")
          .withColumnRenamed("offset", "kafka_offset")
          .withColumnRenamed("timestamp", "kafka_timestamp")
          .withColumn("ingest_time", F.current_timestamp())
          .withColumn("ingest_date", F.to_date("ingest_time")))

(bronze.writeStream.format("parquet")
   .option("path", "/data/lake/bronze/transactions")
   .option("checkpointLocation", "/data/checkpoints/bronze_transactions")
   .partitionBy("ingest_date")
   .trigger(processingTime="30 seconds")
   .start())
```

### 7.2 Query B: fraud rules → alerts

Parse the JSON with an explicit schema (`from_json`), drop records that fail to parse, then apply rules on **event time** with a **10-minute watermark**.

| Rule | ID | Logic | MVP or later |
|---|---|---|---|
| Velocity | `R1_VELOCITY` | More than 5 transactions from the same `sender_wallet` in a 1-minute tumbling window | MVP (streaming) |
| Large cash-out | `R2_LARGE_CASHOUT` | `txn_type = CASH_OUT` and `amount > 25000` | MVP (streaming, simple filter) |
| Pass-through | `R3_PASS_THROUGH` | Wallet receives SEND_MONEY, then CASH_OUT ≥ 90% of it within 2 minutes | **Later.** Needs a stream–stream join; implement first as a dbt model in batch (see 10.4), then try streaming as a stretch goal. |

Alert message schema:

```json
{
  "alert_id": "ALT-<uuid>",
  "rule_id": "R1_VELOCITY",
  "sender_wallet": "01711234567",
  "window_start": "2026-11-02T10:00:00Z",
  "window_end": "2026-11-02T10:01:00Z",
  "txn_count": 9,
  "total_amount": 41000.00,
  "detected_at": "2026-11-02T10:01:40Z"
}
```

Write alerts to Kafka `mfs.fraud_alerts` **and** to `data/lake/bronze/fraud_alerts/` (two sinks, each with its own checkpoint folder).

Sketch of the velocity rule:

```python
velocity = (parsed
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "1 minute"), "sender_wallet")
    .agg(F.count("*").alias("txn_count"), F.sum("amount").alias("total_amount"))
    .filter("txn_count > 5"))
```

**Note:** because duplicates are not removed yet at this stage, a velocity count can be inflated by one or two duplicate messages. That is acceptable for a hot-path alert, and a good discussion point. As an improvement, try `dropDuplicates(["txn_id"])` with the watermark before the rule.

### 7.3 Things to experiment with

- Kill the job (`Ctrl+C`), produce messages, restart it, and confirm there are no gaps and no reprocessing (the checkpoint).
- Delete the checkpoint folder and restart. What happens to bronze? (Answer: everything is re-read from the earliest offset, creating duplicate files. This is why checkpoints matter.)
- Change the watermark to 1 minute and inject late data. Watch late events get ignored by the fraud rules.

---

## 8. Component 4: Spark batch job (bronze → silver)

**File:** `spark/jobs/bronze_to_silver.py`
**Run by:** Airflow, once per day, with argument `--ingest-date YYYY-MM-DD`.

### 8.1 Steps

1. **Read** bronze for exactly one `ingest_date` partition.
2. **Parse** `raw_value` with the explicit schema. Mark records that fail to parse.
3. **Validate** each record. A record is rejected if any of these is true:

| Check | `reject_reason` |
|---|---|
| JSON could not be parsed | `PARSE_ERROR` |
| `txn_id` is null or empty | `MISSING_TXN_ID` |
| `amount` is null or ≤ 0 | `INVALID_AMOUNT` |
| `txn_type` not in the allowed list | `UNKNOWN_TXN_TYPE` |
| `event_time` is null or more than 1 hour in the future | `INVALID_EVENT_TIME` |
| Required field missing for the type (e.g. CASH_OUT without `agent_id`) | `MISSING_REQUIRED_FIELD` |

4. **Quarantine:** write rejected records to `data/lake/quarantine/transactions/ingest_date=.../` with the `reject_reason`.
5. **Dedupe** valid records on `txn_id`, keeping the **first copy that arrived** (lowest `kafka_timestamp`, then lowest offset):

```python
w = Window.partitionBy("txn_id").orderBy("kafka_timestamp", "kafka_partition", "kafka_offset")
silver = (valid.withColumn("rn", F.row_number().over(w))
               .filter("rn = 1")
               .drop("rn"))
```

The same logic in SQL, which you will use often:

```sql
SELECT *
FROM (
  SELECT t.*,
         ROW_NUMBER() OVER (PARTITION BY txn_id
                            ORDER BY kafka_timestamp, kafka_partition, kafka_offset) AS rn
  FROM bronze_transactions t
) x
WHERE rn = 1;
```

6. **Standardise:** cast `amount`/`fee` to decimal, `event_time` to timestamp (UTC), add `event_date = to_date(event_time)`, uppercase `txn_type`, trim strings.
7. **Add lineage columns:** `source_ingest_date`, `processed_at`, `batch_run_id`.
8. **Write silver Parquet**, partitioned by `event_date`, using **dynamic partition overwrite** so that only the affected dates are replaced.
9. **Write to PostgreSQL staging table** `silver.transactions_stage` (overwrite) through JDBC. The next Airflow task performs the upsert (section 9.3).
10. **Emit counts** (read, valid, rejected, duplicates removed) to the log and to `silver.batch_audit`.

### 8.2 The cross-day duplicate problem

Dedupe inside one `ingest_date` doesn't catch a duplicate that arrives on a *different* day. This is handled in the warehouse: `txn_id` is the primary key of `silver.transactions`, and the load uses `ON CONFLICT (txn_id) DO NOTHING`. Two layers of protection, each simple.

---

## 9. Component 5: PostgreSQL warehouse

### 9.1 Databases and schemas

One PostgreSQL instance, separate from Airflow's own metadata database.

| Database | Schema | Contents | Built by |
|---|---|---|---|
| `mfs_dw` | `silver` | `transactions`, `transactions_stage`, `batch_audit`, `fraud_alerts` | Spark + SQL |
| `mfs_dw` | `staging` | dbt staging views | dbt |
| `mfs_dw` | `gold` | Aggregation tables and marts | dbt |

### 9.2 Silver table

```sql
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.transactions (
    txn_id             TEXT PRIMARY KEY,
    txn_type           TEXT          NOT NULL,
    sender_wallet      TEXT          NOT NULL,
    receiver_wallet    TEXT,
    agent_id           TEXT,
    merchant_id        TEXT,
    biller             TEXT,
    amount             NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    fee                NUMERIC(10,2) NOT NULL DEFAULT 0,
    channel            TEXT,
    district           TEXT,
    event_time         TIMESTAMPTZ   NOT NULL,
    event_date         DATE          NOT NULL,
    source_ingest_date DATE          NOT NULL,
    processed_at       TIMESTAMPTZ   NOT NULL,
    batch_run_id       TEXT          NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_silver_txn_event_date ON silver.transactions (event_date);
CREATE INDEX IF NOT EXISTS ix_silver_txn_sender     ON silver.transactions (sender_wallet, event_time);

CREATE TABLE IF NOT EXISTS silver.batch_audit (
    batch_run_id        TEXT PRIMARY KEY,
    ingest_date         DATE NOT NULL,
    rows_read           INT,
    rows_valid          INT,
    rows_rejected       INT,
    duplicates_removed  INT,
    rows_inserted       INT,
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ
);
```

### 9.3 Idempotent load (stage → silver)

```sql
INSERT INTO silver.transactions AS t
SELECT * FROM silver.transactions_stage
ON CONFLICT (txn_id) DO NOTHING;
```

Run the DAG twice for the same day: the row count of `silver.transactions` must not change. This is your proof of idempotency (section 16).

---

## 10. Component 6: dbt project (silver → gold)

**Folder:** `dbt/`
**Adapter:** `dbt-postgres==1.9.1`
**Run by:** Airflow `dbt_build` task (`dbt build`), or manually with `make dbt-run`.

### 10.1 Layout

```
dbt/
  dbt_project.yml
  profiles.yml
  macros/
    generate_schema_name.sql    # outputs exact schema name (not public_gold)
  models/
    staging/
      sources.yml               # declares silver.transactions as a dbt source
      stg_transactions.sql      # view: thin wrapper, no business logic
      schema.yml                # tests: unique, not_null, accepted_values
    gold/
      daily_txn_summary.sql     # table: volume by day + type
      wallet_activity.sql       # table: lifetime stats per wallet
      district_heatmap.sql      # table: volume by district + date + type
      schema.yml                # column docs + tests
```

### 10.2 Models

| Model | Schema | Materialization | Grain | Description |
|---|---|---|---|---|
| `stg_transactions` | `staging` | view | 1 row per transaction | Selects from `silver.transactions`. No business logic. |
| `daily_txn_summary` | `gold` | table | 1 row per day per txn_type | Count, total/avg amount, total fee, unique senders, active districts. |
| `wallet_activity` | `gold` | table | 1 row per wallet | Lifetime totals: sent count, total sent, total fee, avg amount, first/last date. |
| `district_heatmap` | `gold` | table | 1 row per district per day per type | Transaction count and amounts for geographic analysis. |

### 10.3 Schema tests

| Model | Column | Test |
|---|---|---|
| `stg_transactions` | `txn_id` | `unique`, `not_null` |
| `stg_transactions` | `txn_type` | `accepted_values` (5 values) |
| `stg_transactions` | `sender_wallet`, `amount`, `event_date` | `not_null` |
| `wallet_activity` | `wallet_id` | `unique`, `not_null` |
| `daily_txn_summary`, `district_heatmap` | key columns | `not_null` |

### 10.4 Documentation

Run `dbt docs generate` and open `dbt/target/index.html` to view the lineage graph and column descriptions. Screenshot the lineage graph for the README.

> **Version 2** adds: `fct_transactions` (incremental), `dim_date`, `dim_wallet`, `dim_agent`, `mart_agent_liquidity`, `mart_fraud_summary`, `int_send_then_cashout` (R3 pass-through), and `dbt_utils` package tests. See [spec v2](mini-mfs-data-platform-spec-v2.md).

---

## 11. Component 7: Airflow DAG

**File:** `airflow/dags/mfs_daily_pipeline.py`
**Schedule:** daily at 01:00 (Asia/Dhaka), processing the previous day's `ingest_date`.
**Catchup:** enabled, so backfills work.
**Retries:** 2, with a 5-minute delay.

### 11.1 Tasks

```
wait_for_bronze  ──►  bronze_to_silver  ──►  load_silver  ──►  dbt_build  ──►  publish_audit
                                                                   │
                                                          (fails if any dbt test fails)
```

| Task | Operator | What it does |
|---|---|---|
| `wait_for_bronze` | Sensor (file/Python) | Waits until the bronze folder for `ingest_date = ds` exists and contains files. Times out after 1 hour. |
| `bronze_to_silver` | `SparkSubmitOperator` or `BashOperator` running `spark-submit` | Runs the batch job with `--ingest-date {{ ds }}`. |
| `load_silver` | `SQLExecuteQueryOperator` | Runs the upsert from 9.3 and updates `rows_inserted` in `batch_audit`. |
| `dbt_build` | `BashOperator` | `dbt build --project-dir /opt/dbt/mfs_dbt --profiles-dir /opt/dbt` |
| `publish_audit` | `PythonOperator` | Logs a summary: rows read / valid / rejected / inserted, tests passed. |

### 11.2 Rules

- Every task must be **idempotent**: rerunning any task for the same `ds` gives the same result.
- Use `{{ ds }}` (the logical date). Never use "today" inside a task, or backfills will process the wrong day.
- Keep business logic out of the DAG file. The DAG only calls jobs; the logic lives in Spark and dbt.

### 11.3 Experiments

- Clear `dbt_build` for one day and rerun only that task.
- Run a backfill for the last 3 days from the CLI.
- Break a dbt test on purpose (insert a row with a bad `txn_type` directly into silver) and watch the run fail.

---

## 12. Component 8: Dashboard

**Tool:** Apache Superset (replaces Metabase), connected to `mfs_dw.analytics`.

**Why Superset over Metabase:** Superset is fully open-source (Apache project), includes a full SQL Lab editor, supports 40+ chart types, and is used in production at Airbnb, Twitter and Lyft — higher portfolio value.

**Image:** `apache/superset:4.1.1` — port `8088`

### 12.1 Setup (one-time, after first start)

```bash
# Create admin user
docker exec -it superset superset fab create-admin \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --email admin@example.com

# Initialize internal database
docker exec -it superset superset db upgrade
docker exec -it superset superset init
```

Then open **http://localhost:8088**, add a PostgreSQL datasource pointing to `postgres-dw`, and build charts on top of the dbt gold tables.

### 12.2 Suggested charts

| Chart | Gold table | Type |
|---|---|---|
| Transaction volume over time by type | `gold.daily_txn_summary` | Line chart |
| Transaction count by type | `gold.daily_txn_summary` | Bar chart |
| Top wallets by volume | `gold.wallet_activity` | Table (top 10, sort by `total_sent` DESC) |
| Wallet segments | `gold.wallet_activity` | Scatter plot (`txn_sent_count` vs `total_sent`) |
| District activity | `gold.district_heatmap` | Bar chart (`txn_count` by `district`) |

### 12.3 When to add

Add in **Week 4** after the dbt gold tables exist. The `docker-compose.yml` entry is straightforward — Superset depends only on `postgres-dw`.

---

## 13. Infrastructure (Docker Compose)

### 13.1 Services

| Service | Image | Port(s) | Notes |
|---|---|---|---|
| `kafka` | `apache/kafka` (KRaft mode) | 9092 | Single broker |
| `kafka-ui` | `provectuslabs/kafka-ui` | 8080 | Browse topics and messages |
| `spark-master` | `apache/spark` | 7077, 8081 (UI) | |
| `spark-worker` | `apache/spark` | — | 2 cores, 2–3 GB memory |
| `postgres-dw` | `postgres` | 5432 | Warehouse `mfs_dw` |
| `postgres-airflow` | `postgres` | — | Airflow metadata only |
| `airflow` | `apache/airflow` (custom image with Java, Spark client, dbt-postgres) | 8088 | Use the standalone mode for simplicity |
| `generator` | custom Python image | — | Starts producing on launch |
| `superset` | `apache/superset` | 8088 | Dashboard on gold tables — add in Week 4 |

**Version note:** pin the current stable versions when you start building, and make sure the Spark–Kafka connector version matches your Spark version exactly. Mismatched versions are the most common set-up error.

### 13.2 Shared volumes

- `./data` → mounted at `/data` in Spark and Airflow (the lake and checkpoints).
- `./spark/jobs` → `/opt/spark-jobs`
- `./dbt` → `/opt/dbt`
- `./airflow/dags` → `/opt/airflow/dags`

### 13.3 Resource budget

| Component | Approx. memory |
|---|---|
| Kafka | 1 GB |
| Spark master + worker | 3–4 GB |
| Airflow | 2 GB |
| PostgreSQL × 2 | 1 GB |
| Metabase | 1.5 GB |
| **Total** | **~9–10 GB** |

On an 8 GB machine, skip Metabase and stop the generator when you're not testing streaming.

### 13.4 Make targets (convenience)

```
make up          # docker compose up -d
make down
make topics      # create Kafka topics
make stream      # submit the Spark streaming job
make batch D=2026-11-01   # run bronze→silver for one date
make dbt         # dbt build
make reset       # wipe data/ and checkpoints (careful)
```

---

## 14. Repository structure

```
mini-mfs-data-platform/
├── README.md
├── docker-compose.yml
├── Makefile
├── .env.example
├── docs/
│   ├── spec.md                  # this document
│   ├── architecture.png
│   └── decisions.md             # short notes on each design choice
├── generator/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py
│   ├── reference_data.py        # wallets, agents, merchants
│   └── scenarios.py             # duplicates, late data, fraud injection
├── spark/
│   ├── jobs/
│   │   ├── schemas.py           # shared transaction schema
│   │   ├── stream_ingest.py
│   │   └── bronze_to_silver.py
│   └── tests/
│       └── test_bronze_to_silver.py   # pytest + local SparkSession
├── sql/
│   └── init_dw.sql              # schemas and silver tables
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── mfs_daily_pipeline.py
├── dbt/
│   ├── profiles.yml
│   └── mfs_dbt/
│       ├── dbt_project.yml
│       ├── packages.yml         # dbt_utils
│       ├── models/ ...
│       └── tests/ ...
└── data/                        # git-ignored
    ├── lake/{bronze,silver,quarantine}/
    └── checkpoints/
```

---

## 15. Milestones and acceptance criteria

### Week 1 — Infrastructure and ingestion

- [ ] `docker compose up` starts Kafka, Kafka UI, Spark, PostgreSQL and Airflow.
- [ ] Topics `mfs.transactions` and `mfs.fraud_alerts` exist.
- [ ] Generator produces ~20 events/sec; messages visible in Kafka UI.
- [ ] You can explain: topic, partition, offset, consumer group, why the key is `sender_wallet`.
- [ ] Experiments in section 6 done.

### Week 2 — Spark streaming

- [ ] Bronze Parquet files appear under `ingest_date=` folders.
- [ ] You can read bronze in a PySpark shell or with DuckDB and see duplicates and malformed rows.
- [ ] R1 and R2 alerts appear in `mfs.fraud_alerts`.
- [ ] Stop/restart test passes: no missing or reprocessed offsets.
- [ ] You can explain: micro-batch, checkpoint, window, watermark, state.

### Week 3 — Batch and orchestration

- [x] `bronze_to_silver` produces silver Parquet and quarantine files for a given date.
- [x] `silver.transactions` has no duplicate `txn_id` (guaranteed by the primary key).
- [x] `batch_audit` row written for each run with rows_read, rows_valid, rows_rejected, duplicates_removed.
- [x] Airflow DAG runs end to end for one day (5 tasks).
- [x] Rerunning the same day leaves row counts unchanged (idempotency via `ON CONFLICT DO NOTHING`).
- [ ] Backfill of 3 days works. *(verify manually)*

### Week 4 — dbt, serving and documentation

- [x] All dbt models build (`stg_transactions`, `daily_txn_summary`, `wallet_activity`, `district_heatmap`).
- [x] All schema tests pass (unique, not_null, accepted_values).
- [x] dbt wired into Airflow `dbt_build` task — full pipeline runs end to end.
- [x] Apache Superset running on port 8088 with mfs_dw connection pre-registered.
- [x] Dashboard charts built on gold tables.
- [ ] `dbt docs generate` run; lineage screenshot added to README.
- [ ] README explains main design decisions.

> Items moved to v2: `fct_transactions` incremental, dimensional models, `mart_agent_liquidity`, `mart_fraud_summary`, R3 pass-through, pytest unit tests. See [spec v2](mini-mfs-data-platform-spec-v2.md).

---

## 16. Testing and verification

### 16.1 Data checks (SQL)

```sql
-- 1. No duplicates in silver (should return 0 rows)
SELECT txn_id, COUNT(*) FROM silver.transactions GROUP BY txn_id HAVING COUNT(*) > 1;

-- 2. Audit arithmetic for each run (should return 0 rows)
SELECT * FROM silver.batch_audit
WHERE rows_read <> rows_valid + rows_rejected;

-- 3. Gold tables populated
SELECT COUNT(*) FROM gold.daily_txn_summary;
SELECT COUNT(*) FROM gold.wallet_activity;
SELECT COUNT(*) FROM gold.district_heatmap;
```

### 16.3 Idempotency test

1. Run the DAG for `2026-11-01`. Record the row counts of `silver.transactions` and `analytics.fct_transactions`.
2. Clear and rerun the whole DAG for the same date.
3. Row counts must be identical.

### 16.4 Reconciliation (generator vs warehouse)

Have the generator write a small local log of every *unique* valid `txn_id` it produced. After a full day's run, compare that set with `silver.transactions`. Missing or extra IDs mean a bug somewhere. This is exactly the reconciliation discipline from your claims work, applied to data pipelines.

---

## 17. Stretch goals

In order of value for a fintech or banking data role:

1. **CDC with Debezium.** The generator writes to a PostgreSQL "core" database instead of Kafka; Debezium streams the database changes into Kafka. This mirrors how banks extract data from core banking systems.
2. **Lakehouse table format.** Replace plain Parquet folders with **Apache Iceberg** tables on **MinIO** (S3-compatible storage), and query them with **Trino**. Iceberg adds transactions, schema evolution and time travel on top of Parquet.
3. **Streaming pass-through rule.** Implement R3 in Spark with a stream–stream join and watermarks on both sides.
4. **CI/CD.** A GitHub Actions workflow that runs `pytest` for the Spark jobs and `dbt build` against a throwaway PostgreSQL container on every pull request.
5. **Kubernetes.** Deploy to a local `kind` cluster using the Spark Operator, the Strimzi Kafka operator and the official Airflow Helm chart. This uses your EKS experience directly.
6. **Data quality framework.** Add Great Expectations or Soda checks on silver.
7. **Observability.** Export pipeline metrics (rows processed, lag, failures) to Prometheus/Grafana, or use OpenTelemetry.

---

## 18. What you can say in an interview

When the project is finished, you should be able to explain each of these clearly:

- **Why bronze keeps raw data:** replayability. Any bug in parsing or cleaning can be fixed and silver rebuilt from bronze.
- **How duplicates are handled at three levels:** producer idempotence (network retries), dedupe within a batch (`ROW_NUMBER`), and a primary key with upsert across batches.
- **How late data is handled:** event-time partitioning in silver, a 3-day lookback in the incremental dbt model, and watermarks in streaming.
- **Why every batch step is idempotent,** and how you proved it.
- **Hot path vs cold path:** fast, approximate alerts versus slower, exact reporting, and why you need both.
- **Why `sender_wallet` is the Kafka key:** per-wallet ordering, which the fraud rules depend on.
- **Agent liquidity mart:** a real MFS operational problem (agents running out of e-money or cash) solved with a simple dimensional model.
- **What you would change for production:** multi-broker Kafka with replication, cloud object storage, Iceberg, Kubernetes, secrets management, monitoring and alerting, and data access controls for PII.
