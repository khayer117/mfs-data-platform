-- init_dw.sql: PostgreSQL DDL for the MFS data warehouse (mfs_dw)
-- Runs automatically when the postgres-dw container first starts.
-- User: mfs  Database: mfs_dw

-- ---------------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS analytics_staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- silver.transactions — one row per unique txn_id, the source of truth.
-- Primary key enforces deduplication at the database level (second safety net
-- after the ROW_NUMBER dedupe in bronze_to_silver.py).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.transactions (
    txn_id              TEXT          PRIMARY KEY,
    txn_type            TEXT          NOT NULL,
    sender_wallet       TEXT          NOT NULL,
    receiver_wallet     TEXT,
    agent_id            TEXT,
    merchant_id         TEXT,
    biller              TEXT,
    amount              NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    fee                 NUMERIC(10,2) NOT NULL DEFAULT 0,
    channel             TEXT,
    district            TEXT,
    event_time          TIMESTAMPTZ   NOT NULL,
    event_date          DATE          NOT NULL,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    kafka_timestamp     TIMESTAMPTZ,
    source_ingest_date  DATE          NOT NULL,
    processed_at        TIMESTAMPTZ   NOT NULL,
    batch_run_id        TEXT          NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_silver_txn_event_date
    ON silver.transactions (event_date);

CREATE INDEX IF NOT EXISTS ix_silver_txn_sender
    ON silver.transactions (sender_wallet, event_time);

-- ---------------------------------------------------------------------------
-- silver.transactions_stage — overwritten by each Spark batch run.
-- Airflow's load_silver task upserts from here into silver.transactions.
-- No constraints — Spark JDBC overwrite needs a clean target.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.transactions_stage (
    txn_id              TEXT,
    txn_type            TEXT,
    sender_wallet       TEXT,
    receiver_wallet     TEXT,
    agent_id            TEXT,
    merchant_id         TEXT,
    biller              TEXT,
    amount              NUMERIC(12,2),
    fee                 NUMERIC(10,2),
    channel             TEXT,
    district            TEXT,
    event_time          TIMESTAMPTZ,
    event_date          DATE,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    kafka_timestamp     TIMESTAMPTZ,
    source_ingest_date  DATE,
    processed_at        TIMESTAMPTZ,
    batch_run_id        TEXT
);

-- ---------------------------------------------------------------------------
-- silver.batch_audit — one row per Spark batch run.
-- rows_read = rows_valid + rows_rejected (arithmetic check in dbt tests).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.batch_audit (
    batch_run_id        TEXT          PRIMARY KEY,
    ingest_date         DATE          NOT NULL,
    rows_read           INT,
    rows_valid          INT,
    rows_rejected       INT,
    duplicates_removed  INT,
    rows_inserted       INT,
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- silver.fraud_alerts — mirrored from Kafka mfs.fraud_alerts topic.
-- Populated in a future component; schema defined here for consistency.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.fraud_alerts (
    alert_id        TEXT          PRIMARY KEY,
    rule_id         TEXT          NOT NULL,
    sender_wallet   TEXT          NOT NULL,
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    txn_count       INT,
    total_amount    NUMERIC(14,2),
    detected_at     TIMESTAMPTZ   NOT NULL,
    ingest_date     DATE          NOT NULL
);

-- ---------------------------------------------------------------------------
-- Verification query (run manually to confirm setup):
--   SELECT table_schema, table_name FROM information_schema.tables
--   WHERE table_schema IN ('silver','analytics','analytics_staging')
--   ORDER BY 1, 2;
-- ---------------------------------------------------------------------------
