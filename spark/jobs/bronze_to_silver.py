"""
Component 4: Spark batch job — bronze → silver
Argument: --ingest-date YYYY-MM-DD

Steps:
  1. Read bronze for the ingest_date partition
  2. Parse raw_value JSON using TRANSACTION_SCHEMA
  3. Validate each record — assign reject_reason
  4. Write rejected records to quarantine
  5. Deduplicate on txn_id (keep earliest by kafka_timestamp/partition/offset)
  6. Cast/standardise types — produce final silver DataFrame
  7. Write silver Parquet (dynamic overwrite, partitioned by event_date)
  8. Write silver staging table to PostgreSQL (overwrite — fresh each run)
  9. Append audit row to silver.batch_audit

Submit:
  spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.7.3 \\
    --py-files /opt/spark-jobs/schemas.py \\
    /opt/spark-jobs/bronze_to_silver.py \\
    --ingest-date 2026-11-09
"""
import argparse
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schemas

# ---------------------------------------------------------------------------
# Config — override via environment variables
# ---------------------------------------------------------------------------
LAKE_BASE   = os.getenv("LAKE_BASE",    "/data/lake")
PG_URL      = os.getenv("PG_URL",       "jdbc:postgresql://postgres-dw:5432/mfs_dw")
PG_USER     = os.getenv("PG_USER",      "mfs")
PG_PASSWORD = os.getenv("PG_PASSWORD",  "mfs")

VALID_TXN_TYPES = {"CASH_IN", "CASH_OUT", "SEND_MONEY", "MERCHANT_PAY", "BILL_PAY"}

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="MFS bronze → silver batch job")
parser.add_argument(
    "--ingest-date", required=True,
    help="YYYY-MM-DD partition to process (must exist in bronze/transactions/)",
)
args         = parser.parse_args()
INGEST_DATE  = args.ingest_date
BATCH_RUN_ID = f"batch-{INGEST_DATE}-{uuid.uuid4().hex[:8]}"

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
    .appName(f"mfs-bronze-to-silver-{INGEST_DATE}")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    # dynamic overwrite: only replace event_date partitions that this run touches
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # use raw local filesystem to avoid Hadoop .crc checksum files on Windows bind mounts
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

started_at = datetime.now(timezone.utc)
print(f"\n[bronze_to_silver] STARTING")
print(f"  batch_run_id = {BATCH_RUN_ID}")
print(f"  ingest_date  = {INGEST_DATE}")

# ---------------------------------------------------------------------------
# 1. Read bronze partition for this ingest_date
# ---------------------------------------------------------------------------
bronze_path = f"{LAKE_BASE}/bronze/transactions/ingest_date={INGEST_DATE}"
print(f"\n[1] Reading bronze: {bronze_path}")

bronze    = spark.read.parquet(bronze_path)
rows_read = bronze.count()
print(f"    rows_read = {rows_read}")

# ---------------------------------------------------------------------------
# 2. Parse raw_value JSON using the shared transaction schema
# ---------------------------------------------------------------------------
parsed = (bronze
    .withColumn("d", F.from_json(F.col("raw_value"), schemas.TRANSACTION_SCHEMA))
    .select(
        F.col("d.txn_id"),
        F.col("d.txn_type"),
        F.col("d.sender_wallet"),
        F.col("d.receiver_wallet"),
        F.col("d.agent_id"),
        F.col("d.merchant_id"),
        F.col("d.biller"),
        F.col("d.amount"),
        F.col("d.fee"),
        F.col("d.channel"),
        F.col("d.district"),
        F.col("d.event_time"),
        # Keep Kafka provenance for deduplication ordering
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("raw_value"),   # retained for quarantine records
    ))

# ---------------------------------------------------------------------------
# 3. Validate — assign reject_reason (null = valid)
#
# PARSE_ERROR heuristic: if txn_id AND txn_type AND amount are all null,
# from_json failed to parse the whole record. Individual missing fields
# fall through to their own checks below.
# ---------------------------------------------------------------------------
future_cutoff = F.current_timestamp() + F.expr("INTERVAL 1 HOUR")

validated = (parsed
    .withColumn("event_time_ts", F.to_timestamp(F.col("event_time")))
    .withColumn("reject_reason",
        F.when(
            F.col("txn_id").isNull() & F.col("txn_type").isNull() & F.col("amount").isNull(),
            F.lit("PARSE_ERROR"),
        ).when(
            F.col("txn_id").isNull() | (F.length(F.trim(F.col("txn_id"))) == 0),
            F.lit("MISSING_TXN_ID"),
        ).when(
            F.col("amount").isNull() | (F.col("amount") <= 0),
            F.lit("INVALID_AMOUNT"),
        ).when(
            ~F.col("txn_type").isin(list(VALID_TXN_TYPES)),
            F.lit("UNKNOWN_TXN_TYPE"),
        ).when(
            F.col("event_time_ts").isNull() | (F.col("event_time_ts") > future_cutoff),
            F.lit("INVALID_EVENT_TIME"),
        ).otherwise(F.lit(None).cast("string"))
    ))

# Split into rejected and valid sets (cache valid — used twice: count + dedupe)
rejected = validated.filter(F.col("reject_reason").isNotNull())
valid    = validated.filter(F.col("reject_reason").isNull()).cache()

rows_rejected            = rejected.count()
rows_valid_before_dedupe = valid.count()
print(f"\n[2/3] Parse + Validate")
print(f"      rows_valid    = {rows_valid_before_dedupe}")
print(f"      rows_rejected = {rows_rejected}")

# ---------------------------------------------------------------------------
# 4. Write quarantine (rejected records with reject_reason column)
#    Written as JSON lines via Python native I/O to avoid Hadoop _temporary
#    staging issues on Windows Docker bind mounts.
# ---------------------------------------------------------------------------
if rows_rejected > 0:
    import os
    import json as _json
    quarantine_path = f"{LAKE_BASE}/quarantine/transactions/ingest_date={INGEST_DATE}"
    os.makedirs(quarantine_path, exist_ok=True)
    quarantine_file = f"{quarantine_path}/rejected_{BATCH_RUN_ID}.jsonl"
    rows = (rejected
        .select(
            "raw_value", "reject_reason",
            "txn_id", "txn_type", "amount",
            "kafka_partition", "kafka_offset", "kafka_timestamp",
        )
        .toJSON()
        .collect())
    with open(quarantine_file, "w") as f:
        for row in rows:
            f.write(row + "\n")
    print(f"[4] Quarantine written: {quarantine_file}  ({rows_rejected} rows)")
else:
    print("[4] No rejected records — quarantine skipped")

# ---------------------------------------------------------------------------
# 5. Deduplicate on txn_id
# Keep the copy that arrived first: lowest kafka_timestamp, then partition,
# then offset. This handles both same-day and cross-day duplicates.
# Cross-day duplicates are caught again by ON CONFLICT in the warehouse.
# ---------------------------------------------------------------------------
w = (Window
    .partitionBy("txn_id")
    .orderBy("kafka_timestamp", "kafka_partition", "kafka_offset"))

deduped = (valid
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn"))

duplicates_removed = rows_valid_before_dedupe - deduped.count()
print(f"\n[5] Deduplication")
print(f"    duplicates_removed = {duplicates_removed}")

# ---------------------------------------------------------------------------
# 6. Standardise types and add lineage columns
# ---------------------------------------------------------------------------
silver_df = (deduped
    .withColumn("txn_type",   F.upper(F.trim(F.col("txn_type"))))
    .withColumn("amount",     F.col("amount").cast(DecimalType(12, 2)))
    .withColumn("fee",        F.coalesce(F.col("fee"), F.lit(0.0)).cast(DecimalType(10, 2)))
    .withColumn("event_time", F.col("event_time_ts"))
    .withColumn("event_date", F.to_date(F.col("event_time_ts")))
    .withColumn("source_ingest_date", F.lit(INGEST_DATE).cast("date"))
    .withColumn("processed_at",       F.current_timestamp())
    .withColumn("batch_run_id",       F.lit(BATCH_RUN_ID))
    .drop("event_time_ts", "reject_reason", "raw_value")
    .select(
        "txn_id", "txn_type", "sender_wallet", "receiver_wallet",
        "agent_id", "merchant_id", "biller",
        "amount", "fee", "channel", "district",
        "event_time", "event_date",
        "kafka_partition", "kafka_offset", "kafka_timestamp",
        "source_ingest_date", "processed_at", "batch_run_id",
    ))

rows_silver = silver_df.count()
print(f"\n[6] Silver rows = {rows_silver}")

# ---------------------------------------------------------------------------
# 7. Write silver Parquet (dynamic partition overwrite by event_date)
# A late transaction (event_date = yesterday) replaces only yesterday's
# partition, leaving other dates intact.
# ---------------------------------------------------------------------------
silver_path = f"{LAKE_BASE}/silver/transactions"
(silver_df
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(silver_path))
print(f"[7] Silver Parquet written: {silver_path}")

# ---------------------------------------------------------------------------
# 8. Write PostgreSQL staging table (overwrite — fresh each run)
# Airflow's load_silver task then upserts from here into silver.transactions.
# ---------------------------------------------------------------------------
pg_props = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

(silver_df
    .write
    .mode("overwrite")
    .jdbc(PG_URL, "silver.transactions_stage", properties=pg_props))
print("[8] PostgreSQL: silver.transactions_stage written")

# ---------------------------------------------------------------------------
# 9. Append audit row to silver.batch_audit
# ---------------------------------------------------------------------------
finished_at = datetime.now(timezone.utc)

audit_data = [(
    BATCH_RUN_ID,
    INGEST_DATE,
    rows_read,
    rows_valid_before_dedupe,
    rows_rejected,
    duplicates_removed,
    rows_silver,
    started_at.isoformat(),
    finished_at.isoformat(),
)]
audit_cols = [
    "batch_run_id", "ingest_date",
    "rows_read", "rows_valid", "rows_rejected",
    "duplicates_removed", "rows_inserted",
    "started_at", "finished_at",
]
audit_df = (spark.createDataFrame(audit_data, schema=audit_cols)
    .withColumn("ingest_date",  F.col("ingest_date").cast("date"))
    .withColumn("started_at",   F.col("started_at").cast("timestamp"))
    .withColumn("finished_at",  F.col("finished_at").cast("timestamp")))

(audit_df
    .write
    .mode("append")
    .jdbc(PG_URL, "silver.batch_audit", properties=pg_props))
print("[9] PostgreSQL: silver.batch_audit row appended")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
duration = (finished_at - started_at).total_seconds()
print(f"""
{'='*52}
  bronze_to_silver COMPLETE
  batch_run_id      : {BATCH_RUN_ID}
  ingest_date       : {INGEST_DATE}
  rows_read         : {rows_read}
  rows_valid        : {rows_valid_before_dedupe}
  rows_rejected     : {rows_rejected}
  duplicates_removed: {duplicates_removed}
  rows_silver       : {rows_silver}
  duration          : {duration:.1f}s
{'='*52}
""")
