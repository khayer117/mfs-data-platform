"""
Component 3: Spark Structured Streaming — MFS stream ingest
Runs three continuous streaming queries:

  Query A : mfs.transactions → Bronze Parquet (raw, partitioned by ingest_date)
  Query B1: R1_VELOCITY     → Kafka mfs.fraud_alerts + Bronze Parquet
  Query B2: R2_LARGE_CASHOUT→ Kafka mfs.fraud_alerts + Bronze Parquet

Submit with:
  spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
    --py-files /opt/spark-jobs/schemas.py \\
    /opt/spark-jobs/stream_ingest.py
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Allow local imports when schemas.py is in the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schemas

# ---------------------------------------------------------------------------
# Config — override via environment variables
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "kafka:29092")
SOURCE_TOPIC    = os.getenv("SOURCE_TOPIC",      "mfs.transactions")
ALERT_TOPIC     = os.getenv("ALERT_TOPIC",       "mfs.fraud_alerts")
LAKE_BASE       = os.getenv("LAKE_BASE",         "/data/lake")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE",   "/data/checkpoints")
TRIGGER_SECS    = os.getenv("TRIGGER_SECONDS",   "30")

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
    .appName("mfs-stream-ingest")
    .config("spark.sql.shuffle.partitions", "3")   # low = less overhead on 1 worker
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Read raw stream from Kafka
# startingOffsets=earliest: on first run, replay everything from offset 0.
# On restarts, the checkpoint overrides this and resumes from where we left off.
# ---------------------------------------------------------------------------
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")   # skip deleted offsets instead of crashing
    .load())

# ---------------------------------------------------------------------------
# Query A: Raw events → Bronze Parquet
#
# Store the original JSON exactly as received plus Kafka metadata.
# Never parse or transform — bronze is the source of truth for rebuilds.
# ---------------------------------------------------------------------------
bronze_df = (raw
    .select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn("ingest_time", F.current_timestamp())
    .withColumn("ingest_date", F.to_date(F.col("ingest_time"))))

bronze_query = (bronze_df.writeStream
    .queryName("bronze_transactions")
    .format("parquet")
    .option("path", f"{LAKE_BASE}/bronze/transactions")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_transactions")
    .partitionBy("ingest_date")
    .trigger(processingTime=f"{TRIGGER_SECS} seconds")
    .start())

print(f"[Query A] Bronze ingestion started → {LAKE_BASE}/bronze/transactions/")

# ---------------------------------------------------------------------------
# Parse JSON for fraud rules
#
# Explicit schema: from_json returns null for unparseable messages.
# We drop those (filter on txn_id) — they still land safely in bronze above.
# Watermark: tell Spark to wait up to 10 min for late events before closing
# a window. After 10 min, the window is finalised and state is freed.
# ---------------------------------------------------------------------------
parsed = (raw
    .select(
        F.from_json(
            F.col("value").cast("string"),
            schemas.TRANSACTION_SCHEMA,
        ).alias("d")
    )
    .select("d.*")
    .filter(F.col("txn_id").isNotNull() & F.col("event_time").isNotNull())
    .withColumn("event_time", F.to_timestamp(F.col("event_time")))
    .withWatermark("event_time", "10 minutes"))

# ---------------------------------------------------------------------------
# Shared alert writer — called by foreachBatch in both fraud queries.
# Writes one micro-batch to Kafka (as JSON) AND to bronze Parquet.
# Using foreachBatch lets a single streaming query write to two sinks.
# ---------------------------------------------------------------------------
def _write_alerts(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Kafka sink: key = sender_wallet (bytes), value = JSON (bytes)
    (batch_df
        .select(
            F.col("sender_wallet").cast("string").alias("key"),
            F.to_json(F.struct(
                "alert_id", "rule_id", "sender_wallet",
                "window_start", "window_end",
                "txn_count", "total_amount", "detected_at",
            )).alias("value"),
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", ALERT_TOPIC)
        .save())

    # Bronze Parquet sink: partition by alert date for easy daily querying
    (batch_df
        .withColumn("ingest_date", F.to_date(F.col("detected_at")))
        .write
        .mode("append")
        .partitionBy("ingest_date")
        .parquet(f"{LAKE_BASE}/bronze/fraud_alerts"))


# ---------------------------------------------------------------------------
# Query B1: R1_VELOCITY
# More than 5 transactions from the same wallet in a 1-minute tumbling window.
#
# outputMode("append"): only emit a window AFTER the watermark has passed it
# (i.e., after all late data for that minute has had a chance to arrive).
# This avoids false alerts from partial windows.
# ---------------------------------------------------------------------------
velocity_df = (parsed
    .groupBy(
        F.window("event_time", "1 minute"),
        F.col("sender_wallet"),
    )
    .agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("txn_count") > 5)
    .select(
        F.concat(F.lit("ALT-"), F.expr("uuid()")).alias("alert_id"),
        F.lit("R1_VELOCITY").alias("rule_id"),
        F.col("sender_wallet"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("txn_count"),
        F.col("total_amount"),
        F.current_timestamp().alias("detected_at"),
    ))

velocity_query = (velocity_df.writeStream
    .queryName("fraud_r1_velocity")
    .outputMode("append")
    .foreachBatch(_write_alerts)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/fraud_r1_velocity")
    .trigger(processingTime=f"{TRIGGER_SECS} seconds")
    .start())

print("[Query B1] R1_VELOCITY fraud rule started (>5 txns/wallet/minute)")

# ---------------------------------------------------------------------------
# Query B2: R2_LARGE_CASHOUT
# Any single CASH_OUT transaction with amount > ৳25,000.
# Simple filter — no windowing or state needed.
# ---------------------------------------------------------------------------
large_cashout_df = (parsed
    .filter(
        (F.col("txn_type") == "CASH_OUT") &
        (F.col("amount") > 25_000)
    )
    .select(
        F.concat(F.lit("ALT-"), F.expr("uuid()")).alias("alert_id"),
        F.lit("R2_LARGE_CASHOUT").alias("rule_id"),
        F.col("sender_wallet"),
        F.col("event_time").alias("window_start"),
        F.col("event_time").alias("window_end"),
        F.lit(1).cast("long").alias("txn_count"),
        F.col("amount").alias("total_amount"),
        F.current_timestamp().alias("detected_at"),
    ))

large_cashout_query = (large_cashout_df.writeStream
    .queryName("fraud_r2_large_cashout")
    .outputMode("append")
    .foreachBatch(_write_alerts)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/fraud_r2_large_cashout")
    .trigger(processingTime=f"{TRIGGER_SECS} seconds")
    .start())

print("[Query B2] R2_LARGE_CASHOUT fraud rule started (CASH_OUT > 25,000)")
print()
print("All 3 streaming queries running. Ctrl+C to stop.")
print(f"  Bronze transactions : {LAKE_BASE}/bronze/transactions/")
print(f"  Bronze fraud alerts : {LAKE_BASE}/bronze/fraud_alerts/")
print(f"  Kafka alert topic   : {ALERT_TOPIC}")
print(f"  Trigger interval    : {TRIGGER_SECS}s")
print()

# Block until any query fails or is manually stopped.
# On Ctrl+C, Spark shuts down all queries and saves checkpoints.
spark.streams.awaitAnyTermination()
