"""
Shared Spark schemas for MFS transactions and fraud alerts.
Used by both stream_ingest.py (streaming) and bronze_to_silver.py (batch).
"""
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# amount/fee use DoubleType because JSON has no decimal type.
# They are cast to DecimalType(12,2) / DecimalType(10,2) in bronze_to_silver.py.
# event_time is kept as StringType here and cast to TimestampType in each job
# for explicit control over timezone handling.
TRANSACTION_SCHEMA = StructType([
    StructField("txn_id",          StringType(), True),
    StructField("txn_type",        StringType(), True),
    StructField("sender_wallet",   StringType(), True),
    StructField("receiver_wallet", StringType(), True),
    StructField("agent_id",        StringType(), True),
    StructField("merchant_id",     StringType(), True),
    StructField("biller",          StringType(), True),
    StructField("amount",          DoubleType(), True),
    StructField("fee",             DoubleType(), True),
    StructField("channel",         StringType(), True),
    StructField("district",        StringType(), True),
    StructField("event_time",      StringType(), True),  # ISO-8601 UTC string
])

# Schema of a fraud alert row (written to Kafka + bronze Parquet).
ALERT_SCHEMA = StructType([
    StructField("alert_id",      StringType(),   False),
    StructField("rule_id",       StringType(),   False),
    StructField("sender_wallet", StringType(),   False),
    StructField("window_start",  TimestampType(), True),
    StructField("window_end",    TimestampType(), True),
    StructField("txn_count",     LongType(),      True),
    StructField("total_amount",  DoubleType(),    True),
    StructField("detected_at",   TimestampType(), False),
])
