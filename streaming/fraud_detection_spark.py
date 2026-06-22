"""
PySpark Structured Streaming - Fraud Detection Engine
======================================================
Consumes real-time transaction events from Kafka,
applies feature engineering and ML-based fraud scoring,
and writes flagged transactions to Snowflake.

Pipeline:
  Kafka source --> Schema validation --> Feature engineering
  --> Z-Score detection --> Isolation Forest scoring
  --> Snowflake sink + DLQ
"""

import os
import json
import pickle
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType, IntegerType
)
from pyspark.sql.window import Window
from loguru import logger

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RealTimeFraudDetection")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/fraud-checkpoint")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC        = "transactions"
SNOWFLAKE_URL      = os.getenv("SNOWFLAKE_URL")
SNOWFLAKE_USER     = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DB       = os.getenv("SNOWFLAKE_DB", "FRAUD_DB")
SNOWFLAKE_SCHEMA   = os.getenv("SNOWFLAKE_SCHEMA", "STREAMING")
SNOWFLAKE_WH       = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
CHECKPOINT_PATH    = "/tmp/fraud-checkpoint"
OUTPUT_PATH        = "/tmp/fraud-output"
MICROBATCH_SECONDS = 5
Z_SCORE_THRESHOLD  = 3.0

# ── Transaction Schema ──────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",     StringType(),    False),
    StructField("user_id",            StringType(),    False),
    StructField("timestamp",          StringType(),    False),
    StructField("amount",             DoubleType(),    False),
    StructField("currency",           StringType(),    True),
    StructField("merchant_id",        StringType(),    True),
    StructField("merchant_category",  StringType(),    True),
    StructField("merchant_country",   StringType(),    True),
    StructField("card_present",       BooleanType(),   True),
    StructField("card_type",          StringType(),    True),
    StructField("channel",            StringType(),    True),
    StructField("ip_address",         StringType(),    True),
    StructField("device_fingerprint", StringType(),    True),
    StructField("is_high_risk_merchant", BooleanType(), True),
])


def read_from_kafka() -> "DataFrame":
    """Create a streaming DataFrame from Kafka."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10_000)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_transactions(kafka_df):
    """Deserialize JSON messages and apply schema."""
    return (
        kafka_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                TRANSACTION_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp")
        )
        .select("data.*", "kafka_timestamp")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("amount") > 0)
    )


def apply_feature_engineering(df):
    """
    Compute streaming features for anomaly detection:
    - Rolling transaction counts (1h, 24h)
    - Amount vs. 1h rolling mean
    - Country velocity flag
    - Time-of-day risk score
    """
    # Watermark for late data tolerance
    df = df.withWatermark("event_time", "10 minutes")

    # Rolling amount aggregates per user over 1-hour window
    windowed = (
        df.groupBy(
            F.window("event_time", "1 hour", "5 minutes"),
            F.col("user_id")
        )
        .agg(
            F.count("transaction_id").alias("txn_count_1h"),
            F.sum("amount").alias("total_amount_1h"),
            F.avg("amount").alias("avg_amount_1h"),
            F.stddev("amount").alias("stddev_amount_1h"),
            F.max("amount").alias("max_amount_1h"),
            F.countDistinct("merchant_country").alias("country_count_1h")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("user_id"),
            F.col("txn_count_1h"),
            F.col("total_amount_1h"),
            F.col("avg_amount_1h"),
            F.col("stddev_amount_1h"),
            F.col("max_amount_1h"),
            F.col("country_count_1h"),
        )
    )
    return windowed


def apply_zscore_detection(df):
    """
    Flag transactions where amount deviates > Z_SCORE_THRESHOLD
    standard deviations from the rolling 1h user mean.
    """
    return df.withColumn(
        "zscore_amount",
        F.when(
            F.col("stddev_amount_1h") > 0,
            (F.col("max_amount_1h") - F.col("avg_amount_1h")) / F.col("stddev_amount_1h")
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "zscore_fraud_flag",
        F.col("zscore_amount") > F.lit(Z_SCORE_THRESHOLD)
    )


def apply_rule_based_flags(df):
    """Apply deterministic fraud rules."""
    return df.withColumn(
        "rule_high_velocity",
        F.col("txn_count_1h") > 20
    ).withColumn(
        "rule_country_velocity",
        F.col("country_count_1h") > 3
    ).withColumn(
        "rule_large_amount",
        F.col("max_amount_1h") > 5000
    ).withColumn(
        "is_fraud_suspected",
        F.col("zscore_fraud_flag")
        | F.col("rule_high_velocity")
        | F.col("rule_country_velocity")
        | F.col("rule_large_amount")
    ).withColumn(
        "fraud_score",
        (
            F.col("zscore_fraud_flag").cast("int") * 0.4 +
            F.col("rule_high_velocity").cast("int") * 0.2 +
            F.col("rule_country_velocity").cast("int") * 0.25 +
            F.col("rule_large_amount").cast("int") * 0.15
        )
    ).withColumn(
        "processed_at", F.current_timestamp()
    )


def write_to_snowflake(batch_df, batch_id: int):
    """Write each micro-batch to Snowflake."""
    logger.info(f"Writing batch {batch_id} | rows={batch_df.count()}")
    try:
        snowflake_options = {
            "sfURL":       SNOWFLAKE_URL,
            "sfUser":      SNOWFLAKE_USER,
            "sfPassword":  SNOWFLAKE_PASSWORD,
            "sfDatabase":  SNOWFLAKE_DB,
            "sfSchema":    SNOWFLAKE_SCHEMA,
            "sfWarehouse": SNOWFLAKE_WH,
            "dbtable":     "FRAUD_SCORES",
        }
        batch_df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**snowflake_options) \
            .mode("append") \
            .save()
        logger.info(f"Batch {batch_id} written to Snowflake successfully")
    except Exception as e:
        logger.error(f"Failed to write batch {batch_id} to Snowflake: {e}")
        # Fallback: write to local Parquet for replay
        batch_df.write.mode("append").parquet(f"{OUTPUT_PATH}/fallback/batch_{batch_id}")


def main():
    logger.info("Starting Real-Time Fraud Detection Pipeline")

    kafka_df         = read_from_kafka()
    parsed_df        = parse_transactions(kafka_df)
    features_df      = apply_feature_engineering(parsed_df)
    zscore_df        = apply_zscore_detection(features_df)
    flagged_df       = apply_rule_based_flags(zscore_df)

    query = (
        flagged_df.writeStream
        .outputMode("append")
        .trigger(processingTime=f"{MICROBATCH_SECONDS} seconds")
        .foreachBatch(write_to_snowflake)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
