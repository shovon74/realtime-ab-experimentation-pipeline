"""Spark 3.5 Structured Streaming job for the experimentation event pipeline.

Phase 1 (Streaming Core):
  - Connects to Apache Kafka (`exposures` and `conversions` topics).
  - Deserializes JSON payloads using explicit PySpark `StructType` schemas.
  - Computes real-time micro-batch aggregates per experiment/variant and prints
    live summaries to the console.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("spark_streaming")


# Explicit schemas matching Section 6 of the build specification
EXPOSURE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("experiment_id", StringType(), nullable=False),
        StructField("variant", StringType(), nullable=False),
        StructField("event_ts", StringType(), nullable=False),
    ]
)

CONVERSION_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("experiment_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("revenue", DoubleType(), nullable=False),
        StructField("event_ts", StringType(), nullable=False),
    ]
)


@dataclass(frozen=True)
class StreamingConfig:
    """Configuration parameters for the Spark Structured Streaming job."""

    app_name: str = "ExperimentationEventStreaming"
    master_url: str = os.getenv("SPARK_MASTER_URL", "local[2]")
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_INTERNAL_BOOTSTRAP_SERVERS",
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    exposures_topic: str = os.getenv("KAFKA_TOPIC_EXPOSURES", "exposures")
    conversions_topic: str = os.getenv("KAFKA_TOPIC_CONVERSIONS", "conversions")
    checkpoint_base_path: str = os.getenv("CHECKPOINT_BASE_PATH", "./checkpoints")
    trigger_seconds: int = int(os.getenv("MICROBATCH_TRIGGER_SECONDS", "10"))
    starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")


def create_spark_session(config: StreamingConfig) -> SparkSession:
    """Create and configure a SparkSession with the Kafka 0-10 SQL connector."""
    spark_kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    builder = (
        SparkSession.builder.appName(config.app_name)
        .master(config.master_url)
        .config("spark.jars.packages", spark_kafka_pkg)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Subscribe to a single Kafka topic as a streaming DataFrame."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_exposures(raw_df: DataFrame) -> DataFrame:
    """Deserialize exposure events from Kafka value JSON using explicit schema."""
    return (
        raw_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), EXPOSURE_SCHEMA).alias("data"),
            F.col("timestamp").alias("kafka_ingest_ts"),
        )
        .select("kafka_key", "data.*", "kafka_ingest_ts")
        .filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    )


def parse_conversions(raw_df: DataFrame) -> DataFrame:
    """Deserialize conversion events from Kafka value JSON using explicit schema."""
    return (
        raw_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), CONVERSION_SCHEMA).alias(
                "data"
            ),
            F.col("timestamp").alias("kafka_ingest_ts"),
        )
        .select("kafka_key", "data.*", "kafka_ingest_ts")
        .filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    )


def run_streaming_console(
    config: Optional[StreamingConfig] = None,
    await_timeout_sec: Optional[int] = None,
) -> None:
    """Start streaming queries for exposures and conversions, printing aggregates to console."""
    cfg = config or StreamingConfig()
    spark = create_spark_session(cfg)

    logger.info(
        "Starting Spark Structured Streaming (Phase 1 Console Core) | broker=%s",
        cfg.kafka_bootstrap_servers,
    )

    raw_exposures = read_kafka_stream(
        spark,
        cfg.kafka_bootstrap_servers,
        cfg.exposures_topic,
        cfg.starting_offsets,
    )
    raw_conversions = read_kafka_stream(
        spark,
        cfg.kafka_bootstrap_servers,
        cfg.conversions_topic,
        cfg.starting_offsets,
    )

    exposures_df = parse_exposures(raw_exposures)
    conversions_df = parse_conversions(raw_conversions)

    # Aggregate 1: Exposure counts per experiment_id and variant
    exposure_agg_df = exposures_df.groupBy("experiment_id", "variant").agg(
        F.count("*").alias("raw_exposure_events"),
        F.approx_count_distinct("user_id").alias("approx_unique_users"),
        F.min("event_timestamp").alias("min_event_ts"),
        F.max("event_timestamp").alias("max_event_ts"),
    )

    # Aggregate 2: Conversion counts and revenue per experiment_id and event_type
    conversion_agg_df = conversions_df.groupBy("experiment_id", "event_type").agg(
        F.count("*").alias("raw_conversion_events"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_order_value"),
        F.max("event_timestamp").alias("latest_conversion_ts"),
    )

    trigger_str = f"{cfg.trigger_seconds} seconds"

    exp_query = (
        exposure_agg_df.writeStream.queryName("exposures_console_aggregates")
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option(
            "checkpointLocation",
            os.path.join(cfg.checkpoint_base_path, "phase1_exposures_console"),
        )
        .trigger(processingTime=trigger_str)
        .start()
    )

    conv_query = (
        conversion_agg_df.writeStream.queryName("conversions_console_aggregates")
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option(
            "checkpointLocation",
            os.path.join(cfg.checkpoint_base_path, "phase1_conversions_console"),
        )
        .trigger(processingTime=trigger_str)
        .start()
    )

    logger.info(
        "Streaming queries started: %s, %s", exp_query.name, conv_query.name
    )

    if await_timeout_sec is not None:
        exp_query.awaitTermination(await_timeout_sec)
        exp_query.stop()
        conv_query.stop()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    timeout_env = os.getenv("SPARK_AWAIT_TIMEOUT_SEC")
    timeout_val = int(timeout_env) if timeout_env else None
    run_streaming_console(await_timeout_sec=timeout_val)
