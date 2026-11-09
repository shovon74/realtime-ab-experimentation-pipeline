"""Spark 3.5 Structured Streaming job for the experimentation event pipeline.

Phase 2 (Data Lake — Watermarks, Deduplication, Stream-Stream Join, Orphans & Parquet):
  - Reads `exposures` and `conversions` Kafka topics using explicit `StructType` schemas.
  - Applies configurable event-time watermarks:
      * 10 minutes on `exposures` (`EXPOSURE_WATERMARK_MINUTES`)
      * 30 minutes on `conversions` (`CONVERSION_WATERMARK_MINUTES`)
  - Deduplicates both streams on `event_id` within the watermark state.
  - Performs a time-bounded left stream-stream join (`exposures` LEFT JOIN `conversions`)
    on `user_id` and `experiment_id` within the attribution window so non-converting
    users are preserved.
  - Isolates unattributed ("orphan" and out-of-window late) conversions into a separate
    `lake/orphans/` sink so they are never attributed to any experiment variant.
  - Writes coalesced Parquet partitioned by `event_date` and `experiment_id` with
    checkpointing to:
      * `lake/exposures/event_date=YYYY-MM-DD/experiment_id=<id>/`
      * `lake/conversions/event_date=YYYY-MM-DD/experiment_id=<id>/`
      * `lake/joined/event_date=YYYY-MM-DD/experiment_id=<id>/`
      * `lake/orphans/event_date=YYYY-MM-DD/experiment_id=<id>/`
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark.lake_processor import (
    deduplicate_events_within_watermark,
    join_exposures_and_conversions,
    resolve_storage_path,
    write_partitioned_parquet,
)

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
    """Configuration parameters for Spark Structured Streaming and the Parquet Lake."""

    app_name: str = "ExperimentationEventStreaming"
    master_url: str = os.getenv("SPARK_MASTER_URL", "local[2]")
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_INTERNAL_BOOTSTRAP_SERVERS",
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    exposures_topic: str = os.getenv("KAFKA_TOPIC_EXPOSURES", "exposures")
    conversions_topic: str = os.getenv("KAFKA_TOPIC_CONVERSIONS", "conversions")
    lake_base_path: str = os.getenv("LAKE_BASE_PATH", "./lake")
    checkpoint_base_path: str = os.getenv("CHECKPOINT_BASE_PATH", "./checkpoints")

    exposure_watermark_minutes: int = int(
        os.getenv("EXPOSURE_WATERMARK_MINUTES", "10")
    )
    conversion_watermark_minutes: int = int(
        os.getenv("CONVERSION_WATERMARK_MINUTES", "30")
    )
    attribution_window_hours: int = int(
        os.getenv("ATTRIBUTION_WINDOW_HOURS", "24")
    )
    demo_attribution_window_minutes: int = int(
        os.getenv("DEMO_ATTRIBUTION_WINDOW_MINUTES", "30")
    )
    trigger_seconds: int = int(os.getenv("MICROBATCH_TRIGGER_SECONDS", "10"))
    partitions_coalesce: int = int(os.getenv("PARTITIONS_COALESCE", "1"))
    output_mode: str = os.getenv("STREAM_OUTPUT_MODE", "lake")
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


def parse_and_deduplicate_exposures(
    raw_df: DataFrame, watermark_minutes: int
) -> DataFrame:
    """Deserialize exposures JSON, apply watermark, and deduplicate on `event_id`."""
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
        .withColumn(
            "event_date",
            F.date_format(F.col("event_timestamp"), "yyyy-MM-dd"),
        )
        .withWatermark("event_timestamp", f"{watermark_minutes} minutes")
        .dropDuplicates(["event_id"])
    )


def parse_and_deduplicate_conversions(
    raw_df: DataFrame, watermark_minutes: int
) -> DataFrame:
    """Deserialize conversions JSON, apply watermark, and deduplicate on `event_id`."""
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
        .withColumn(
            "event_date",
            F.date_format(F.col("event_timestamp"), "yyyy-MM-dd"),
        )
        .withWatermark("event_timestamp", f"{watermark_minutes} minutes")
        .dropDuplicates(["event_id"])
    )


def build_stream_stream_left_join(
    exposures_df: DataFrame,
    conversions_df: DataFrame,
    attribution_window_minutes: int,
) -> DataFrame:
    """Build Spark Structured Streaming time-bounded left join (exposures -> conversions).

    Uses a left outer join with an event-time interval condition so non-converting
    exposed users are retained (`converted = False`, `revenue = 0.0`).
    """
    exp = exposures_df.select(
        F.col("event_id").alias("exposure_event_id"),
        F.col("user_id").alias("exp_user_id"),
        F.col("experiment_id").alias("exp_experiment_id"),
        F.col("variant"),
        F.col("event_ts").alias("exposure_ts"),
        F.col("event_timestamp").alias("exp_timestamp"),
        F.col("event_date"),
    )

    conv = conversions_df.select(
        F.col("event_id").alias("conversion_event_id"),
        F.col("user_id").alias("conv_user_id"),
        F.col("experiment_id").alias("conv_experiment_id"),
        F.col("event_type"),
        F.col("revenue").alias("conv_revenue"),
        F.col("event_ts").alias("conversion_ts"),
        F.col("event_timestamp").alias("conv_timestamp"),
    )

    join_condition = F.expr(
        f"""
        exp_user_id = conv_user_id
        AND exp_experiment_id = conv_experiment_id
        AND conv_timestamp >= exp_timestamp
        AND conv_timestamp <= exp_timestamp + INTERVAL {attribution_window_minutes} MINUTES
        """
    )

    joined = exp.join(conv, join_condition, how="leftOuter")

    return joined.select(
        F.col("exposure_event_id"),
        F.col("exp_user_id").alias("user_id"),
        F.col("exp_experiment_id").alias("experiment_id"),
        F.col("variant"),
        F.col("exposure_ts"),
        F.col("conversion_event_id"),
        F.col("event_type"),
        F.when(F.col("conversion_event_id").isNotNull(), F.lit(True))
        .otherwise(F.lit(False))
        .alias("converted"),
        F.coalesce(F.col("conv_revenue"), F.lit(0.0)).alias("revenue"),
        F.col("conversion_ts"),
        F.when(
            F.col("conv_timestamp").isNotNull(),
            F.round(
                F.col("conv_timestamp").cast("double")
                - F.col("exp_timestamp").cast("double"),
                2,
            ),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("conversion_delay_sec"),
        F.col("event_date"),
    )


def make_parquet_foreach_batch_writer(
    dataset_path: str, coalesce_partitions: int = 1
):
    """Create a foreachBatch callback that writes coalesced partitioned Parquet."""

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        (
            batch_df.coalesce(coalesce_partitions)
            .write.mode("append")
            .partitionBy("event_date", "experiment_id")
            .parquet(dataset_path)
        )
        logger.info(
            "Wrote micro-batch %d to partitioned Parquet sink: %s",
            batch_id,
            dataset_path,
        )

    return _write_batch


def make_orphan_foreach_batch_writer(
    exposures_lake_path: str,
    orphans_lake_path: str,
    attribution_window_minutes: int,
    coalesce_partitions: int = 1,
):
    """Create a foreachBatch writer that isolates unattributed/orphan conversions.

    Checks incoming conversion micro-batches against existing exposures in the lake.
    Any conversion with no matching (`user_id`, `experiment_id`) exposure or arriving
    outside `[exposure_ts, exposure_ts + attribution_window]` is written to
    `lake/orphans/` and never attributed to a variant.
    """

    def _write_orphans(conv_batch_df: DataFrame, batch_id: int) -> None:
        if conv_batch_df.isEmpty():
            return
        spark = conv_batch_df.sparkSession

        try:
            exp_static_df = (
                spark.read.parquet(exposures_lake_path)
                .select(
                    F.col("user_id").alias("exp_user_id"),
                    F.col("experiment_id").alias("exp_experiment_id"),
                    F.col("event_timestamp").alias("exp_timestamp"),
                )
                .dropDuplicates(["exp_user_id", "exp_experiment_id"])
            )

            matched = conv_batch_df.join(
                exp_static_df,
                (conv_batch_df["user_id"] == exp_static_df["exp_user_id"])
                & (
                    conv_batch_df["experiment_id"]
                    == exp_static_df["exp_experiment_id"]
                ),
                how="left",
            )

            orphan_df = (
                matched.filter(
                    F.col("exp_timestamp").isNull()
                    | (F.col("event_timestamp") < F.col("exp_timestamp"))
                    | (
                        F.col("event_timestamp")
                        > F.col("exp_timestamp")
                        + F.expr(f"INTERVAL {attribution_window_minutes} MINUTES")
                    )
                )
                .withColumn(
                    "orphan_reason",
                    F.when(
                        F.col("exp_timestamp").isNull(),
                        F.lit("no_matching_exposure"),
                    )
                    .when(
                        F.col("event_timestamp") < F.col("exp_timestamp"),
                        F.lit("conversion_before_exposure"),
                    )
                    .otherwise(F.lit("late_past_watermark")),
                )
                .select(
                    F.col("event_id").alias("conversion_event_id"),
                    F.col("user_id"),
                    F.col("experiment_id"),
                    F.col("event_type"),
                    F.col("revenue"),
                    F.col("event_ts").alias("conversion_ts"),
                    F.col("orphan_reason"),
                    F.col("event_date"),
                )
            )
        except Exception:
            # If no exposures exist in the lake yet, all conversions in this batch are orphans
            orphan_df = conv_batch_df.select(
                F.col("event_id").alias("conversion_event_id"),
                F.col("user_id"),
                F.col("experiment_id"),
                F.col("event_type"),
                F.col("revenue"),
                F.col("event_ts").alias("conversion_ts"),
                F.lit("no_matching_exposure").alias("orphan_reason"),
                F.col("event_date"),
            )

        if not orphan_df.isEmpty():
            (
                orphan_df.coalesce(coalesce_partitions)
                .write.mode("append")
                .partitionBy("event_date", "experiment_id")
                .parquet(orphans_lake_path)
            )
            logger.info(
                "Isolated orphan conversions in micro-batch %d -> %s",
                batch_id,
                orphans_lake_path,
            )

    return _write_orphans


def run_streaming_pipeline(
    config: Optional[StreamingConfig] = None,
    await_timeout_sec: Optional[int] = None,
) -> None:
    """Start all Phase 2 Spark Structured Streaming queries (Lake + optional Console)."""
    cfg = config or StreamingConfig()
    spark = create_spark_session(cfg)

    logger.info(
        "Starting Spark Structured Streaming (Phase 2 Lake) | broker=%s | lake=%s | mode=%s",
        cfg.kafka_bootstrap_servers,
        cfg.lake_base_path,
        cfg.output_mode,
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

    exposures_df = parse_and_deduplicate_exposures(
        raw_exposures, cfg.exposure_watermark_minutes
    )
    conversions_df = parse_and_deduplicate_conversions(
        raw_conversions, cfg.conversion_watermark_minutes
    )

    joined_df = build_stream_stream_left_join(
        exposures_df,
        conversions_df,
        cfg.demo_attribution_window_minutes,
    )

    trigger_str = f"{cfg.trigger_seconds} seconds"
    queries: List[StreamingQuery] = []

    exposures_path = resolve_storage_path(cfg.lake_base_path, "exposures")
    conversions_path = resolve_storage_path(cfg.lake_base_path, "conversions")
    joined_path = resolve_storage_path(cfg.lake_base_path, "joined")
    orphans_path = resolve_storage_path(cfg.lake_base_path, "orphans")

    if cfg.output_mode in {"lake", "both"}:
        # 1. Deduplicated Exposures -> lake/exposures/
        q_exp = (
            exposures_df.writeStream.queryName("lake_exposures_sink")
            .outputMode("append")
            .foreachBatch(
                make_parquet_foreach_batch_writer(
                    exposures_path, cfg.partitions_coalesce
                )
            )
            .option(
                "checkpointLocation",
                os.path.join(cfg.checkpoint_base_path, "lake_exposures"),
            )
            .trigger(processingTime=trigger_str)
            .start()
        )
        queries.append(q_exp)

        # 2. Deduplicated Conversions -> lake/conversions/
        q_conv = (
            conversions_df.writeStream.queryName("lake_conversions_sink")
            .outputMode("append")
            .foreachBatch(
                make_parquet_foreach_batch_writer(
                    conversions_path, cfg.partitions_coalesce
                )
            )
            .option(
                "checkpointLocation",
                os.path.join(cfg.checkpoint_base_path, "lake_conversions"),
            )
            .trigger(processingTime=trigger_str)
            .start()
        )
        queries.append(q_conv)

        # 3. Stream-Stream Left Join -> lake/joined/
        q_joined = (
            joined_df.writeStream.queryName("lake_joined_sink")
            .outputMode("append")
            .foreachBatch(
                make_parquet_foreach_batch_writer(
                    joined_path, cfg.partitions_coalesce
                )
            )
            .option(
                "checkpointLocation",
                os.path.join(cfg.checkpoint_base_path, "lake_joined"),
            )
            .trigger(processingTime=trigger_str)
            .start()
        )
        queries.append(q_joined)

        # 4. Unattributed Orphan Conversions -> lake/orphans/
        q_orphans = (
            conversions_df.writeStream.queryName("lake_orphans_sink")
            .outputMode("append")
            .foreachBatch(
                make_orphan_foreach_batch_writer(
                    exposures_lake_path=exposures_path,
                    orphans_lake_path=orphans_path,
                    attribution_window_minutes=cfg.demo_attribution_window_minutes,
                    coalesce_partitions=cfg.partitions_coalesce,
                )
            )
            .option(
                "checkpointLocation",
                os.path.join(cfg.checkpoint_base_path, "lake_orphans"),
            )
            .trigger(processingTime=trigger_str)
            .start()
        )
        queries.append(q_orphans)

    if cfg.output_mode in {"console", "both"}:
        exp_agg = exposures_df.groupBy("experiment_id", "variant").agg(
            F.count("*").alias("deduped_exposures"),
            F.approx_count_distinct("user_id").alias("approx_unique_users"),
        )
        q_console = (
            exp_agg.writeStream.queryName("console_exposure_summary")
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option(
                "checkpointLocation",
                os.path.join(cfg.checkpoint_base_path, "console_summary"),
            )
            .trigger(processingTime=trigger_str)
            .start()
        )
        queries.append(q_console)

    logger.info(
        "Active streaming queries (%d): %s",
        len(queries),
        [q.name for q in queries],
    )

    if await_timeout_sec is not None and queries:
        queries[0].awaitTermination(await_timeout_sec)
        for q in queries:
            q.stop()
    else:
        spark.streams.awaitAnyTermination()


def seed_lake_from_synthetic_stream(
    num_users: int = 3000,
    lake_base_path: Optional[str] = None,
) -> Dict[str, int]:
    """Generate synthetic events and materialize all 4 partitioned Parquet lake datasets.

    Useful for local validation, CI testing, and seeding the data lake before
    running warehouse loads or dbt tests.
    """
    from producer.config import ProducerConfig
    from producer.producer import ExperimentEventGenerator

    cfg = StreamingConfig()
    target_lake = lake_base_path or cfg.lake_base_path

    prod_cfg = ProducerConfig(total_users=num_users)
    generator = ExperimentEventGenerator(prod_cfg)

    raw_exposures = []
    raw_conversions = []
    for idx in range(1, num_users + 1):
        for topic, _, payload in generator.generate_batch_for_user(user_index=idx):
            if topic == prod_cfg.exposures_topic:
                raw_exposures.append(payload)
            else:
                raw_conversions.append(payload)

    dedup_exp, dup_exp_cnt, late_exp_cnt = deduplicate_events_within_watermark(
        raw_exposures, watermark_minutes=cfg.exposure_watermark_minutes
    )
    dedup_conv, dup_conv_cnt, late_conv_cnt = deduplicate_events_within_watermark(
        raw_conversions, watermark_minutes=cfg.conversion_watermark_minutes
    )

    joined_rows, orphan_rows = join_exposures_and_conversions(
        dedup_exp,
        dedup_conv,
        attribution_window_minutes=cfg.demo_attribution_window_minutes,
    )

    write_partitioned_parquet(dedup_exp, target_lake, "exposures", batch_id=1)
    write_partitioned_parquet(dedup_conv, target_lake, "conversions", batch_id=1)
    write_partitioned_parquet(joined_rows, target_lake, "joined", batch_id=1)
    write_partitioned_parquet(orphan_rows, target_lake, "orphans", batch_id=1)

    summary = {
        "raw_exposures": len(raw_exposures),
        "deduped_exposures": len(dedup_exp),
        "duplicate_exposures_dropped": dup_exp_cnt,
        "raw_conversions": len(raw_conversions),
        "deduped_conversions": len(dedup_conv),
        "late_conversions_detected": late_conv_cnt,
        "joined_rows": len(joined_rows),
        "orphan_rows": len(orphan_rows),
    }
    logger.info("Seeded partitioned Parquet data lake at %s: %s", target_lake, summary)
    return summary


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the Spark streaming job."""
    parser = argparse.ArgumentParser(
        description="Run Spark Structured Streaming job for experimentation events."
    )
    parser.add_argument(
        "--seed-lake",
        action="store_true",
        help="Seed the partitioned Parquet lake directly from synthetic producer.",
    )
    parser.add_argument(
        "--num-users",
        type=int,
        default=3000,
        help="Number of synthetic users when running --seed-lake (default: 3000).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.seed_lake:
        seed_lake_from_synthetic_stream(num_users=args.num_users)
    else:
        timeout_env = os.getenv("SPARK_AWAIT_TIMEOUT_SEC")
        timeout_val = int(timeout_env) if timeout_env else None
        run_streaming_pipeline(await_timeout_sec=timeout_val)
