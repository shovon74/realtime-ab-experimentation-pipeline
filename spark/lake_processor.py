"""Data Lake Partitioned Parquet Processor & Attribution Join Engine.

Provides deterministic event-time watermark deduplication, time-bounded left
exposure-to-conversion join, unattributed orphan conversion isolation, and
partitioned Parquet writer (`lake/<dataset>/event_date=YYYY-MM-DD/experiment_id=<id>/`).

Partitioning Strategy & Why (Partition Pruning):
------------------------------------------------
All four lake datasets (`exposures`, `conversions`, `joined`, `orphans`) are
Hive-partitioned by `event_date` (YYYY-MM-DD) and `experiment_id`:
  `lake/<dataset>/event_date=2026-11-09/experiment_id=buy_button_color/part-*.parquet`

1. Incremental Daily/Hourly Loads: Downstream Airflow warehouse loads filter by
   `event_date`, allowing Spark, BigQuery external tables, DuckDB, and PyArrow to
   skip scanning historical date folders via directory-level partition pruning.
2. Per-Experiment Analysis: Analysts and statistical jobs frequently query a
   single experiment (`WHERE experiment_id = 'buy_button_color'`). Sub-partitioning
   by `experiment_id` avoids reading unrelated experiment traffic.
3. Bounded Cardinality: `event_date` (~365/year) x `experiment_id` (3-50 active
   experiments) has low cardinality, preventing the "small-file explosion" that
   would occur if partitioning by high-cardinality keys like `user_id`.
4. Micro-Batch Coalescing: Each micro-batch is coalesced (`coalesce(1)`) per
   partition before writing so file sizes remain healthy.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("lake_processor")


def parse_iso_ts(ts_str: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp string (e.g. '2026-11-09T10:15:02Z')."""
    cleaned = ts_str.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    return datetime.fromisoformat(cleaned).astimezone(timezone.utc)


def resolve_storage_path(base_uri: str, dataset: str) -> str:
    """Resolve dataset path supporting local filesystem, gs://, and s3a:// URIs."""
    clean_base = base_uri.rstrip("/\\")
    if clean_base.startswith(("gs://", "s3://", "s3a://")):
        return f"{clean_base}/{dataset}"
    return str(Path(clean_base) / dataset)


def deduplicate_events_within_watermark(
    events: Iterable[Dict[str, Any]],
    watermark_minutes: int,
) -> Tuple[List[Dict[str, Any]], int, int]:
    """Deduplicate events on `event_id` while enforcing an event-time watermark.

    Tracks `max_event_ts` seen in the stream. Events arriving with
    `event_ts < max_event_ts - watermark_minutes` are flagged as late past the
    watermark. Duplicate `event_id`s within the watermark state are dropped.

    Returns:
        Tuple of (deduplicated_events, duplicate_count, late_past_watermark_count).
    """
    seen_ids: Dict[str, datetime] = {}
    max_event_ts: Optional[datetime] = None
    watermark_delta = timedelta(minutes=watermark_minutes)

    deduped: List[Dict[str, Any]] = []
    duplicate_count = 0
    late_count = 0

    for raw in events:
        event = dict(raw)
        event_id = str(event["event_id"])
        event_ts = parse_iso_ts(str(event["event_ts"]))

        if max_event_ts is None or event_ts > max_event_ts:
            max_event_ts = event_ts

        watermark_threshold = max_event_ts - watermark_delta

        # Evict expired state older than watermark threshold
        expired_keys = [
            eid for eid, ts in seen_ids.items() if ts < watermark_threshold
        ]
        for eid in expired_keys:
            del seen_ids[eid]

        is_late_watermark = event_ts < watermark_threshold
        if is_late_watermark:
            late_count += 1

        if event_id in seen_ids:
            duplicate_count += 1
            continue

        seen_ids[event_id] = event_ts
        event["event_timestamp"] = event_ts
        event["event_date"] = event_ts.strftime("%Y-%m-%d")
        event["is_late_watermark"] = is_late_watermark
        deduped.append(event)

    return deduped, duplicate_count, late_count


def join_exposures_and_conversions(
    exposures: List[Dict[str, Any]],
    conversions: List[Dict[str, Any]],
    attribution_window_minutes: int = 30,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Perform left stream-stream attribution join and isolate orphan conversions.

    Rules (Section 7.2):
    - Left join from `exposures` to `conversions` on (`user_id`, `experiment_id`)
      where `conversion_ts` falls within `[exposure_ts, exposure_ts + window]`.
    - Non-converting exposures are preserved with `converted = False` and
      `revenue = 0.0`.
    - Conversions with no matching exposure OR arriving outside the attribution
      window (`conversion_ts > exposure_ts + window` or `conversion_ts < exposure_ts`)
      are NEVER attributed to a variant and are routed to `orphans`.

    Returns:
        Tuple of (joined_rows, orphan_rows).
    """
    window_delta = timedelta(minutes=attribution_window_minutes)

    # Index earliest exposure per (user_id, experiment_id)
    exposure_index: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for exp in exposures:
        key = (str(exp["user_id"]), str(exp["experiment_id"]))
        exposure_index.setdefault(key, []).append(exp)

    for key in exposure_index:
        exposure_index[key].sort(key=lambda x: x["event_timestamp"])

    matched_conversions_by_exp_id: Dict[str, List[Dict[str, Any]]] = {}
    orphan_rows: List[Dict[str, Any]] = []

    for conv in conversions:
        key = (str(conv["user_id"]), str(conv["experiment_id"]))
        conv_ts: datetime = conv["event_timestamp"]
        candidate_exposures = exposure_index.get(key, [])

        if not candidate_exposures:
            orphan_rows.append(
                {
                    "conversion_event_id": str(conv["event_id"]),
                    "user_id": str(conv["user_id"]),
                    "experiment_id": str(conv["experiment_id"]),
                    "event_type": str(conv["event_type"]),
                    "revenue": float(conv["revenue"]),
                    "conversion_ts": str(conv["event_ts"]),
                    "orphan_reason": "no_matching_exposure",
                    "event_date": str(conv["event_date"]),
                }
            )
            continue

        # Find the first exposure where conv_ts >= exp_ts
        first_exp = candidate_exposures[0]
        exp_ts: datetime = first_exp["event_timestamp"]
        delay_seconds = (conv_ts - exp_ts).total_seconds()

        if conv.get("is_late_watermark", False) or (conv_ts - exp_ts) > window_delta or conv_ts < exp_ts:
            orphan_rows.append(
                {
                    "conversion_event_id": str(conv["event_id"]),
                    "user_id": str(conv["user_id"]),
                    "experiment_id": str(conv["experiment_id"]),
                    "event_type": str(conv["event_type"]),
                    "revenue": float(conv["revenue"]),
                    "conversion_ts": str(conv["event_ts"]),
                    "orphan_reason": "late_past_watermark",
                    "delay_seconds": round(delay_seconds, 2),
                    "event_date": str(conv["event_date"]),
                }
            )
        else:
            exp_id = str(first_exp["event_id"])
            matched_conversions_by_exp_id.setdefault(exp_id, []).append(conv)

    # Build left-joined rows from all deduplicated exposures
    joined_rows: List[Dict[str, Any]] = []
    for exp in exposures:
        exp_id = str(exp["event_id"])
        exp_ts = exp["event_timestamp"]
        matched_convs = matched_conversions_by_exp_id.get(exp_id, [])

        if not matched_convs:
            joined_rows.append(
                {
                    "exposure_event_id": exp_id,
                    "user_id": str(exp["user_id"]),
                    "experiment_id": str(exp["experiment_id"]),
                    "variant": str(exp["variant"]),
                    "exposure_ts": str(exp["event_ts"]),
                    "conversion_event_id": None,
                    "event_type": None,
                    "converted": False,
                    "revenue": 0.0,
                    "conversion_ts": None,
                    "conversion_delay_sec": None,
                    "event_date": str(exp["event_date"]),
                }
            )
        else:
            for conv in matched_convs:
                conv_ts = conv["event_timestamp"]
                joined_rows.append(
                    {
                        "exposure_event_id": exp_id,
                        "user_id": str(exp["user_id"]),
                        "experiment_id": str(exp["experiment_id"]),
                        "variant": str(exp["variant"]),
                        "exposure_ts": str(exp["event_ts"]),
                        "conversion_event_id": str(conv["event_id"]),
                        "event_type": str(conv["event_type"]),
                        "converted": True,
                        "revenue": float(conv["revenue"]),
                        "conversion_ts": str(conv["event_ts"]),
                        "conversion_delay_sec": round(
                            (conv_ts - exp_ts).total_seconds(), 2
                        ),
                        "event_date": str(exp["event_date"]),
                    }
                )

    return joined_rows, orphan_rows


def write_partitioned_parquet(
    rows: List[Dict[str, Any]],
    lake_base_path: str,
    dataset_name: str,
    batch_id: Optional[int] = None,
) -> List[Path]:
    """Write rows as coalesced Parquet partitioned by `event_date` and `experiment_id`.

    Layout:
      `<lake_base_path>/<dataset_name>/event_date=YYYY-MM-DD/experiment_id=<id>/part-<batch>-<uuid>.parquet`
    """
    if not rows:
        return []

    df = pd.DataFrame(rows)
    # Drop transient Python datetime objects if present to keep Parquet schema clean
    if "event_timestamp" in df.columns:
        df = df.drop(columns=["event_timestamp"])

    written_files: List[Path] = []
    dataset_root = Path(resolve_storage_path(lake_base_path, dataset_name))

    for (event_date, experiment_id), group_df in df.groupby(
        ["event_date", "experiment_id"], dropna=False
    ):
        part_dir = (
            dataset_root
            / f"event_date={event_date}"
            / f"experiment_id={experiment_id}"
        )
        part_dir.mkdir(parents=True, exist_ok=True)

        batch_tag = f"{batch_id:05d}" if batch_id is not None else "00000"
        short_uid = uuid.uuid4().hex[:8]
        file_path = part_dir / f"part-{batch_tag}-{short_uid}.parquet"

        # Match Spark partitionBy("event_date", "experiment_id") behavior:
        # partition columns live in the Hive directory path
        payload_df = group_df.drop(
            columns=["event_date", "experiment_id"], errors="ignore"
        ).reset_index(drop=True)
        table = pa.Table.from_pandas(payload_df, preserve_index=False)
        pq.write_table(table, file_path, compression="snappy")
        written_files.append(file_path)

    return written_files


def seed_lake_from_synthetic_stream(
    num_users: int = 3000,
    lake_base_path: Optional[str] = None,
    exposure_watermark_minutes: int = 10,
    conversion_watermark_minutes: int = 30,
    attribution_window_minutes: int = 30,
) -> Dict[str, int]:
    """Generate synthetic events and materialize all 4 partitioned Parquet lake datasets."""
    from producer.config import ProducerConfig
    from producer.producer import ExperimentEventGenerator

    target_lake = lake_base_path or os.getenv("LAKE_BASE_PATH", "./lake")
    prod_cfg = ProducerConfig(total_users=num_users)
    generator = ExperimentEventGenerator(prod_cfg)

    raw_exposures: List[Dict[str, Any]] = []
    raw_conversions: List[Dict[str, Any]] = []
    for idx in range(1, num_users + 1):
        for topic, _, payload in generator.generate_batch_for_user(user_index=idx):
            if topic == prod_cfg.exposures_topic:
                raw_exposures.append(payload)
            else:
                raw_conversions.append(payload)

    dedup_exp, dup_exp_cnt, late_exp_cnt = deduplicate_events_within_watermark(
        raw_exposures, watermark_minutes=exposure_watermark_minutes
    )
    dedup_conv, dup_conv_cnt, late_conv_cnt = deduplicate_events_within_watermark(
        raw_conversions, watermark_minutes=conversion_watermark_minutes
    )

    joined_rows, orphan_rows = join_exposures_and_conversions(
        dedup_exp,
        dedup_conv,
        attribution_window_minutes=attribution_window_minutes,
    )

    write_partitioned_parquet(dedup_exp, target_lake, "exposures", batch_id=1)
    write_partitioned_parquet(dedup_conv, target_lake, "conversions", batch_id=1)
    write_partitioned_parquet(joined_rows, target_lake, "joined", batch_id=1)
    write_partitioned_parquet(orphan_rows, target_lake, "orphans", batch_id=1)

    return {
        "raw_exposures": len(raw_exposures),
        "deduped_exposures": len(dedup_exp),
        "duplicate_exposures_dropped": dup_exp_cnt,
        "raw_conversions": len(raw_conversions),
        "deduped_conversions": len(dedup_conv),
        "late_conversions_detected": late_conv_cnt,
        "joined_rows": len(joined_rows),
        "orphan_rows": len(orphan_rows),
    }
