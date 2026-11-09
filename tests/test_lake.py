"""Unit and integration tests for Phase 2 Data Lake (watermarks, dedup, join, orphans, Parquet)."""

from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds

from spark.lake_processor import (
    deduplicate_events_within_watermark,
    join_exposures_and_conversions,
    resolve_storage_path,
    write_partitioned_parquet,
)


def test_watermark_deduplication_and_late_detection() -> None:
    """Verify duplicate event_ids are dropped and late events past watermark are flagged."""
    events = [
        {
            "event_id": "e-1",
            "user_id": "u1",
            "experiment_id": "buy_button_color",
            "variant": "blue",
            "event_ts": "2026-11-09T10:00:00Z",
        },
        # Exact duplicate of e-1 within 10-minute watermark
        {
            "event_id": "e-1",
            "user_id": "u1",
            "experiment_id": "buy_button_color",
            "variant": "blue",
            "event_ts": "2026-11-09T10:00:00Z",
        },
        # Advance stream watermark to 10:25:00Z (watermark threshold = 10:15:00Z)
        {
            "event_id": "e-2",
            "user_id": "u2",
            "experiment_id": "buy_button_color",
            "variant": "green",
            "event_ts": "2026-11-09T10:25:00Z",
        },
        # Late event arriving with timestamp 10:05:00Z (< 10:15:00Z watermark threshold)
        {
            "event_id": "e-3",
            "user_id": "u3",
            "experiment_id": "buy_button_color",
            "variant": "blue",
            "event_ts": "2026-11-09T10:05:00Z",
        },
    ]

    deduped, dup_count, late_count = deduplicate_events_within_watermark(
        events, watermark_minutes=10
    )

    assert len(deduped) == 3
    assert dup_count == 1
    assert late_count == 1
    by_id = {row["event_id"]: row for row in deduped}
    assert by_id["e-1"]["is_late_watermark"] is False
    assert by_id["e-3"]["is_late_watermark"] is True


def test_left_join_and_orphan_routing() -> None:
    """Verify left join keeps non-converters, attributes valid conversions, and isolates orphans."""
    raw_exposures = [
        # u1 converts within 3 minutes (valid)
        {
            "event_id": "exp-u1",
            "user_id": "u1",
            "experiment_id": "buy_button_color",
            "variant": "green",
            "event_ts": "2026-11-09T10:00:00Z",
        },
        # u2 never converts (must be preserved by left join)
        {
            "event_id": "exp-u2",
            "user_id": "u2",
            "experiment_id": "buy_button_color",
            "variant": "blue",
            "event_ts": "2026-11-09T10:01:00Z",
        },
        # u3 converts 65 minutes later (past 30-min demo attribution window -> routed to orphans)
        {
            "event_id": "exp-u3",
            "user_id": "u3",
            "experiment_id": "buy_button_color",
            "variant": "green",
            "event_ts": "2026-11-09T10:02:00Z",
        },
    ]

    raw_conversions = [
        # Valid conversion for u1
        {
            "event_id": "conv-u1",
            "user_id": "u1",
            "experiment_id": "buy_button_color",
            "event_type": "purchase",
            "revenue": 49.0,
            "event_ts": "2026-11-09T10:03:00Z",
        },
        # Late conversion for u3 (65 mins after exposure)
        {
            "event_id": "conv-u3-late",
            "user_id": "u3",
            "experiment_id": "buy_button_color",
            "event_type": "purchase",
            "revenue": 55.0,
            "event_ts": "2026-11-09T11:07:00Z",
        },
        # True orphan conversion (u_orphan_99 has no exposure at all)
        {
            "event_id": "conv-orphan-99",
            "user_id": "u_orphan_99",
            "experiment_id": "buy_button_color",
            "event_type": "purchase",
            "revenue": 35.0,
            "event_ts": "2026-11-09T10:05:00Z",
        },
    ]

    dedup_exp, _, _ = deduplicate_events_within_watermark(
        raw_exposures, watermark_minutes=10
    )
    dedup_conv, _, _ = deduplicate_events_within_watermark(
        raw_conversions, watermark_minutes=30
    )

    joined_rows, orphan_rows = join_exposures_and_conversions(
        dedup_exp, dedup_conv, attribution_window_minutes=30
    )

    # All 3 exposed users are retained in joined_rows
    assert len(joined_rows) == 3
    joined_by_user = {r["user_id"]: r for r in joined_rows}

    # u1 is attributed to 'green'
    assert joined_by_user["u1"]["converted"] is True
    assert joined_by_user["u1"]["variant"] == "green"
    assert joined_by_user["u1"]["revenue"] == 49.0
    assert joined_by_user["u1"]["conversion_delay_sec"] == 180.0

    # u2 is preserved with converted=False, revenue=0.0
    assert joined_by_user["u2"]["converted"] is False
    assert joined_by_user["u2"]["revenue"] == 0.0

    # u3's late conversion was NOT attributed to 'green'
    assert joined_by_user["u3"]["converted"] is False
    assert joined_by_user["u3"]["revenue"] == 0.0

    # Orphans contain both u_orphan_99 and u3's late conversion
    assert len(orphan_rows) == 2
    orphans_by_id = {r["conversion_event_id"]: r for r in orphan_rows}
    assert (
        orphans_by_id["conv-orphan-99"]["orphan_reason"]
        == "no_matching_exposure"
    )
    assert (
        orphans_by_id["conv-u3-late"]["orphan_reason"] == "late_past_watermark"
    )


def test_partitioned_parquet_layout_and_pruning(tmp_path: Path) -> None:
    """Verify Parquet output uses event_date=YYYY-MM-DD/experiment_id=<id>/ layout."""
    sample_rows = [
        {
            "event_id": "e1",
            "user_id": "u1",
            "experiment_id": "buy_button_color",
            "variant": "green",
            "event_ts": "2026-11-09T10:15:02Z",
            "event_date": "2026-11-09",
        },
        {
            "event_id": "e2",
            "user_id": "u2",
            "experiment_id": "checkout_flow_v2",
            "variant": "control",
            "event_ts": "2026-11-09T10:16:00Z",
            "event_date": "2026-11-09",
        },
    ]

    written = write_partitioned_parquet(
        sample_rows, str(tmp_path), "exposures", batch_id=1
    )
    assert len(written) == 2

    expected_dir_1 = (
        tmp_path
        / "exposures"
        / "event_date=2026-11-09"
        / "experiment_id=buy_button_color"
    )
    expected_dir_2 = (
        tmp_path
        / "exposures"
        / "event_date=2026-11-09"
        / "experiment_id=checkout_flow_v2"
    )
    assert expected_dir_1.exists()
    assert expected_dir_2.exists()
    assert len(list(expected_dir_1.glob("part-*.parquet"))) == 1
    assert len(list(expected_dir_2.glob("part-*.parquet"))) == 1

    # Verify Hive partition pruning read via PyArrow dataset
    dataset = ds.dataset(
        str(tmp_path / "exposures"),
        format="parquet",
        partitioning="hive",
    )
    filtered_table = dataset.to_table(
        filter=ds.field("experiment_id") == "buy_button_color"
    )
    assert filtered_table.num_rows == 1
    assert filtered_table.column("user_id").to_pylist() == ["u1"]


def test_storage_uri_config_switch() -> None:
    """Verify LAKE_BASE_PATH switch supports local paths, gs://, and s3a://."""
    assert (
        resolve_storage_path("gs://exp-lake-bucket/v1", "exposures")
        == "gs://exp-lake-bucket/v1/exposures"
    )
    assert (
        resolve_storage_path("s3a://exp-lake-bucket/v1/", "orphans")
        == "s3a://exp-lake-bucket/v1/orphans"
    )
    assert resolve_storage_path("./lake", "joined").endswith("joined")
