"""Unit tests for the synthetic experimentation event producer (Phase 1)."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from producer.config import ProducerConfig
from producer.producer import (
    ExperimentEventGenerator,
    assign_variant,
    should_user_convert,
)


def test_exposure_and_conversion_schemas() -> None:
    """Verify exposure and conversion events strictly conform to Section 6 schemas."""
    cfg = ProducerConfig(
        inject_duplicate_exposures=False,
        inject_late_conversions=False,
        inject_orphan_conversions=False,
    )
    gen = ExperimentEventGenerator(cfg)
    exp = gen.experiments[0]
    ts = datetime(2026, 11, 9, 10, 15, 2, tzinfo=timezone.utc)

    exposure = gen.make_exposure_event(
        user_id="u123",
        experiment=exp,
        variant="green",
        event_ts=ts,
    )
    assert set(exposure.keys()) == {
        "event_id",
        "user_id",
        "experiment_id",
        "variant",
        "event_ts",
    }
    assert exposure["user_id"] == "u123"
    assert exposure["experiment_id"] == "buy_button_color"
    assert exposure["variant"] == "green"
    assert exposure["event_ts"] == "2026-11-09T10:15:02Z"

    conversion = gen.make_conversion_event(
        user_id="u123",
        experiment=exp,
        event_ts=ts,
    )
    assert set(conversion.keys()) == {
        "event_id",
        "user_id",
        "experiment_id",
        "event_type",
        "revenue",
        "event_ts",
    }
    assert conversion["user_id"] == "u123"
    assert conversion["experiment_id"] == "buy_button_color"
    assert conversion["event_type"] == "purchase"
    assert isinstance(conversion["revenue"], float)
    assert conversion["revenue"] > 0.0
    assert conversion["event_ts"] == "2026-11-09T10:15:02Z"


def test_seeded_variant_assignment_is_reproducible() -> None:
    """Verify the same user_id and seed always map to the exact same variant."""
    cfg = ProducerConfig(random_seed=42)
    exp = cfg.build_experiments()[0]

    assignments_run_1 = [
        assign_variant(f"u{i}", exp, seed=42) for i in range(500)
    ]
    assignments_run_2 = [
        assign_variant(f"u{i}", exp, seed=42) for i in range(500)
    ]
    assert assignments_run_1 == assignments_run_2


def test_balanced_vs_srm_allocation_splits() -> None:
    """Verify normal experiment is ~50/50 and SRM experiment is ~55/45."""
    cfg = ProducerConfig(inject_srm_experiment=True, srm_control_split=0.55)
    experiments = {e.experiment_id: e for e in cfg.build_experiments()}

    n_users = 10000
    balanced_counts = Counter(
        assign_variant(f"u{i}", experiments["buy_button_color"], seed=42)
        for i in range(n_users)
    )
    blue_ratio = balanced_counts["blue"] / n_users
    assert 0.48 <= blue_ratio <= 0.52

    srm_counts = Counter(
        assign_variant(f"u{i}", experiments["checkout_flow_v2"], seed=42)
        for i in range(n_users)
    )
    control_ratio = srm_counts["control"] / n_users
    assert 0.53 <= control_ratio <= 0.57


def test_planted_ground_truth_conversion_lift() -> None:
    """Verify buy_button_color exhibits planted lift (green ~5.2% vs blue ~4.5%)."""
    cfg = ProducerConfig(random_seed=42)
    exp = cfg.build_experiments()[0]
    assert exp.experiment_id == "buy_button_color"

    n_users = 50000
    blue_conv = sum(
        1
        for i in range(n_users)
        if should_user_convert(f"u{i}", exp, "blue", seed=42)
    )
    green_conv = sum(
        1
        for i in range(n_users)
        if should_user_convert(f"u{i}", exp, "green", seed=42)
    )

    blue_rate = blue_conv / n_users
    green_rate = green_conv / n_users
    assert abs(blue_rate - 0.045) < 0.004
    assert abs(green_rate - 0.052) < 0.004
    assert green_rate > blue_rate


def test_fault_injection_flags_and_kafka_keys() -> None:
    """Verify duplicate exposures, orphan conversions, and Kafka key == user_id."""
    cfg_clean = ProducerConfig(
        inject_duplicate_exposures=False,
        inject_late_conversions=False,
        inject_orphan_conversions=False,
    )
    gen_clean = ExperimentEventGenerator(cfg_clean)
    clean_events = list(gen_clean.stream_events(max_events=1500))

    clean_exposure_ids = [
        payload["event_id"]
        for topic, key, payload in clean_events
        if topic == cfg_clean.exposures_topic
    ]
    assert len(clean_exposure_ids) == len(set(clean_exposure_ids))
    for _, key, payload in clean_events:
        assert key == payload["user_id"]
        assert not payload["user_id"].startswith("u_orphan_")

    # Now enable fault injections and verify duplicates and orphans appear
    cfg_faulty = ProducerConfig(
        inject_duplicate_exposures=True,
        duplicate_exposure_rate=0.05,
        inject_orphan_conversions=True,
        orphan_conversion_rate=0.03,
    )
    gen_faulty = ExperimentEventGenerator(cfg_faulty)
    faulty_events = list(gen_faulty.stream_events(max_events=2000))

    faulty_exposure_ids = [
        payload["event_id"]
        for topic, _, payload in faulty_events
        if topic == cfg_faulty.exposures_topic
    ]
    assert len(faulty_exposure_ids) > len(set(faulty_exposure_ids))

    orphan_events = [
        payload
        for topic, _, payload in faulty_events
        if topic == cfg_faulty.conversions_topic
        and payload["user_id"].startswith("u_orphan_")
    ]
    assert len(orphan_events) > 0
