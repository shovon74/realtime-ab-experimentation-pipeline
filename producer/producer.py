"""Synthetic experimentation event producer for Apache Kafka.

Simulates an A/B experimentation platform emitting user `exposures` and
`conversions` events to Kafka topics keyed by `user_id`. Supports deterministic
seeded hashing for variant assignment and controlled fault injection:
  - Duplicate exposures (~2%) sharing the same `event_id`
  - Late conversions (~3%) arriving past the streaming watermark
  - Orphan conversions (~1%) with no corresponding exposure event
  - Sample Ratio Mismatch (SRM) on a configured experiment (55/45 split)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

from producer.config import ExperimentSpec, ProducerConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("producer")


def format_iso_utc(dt: datetime) -> str:
    """Format a UTC datetime as an ISO-8601 string with trailing 'Z'."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def seeded_uniform(key: str) -> float:
    """Map an arbitrary string key deterministically to a float in [0.0, 1.0)."""
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    int_val = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return int_val / float(1 << 64)


def assign_variant(user_id: str, experiment: ExperimentSpec, seed: int) -> str:
    """Deterministically assign a user to an experiment variant via seeded hash.

    Args:
        user_id: Unique user identifier (e.g., 'u123').
        experiment: Experiment specification containing variants and weights.
        seed: Integer random seed for reproducibility.

    Returns:
        Assigned variant name string.
    """
    bucket = seeded_uniform(f"assign:{seed}:{experiment.experiment_id}:{user_id}")
    cumulative = 0.0
    for variant in experiment.variants:
        cumulative += experiment.allocation_weights[variant]
        if bucket < cumulative:
            return variant
    return experiment.variants[-1]


def should_user_convert(
    user_id: str, experiment: ExperimentSpec, variant: str, seed: int
) -> bool:
    """Deterministically decide if a user converts based on variant ground truth."""
    prob = experiment.conversion_rates[variant]
    draw = seeded_uniform(
        f"convert:{seed}:{experiment.experiment_id}:{variant}:{user_id}"
    )
    return draw < prob


class ExperimentEventGenerator:
    """Generates synthetic exposure and conversion events with fault injection."""

    def __init__(self, config: Optional[ProducerConfig] = None) -> None:
        self.config = config or ProducerConfig()
        self.experiments: List[ExperimentSpec] = self.config.build_experiments()
        self.rng = random.Random(self.config.random_seed)
        self._orphan_counter = 0

    def _deterministic_uuid(self, namespace_key: str) -> str:
        """Create a deterministic UUIDv5-style string from a seed key."""
        digest = hashlib.sha256(
            f"{self.config.random_seed}:{namespace_key}".encode("utf-8")
        ).hexdigest()
        return str(uuid.UUID(digest[:32]))

    def make_exposure_event(
        self,
        user_id: str,
        experiment: ExperimentSpec,
        variant: str,
        event_ts: datetime,
        event_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Construct an exposure event matching the Section 6 schema."""
        eid = event_id or self._deterministic_uuid(
            f"exp:{experiment.experiment_id}:{user_id}:{format_iso_utc(event_ts)}"
        )
        return {
            "event_id": eid,
            "user_id": user_id,
            "experiment_id": experiment.experiment_id,
            "variant": variant,
            "event_ts": format_iso_utc(event_ts),
        }

    def make_conversion_event(
        self,
        user_id: str,
        experiment: ExperimentSpec,
        event_ts: datetime,
        event_id: Optional[str] = None,
        event_type: str = "purchase",
    ) -> Dict[str, Any]:
        """Construct a conversion event matching the Section 6 schema."""
        eid = event_id or self._deterministic_uuid(
            f"conv:{experiment.experiment_id}:{user_id}:{format_iso_utc(event_ts)}"
        )
        raw_rev = self.rng.gauss(experiment.mean_revenue, experiment.std_revenue)
        revenue = round(max(5.0, raw_rev), 2)
        return {
            "event_id": eid,
            "user_id": user_id,
            "experiment_id": experiment.experiment_id,
            "event_type": event_type,
            "revenue": revenue,
            "event_ts": format_iso_utc(event_ts),
        }

    def generate_batch_for_user(
        self,
        user_index: int,
        base_time: Optional[datetime] = None,
    ) -> List[Tuple[str, str, Dict[str, Any]]]:
        """Generate (topic, key, payload) tuples for a given user across experiments.

        Returns:
            List of (topic_name, kafka_key, event_dict) tuples.
        """
        now = base_time or datetime.now(timezone.utc)
        user_id = f"u{user_index}"
        records: List[Tuple[str, str, Dict[str, Any]]] = []

        for experiment in self.experiments:
            variant = assign_variant(
                user_id=user_id,
                experiment=experiment,
                seed=self.config.random_seed,
            )
            exposure_ts = now
            exposure_event = self.make_exposure_event(
                user_id=user_id,
                experiment=experiment,
                variant=variant,
                event_ts=exposure_ts,
            )
            records.append(
                (self.config.exposures_topic, user_id, exposure_event)
            )

            # 1) Fault Injection: Duplicate exposures (~2%) with identical event_id
            if (
                self.config.inject_duplicate_exposures
                and self.rng.random() < self.config.duplicate_exposure_rate
            ):
                records.append(
                    (
                        self.config.exposures_topic,
                        user_id,
                        dict(exposure_event),
                    )
                )

            # 2) Normal vs. Late Conversion generation
            if should_user_convert(
                user_id=user_id,
                experiment=experiment,
                variant=variant,
                seed=self.config.random_seed,
            ):
                is_late = (
                    self.config.inject_late_conversions
                    and self.rng.random() < self.config.late_conversion_rate
                )
                if is_late:
                    # Late conversion: delayed past the 30-min watermark / attribution window
                    delay_sec = self.rng.randint(
                        self.config.late_delay_min_sec,
                        self.config.late_delay_max_sec,
                    )
                else:
                    delay_sec = self.rng.randint(
                        self.config.normal_delay_min_sec,
                        self.config.normal_delay_max_sec,
                    )
                conversion_ts = exposure_ts + timedelta(seconds=delay_sec)
                conversion_event = self.make_conversion_event(
                    user_id=user_id,
                    experiment=experiment,
                    event_ts=conversion_ts,
                )
                records.append(
                    (self.config.conversions_topic, user_id, conversion_event)
                )

            # 3) Fault Injection: Orphan conversion (~1% of exposures trigger an unexposed user conversion)
            if (
                self.config.inject_orphan_conversions
                and self.rng.random() < self.config.orphan_conversion_rate
            ):
                self._orphan_counter += 1
                orphan_user_id = f"u_orphan_{self._orphan_counter}_{user_index}"
                orphan_ts = now + timedelta(
                    seconds=self.rng.randint(5, 120)
                )
                orphan_event = self.make_conversion_event(
                    user_id=orphan_user_id,
                    experiment=experiment,
                    event_ts=orphan_ts,
                )
                records.append(
                    (
                        self.config.conversions_topic,
                        orphan_user_id,
                        orphan_event,
                    )
                )

        return records

    def stream_events(
        self,
        max_events: Optional[int] = None,
        base_time: Optional[datetime] = None,
    ) -> Iterator[Tuple[str, str, Dict[str, Any]]]:
        """Yield generated (topic, key, event_dict) tuples continuously or up to max_events."""
        emitted = 0
        user_idx = 1
        anchor_time = base_time or datetime.now(timezone.utc)

        while True:
            # Advance synthetic base time slightly as users arrive
            step_time = anchor_time + timedelta(milliseconds=user_idx * 50)
            batch = self.generate_batch_for_user(
                user_index=((user_idx - 1) % self.config.total_users) + 1,
                base_time=step_time,
            )
            for record in batch:
                yield record
                emitted += 1
                if max_events is not None and emitted >= max_events:
                    return
            user_idx += 1


def create_kafka_producer(bootstrap_servers: str, retries: int = 15) -> Any:
    """Initialize a KafkaProducer with retry backoff while Kafka starts up."""
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                linger_ms=10,
            )
            logger.info("Connected to Kafka broker(s) at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka broker not ready at %s (attempt %d/%d); retrying in 3s...",
                bootstrap_servers,
                attempt,
                retries,
            )
            time.sleep(3)
    raise RuntimeError(
        f"Failed to connect to Kafka at {bootstrap_servers} after {retries} attempts."
    )


def run_producer(
    config: Optional[ProducerConfig] = None,
    max_events: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Run the producer loop, sending events to Kafka (or stdout in dry-run mode)."""
    cfg = config or ProducerConfig()
    generator = ExperimentEventGenerator(cfg)
    producer = None if dry_run else create_kafka_producer(cfg.bootstrap_servers)

    counts = {cfg.exposures_topic: 0, cfg.conversions_topic: 0}
    sleep_interval = (
        1.0 / cfg.event_rate_per_sec if cfg.event_rate_per_sec > 0 else 0.0
    )

    logger.info(
        "Starting producer | experiments=%d | rate=%.1f/s | total_users=%d | seed=%d | dry_run=%s",
        cfg.num_experiments,
        cfg.event_rate_per_sec,
        cfg.total_users,
        cfg.random_seed,
        dry_run,
    )

    try:
        for idx, (topic, key, payload) in enumerate(
            generator.stream_events(max_events=max_events), start=1
        ):
            if producer is not None:
                producer.send(topic, key=key, value=payload)
            counts[topic] = counts.get(topic, 0) + 1

            if idx % 100 == 0:
                logger.info(
                    "Emitted %d total events (exposures=%d, conversions=%d)",
                    idx,
                    counts[cfg.exposures_topic],
                    counts[cfg.conversions_topic],
                )
            if sleep_interval > 0 and not dry_run:
                time.sleep(sleep_interval)
    except KeyboardInterrupt:
        logger.info("Stopping producer on keyboard interrupt...")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()

    logger.info("Producer finished. Summary: %s", counts)
    return counts


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for running the synthetic producer."""
    parser = argparse.ArgumentParser(
        description="Emit synthetic A/B experimentation events to Kafka."
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after emitting this many events (default: run indefinitely).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Override event emission rate (events/sec).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate events in memory without connecting to Kafka.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cfg = ProducerConfig()
    if args.rate is not None:
        cfg.event_rate_per_sec = args.rate
    run_producer(config=cfg, max_events=args.max_events, dry_run=args.dry_run)
