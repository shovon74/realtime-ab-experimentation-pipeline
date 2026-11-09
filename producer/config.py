"""Configuration definitions for the synthetic experimentation event producer.

Loads environment variables from `.env` and constructs reproducible experiment
specifications, including planted ground-truth conversion lifts and controlled
data-quality fault injection parameters.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def _parse_bool(value: str | None, default: bool = True) -> bool:
    """Parse a boolean environment variable string."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class ExperimentSpec:
    """Specification for a single synthetic A/B experiment.

    Attributes:
        experiment_id: Unique identifier for the experiment.
        variants: Ordered tuple of variant names (index 0 is control).
        conversion_rates: Ground-truth conversion rate per variant (0.0 to 1.0).
        allocation_weights: Traffic split probability per variant (sums to 1.0).
        mean_revenue: Mean purchase revenue for conversion events.
        std_revenue: Standard deviation of purchase revenue.
    """

    experiment_id: str
    variants: Tuple[str, ...]
    conversion_rates: Dict[str, float]
    allocation_weights: Dict[str, float]
    mean_revenue: float = 49.0
    std_revenue: float = 15.0


@dataclass
class ProducerConfig:
    """Runtime configuration for the Kafka experimentation event producer."""

    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    exposures_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_EXPOSURES", "exposures")
    )
    conversions_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_CONVERSIONS", "conversions")
    )

    num_experiments: int = field(
        default_factory=lambda: int(os.getenv("PRODUCER_NUM_EXPERIMENTS", "3"))
    )
    variants_per_experiment: int = field(
        default_factory=lambda: int(os.getenv("PRODUCER_VARIANTS_PER_EXP", "2"))
    )
    event_rate_per_sec: float = field(
        default_factory=lambda: float(os.getenv("PRODUCER_EVENT_RATE_PER_SEC", "20"))
    )
    total_users: int = field(
        default_factory=lambda: int(os.getenv("PRODUCER_TOTAL_USERS", "10000"))
    )
    random_seed: int = field(
        default_factory=lambda: int(os.getenv("PRODUCER_RANDOM_SEED", "42"))
    )

    # Fault injection flags & rates
    inject_duplicate_exposures: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("INJECT_DUPLICATE_EXPOSURES"), default=True
        )
    )
    duplicate_exposure_rate: float = field(
        default_factory=lambda: float(os.getenv("DUPLICATE_EXPOSURE_RATE", "0.02"))
    )

    inject_late_conversions: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("INJECT_LATE_CONVERSIONS"), default=True
        )
    )
    late_conversion_rate: float = field(
        default_factory=lambda: float(os.getenv("LATE_CONVERSION_RATE", "0.03"))
    )

    inject_orphan_conversions: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("INJECT_ORPHAN_CONVERSIONS"), default=True
        )
    )
    orphan_conversion_rate: float = field(
        default_factory=lambda: float(os.getenv("ORPHAN_CONVERSION_RATE", "0.01"))
    )

    inject_srm_experiment: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("INJECT_SRM_EXPERIMENT"), default=True
        )
    )
    srm_experiment_id: str = field(
        default_factory=lambda: os.getenv("SRM_EXPERIMENT_ID", "checkout_flow_v2")
    )
    srm_control_split: float = field(
        default_factory=lambda: float(os.getenv("SRM_CONTROL_SPLIT", "0.55"))
    )

    # Conversion delay parameters (seconds)
    normal_delay_min_sec: int = 15
    normal_delay_max_sec: int = 600
    # Late conversions exceed the 30-minute conversion watermark (e.g., 45-120 min late)
    late_delay_min_sec: int = 2700
    late_delay_max_sec: int = 7200

    def build_experiments(self) -> List[ExperimentSpec]:
        """Construct the list of experiment specifications based on config settings."""
        srm_control = self.srm_control_split if self.inject_srm_experiment else 0.50
        srm_treatment = round(1.0 - srm_control, 4)

        preset_experiments: List[ExperimentSpec] = [
            # Experiment 1: Planted positive lift (green 5.2% vs blue 4.5%)
            ExperimentSpec(
                experiment_id="buy_button_color",
                variants=("blue", "green"),
                conversion_rates={"blue": 0.045, "green": 0.052},
                allocation_weights={"blue": 0.50, "green": 0.50},
                mean_revenue=49.0,
                std_revenue=12.0,
            ),
            # Experiment 2: SRM scenario (55/45 split when inject_srm_experiment=True)
            ExperimentSpec(
                experiment_id=self.srm_experiment_id,
                variants=("control", "one_click"),
                conversion_rates={"control": 0.060, "one_click": 0.068},
                allocation_weights={
                    "control": srm_control,
                    "one_click": srm_treatment,
                },
                mean_revenue=75.0,
                std_revenue=20.0,
            ),
            # Experiment 3: Null effect experiment (both variants 3.8%)
            ExperimentSpec(
                experiment_id="pricing_page_hero",
                variants=("standard", "social_proof"),
                conversion_rates={"standard": 0.038, "social_proof": 0.038},
                allocation_weights={"standard": 0.50, "social_proof": 0.50},
                mean_revenue=99.0,
                std_revenue=25.0,
            ),
        ]

        experiments: List[ExperimentSpec] = []
        for idx in range(self.num_experiments):
            if idx < len(preset_experiments) and self.variants_per_experiment == 2:
                experiments.append(preset_experiments[idx])
            else:
                exp_id = (
                    preset_experiments[idx].experiment_id
                    if idx < len(preset_experiments)
                    else f"exp_custom_{idx + 1}"
                )
                variants = tuple(
                    "control" if v == 0 else f"variant_{v}"
                    for v in range(self.variants_per_experiment)
                )
                weight = round(1.0 / self.variants_per_experiment, 6)
                weights = {v: weight for v in variants}
                # Normalize final variant weight to ensure sum == 1.0
                weights[variants[-1]] = round(
                    1.0 - weight * (len(variants) - 1), 6
                )
                rates = {
                    v: round(0.045 + (0.005 * i), 4)
                    for i, v in enumerate(variants)
                }
                experiments.append(
                    ExperimentSpec(
                        experiment_id=exp_id,
                        variants=variants,
                        conversion_rates=rates,
                        allocation_weights=weights,
                    )
                )

        return experiments
