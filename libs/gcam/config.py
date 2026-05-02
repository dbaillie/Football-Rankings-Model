"""GCAM (Global Community Alignment Model) configuration — post-hoc layer defaults."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class GCAMConfig:
    """Tunable parameters for connectivity, structural uncertainty, trust, and power score."""

    rolling_weeks: int = 104
    connectivity_floor: float = 0.05
    community_connectivity_floor: float = 0.05
    volume_trust_half_life: float = 24.0
    volume_trust_mode: str = "rational"
    direct_vs_community_blend: float = 0.62
    structural_rd_scale: float = 45.0
    structural_rd_gamma: float = 1.15
    trust_floor: float = 0.2
    trust_rd_scale: float = 175.0
    baseline_mode: str = "blend"
    baseline_global_weight: float = 0.85
    global_baseline_rating: float | None = None
    power_score_mode: str = "rating_rd"
    power_rd_lambda: float = 0.28
    power_connectivity_lambda: float = 0.15
    uefa_weight: float = 1.0
    domestic_weight: float = 1.0
    cup_weight: float = 1.0
    friendly_weight: float = 0.05
    football_domestic_community_suffix: str = "domestic"
    football_cup_community_suffix: str = "cup"
    football_friendly_community_suffix: str = "friendly"

    def __post_init__(self) -> None:
        if self.volume_trust_mode not in ("rational", "exp"):
            raise ValueError("volume_trust_mode must be 'rational' or 'exp'")
        if self.baseline_mode not in ("global", "community", "blend"):
            raise ValueError("baseline_mode must be global|community|blend")
        if self.power_score_mode not in ("rating_only", "rating_rd", "rating_rd_connectivity"):
            raise ValueError("invalid power_score_mode")


DEFAULT_GCAM_CONFIG = GCAMConfig()
