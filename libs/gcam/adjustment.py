"""Structural uncertainty, trust shrinkage, baseline, adjusted rating, and power score."""

from __future__ import annotations

import math

from .config import GCAMConfig


def structural_rd(effective_connectivity: float, cfg: GCAMConfig) -> float:
    """Uncertainty attributable to limited global evidence diversity — rises when connectivity is low."""
    ec = float(max(0.0, min(1.0, effective_connectivity)))
    gap = max(0.0, 1.0 - ec)
    return float(cfg.structural_rd_scale * (gap ** cfg.structural_rd_gamma))


def combine_total_rd(raw_rd: float, struct_rd: float) -> float:
    """Combine Glicko RD with structural RD (orthogonal uncertainties)."""
    r = float(raw_rd)
    s = float(max(0.0, struct_rd))
    return float(math.sqrt(r * r + s * s))


def trust_factor(
    effective_connectivity: float,
    total_rd: float,
    volume_trust: float,
    cfg: GCAMConfig,
) -> float:
    """How far raw ratings can be interpreted as globally comparable, given evidence quality."""
    ec = float(max(0.0, min(1.0, effective_connectivity)))
    vt = float(max(0.0, min(1.0, volume_trust)))
    denom = 1.0 + max(0.0, float(total_rd)) / max(cfg.trust_rd_scale, 1e-6)
    rd_term = 1.0 / denom
    raw_trust = ec * vt * rd_term
    out = cfg.trust_floor + (1.0 - cfg.trust_floor) * raw_trust
    return float(max(cfg.trust_floor, min(1.0, out)))


def baseline_rating(
    raw_rating: float,
    global_mean: float,
    community_mean: float | None,
    cfg: GCAMConfig,
) -> float:
    if cfg.baseline_mode == "global":
        return float(global_mean)
    if cfg.baseline_mode == "community":
        return float(community_mean if community_mean is not None else global_mean)
    gw = float(cfg.baseline_global_weight)
    gw = max(0.0, min(1.0, gw))
    cm = community_mean if community_mean is not None else global_mean
    return float(gw * global_mean + (1.0 - gw) * cm)


def adjusted_rating(raw_rating: float, baseline: float, trust: float) -> float:
    """
    One-sided shrink toward baseline: only pull ratings *down* when raw is above baseline.

    Teams at or below baseline are left unchanged so GCAM never inflates ratings toward a
    higher baseline (no “positive shrinkage”).
    """
    raw = float(raw_rating)
    base = float(baseline)
    t = float(max(0.0, min(1.0, trust)))
    if raw <= base:
        return raw
    return float(t * raw + (1.0 - t) * base)


def power_score(
    adjusted_rating: float,
    total_rd: float,
    effective_connectivity: float,
    cfg: GCAMConfig,
) -> float:
    """Single scalar for rankings — configurable blend of strength and confidence."""
    mode = cfg.power_score_mode
    ar = float(adjusted_rating)
    if mode == "rating_only":
        return ar
    trd = float(max(0.0, total_rd))
    ec = float(max(0.0, min(1.0, effective_connectivity)))
    pen_rd = cfg.power_rd_lambda * trd
    if mode == "rating_rd":
        return float(ar - pen_rd)
    pen_c = cfg.power_connectivity_lambda * (1.0 - ec)
    return float(ar - pen_rd - pen_c)
