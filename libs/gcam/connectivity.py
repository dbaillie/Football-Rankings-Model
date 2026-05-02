"""Connectivity: entropy, volume trust, entity and community structural diversity."""

from __future__ import annotations

import math
from collections import Counter
from typing import Iterable

import numpy as np

from .config import GCAMConfig


def distribution_entropy_counts(counts: dict[str, float]) -> tuple[float, float, int]:
    """
    Shannon entropy (natural log) over positive masses.
    Returns (entropy, normalized_entropy, n_categories).
    Normalized entropy is 0 when fewer than 2 categories or total mass is 0.
    """
    total = sum(counts.values())
    if total <= 0:
        return 0.0, 0.0, 0
    probs = [v / total for v in counts.values() if v > 0]
    k = len(probs)
    if k <= 1:
        return 0.0, 0.0, k
    h = -sum(p * math.log(p + 1e-15) for p in probs)
    h_norm = h / math.log(k)
    return h, h_norm, k


def volume_trust_from_weight(total_weight: float, cfg: GCAMConfig) -> float:
    """Saturating trust from interaction volume in [0, 1)."""
    if total_weight <= 0:
        return 0.0
    tau = max(cfg.volume_trust_half_life, 1e-6)
    if cfg.volume_trust_mode == "exp":
        return float(1.0 - math.exp(-total_weight / tau))
    return float(total_weight / (total_weight + tau))


def blend_direct_connectivity(entropy_norm: float, vol_trust: float, cfg: GCAMConfig) -> float:
    """Combine normalized entropy and volume trust into direct connectivity with a floor."""
    inner = entropy_norm * vol_trust
    return float(cfg.connectivity_floor + (1.0 - cfg.connectivity_floor) * inner)


def opponent_mass_from_slice(
    opp_communities: np.ndarray,
    weights: np.ndarray,
) -> tuple[dict[str, float], float, int, int]:
    """Aggregate weighted opponent-community masses from aligned arrays."""
    if len(opp_communities) == 0:
        return {}, 0.0, 0, 0
    masses: dict[str, float] = {}
    for oc, w in zip(opp_communities.tolist(), weights.tolist()):
        wf = float(w)
        if wf <= 0:
            continue
        masses[str(oc)] = masses.get(str(oc), 0.0) + wf
    total_w = sum(masses.values())
    n_opp_entities = int(len(np.unique(opp_communities))) if len(opp_communities) else 0
    return masses, total_w, len(masses), n_opp_entities


def entity_direct_metrics(
    opp_communities: np.ndarray,
    weights: np.ndarray,
    opp_ids: np.ndarray,
    cfg: GCAMConfig,
) -> dict[str, float]:
    masses, total_w, n_cat, _ = opponent_mass_from_slice(opp_communities, weights)
    h, h_norm, _ = distribution_entropy_counts(masses)
    vol = volume_trust_from_weight(total_w, cfg)
    direct = blend_direct_connectivity(h_norm, vol, cfg)
    n_ops = int(len(np.unique(opp_ids))) if len(opp_ids) else 0
    return {
        "entropy": float(h),
        "normalized_entropy": float(h_norm),
        "volume_trust": float(vol),
        "direct_connectivity": float(direct),
        "n_weighted_interactions": float(total_w),
        "n_interactions": float(len(opp_communities)),
        "n_distinct_opponent_communities": float(n_cat),
        "n_distinct_opponents": float(n_ops),
    }


def primary_own_community(own_communities: np.ndarray, weights: np.ndarray) -> str | None:
    """Weighted mode of own-community labels; falls back to unweighted mode."""
    if len(own_communities) == 0:
        return None
    ctr: Counter[str] = Counter()
    for oc, w in zip(own_communities.tolist(), weights.tolist()):
        wf = float(w)
        if wf <= 0:
            continue
        ctr[str(oc)] += wf
    if not ctr:
        return None
    return ctr.most_common(1)[0][0]


def community_external_metrics_for_window(
    pairs: Iterable[tuple[str, str, float]],
    cfg: GCAMConfig,
) -> dict[str, dict[str, float]]:
    """
    pairs: (home_comm, away_comm, weight)
    For each community C, use external interactions only (opponent community != C).
    """
    external_by_comm: dict[str, dict[str, float]] = {}
    external_vol: dict[str, float] = {}
    internal_by_comm: dict[str, float] = {}

    for a, b, w in pairs:
        wf = float(w)
        if wf <= 0:
            continue
        if a == b:
            internal_by_comm[a] = internal_by_comm.get(a, 0.0) + wf
            continue
        external_vol[a] = external_vol.get(a, 0.0) + wf
        external_vol[b] = external_vol.get(b, 0.0) + wf
        da = external_by_comm.setdefault(a, {})
        da[b] = da.get(b, 0.0) + wf
        db = external_by_comm.setdefault(b, {})
        db[a] = db.get(a, 0.0) + wf

    all_communities = set(internal_by_comm) | set(external_by_comm) | set(external_vol)
    out: dict[str, dict[str, float]] = {}
    for comm in all_communities:
        opp_mass = external_by_comm.get(comm, {})
        ext_w = external_vol.get(comm, 0.0)
        if opp_mass:
            h, h_norm, _ = distribution_entropy_counts(opp_mass)
            vol = volume_trust_from_weight(ext_w, cfg)
            conn = float(
                cfg.community_connectivity_floor
                + (1.0 - cfg.community_connectivity_floor) * h_norm * vol
            )
            n_ext_comm = float(len(opp_mass))
        else:
            h = h_norm = vol = 0.0
            conn = float(cfg.community_connectivity_floor)
            n_ext_comm = 0.0
        out[comm] = {
            "community_entropy": float(h),
            "normalized_community_entropy": float(h_norm),
            "community_volume_trust": float(vol),
            "community_connectivity": float(conn),
            "n_internal_interactions": float(internal_by_comm.get(comm, 0.0)),
            "n_external_interactions": float(ext_w),
            "n_external_communities": n_ext_comm,
        }
    return out


def effective_connectivity(
    direct: float,
    community_conn: float | None,
    cfg: GCAMConfig,
) -> float:
    """Configurable blend of direct evidence connectivity and community-level calibration."""
    cc = float(community_conn if community_conn is not None else cfg.community_connectivity_floor)
    w = float(cfg.direct_vs_community_blend)
    eff = w * direct + (1.0 - w) * cc
    eff = max(cfg.connectivity_floor, min(1.0, eff))
    return float(eff)
