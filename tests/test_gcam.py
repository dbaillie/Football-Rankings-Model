"""Tests for GCAM connectivity and adjustment layer (sport-agnostic core)."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "libs"))

from gcam.adjustment import adjusted_rating, combine_total_rd, structural_rd, trust_factor  # noqa: E402
from gcam.config import GCAMConfig  # noqa: E402
from gcam.connectivity import (  # noqa: E402
    community_external_metrics_for_window,
    distribution_entropy_counts,
    effective_connectivity,
    volume_trust_from_weight,
)
from gcam.football import competition_community_suffix  # noqa: E402
from gcam.pipeline import run_posthoc_gcam  # noqa: E402


def test_entropy_single_mass_normalized_zero():
    h, hn, k = distribution_entropy_counts({"A": 10.0})
    assert h == pytest.approx(0.0)
    assert hn == pytest.approx(0.0)
    assert k == 1


def test_entropy_uniform_normalized_high():
    h, hn, k = distribution_entropy_counts({"A": 25.0, "B": 25.0, "C": 25.0, "D": 25.0})
    assert k == 4
    assert hn == pytest.approx(1.0)
    assert h > 0


def test_volume_trust_increases_with_count():
    cfg = GCAMConfig(volume_trust_half_life=10.0)
    v1 = volume_trust_from_weight(5.0, cfg)
    v2 = volume_trust_from_weight(50.0, cfg)
    assert v2 > v1
    assert v2 < 1.0


def test_effective_connectivity_respects_floor():
    cfg = GCAMConfig(connectivity_floor=0.12, community_connectivity_floor=0.12)
    out = effective_connectivity(0.0, 0.0, cfg)
    assert out >= cfg.connectivity_floor


def test_effective_connectivity_capped_at_one():
    cfg = GCAMConfig(connectivity_floor=0.05)
    out = effective_connectivity(1.0, 1.0, cfg)
    assert out <= 1.0


def test_structural_rd_high_when_low_connectivity():
    cfg = GCAMConfig(structural_rd_scale=100.0, structural_rd_gamma=1.0)
    hi = structural_rd(1.0, cfg)
    lo = structural_rd(0.05, cfg)
    assert lo > hi
    assert hi >= 0


def test_total_rd_ge_raw_rd():
    r_raw = 40.0
    struct = 55.0
    total = combine_total_rd(r_raw, struct)
    assert total >= r_raw - 1e-9


def test_trust_bounded():
    cfg = GCAMConfig(trust_floor=0.1)
    t = trust_factor(0.9, 200.0, 1.0, cfg)
    assert cfg.trust_floor <= t <= 1.0


def test_adjusted_rating_extremes():
    raw, base = 1900.0, 1500.0
    assert adjusted_rating(raw, base, 1.0) == pytest.approx(raw)
    assert adjusted_rating(raw, base, 0.0) == pytest.approx(base)


def test_adjusted_rating_interpolates_toward_baseline():
    raw, base = 1800.0, 1500.0
    mid = adjusted_rating(raw, base, 0.5)
    assert base < mid < raw


def test_adjusted_rating_no_lift_when_below_baseline():
    """Below baseline: never move toward baseline up — keep raw."""
    raw, base = 1400.0, 1500.0
    assert adjusted_rating(raw, base, 0.0) == pytest.approx(raw)
    assert adjusted_rating(raw, base, 0.5) == pytest.approx(raw)
    assert adjusted_rating(raw, base, 1.0) == pytest.approx(raw)


def test_community_connectivity_more_external_diversity():
    cfg = GCAMConfig(community_connectivity_floor=0.02)
    narrow = community_external_metrics_for_window([("A", "B", 1.0), ("A", "B", 1.0)], cfg)
    wide = community_external_metrics_for_window(
        [("A", "B", 1.0), ("A", "C", 1.0), ("A", "D", 1.0)], cfg
    )
    assert wide["A"]["community_connectivity"] >= narrow["A"]["community_connectivity"]


def test_football_domestic_bucket_collapses_league_tiers():
    cfg = GCAMConfig()
    uefa = frozenset({"UCL", "UEL", "UECL", "EURO"})
    lc = pd.Series(["SC0", "SC1", "SC2", "E0", "AUT"])
    suf = competition_community_suffix(lc, cfg, uefa)
    assert list(suf) == ["domestic"] * len(lc)


def test_football_uefa_and_cup_suffixes_distinct():
    cfg = GCAMConfig()
    uefa = frozenset({"UCL", "UEL", "UECL", "EURO"})
    lc = pd.Series(["UCL", "SCOTTISH_CUP", "FRIENDLY_X"])
    suf = competition_community_suffix(lc, cfg, uefa)
    assert suf[0] == "UCL"
    assert suf[1] == cfg.football_cup_community_suffix
    assert suf[2] == cfg.football_friendly_community_suffix


def test_pipeline_posthoc_produces_gcam_columns():
    cfg = GCAMConfig()

    weekly = pd.DataFrame(
        {
            "pid": [1, 2],
            "week": [202510, 202510],
            "rating": [1600.0, 1400.0],
            "rd": [60.0, 60.0],
            "team_name": ["A", "B"],
            "country_name": ["X", "Y"],
        }
    )
    matches = pd.DataFrame(
        {
            "match_date": pd.to_datetime(["2025-03-01", "2025-03-02"]),
            "home_club_id": [1, 2],
            "away_club_id": [2, 1],
            "home_community": ["X|E1", "Y|E2"],
            "away_community": ["Y|E2", "X|E1"],
            "weight": [1.0, 1.0],
        }
    )

    out, comm = run_posthoc_gcam(weekly, matches, cfg)
    assert isinstance(comm, pd.DataFrame)
    row = out.loc[out["pid"] == 1].iloc[0]
    assert math.isfinite(row["adjusted_rating"])
    assert row["total_rd"] >= row["rd"] - 1e-6
    assert "effective_connectivity" in out.columns
    assert "trust_factor" in out.columns
