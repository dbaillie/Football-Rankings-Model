"""Heat diffusion layer on European fixtures for post-hoc comparability.

Each **directed** fixture row contributes **generated heat**: cross-border or UEFA-labelled
fixtures use ``heat_cross_match`` per unit weight; purely local appearances use
``heat_local_match`` (typically much smaller).

For each rating-week snapshot we sum generated heat inside the rolling calendar window,
map raw heat to diffusion seeds ∈ [0, 1) with ``seed = gen / (gen + heat_seed_tau)``, then
**diffuse heat along weighted fixture edges** in that same window via a lazy random walk
with seed re-injection. The diffuse field drives one-sided shrink (``simple_comparability``).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from glicko_engine.core import iso_week_to_sunday

from .football import DEFAULT_UEFA_CODES


def _window_bounds(yyyyww: int, rolling_weeks: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    end = pd.Timestamp(iso_week_to_sunday(int(yyyyww)))
    start = end - timedelta(weeks=int(rolling_weeks))
    return start, end


def _build_directed_edges_week(matches_weighted: pd.DataFrame) -> pd.DataFrame:
    """Directed rows per fixture; keeps ``yyyyww``."""
    base = matches_weighted[
        ["match_date", "yyyyww", "home_club_id", "away_club_id", "home_community", "away_community", "weight"]
    ].copy()
    e_home = base.rename(
        columns={
            "home_club_id": "entity_id",
            "away_club_id": "opp_id",
            "home_community": "own_community",
            "away_community": "opp_community",
        }
    )
    e_away = base.rename(
        columns={
            "away_club_id": "entity_id",
            "home_club_id": "opp_id",
            "away_community": "own_community",
            "home_community": "opp_community",
        }
    )
    out = pd.concat([e_home, e_away], ignore_index=True)
    out["entity_id"] = out["entity_id"].astype(int)
    out["opp_id"] = out["opp_id"].astype(int)
    out["yyyyww"] = out["yyyyww"].astype(int)
    return out.sort_values(["entity_id", "match_date"], kind="mergesort").reset_index(drop=True)


def _week_rating_arrays_by_pid(
    wr: pd.DataFrame,
    pid_col: str,
    week_col: str,
    rating_col: str,
) -> dict[int, tuple[np.ndarray, np.ndarray]]:
    by_pid: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    for pid, g in wr.groupby(pid_col, sort=False):
        g2 = g.sort_values(week_col, kind="mergesort")
        by_pid[int(pid)] = (
            g2[week_col].astype(int).to_numpy(),
            g2[rating_col].astype(float).to_numpy(),
        )
    return by_pid


def _lookup_pre_rating_week(
    by_pid: dict[int, tuple[np.ndarray, np.ndarray]],
    opp_id: int,
    fixture_yyyyww: int,
) -> float:
    if opp_id not in by_pid:
        return float("nan")
    wks, rats = by_pid[opp_id]
    idx = int(np.searchsorted(wks, int(fixture_yyyyww), side="left") - 1)
    if idx >= 0:
        return float(rats[idx])
    return float("nan")


def parse_community_country_suffix(community: str) -> tuple[str, str]:
    """``country|suffix`` → (country, suffix_upper)."""
    s = str(community).strip()
    if "|" not in s:
        return s, ""
    a, b = s.split("|", 1)
    return a.strip(), b.strip().upper()


def is_cross_context_match(
    entity_country: str,
    opp_community: str,
    uefa_codes: frozenset[str],
) -> bool:
    """True if opponent national bucket differs from club or UEFA/EURO competition suffix."""
    ent = str(entity_country).strip()
    opp_c, suf = parse_community_country_suffix(opp_community)
    opp_c = str(opp_c).strip()
    if ent != opp_c:
        return True
    if suf in uefa_codes:
        return True
    return False


def comparability_rational(cross_weight_sum: float, half_saturation: float) -> float:
    """Monotone map [0, ∞) → [0, 1): w / (w + τ). Legacy helper / tests."""
    w = float(max(0.0, cross_weight_sum))
    tau = float(max(half_saturation, 1e-9))
    return float(w / (w + tau))


def _fixture_pair_weights_window(
    matches_weighted: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    md = matches_weighted["match_date"]
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    msk = (md > start_ts) & (md <= end_ts)
    sub = matches_weighted.loc[msk, ["home_club_id", "away_club_id", "weight"]].copy()
    if sub.empty:
        return pd.DataFrame(columns=["lo", "hi", "w"])
    a = sub["home_club_id"].astype(int).to_numpy()
    b = sub["away_club_id"].astype(int).to_numpy()
    lo = np.minimum(a, b)
    hi = np.maximum(a, b)
    g = (
        pd.DataFrame({"lo": lo, "hi": hi, "w": sub["weight"].astype(float)})
        .groupby(["lo", "hi"], as_index=False)["w"]
        .sum()
    )
    return g


def _build_W_from_pairs(g: pd.DataFrame, pid_to_ix: dict[int, int], n: int) -> np.ndarray:
    wmat = np.zeros((n, n), dtype=np.float64)
    for _, r in g.iterrows():
        i = pid_to_ix.get(int(r["lo"]))
        j = pid_to_ix.get(int(r["hi"]))
        if i is None or j is None:
            continue
        wt = float(r["w"])
        if wt <= 0:
            continue
        wmat[i, j] += wt
        wmat[j, i] += wt
    return wmat


def diffuse_seed_on_fixture_graph(
    wmat: np.ndarray,
    seed: np.ndarray,
    iterations: int,
    damping: float,
) -> np.ndarray:
    """
    Row-normalized lazy random walk with seed anchoring::

        x <- damping * (x @ R) + (1 - damping) * seed

    ``R_ij = W_ij / sum_k W_ik``; isolated nodes use ``R_ii = 1``.
    """
    n = wmat.shape[0]
    if n == 0:
        return seed
    deg = wmat.sum(axis=1).astype(np.float64)
    rmat = np.zeros((n, n), dtype=np.float64)
    for i in range(n):
        if deg[i] > 1e-12:
            rmat[i, :] = wmat[i, :] / deg[i]
        else:
            rmat[i, i] = 1.0
    d = float(damping)
    x = seed.astype(np.float64).copy()
    for _ in range(max(0, int(iterations))):
        x = d * (x @ rmat) + (1.0 - d) * seed
    return np.clip(x, 0.0, 1.0)


def _diffusion_map_for_rating_week(
    matches_weighted: pd.DataFrame,
    rating_yyyyww: int,
    rolling_weeks: int,
    wr: pd.DataFrame,
    pid_col: str,
    week_col: str,
    heat_gen_by_pid: dict[int, float],
    heat_seed_tau: float,
    iterations: int,
    damping: float,
) -> dict[int, float]:
    """Diffuse match-generated heat; returns per-pid ``simple_comparability`` ∈ [0,1]."""
    start_roll, end_roll = _window_bounds(int(rating_yyyyww), int(rolling_weeks))
    g = _fixture_pair_weights_window(matches_weighted, start_roll, end_roll)
    active_pids_wr = sorted(
        {int(x) for x in wr.loc[wr[week_col].astype(int) == int(rating_yyyyww), pid_col].astype(int)}
    )
    fixture_pids: set[int] = set()
    if not g.empty:
        fixture_pids.update(int(x) for x in g["lo"])
        fixture_pids.update(int(x) for x in g["hi"])
    universe = sorted(set(active_pids_wr) | fixture_pids)
    if not universe:
        return {}

    tau = max(float(heat_seed_tau), 1e-9)
    pid_to_ix = {p: i for i, p in enumerate(universe)}
    n = len(universe)
    wmat = _build_W_from_pairs(g, pid_to_ix, n)
    seed = np.zeros(n, dtype=np.float64)
    for p in universe:
        gen = max(0.0, float(heat_gen_by_pid.get(int(p), 0.0)))
        seed[pid_to_ix[p]] = gen / (gen + tau)
    dv = diffuse_seed_on_fixture_graph(wmat, seed, iterations, damping)
    return {universe[i]: float(dv[i]) for i in range(n)}


def adjusted_rating_simple(raw_rating: float, anchor: float, comparability: float) -> float:
    """One-sided shrink toward anchor when raw > anchor."""
    raw = float(raw_rating)
    a = float(anchor)
    c = float(max(0.0, min(1.0, comparability)))
    if raw <= a:
        return raw
    return float(a + c * (raw - a))


@dataclass
class GCAMSimplifiedConfig:
    """Rolling window, anchor policy, match heat rates, diffusion, optional SOS anchor."""

    rolling_weeks: int = 104
    anchor_mode: str = "global"
    global_anchor_rating: float | None = None
    anchor_recent_n_matches: int = 60
    heat_cross_match: float = 1.0
    heat_local_match: float = 0.12
    heat_seed_tau: float = 28.0
    diffusion_iterations: int = 6
    diffusion_damping: float = 0.82
    uefa_codes: frozenset[str] = field(default_factory=lambda: frozenset(DEFAULT_UEFA_CODES))

    def __post_init__(self) -> None:
        if self.anchor_mode not in ("global", "domestic", "oppo_recent_mean"):
            raise ValueError("anchor_mode must be 'global', 'domestic', or 'oppo_recent_mean'")
        if int(self.anchor_recent_n_matches) < 1:
            raise ValueError("anchor_recent_n_matches must be >= 1")
        if float(self.heat_cross_match) <= 0 or float(self.heat_local_match) < 0:
            raise ValueError("heat_cross_match must be > 0 and heat_local_match >= 0")
        if float(self.heat_seed_tau) <= 0:
            raise ValueError("heat_seed_tau must be > 0")
        if int(self.diffusion_iterations) < 1:
            raise ValueError("diffusion_iterations must be >= 1 (diffusion-only mode)")
        dmp = float(self.diffusion_damping)
        if not 0.0 < dmp < 1.0:
            raise ValueError("diffusion_damping must be in (0, 1)")


def run_simple_comparability(
    weekly_ratings: pd.DataFrame,
    matches_weighted: pd.DataFrame,
    pid_to_country: dict[int, str],
    cfg: GCAMSimplifiedConfig | None = None,
    rating_col: str = "rating",
    pid_col: str = "pid",
    week_col: str = "week",
    country_col: str = "country_name",
) -> pd.DataFrame:
    """
    Add ``simple_*`` columns: generated heat, cross diagnostics, diffused ``simple_comparability``,
    anchor, ``simple_adjusted_rating``.
    """
    cfg = cfg or GCAMSimplifiedConfig()
    wr = weekly_ratings.copy()
    matches_weighted = matches_weighted.copy()
    matches_weighted["match_date"] = pd.to_datetime(matches_weighted["match_date"]).dt.normalize()

    if country_col not in wr.columns:
        raise ValueError(f"weekly_ratings must contain {country_col!r} for simplified comparability")
    if "yyyyww" not in matches_weighted.columns:
        raise ValueError("matches_weighted must contain column 'yyyyww'")

    edges = _build_directed_edges_week(matches_weighted)

    by_pid_rating = _week_rating_arrays_by_pid(wr, pid_col, week_col, rating_col)
    opp_pre = np.array(
        [
            _lookup_pre_rating_week(by_pid_rating, int(oid), int(ww))
            for oid, ww in zip(edges["opp_id"].astype(int), edges["yyyyww"].astype(int))
        ],
        dtype=np.float64,
    )
    edges["_opp_pre_rating"] = opp_pre

    ent_c = edges["entity_id"].map(lambda x: str(pid_to_country.get(int(x), "unknown")).strip())
    op_parts = edges["opp_community"].astype(str).str.split("|", n=1, expand=True)
    opp_country = op_parts[0].fillna("").str.strip()
    suf_col = op_parts[1] if op_parts.shape[1] > 1 else pd.Series("", index=edges.index)
    suf = suf_col.fillna("").str.strip().str.upper()
    cross_flags = ((ent_c != opp_country) | suf.isin(set(cfg.uefa_codes))).to_numpy(dtype=bool)
    w_vals = edges["weight"].astype(np.float64).to_numpy()
    cross_weight_row = np.where(cross_flags, w_vals, 0.0)
    hc = float(cfg.heat_cross_match)
    hl = float(cfg.heat_local_match)
    heat_row = np.where(cross_flags, hc * w_vals, hl * w_vals)

    weeks_by_pid: dict[int, list[int]] = {}
    for pid, ws in wr.groupby(pid_col, sort=False)[week_col]:
        weeks_by_pid[int(pid)] = sorted({int(x) for x in ws.astype(int).unique().tolist()})

    gmean_by_week = wr.groupby(week_col)[rating_col].mean()

    pass1_rows: list[dict[str, Any]] = []
    n_anchor = max(1, int(cfg.anchor_recent_n_matches))
    use_oppo_anchor = cfg.anchor_mode == "oppo_recent_mean"

    for entity_id, grp in edges.groupby("entity_id", sort=False):
        eid = int(entity_id)
        pid_weeks = weeks_by_pid.get(eid)
        if not pid_weeks:
            continue

        ord_sub = grp.sort_values("match_date", kind="mergesort")
        ix = ord_sub.index.to_numpy(dtype=np.intp)
        md = ord_sub["match_date"].to_numpy(dtype="datetime64[ns]")
        cw_row = cross_weight_row[ix]
        hf_row = heat_row[ix]
        cf = cross_flags[ix]
        oi = ord_sub["opp_id"].astype(int).to_numpy()
        opr = ord_sub["_opp_pre_rating"].to_numpy(dtype=np.float64)

        lo_ptr = 0
        hi_ptr = 0
        ptr_cum = 0
        for w in sorted(pid_weeks):
            end_dt = pd.Timestamp(iso_week_to_sunday(int(w)))
            end_ns = np.datetime64(end_dt)
            start_roll, end_roll = _window_bounds(w, cfg.rolling_weeks)
            start_ns = np.datetime64(pd.Timestamp(start_roll))
            end_roll_ns = np.datetime64(pd.Timestamp(end_roll))

            while ptr_cum < len(md) and md[ptr_cum] <= end_ns:
                ptr_cum += 1

            while lo_ptr < len(md) and md[lo_ptr] <= start_ns:
                lo_ptr += 1
            while hi_ptr < len(md) and md[hi_ptr] <= end_roll_ns:
                hi_ptr += 1

            w_sum = float(np.sum(cw_row[lo_ptr:hi_ptr]))
            h_sum = float(np.sum(hf_row[lo_ptr:hi_ptr]))
            opp_win = oi[lo_ptr:hi_ptr]
            cf_win = cf[lo_ptr:hi_ptr]
            distinct_x = int(len(np.unique(opp_win[cf_win]))) if np.any(cf_win) else 0

            row: dict[str, Any] = {
                pid_col: eid,
                week_col: int(w),
                "simple_cross_weight_sum": w_sum,
                "simple_heat_generated": h_sum,
                "simple_n_distinct_cross_opponents": float(distinct_x),
            }
            if use_oppo_anchor:
                slab = opr[:ptr_cum]
                take = slab[-n_anchor:] if len(slab) else slab
                opp_mean = float(np.nanmean(take)) if len(take) else float("nan")
                if math.isnan(opp_mean):
                    if int(w) in gmean_by_week.index:
                        opp_mean = float(gmean_by_week.loc[int(w)])
                    else:
                        opp_mean = float(wr[rating_col].mean())
                row["simple_mean_opp_pre_rating"] = opp_mean
                row["simple_anchor_n_matches_used"] = float(len(take))
            pass1_rows.append(row)

    met = pd.DataFrame(pass1_rows)

    sig_rows: list[dict[str, Any]] = []
    if not met.empty:
        for wk in sorted(met[week_col].astype(int).unique()):
            sub_w = met.loc[met[week_col].astype(int) == int(wk)]
            heat_by_pid = {
                int(r[pid_col]): float(r["simple_heat_generated"]) for _, r in sub_w.iterrows()
            }
            dmap = _diffusion_map_for_rating_week(
                matches_weighted,
                int(wk),
                int(cfg.rolling_weeks),
                wr,
                pid_col,
                week_col,
                heat_by_pid,
                float(cfg.heat_seed_tau),
                int(cfg.diffusion_iterations),
                float(cfg.diffusion_damping),
            )
            for pid_u, sig in dmap.items():
                sig_rows.append({pid_col: int(pid_u), week_col: int(wk), "simple_comparability": float(sig)})

    sig_df = pd.DataFrame(sig_rows)

    if met.empty:
        out = wr.copy()
        out["simple_cross_weight_sum"] = 0.0
        out["simple_heat_generated"] = 0.0
        out["simple_n_distinct_cross_opponents"] = 0.0
        out["simple_comparability"] = 0.0
        if use_oppo_anchor:
            out["simple_mean_opp_pre_rating"] = np.nan
            out["simple_anchor_n_matches_used"] = 0.0
    else:
        out = wr.merge(met, on=[pid_col, week_col], how="left")
        out = out.merge(sig_df, on=[pid_col, week_col], how="left")
        out["simple_cross_weight_sum"] = out["simple_cross_weight_sum"].fillna(0.0)
        out["simple_heat_generated"] = out["simple_heat_generated"].fillna(0.0)
        out["simple_n_distinct_cross_opponents"] = out["simple_n_distinct_cross_opponents"].fillna(0.0)
        out["simple_comparability"] = out["simple_comparability"].fillna(0.0)
        if use_oppo_anchor:
            out["simple_anchor_n_matches_used"] = out["simple_anchor_n_matches_used"].fillna(0.0)

    gmean = out.groupby(week_col)[rating_col].transform("mean")
    if cfg.global_anchor_rating is not None:
        out["simple_anchor_rating"] = float(cfg.global_anchor_rating)
    elif cfg.anchor_mode == "global":
        out["simple_anchor_rating"] = gmean.astype(np.float64)
    elif cfg.anchor_mode == "domestic":
        dmean = out.groupby([week_col, country_col])[rating_col].transform("mean")
        out["simple_anchor_rating"] = dmean.fillna(gmean).astype(np.float64)
    else:
        if "simple_mean_opp_pre_rating" not in out.columns:
            raise ValueError("oppo_recent_mean anchor requires pass1 metrics with opponent pre-ratings (edges empty?)")
        out["simple_anchor_rating"] = out["simple_mean_opp_pre_rating"].astype(float)
        miss = out["simple_anchor_rating"].isna()
        if miss.any():
            out.loc[miss, "simple_anchor_rating"] = gmean[miss].astype(float)

    raw = out[rating_col].astype(np.float64).to_numpy()
    anch = out["simple_anchor_rating"].astype(np.float64).to_numpy()
    comp = np.clip(out["simple_comparability"].astype(np.float64).to_numpy(), 0.0, 1.0)

    adj = np.where(
        raw <= anch,
        raw,
        anch + comp * (raw - anch),
    )
    out["simple_adjusted_rating"] = adj

    return out


__all__ = [
    "GCAMSimplifiedConfig",
    "adjusted_rating_simple",
    "comparability_rational",
    "diffuse_seed_on_fixture_graph",
    "is_cross_context_match",
    "parse_community_country_suffix",
    "run_simple_comparability",
]
