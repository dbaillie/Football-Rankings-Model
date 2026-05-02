"""Orchestrate post-hoc GCAM: connectivity windows, baselines, adjustments, diagnostics."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from glicko_engine.core import iso_week_to_sunday

from .config import GCAMConfig
from .connectivity import (
    community_external_metrics_for_window,
    effective_connectivity,
    entity_direct_metrics,
    primary_own_community,
)
from .football import iter_weighted_match_pairs


def _window_slice_sorted(
    md_ns: np.ndarray,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[int, int]:
    """Indices ``lo:hi`` into ascending ``md_ns`` with dates in ``(start, end]`` (exclusive start)."""
    start_ns = np.datetime64(pd.Timestamp(start))
    end_ns = np.datetime64(pd.Timestamp(end))
    lo = int(np.searchsorted(md_ns, start_ns, side="right"))
    hi = int(np.searchsorted(md_ns, end_ns, side="right"))
    return lo, hi


def build_directed_edges(matches_weighted: pd.DataFrame) -> pd.DataFrame:
    """Two directed rows per fixture (home and away perspectives)."""
    base = matches_weighted[
        ["match_date", "home_club_id", "away_club_id", "home_community", "away_community", "weight"]
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
    return out.sort_values(["entity_id", "match_date"]).reset_index(drop=True)


def _window_bounds(yyyyww: int, cfg: GCAMConfig) -> tuple[pd.Timestamp, pd.Timestamp]:
    end = pd.Timestamp(iso_week_to_sunday(int(yyyyww)))
    start = end - timedelta(weeks=int(cfg.rolling_weeks))
    return start, end


def run_posthoc_gcam(
    weekly_ratings: pd.DataFrame,
    matches_weighted: pd.DataFrame,
    cfg: GCAMConfig | None = None,
    rating_col: str = "rating",
    rd_col: str = "rd",
    pid_col: str = "pid",
    week_col: str = "week",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute GCAM columns for each (entity, rating_period) row in ``weekly_ratings``.

    Returns (weekly_with_gcam, community_weekly_metrics).
    """
    cfg = cfg or GCAMConfig()
    wr = weekly_ratings.copy()
    matches_weighted = matches_weighted.copy()
    matches_weighted["match_date"] = pd.to_datetime(matches_weighted["match_date"]).dt.normalize()

    weeks_u = sorted(pd.unique(wr[week_col].astype(int)))

    msort = matches_weighted.sort_values("match_date", kind="mergesort").reset_index(drop=True)
    md_all = msort["match_date"].to_numpy(dtype="datetime64[ns]")

    week_comm_maps: dict[int, dict[str, dict[str, float]]] = {}
    for w in weeks_u:
        start, end = _window_bounds(w, cfg)
        lo, hi = _window_slice_sorted(md_all, start, end)
        if lo >= hi:
            week_comm_maps[w] = {}
            continue
        sub_slice = msort.iloc[lo:hi]
        week_comm_maps[w] = community_external_metrics_for_window(iter_weighted_match_pairs(sub_slice), cfg)

    edges = build_directed_edges(matches_weighted)

    weeks_by_pid: dict[int, list[int]] = {}
    for pid, ws in wr.groupby(pid_col, sort=False)[week_col]:
        weeks_by_pid[int(pid)] = sorted({int(x) for x in ws.astype(int).unique().tolist()})

    entity_metrics_rows: list[dict[str, Any]] = []

    for entity_id, grp in edges.groupby("entity_id", sort=False):
        eid = int(entity_id)
        pid_weeks = weeks_by_pid.get(eid)
        if not pid_weeks:
            continue

        md = grp["match_date"].to_numpy(dtype="datetime64[ns]")
        opp_c = grp["opp_community"].astype(str).to_numpy()
        own_c = grp["own_community"].astype(str).to_numpy()
        wts = grp["weight"].astype(np.float64).to_numpy()
        opp_ids = grp["opp_id"].astype(int).to_numpy()

        lo_ptr = 0
        hi_ptr = 0
        for w in pid_weeks:
            start, end = _window_bounds(w, cfg)
            start_ns = np.datetime64(pd.Timestamp(start))
            end_ns = np.datetime64(pd.Timestamp(end))
            while lo_ptr < len(md) and md[lo_ptr] <= start_ns:
                lo_ptr += 1
            while hi_ptr < len(md) and md[hi_ptr] <= end_ns:
                hi_ptr += 1

            oc = opp_c[lo_ptr:hi_ptr]
            ow = wts[lo_ptr:hi_ptr]
            oi = opp_ids[lo_ptr:hi_ptr]
            owc = own_c[lo_ptr:hi_ptr]
            em = entity_direct_metrics(oc, ow, oi, cfg)
            prim = primary_own_community(owc, ow)
            cc_lookup = week_comm_maps.get(int(w), {}).get(prim or "", {})
            cc_val = float(cc_lookup.get("community_connectivity", cfg.community_connectivity_floor))
            eff = effective_connectivity(em["direct_connectivity"], cc_val, cfg)
            entity_metrics_rows.append(
                {
                    pid_col: eid,
                    week_col: int(w),
                    "primary_community": prim or "",
                    **em,
                    "community_connectivity": cc_val,
                    "effective_connectivity": eff,
                }
            )

    met_df = pd.DataFrame(entity_metrics_rows)
    if met_df.empty:
        return wr, pd.DataFrame()

    merged = wr.merge(met_df, on=[pid_col, week_col], how="left")
    merged["primary_community"] = merged["primary_community"].fillna("").astype(str)

    merged["global_mean_rating"] = merged.groupby(week_col)[rating_col].transform("mean")
    merged["community_mean_rating"] = merged.groupby([week_col, "primary_community"])[rating_col].transform("mean")
    gfix = cfg.global_baseline_rating
    if gfix is not None:
        merged["global_mean_rating"] = float(gfix)

    g = merged["global_mean_rating"].to_numpy(dtype=np.float64)
    c = merged["community_mean_rating"].to_numpy(dtype=np.float64)
    has_pc = merged["primary_community"].astype(str).str.len().to_numpy()
    if cfg.baseline_mode == "global":
        baseline_arr = g
    elif cfg.baseline_mode == "community":
        baseline_arr = np.where(has_pc > 0, c, g)
    else:
        gw = max(0.0, min(1.0, float(cfg.baseline_global_weight)))
        baseline_arr = gw * g + (1.0 - gw) * c
    merged["baseline_rating"] = baseline_arr

    raw_rat = merged[rating_col].to_numpy(dtype=np.float64)
    raw_rd = merged[rd_col].to_numpy(dtype=np.float64)
    ec = np.clip(merged["effective_connectivity"].to_numpy(dtype=np.float64), 0.0, 1.0)
    gap = np.maximum(0.0, 1.0 - ec)
    struct = cfg.structural_rd_scale * (gap ** cfg.structural_rd_gamma)
    merged["structural_rd"] = struct
    merged["total_rd"] = np.sqrt(raw_rd * raw_rd + struct * struct)

    vt = np.clip(merged["volume_trust"].to_numpy(dtype=np.float64), 0.0, 1.0)
    trd = merged["total_rd"].to_numpy(dtype=np.float64)
    denom = 1.0 + np.maximum(0.0, trd) / max(cfg.trust_rd_scale, 1e-6)
    rd_term = 1.0 / denom
    raw_trust = ec * vt * rd_term
    tf = cfg.trust_floor + (1.0 - cfg.trust_floor) * raw_trust
    merged["trust_factor"] = np.clip(tf, cfg.trust_floor, 1.0)

    tr = merged["trust_factor"].to_numpy(dtype=np.float64)
    shrunk = tr * raw_rat + (1.0 - tr) * baseline_arr
    merged["adjusted_rating"] = np.where(raw_rat <= baseline_arr, raw_rat, shrunk)

    adj = merged["adjusted_rating"].to_numpy(dtype=np.float64)
    mode = cfg.power_score_mode
    if mode == "rating_only":
        merged["power_score"] = adj
    elif mode == "rating_rd":
        merged["power_score"] = adj - cfg.power_rd_lambda * trd
    else:
        merged["power_score"] = (
            adj - cfg.power_rd_lambda * trd - cfg.power_connectivity_lambda * (1.0 - ec)
        )

    mean_cols = merged.groupby([week_col, "primary_community"], sort=False).agg(
        raw_mean_rating=(rating_col, "mean"),
        adjusted_mean_rating=("adjusted_rating", "mean"),
        baseline_mean=("baseline_rating", "mean"),
    )
    mean_cols = mean_cols.reset_index()

    comm_export_rows: list[dict[str, Any]] = []
    for w in weeks_u:
        cmap = week_comm_maps.get(w, {})
        sub_means = mean_cols.loc[mean_cols[week_col] == w]
        mean_map = {
            str(r["primary_community"]): (
                float(r["raw_mean_rating"]),
                float(r["adjusted_mean_rating"]),
                float(r["baseline_mean"]),
            )
            for _, r in sub_means.iterrows()
        }
        for comm, met in cmap.items():
            raw_mean, adj_mean, base_mean = mean_map.get(str(comm), (float("nan"), float("nan"), float("nan")))
            row = {
                "rating_period": int(w),
                "community_id": comm,
                "raw_mean_rating": raw_mean,
                "adjusted_mean_rating": adj_mean,
                "baseline_rating": base_mean,
                **met,
            }
            comm_export_rows.append(row)

    comm_df = pd.DataFrame(comm_export_rows)
    return merged, comm_df


def build_gcam_diagnostics(
    weekly_gcam: pd.DataFrame,
    team_names: dict[int, str],
    week_col: str = "week",
    pid_col: str = "pid",
) -> dict[str, Any]:
    """Compact diagnostic slices for dashboards (latest week only)."""
    if weekly_gcam.empty or week_col not in weekly_gcam.columns:
        return {}

    wmax = int(pd.to_numeric(weekly_gcam[week_col], errors="coerce").max())
    snap = weekly_gcam.loc[weekly_gcam[week_col] == wmax].copy()
    if snap.empty:
        return {}

    def label(pid: int) -> dict[str, Any]:
        return {"pid": int(pid), "team_name": team_names.get(int(pid), str(pid))}

    def top_n(col: str, ascending: bool, n: int = 15) -> list[dict[str, Any]]:
        s = snap.sort_values(col, ascending=ascending).head(n)
        out = []
        for _, r in s.iterrows():
            d = label(int(r[pid_col]))
            d[col] = float(r[col])
            if "adjusted_rating" in snap.columns and "rating" in snap.columns:
                d["rating"] = float(r["rating"])
                d["adjusted_rating"] = float(r["adjusted_rating"])
            out.append(d)
        return out

    diag: dict[str, Any] = {"latest_rating_period": wmax}
    if "effective_connectivity" in snap.columns:
        diag["highest_connectivity_entities"] = top_n("effective_connectivity", ascending=False)
        diag["lowest_connectivity_entities"] = top_n("effective_connectivity", ascending=True)
    if "rating" in snap.columns and "trust_factor" in snap.columns:
        hi_raw_low_trust = snap.sort_values(["rating", "trust_factor"], ascending=[False, True]).head(15)
        diag["highest_raw_ratings_low_trust"] = [
            {**label(int(r[pid_col])), "rating": float(r["rating"]), "trust_factor": float(r["trust_factor"])}
            for _, r in hi_raw_low_trust.iterrows()
        ]
    if "adjusted_rating" in snap.columns and "rating" in snap.columns:
        snap = snap.assign(_delta=snap["adjusted_rating"] - snap["rating"])
        diag["largest_downward_adjustments"] = [
            {**label(int(r[pid_col])), "rating": float(r["rating"]), "adjusted_rating": float(r["adjusted_rating"]), "delta": float(r["_delta"])}
            for _, r in snap.sort_values("_delta").head(15).iterrows()
        ]
        diag["largest_upward_or_least_shrunk"] = [
            {**label(int(r[pid_col])), "rating": float(r["rating"]), "adjusted_rating": float(r["adjusted_rating"]), "delta": float(r["_delta"])}
            for _, r in snap.sort_values("_delta", ascending=False).head(15).iterrows()
        ]
        diag["highest_adjusted_ratings"] = top_n("adjusted_rating", ascending=False)

    return diag
