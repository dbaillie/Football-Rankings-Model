"""Club-level narrative: rating trajectory + domestic and continental ladder context."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pendulum
from jinja2 import BaseLoader, Environment

from .country_narrative import _detect_change_points_with_backend, _human_week, _oxford_join_phrases
from .data_service import (
    NARRATIVE_LADDER_DROP_FIRST_N_WEEKS,
    _strip_international,
    filter_weekly_for_narrative_ladder,
    load_weekly_ratings,
    narrative_ladder_week_allowlist,
)


def _title_place(raw: str) -> str:
    s = str(raw).strip()
    return s.title() if s.islower() else s


def _fmt_rank(n: float | int) -> str:
    x = float(n)
    if abs(x - round(x)) < 1e-6:
        return str(int(round(x)))
    return f"{x:.1f}"


def _share_sentence(prefix: str, cutoffs: tuple[int, ...], shares: dict[int, float]) -> str:
    parts = []
    for k in sorted(cutoffs):
        pct = int(round(100.0 * shares.get(k, 0.0)))
        parts.append(f"**{pct}%** of rated weeks in the **top {k}**")
    joined = _oxford_join_phrases(parts)
    return f"{prefix}{joined}."


def _rank_frames(weekly_df: pd.DataFrame, country_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    eu = weekly_df.sort_values(["week", "rating", "pid"], ascending=[True, False, True]).copy()
    eu["eu_rank"] = eu.groupby("week", sort=False).cumcount() + 1
    dom_base = weekly_df.loc[weekly_df["country_name"].str.lower() == country_key].copy()
    dom_base = dom_base.sort_values(["week", "rating", "pid"], ascending=[True, False, True])
    dom_base["dom_rank"] = dom_base.groupby("week", sort=False).cumcount() + 1
    return eu, dom_base


def _merge_team_ranks(eu: pd.DataFrame, dom_base: pd.DataFrame, tid: int) -> pd.DataFrame:
    eu_team = eu.loc[eu["pid"] == tid, ["week", "week_date", "rating", "eu_rank"]].copy()
    dom_team = dom_base.loc[dom_base["pid"] == tid, ["week", "dom_rank"]].copy()
    m = eu_team.merge(dom_team, on="week", how="left").sort_values("week").reset_index(drop=True)
    if not m.empty and m["dom_rank"].isna().any():
        m["dom_rank"] = m["dom_rank"].ffill().bfill()
    return m


def build_team_narrative(
    team_id: int,
    domestic_cutoffs: tuple[int, ...] = (5, 10, 25),
    continental_cutoffs: tuple[int, ...] = (25, 50, 100),
) -> dict[str, Any] | None:
    tid = int(team_id)
    weekly_all = _strip_international(load_weekly_ratings())
    if weekly_all.empty:
        return None

    tw = weekly_all.loc[weekly_all["pid"] == tid].copy()
    if tw.empty:
        return None

    country_raw = str(tw.iloc[0]["country_name"]).strip()
    team_name = str(tw.iloc[0]["team_name"]).strip()
    country_key = country_raw.lower()

    allow = narrative_ladder_week_allowlist(weekly_all)
    weekly_ladder = filter_weekly_for_narrative_ladder(weekly_all)

    eu_l, dom_l = _rank_frames(weekly_ladder, country_key)
    m_l = _merge_team_ranks(eu_l, dom_l, tid)

    ladder_applied = allow is not None
    if ladder_applied and m_l.empty:
        eu_l, dom_l = _rank_frames(weekly_all, country_key)
        m_l = _merge_team_ranks(eu_l, dom_l, tid)
        ladder_applied = False

    eu_f, dom_f = _rank_frames(weekly_all, country_key)
    m_full = _merge_team_ranks(eu_f, dom_f, tid)
    if m_full.empty:
        return None

    dates = m_full["week_date"].astype(str).tolist()
    ratings = m_full["rating"].to_numpy(dtype=float)
    n_weeks = len(m_full)
    latest_rating = float(ratings[-1])
    hist_mean_rating = float(np.mean(ratings))

    latest_dom = int(m_full["dom_rank"].iloc[-1])
    latest_eu = int(m_full["eu_rank"].iloc[-1])

    src = m_l if not m_l.empty else m_full
    dom_r = src["dom_rank"].to_numpy(dtype=float)
    eu_r = src["eu_rank"].to_numpy(dtype=float)
    n_ladder = len(src)

    x = np.arange(n_weeks, dtype=float)
    slope_py = 0.0
    if n_weeks >= 3:
        slope_py = float(np.polyfit(x, ratings, 1)[0]) * 52.0
    vol = float(np.std(np.diff(ratings))) if n_weeks >= 2 else 0.0

    trend_word = (
        "rose"
        if slope_py > 0.35
        else ("fell" if slope_py < -0.35 else "held roughly steady")
    )

    mean_dom = float(np.mean(dom_r))
    best_dom = int(np.min(dom_r))
    mean_eu = float(np.mean(eu_r))
    best_eu = int(np.min(eu_r))

    dom_shares: dict[int, float] = {}
    for k in domestic_cutoffs:
        dom_shares[k] = float(np.mean(dom_r <= k))

    eu_shares: dict[int, float] = {}
    for z in continental_cutoffs:
        eu_shares[z] = float(np.mean(eu_r <= z))

    cp_dates = src["week_date"].astype(str).tolist() if ladder_applied and len(src) >= 8 else dates
    cp_ratings = src["rating"].to_numpy(dtype=float) if ladder_applied and len(src) >= 8 else ratings
    regimes, change_backend = _detect_change_points_with_backend(cp_dates, cp_ratings)
    if len(regimes) > 1:
        deltas = [
            abs(regimes[i]["segment_mean"] - regimes[i - 1]["segment_mean"])
            for i in range(1, len(regimes))
        ]
        regime_jump = round(float(np.mean(deltas)), 1)
    else:
        regime_jump = 0.0

    country_display = _title_place(country_raw)

    dom_prefix = "Domestically, "
    eu_prefix = "Across Europe, "

    ctx: dict[str, Any] = {
        "team_name": team_name,
        "country_display": country_display,
        "ladder_applied": ladder_applied,
        "ladder_weeks_dropped": int(NARRATIVE_LADDER_DROP_FIRST_N_WEEKS) if ladder_applied else 0,
        "n_weeks_fmt": f"{n_weeks:,}",
        "first_month": _human_week(dates[0]),
        "latest_month": _human_week(dates[-1]),
        "latest_rating": round(latest_rating, 1),
        "hist_mean_rating": round(hist_mean_rating, 1),
        "above_hist_fmt": f"{latest_rating - hist_mean_rating:+.1f}",
        "slope_per_year": round(slope_py, 2),
        "trend_word": trend_word,
        "volatility": round(vol, 2),
        "mean_dom_rank_fmt": _fmt_rank(mean_dom),
        "latest_dom_rank": latest_dom,
        "best_dom_rank": best_dom,
        "mean_eu_rank_fmt": _fmt_rank(mean_eu),
        "latest_eu_rank": latest_eu,
        "best_eu_rank": best_eu,
        "domestic_share_sentence": _share_sentence(dom_prefix, domestic_cutoffs, dom_shares),
        "continental_share_sentence": _share_sentence(eu_prefix, continental_cutoffs, eu_shares),
        "has_regimes": len(regimes) > 1,
        "n_regimes": len(regimes),
        "regime_jump": regime_jump,
        "domestic_cutoffs": list(domestic_cutoffs),
        "continental_cutoffs": list(continental_cutoffs),
    }

    env = Environment(loader=BaseLoader(), autoescape=False)
    body = env.from_string(_TEAM_NARRATIVE_TEMPLATE).render(**ctx)
    paragraphs = [p.strip() for p in body.split("\n\n") if p.strip()]

    generated = pendulum.now("UTC").to_iso8601_string()

    return {
        "team_id": tid,
        "team_name": team_name,
        "country_name": country_raw,
        "generated_at": generated,
        "paragraphs": paragraphs,
        "facts": {
            "rated_weeks": n_weeks,
            "ladder_weeks_used_for_rank_summaries": n_ladder,
            "narrative_warmup_weeks_dropped_from_ladder": ctx["ladder_weeks_dropped"],
            "mean_domestic_rank": round(mean_dom, 3),
            "latest_domestic_rank": latest_dom,
            "best_domestic_rank": best_dom,
            "mean_continental_rank": round(mean_eu, 3),
            "latest_continental_rank": latest_eu,
            "best_continental_rank": best_eu,
            "domestic_top_share": {str(k): round(dom_shares[k], 4) for k in domestic_cutoffs},
            "continental_top_share": {str(z): round(eu_shares[z], 4) for z in continental_cutoffs},
            "change_point_segments": len(regimes),
            "change_point_backend": change_backend,
            "slope_rating_points_per_year": ctx["slope_per_year"],
            "rating_week_to_week_volatility": ctx["volatility"],
        },
    }


_TEAM_NARRATIVE_TEMPLATE = """**{{ team_name }}** ({{ country_display }}) appears in **{{ n_weeks_fmt }}** weekly rating rows from **{{ first_month }}** through **{{ latest_month }}**. The latest rating is **{{ latest_rating }}** versus **{{ hist_mean_rating }}** averaged across those weeks (**{{ above_hist_fmt }}** vs that club-level mean). Week-to-week rating noise has standard deviation **{{ volatility }}** points; a linear trend **{{ trend_word }}** at about **{{ slope_per_year }}** points per year.

**Domestic ladder** (within {{ country_display }} only each week; rank **1** is strongest): mean weekly slot **{{ mean_dom_rank_fmt }}**, **{{ latest_dom_rank }}** in the latest row, best weekly slot **{{ best_dom_rank }}**. {{ domestic_share_sentence }}

**Continental ladder** (every European club in the dataset ranked each week): mean rank **{{ mean_eu_rank_fmt }}**, **{{ latest_eu_rank }}** latest, best-ever weekly rank **{{ best_eu_rank }}**. {{ continental_share_sentence }}

{% if has_regimes %}The rating trajectory splits into **{{ n_regimes }}** broad segments with typical steps around **{{ regime_jump }}** points between segment averages.{% endif %}"""
