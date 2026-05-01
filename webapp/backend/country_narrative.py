"""Country-level prose narrative from weekly rating aggregates (pandas/numpy + Jinja2 + pendulum + ruptures)."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pendulum
from jinja2 import BaseLoader, Environment

from .data_service import (
    NARRATIVE_LADDER_DROP_FIRST_N_WEEKS,
    _strip_international,
    filter_weekly_for_narrative_ladder,
    get_country_timeseries,
    get_country_top_n_timeseries,
    load_weekly_ratings,
    narrative_ladder_week_allowlist,
)


def _continental_top_presence(
    country_slug: str, cutoffs: tuple[int, ...] = (25, 50, 100)
) -> tuple[dict[str, Any], int]:
    """
    Per weekly European snapshot: rank all clubs by rating (ties keep CSV order).
    For each cutoff Z, count how many clubs from `country_slug` fall in the continent-wide top Z.
    Returns (stats_by_cutoff, n_european_weeks).
    """
    eu = _strip_international(load_weekly_ratings())
    if eu.empty:
        return {}, 0

    eu = filter_weekly_for_narrative_ladder(eu)
    eu = eu.sort_values(["week", "rating", "pid"], ascending=[True, False, True]).copy()
    eu["eu_rank"] = eu.groupby("week", sort=False).cumcount() + 1

    cc_mask = eu["country_name"].str.lower() == country_slug.strip().lower()
    cc_df = eu.loc[cc_mask].copy()
    full_week_index = pd.Index(sorted(eu["week"].unique()), name="week")
    n_eu_weeks = int(len(full_week_index))

    for z in cutoffs:
        cc_df[f"hit_{z}"] = (cc_df["eu_rank"] <= z).astype(np.int32)

    hit_cols = [f"hit_{z}" for z in cutoffs]
    if cc_df.empty:
        stats = {
            str(z): {
                "mean_teams": 0.0,
                "latest_teams": 0,
                "max_teams": 0,
                "weeks_with_at_least_one": 0,
                "share_weeks_with_any": 0.0,
            }
            for z in cutoffs
        }
        return stats, n_eu_weeks

    agg = cc_df.groupby("week", sort=False)[hit_cols].sum().reindex(full_week_index, fill_value=0)

    stats: dict[str, Any] = {}
    for z in cutoffs:
        col = f"hit_{z}"
        series = agg[col].astype(float)
        latest_teams = int(series.iloc[-1]) if len(series) else 0
        weeks_any = int((series >= 1).sum())
        stats[str(z)] = {
            "mean_teams": float(series.mean()),
            "latest_teams": latest_teams,
            "max_teams": int(series.max()),
            "weeks_with_at_least_one": weeks_any,
            "share_weeks_with_any": float(weeks_any / n_eu_weeks) if n_eu_weeks else 0.0,
        }

    return stats, n_eu_weeks


def _oxford_join_phrases(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


def _continental_paragraph_for_template(
    country_display: str,
    continental_cutoffs: tuple[int, ...],
    continental_stats: dict[str, Any],
    n_eu_weeks: int,
    first_month: str,
    ladder_weeks_dropped: int = 0,
) -> str:
    """Single narrative paragraph; uses ** for frontend bold."""
    if n_eu_weeks <= 0 or not continental_cutoffs:
        return ""

    z_sorted = tuple(sorted(int(z) for z in continental_cutoffs))
    eu_weeks_fmt = f"{n_eu_weeks:,}"

    mean_chunks: list[str] = []
    latest_parts: list[str] = []
    max_parts: list[str] = []
    labels_z: list[str] = []
    share_smallest: float | None = None

    for z in z_sorted:
        b = continental_stats.get(str(z), {})
        mean_tm = float(b.get("mean_teams") or 0.0)
        m_txt = f"{mean_tm:.2f}".rstrip("0").rstrip(".")
        mean_chunks.append(f"**{m_txt}** clubs in the continental **top {z}**")
        latest_parts.append(f"**{int(b.get('latest_teams') or 0)}**")
        max_parts.append(f"**{int(b.get('max_teams') or 0)}**")
        labels_z.append(f"top {z}")
        if share_smallest is None:
            share_smallest = float(b.get("share_weeks_with_any") or 0.0)

    mean_joined = _oxford_join_phrases(mean_chunks)
    latest_joined = ", ".join(latest_parts)
    max_joined = ", ".join(max_parts)
    labels_joined = ", ".join(labels_z)
    pct = int(round(100.0 * share_smallest)) if share_smallest is not None else 0

    tail = ""
    if ladder_weeks_dropped > 0:
        tail = (
            f" Continental ladder counts omit the first **{ladder_weeks_dropped}** chronological rating "
            f"weeks while mass ratings / ties stop dominating the sort."
        )

    return (
        f"Relative to **continental** peers (each week, every club in the European dataset is ranked by "
        f"rating; ties keep file order): averaged across **{eu_weeks_fmt}** weekly snapshots, "
        f"**{country_display}** typically places {mean_joined}. "
        f"In the **latest** week those counts are {latest_joined} "
        f"({labels_joined} respectively); peak counts in a single week since **{first_month}** were "
        f"{max_joined}. At least one club from **{country_display}** sat in the continental "
        f"**top {z_sorted[0]}** in about **{pct}%** of European weeks.{tail}"
    )


def _country_display_name(country_slug: str) -> str:
    w = load_weekly_ratings()
    sub = w[w["country_name"].str.lower() == country_slug.lower()]
    sub = _strip_international(sub)
    if sub.empty:
        raw = country_slug.replace("-", " ").strip()
        return raw.title() if raw.islower() else raw
    mode = sub["country_name"].mode()
    raw = str(mode.iloc[0]) if len(mode) else country_slug
    return raw.title() if raw.islower() else raw


def _human_week(iso_date: str | None) -> str:
    if not iso_date:
        return "unknown date"
    try:
        dt = pendulum.parse(str(iso_date), strict=False)
        return dt.format("MMMM YYYY")
    except Exception:
        return str(iso_date)


def _segment_sse(seg: np.ndarray) -> float:
    if seg.size == 0:
        return 0.0
    m = float(np.mean(seg))
    return float(np.sum((seg - m) ** 2))


def _best_single_split(sub: np.ndarray, min_size: int) -> tuple[int | None, float]:
    n = len(sub)
    full = _segment_sse(sub)
    best_t: int | None = None
    best_c = float("inf")
    for t in range(min_size, n - min_size):
        c = _segment_sse(sub[:t]) + _segment_sse(sub[t:])
        if c < best_c:
            best_c = c
            best_t = t
    if best_t is None:
        return None, 0.0
    return best_t, full - best_c


def _segments_to_rows(
    segments: list[tuple[int, int]], dates: list[str], values: np.ndarray
) -> list[dict[str, Any]]:
    v = np.asarray(values, dtype=float)
    out: list[dict[str, Any]] = []
    for s, e in segments:
        end_idx = e - 1
        seg = v[s:e]
        out.append(
            {
                "segment_start_date": dates[s] if s < len(dates) else "",
                "segment_end_date": dates[end_idx] if end_idx < len(dates) else "",
                "segment_label_end": _human_week(dates[end_idx] if end_idx < len(dates) else None),
                "segment_mean": float(np.mean(seg)),
            }
        )
    return out


def _detect_change_points_numpy(
    dates: list[str], values: np.ndarray, max_splits: int = 4, min_size: int = 8
) -> list[dict[str, Any]]:
    """Greedy binary segmentation minimizing within-segment SSE (pure numpy)."""
    v = np.asarray(values, dtype=float)
    n = len(v)
    if n < 36:
        return []
    full_sse = _segment_sse(v)
    threshold = max(full_sse * 0.015, float(np.var(v)) * 6.0)

    segments: list[tuple[int, int]] = [(0, n)]
    for _ in range(max_splits):
        best_gain = -1.0
        best_choice: tuple[int, int] | None = None
        for si, (s, e) in enumerate(segments):
            if e - s < 2 * min_size:
                continue
            sub = v[s:e]
            t_rel, gain = _best_single_split(sub, min_size)
            if t_rel is None:
                continue
            if gain > best_gain:
                best_gain = gain
                best_choice = (si, s + t_rel)
        if best_choice is None or best_gain < threshold:
            break
        si, t_abs = best_choice
        s, e = segments.pop(si)
        segments.insert(si, (t_abs, e))
        segments.insert(si, (s, t_abs))
        segments.sort(key=lambda x: x[0])

    return _segments_to_rows(segments, dates, v)


def _detect_change_points_ruptures(
    dates: list[str], values: np.ndarray, max_segments: int = 8
) -> list[dict[str, Any]]:
    import ruptures as rpt

    n = len(values)
    signal = np.asarray(values, dtype=float).reshape(-1, 1)
    var = float(np.var(signal))
    pen = max(var * 5.0, 80.0)
    algo = rpt.Pelt(model="l2", min_size=6, jump=2).fit(signal)
    bkps = algo.predict(pen=pen)

    out: list[dict[str, Any]] = []
    prev = 0
    for end in bkps:
        if end <= prev:
            continue
        seg = values[prev:end]
        end_idx = end - 1
        out.append(
            {
                "segment_start_date": dates[prev] if prev < len(dates) else "",
                "segment_end_date": dates[end_idx] if end_idx < len(dates) else "",
                "segment_label_end": _human_week(dates[end_idx] if end_idx < len(dates) else None),
                "segment_mean": float(np.mean(seg)),
            }
        )
        prev = end
        if len(out) >= max_segments:
            break
    return out


def _detect_change_points_with_backend(
    dates: list[str], values: np.ndarray,
) -> tuple[list[dict[str, Any]], str]:
    """
    Prefer ruptures PELT when importable; otherwise numpy binary segmentation.
    (Some Windows/Python builds lack ruptures wheels — MSVC required to compile.)
    """
    n = len(values)
    if n < 36:
        return [], "none"

    try:
        rows = _detect_change_points_ruptures(dates, values)
        if len(rows) > 1:
            return rows, "ruptures_pelt"
    except Exception:
        pass

    rows = _detect_change_points_numpy(dates, values)
    return rows, "numpy_sse_splits"


def build_country_narrative(
    country_slug: str, top_n: int = 5, continental_cutoffs: tuple[int, ...] = (25, 50, 100)
) -> dict[str, Any] | None:
    """
    Returns structured narrative payload, or None if the country has no weekly history.
    """
    slug = country_slug.strip().lower()
    rows = get_country_timeseries(slug)
    if not rows:
        return None

    base_w = _strip_international(load_weekly_ratings())
    allow = narrative_ladder_week_allowlist(base_w)
    ladder_weeks_dropped = int(NARRATIVE_LADDER_DROP_FIRST_N_WEEKS) if allow is not None else 0

    df = pd.DataFrame(rows)
    df = df.sort_values("week").reset_index(drop=True)
    if allow is not None:
        df = df.loc[df["week"].isin(allow)].reset_index(drop=True)
    if df.empty:
        return None
    dates = df["week_date"].astype(str).tolist()
    avg = df["average_rating"].to_numpy(dtype=float)
    top_r = df["top_rating"].to_numpy(dtype=float)
    bot_r = df["bottom_rating"].to_numpy(dtype=float)
    active = df["active_teams"].to_numpy(dtype=float)

    n_weeks = len(df)
    latest_avg = float(avg[-1])
    hist_mean = float(np.mean(avg))
    latest_spread = float(top_r[-1] - bot_r[-1])
    mean_spread = float(np.mean(top_r - bot_r))

    x = np.arange(n_weeks, dtype=float)
    slope_per_week = 0.0
    if n_weeks >= 3:
        coef = np.polyfit(x, avg, 1)
        slope_per_week = float(coef[0])
    slope_per_year = slope_per_week * 52.0

    w2w = np.diff(avg)
    vol = float(np.std(w2w)) if len(w2w) else 0.0

    active_latest = int(active[-1]) if len(active) else 0
    active_mean = float(np.mean(active)) if len(active) else 0.0

    tops = get_country_top_n_timeseries(slug, n=top_n)
    team_names = [str(t["team_name"]) for t in tops.get("teams") or []]

    regimes, change_backend = _detect_change_points_with_backend(dates, avg)
    if len(regimes) > 1:
        deltas = []
        for i in range(1, len(regimes)):
            deltas.append(abs(regimes[i]["segment_mean"] - regimes[i - 1]["segment_mean"]))
        regime_delta_typical = float(np.mean(deltas)) if deltas else 0.0
    else:
        regime_delta_typical = 0.0

    continental_stats, n_eu_weeks = _continental_top_presence(slug, cutoffs=continental_cutoffs)
    country_disp = _country_display_name(slug)
    continental_paragraph = _continental_paragraph_for_template(
        country_disp,
        continental_cutoffs,
        continental_stats,
        n_eu_weeks,
        _human_week(dates[0]),
        ladder_weeks_dropped=ladder_weeks_dropped,
    )

    ctx: dict[str, Any] = {
        "country_display": country_disp,
        "latest_month": _human_week(dates[-1]),
        "first_month": _human_week(dates[0]),
        "ladder_weeks_dropped": ladder_weeks_dropped,
        "n_weeks": n_weeks,
        "n_weeks_fmt": f"{n_weeks:,}",
        "active_latest": active_latest,
        "active_mean": round(active_mean, 1),
        "latest_avg": round(latest_avg, 1),
        "hist_mean": round(hist_mean, 1),
        "above_hist": latest_avg - hist_mean,
        "above_hist_fmt": f"{latest_avg - hist_mean:+.1f}",
        "latest_spread": round(latest_spread, 1),
        "mean_spread": round(mean_spread, 1),
        "volatility": round(vol, 2),
        "slope_per_year": round(slope_per_year, 2),
        "trend_word": "rose"
        if slope_per_year > 0.35
        else ("fell" if slope_per_year < -0.35 else "held roughly steady"),
        "top_clubs": team_names,
        "regimes": regimes,
        "has_regimes": len(regimes) > 1,
        "n_regimes": len(regimes),
        "regime_shift_typical": round(regime_delta_typical, 1),
        "continental_paragraph": continental_paragraph,
    }

    env = Environment(loader=BaseLoader(), autoescape=False)
    template = env.from_string(_NARRATIVE_TEMPLATE)
    body = template.render(**ctx)
    paragraphs = [p.strip() for p in body.split("\n\n") if p.strip()]

    generated = pendulum.now("UTC").to_iso8601_string()

    return {
        "country": slug,
        "country_display": ctx["country_display"],
        "generated_at": generated,
        "paragraphs": paragraphs,
        "facts": {
            "n_weeks": n_weeks,
            "n_european_weeks": n_eu_weeks,
            "narrative_warmup_weeks_dropped_from_ladder": ladder_weeks_dropped,
            "continental_top_buckets": continental_stats,
            "continental_cutoffs": list(continental_cutoffs),
            "slope_rating_points_per_year": ctx["slope_per_year"],
            "national_avg_volatility_week_to_week": ctx["volatility"],
            "latest_elite_gap_top_minus_bottom": ctx["latest_spread"],
            "change_point_segments": ctx["n_regimes"],
            "change_point_backend": change_backend,
        },
    }


_NARRATIVE_TEMPLATE = """{% macro oxford(names) -%}
{%- if names|length == 0 -%}
(no clubs listed)
{%- elif names|length == 1 -%}
{{ names[0] }}
{%- elif names|length == 2 -%}
{{ names[0] }} and {{ names[1] }}
{%- else -%}
{{ names[:-1]|join(', ') }}, and {{ names[-1] }}
{%- endif -%}
{%- endmacro %}

The model covers **{{ country_display }}** over **{{ n_weeks_fmt }}** weekly rating snapshots, from **{{ first_month }}** through **{{ latest_month }}**{% if ladder_weeks_dropped %} (**after dropping the first {{ ladder_weeks_dropped }} chronological weeks** where default masses make ladder rankings unreliable){% endif %}. In the latest week there are **{{ active_latest }}** rated clubs (historically about **{{ active_mean }}** active per week on average), with a national mean rating of **{{ latest_avg }}** versus **{{ hist_mean }}** for the full span — about **{{ above_hist_fmt }}** points versus that long-run average.

Across the whole series the gap between the strongest and weakest club each week averages **{{ mean_spread }}** rating points; most recently that elite gap is **{{ latest_spread }}**. Typical week-to-week movement in the national mean is **{{ volatility }}** points (standard deviation of weekly changes), so aggregate swings are {% if volatility > 12 %}quite lively{% elif volatility > 7 %}moderate{% else %}fairly smooth{% endif %} at country scale.

On a slow-moving trend line through the national average, ratings have **{{ trend_word }}** at roughly **{{ slope_per_year }}** points per year (a coarse linear fit through time; European fixtures and model tuning both influence this).

{% if continental_paragraph %}{{ continental_paragraph }}{% endif %}

{% if has_regimes %}Change-point style splits on the national average divide the timeline into **{{ n_regimes }}** broad segments since {{ first_month }} (ruptures PELT when available, otherwise greedy SSE splits — both are coarse summaries). Typical jumps between adjacent segment means are about **{{ regime_shift_typical }}** rating points.{% endif %}

Today’s strongest clubs in this country (latest rating week) include {{ oxford(top_clubs) }} — open individual club pages for match-by-match detail."""
