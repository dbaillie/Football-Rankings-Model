from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _europe_output_dir() -> Path:
    """
    Default: repo-relative output/europe.
    On Render (or any host) set FOOTBALL_OUTPUT_EUROPE_DIR to an absolute path where the CSVs live.
    """
    override = os.environ.get("FOOTBALL_OUTPUT_EUROPE_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return (Path(__file__).resolve().parents[2] / "output" / "europe").resolve()


OUTPUT_DIR = _europe_output_dir()
ANALYTICAL_START_WEEK = 200531
ANALYTICAL_START_DATE = pd.Timestamp("2005-07-01")


def _use_database() -> bool:
    """When True, heavy tables are read from Postgres (DATABASE_URL) instead of CSV files."""
    try:
        from .database import use_database

        return use_database()
    except Exception:
        return False


def _load_recent_calendar_years_limit() -> int | None:
    """
    If set to a positive integer N, only load match rows and weekly rating rows from the last N
    calendar years (including the current year). Example: N=10 and year 2026 → weeks with YYYY ≥ 2017.
    Unset or 0 = no trimming (full CSV in memory — can exceed ~512 MiB on small hosts).
    Env: FOOTBALL_LOAD_LAST_CALENDAR_YEARS
    """
    raw = os.environ.get("FOOTBALL_LOAD_LAST_CALENDAR_YEARS", "").strip()
    if not raw.isdigit():
        return None
    n = int(raw)
    return n if n > 0 else None


def _min_calendar_year_for_recent_load() -> int | None:
    """Smallest calendar year to keep when trimming (inclusive)."""
    n = _load_recent_calendar_years_limit()
    if n is None:
        return None
    from datetime import date

    return date.today().year - n + 1

# Clubs appear in map aggregates, top snapshot, country charts, and /api/teams only if they have strictly more than
# this many dated matches in each listed calendar year (home or away counts once per fixture).
CLUB_VISIBILITY_MIN_MATCHES_PER_YEAR = max(
    0, int(os.environ.get("FOOTBALL_CLUB_VISIBILITY_MIN_MATCHES_PER_YEAR", "5"))
)
_CLUB_VISIBILITY_YEARS_RAW = os.environ.get("FOOTBALL_CLUB_VISIBILITY_YEARS", "2024,2025,2026")


def club_visibility_calendar_years() -> tuple[int, ...]:
    years: list[int] = []
    for part in _CLUB_VISIBILITY_YEARS_RAW.split(","):
        p = part.strip()
        if p.isdigit():
            yi = int(p)
            if 1900 <= yi <= 2100:
                years.append(yi)
    return tuple(sorted(set(years))) if years else (2024, 2025, 2026)

# Narratives: first N *chronological* rating weeks are skipped for ladder / rank logic (ties ~1500 dominate sorts).
NARRATIVE_LADDER_DROP_FIRST_N_WEEKS = max(
    0, int(os.environ.get("FOOTBALL_NARRATIVE_LADDER_DROP_FIRST_N_WEEKS", "52"))
)


def ladder_sort_column(weekly: pd.DataFrame) -> str:
    """Rank column: simple adjusted strength > GCAM adjusted > raw Glicko."""
    if weekly.empty:
        return "rating"
    if "simple_adjusted_rating" in weekly.columns:
        return "simple_adjusted_rating"
    if "adjusted_rating" in weekly.columns and weekly["adjusted_rating"].notna().any():
        return "adjusted_rating"
    return "rating"


def strength_chart_column(weekly: pd.DataFrame) -> str:
    """Time-series strength: simple adjusted when present, else GCAM adjusted, else raw Glicko."""
    if weekly.empty:
        return "rating"
    if "simple_adjusted_rating" in weekly.columns:
        return "simple_adjusted_rating"
    if "adjusted_rating" in weekly.columns and weekly["adjusted_rating"].notna().any():
        return "adjusted_rating"
    return "rating"


def narrative_ladder_week_allowlist(weekly: pd.DataFrame) -> frozenset[int] | None:
    """
    Week ids to KEEP for ladder statistics. Returns None if no warmup trim is applied
    (setting is 0 or not enough distinct weeks to drop).
    """
    if weekly.empty or "week" not in weekly.columns:
        return None
    if NARRATIVE_LADDER_DROP_FIRST_N_WEEKS <= 0:
        return None
    u = sorted(pd.unique(weekly["week"].astype(int)))
    drop = NARRATIVE_LADDER_DROP_FIRST_N_WEEKS
    if len(u) <= drop:
        return None
    return frozenset(u[drop:])


def filter_weekly_for_narrative_ladder(weekly: pd.DataFrame) -> pd.DataFrame:
    allow = narrative_ladder_week_allowlist(weekly)
    if allow is None:
        return weekly.copy()
    out = weekly.loc[weekly["week"].isin(allow)].copy()
    return out if not out.empty else weekly.copy()


class _MtimeCsvCache:
    """Reload CSV-derived frames when underlying file(s) change (avoids stale data after pipeline reruns)."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str, paths: list[Path], builder) -> Any:
        mt = max((p.stat().st_mtime_ns if p.exists() else 0 for p in paths), default=0)
        cur = self._store.get(key)
        if cur is None or cur[0] != mt:
            self._store[key] = (mt, builder())
        return self._store[key][1]

    def clear(self) -> None:
        self._store.clear()


_csv_cache = _MtimeCsvCache()


def _strip_international(df: pd.DataFrame) -> pd.DataFrame:
    """Drop synthetic UEFA aggregate rows (country International / * International names)."""
    if df.empty or "country_name" not in df.columns:
        return df
    bad = df["country_name"].str.lower().eq("international")
    if "team_name" in df.columns:
        bad = bad | df["team_name"].str.endswith(" International", na=False)
    return df[~bad]


def week_to_date(week_value: int) -> pd.Timestamp | None:
    """Convert YYYYWW week integer into an ISO date (week start)."""
    if pd.isna(week_value):
        return None
    week_string = str(int(week_value)).zfill(6)
    return pd.to_datetime(f"{week_string}1", format="%G%V%u", errors="coerce")


@lru_cache(maxsize=1)
def load_teams() -> pd.DataFrame:
    if _use_database():
        from sqlalchemy import text

        from .database import get_engine

        teams = pd.read_sql(text("SELECT * FROM fr_teams"), get_engine())
        teams = teams.rename(columns={"team_id": "pid"})
        teams["pid"] = teams["pid"].astype(int)
        teams["country_name"] = teams["country_name"].fillna("unknown")
        return teams

    teams = pd.read_csv(OUTPUT_DIR / "europe_teams.csv")
    teams = teams.rename(columns={"team_id": "pid"})
    teams["pid"] = teams["pid"].astype(int)
    teams["country_name"] = teams["country_name"].fillna("unknown")
    return teams


def _finalize_weekly_ratings_frame(weekly: pd.DataFrame) -> pd.DataFrame:
    """Shared cleanup after load (full or chunked)."""
    if weekly.empty:
        return weekly
    weekly["week"] = weekly["week"].astype(int)
    weekly["pid"] = weekly["pid"].astype(int)
    weekly = weekly[weekly["week"] >= ANALYTICAL_START_WEEK].copy()
    weekly["week_date"] = weekly["week"].apply(week_to_date)
    weekly["week_date"] = weekly["week_date"].dt.strftime("%Y-%m-%d")
    weekly["country_name"] = weekly["country_name"].fillna("unknown").astype(str)
    weekly["team_name"] = weekly["team_name"].fillna("Unknown Team").astype(str)
    return weekly


@lru_cache(maxsize=1)
def load_weekly_ratings() -> pd.DataFrame:
    if _use_database():
        from sqlalchemy import text

        from .database import get_engine

        eng = get_engine()
        min_cal_year = _min_calendar_year_for_recent_load()
        if min_cal_year is None:
            q = text(
                """
                SELECT * FROM fr_weekly_ratings
                WHERE week >= :aw
                """
            )
            weekly = pd.read_sql(q, eng, params={"aw": ANALYTICAL_START_WEEK})
        else:
            q = text(
                """
                SELECT * FROM fr_weekly_ratings
                WHERE week >= :aw AND (week / 100) >= :min_y
                """
            )
            weekly = pd.read_sql(
                q, eng, params={"aw": ANALYTICAL_START_WEEK, "min_y": min_cal_year}
            )
        return _finalize_weekly_ratings_frame(weekly)

    path = OUTPUT_DIR / "europe_weekly_ratings.csv"
    dtype_kw = {"dtype": {"country_name": "string", "team_name": "string"}, "low_memory": False}
    min_cal_year = _min_calendar_year_for_recent_load()

    if min_cal_year is None:
        weekly = pd.read_csv(path, **dtype_kw)
        return _finalize_weekly_ratings_frame(weekly)

    pieces: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, chunksize=150_000, **dtype_kw):
        chunk["week"] = chunk["week"].astype(int)
        chunk["pid"] = chunk["pid"].astype(int)
        wy = chunk["week"] // 100
        chunk = chunk.loc[
            (chunk["week"] >= ANALYTICAL_START_WEEK) & (wy.astype(int) >= min_cal_year)
        ]
        if not chunk.empty:
            pieces.append(chunk)

    weekly = pd.concat(pieces, ignore_index=True) if pieces else pd.read_csv(path, nrows=0, **dtype_kw)
    return _finalize_weekly_ratings_frame(weekly)


@lru_cache(maxsize=1)
def load_final_ratings() -> pd.DataFrame:
    if _use_database():
        from sqlalchemy import text

        from .database import get_engine

        ratings = pd.read_sql(text("SELECT * FROM fr_europe_ratings"), get_engine())
        ratings["country_name"] = ratings["country_name"].fillna("unknown").astype(str)
        ratings["team_name"] = ratings["team_name"].fillna("Unknown Team").astype(str)
        return ratings

    ratings = pd.read_csv(
        OUTPUT_DIR / "europe_ratings.csv",
        dtype={"country_name": "string", "team_name": "string"},
        low_memory=False,
    )
    ratings["country_name"] = ratings["country_name"].fillna("unknown").astype(str)
    ratings["team_name"] = ratings["team_name"].fillna("Unknown Team").astype(str)
    return ratings


def _load_match_results_raw_csv() -> pd.DataFrame:
    path = OUTPUT_DIR / "europe_match_results.csv"
    min_cal_year = _min_calendar_year_for_recent_load()

    if min_cal_year is None:
        matches = pd.read_csv(path)
    else:
        pieces: list[pd.DataFrame] = []
        for chunk in pd.read_csv(path, chunksize=120_000):
            chunk["match_date"] = pd.to_datetime(chunk["match_date"], errors="coerce")
            wk = chunk["week"].astype(int)
            md = chunk["match_date"]
            mask = (
                (wk >= ANALYTICAL_START_WEEK)
                & (md >= ANALYTICAL_START_DATE)
                & md.notna()
                & (md.dt.year >= min_cal_year)
            )
            chunk = chunk.loc[mask].copy()
            if not chunk.empty:
                pieces.append(chunk)
        matches = pd.concat(pieces, ignore_index=True) if pieces else pd.read_csv(path, nrows=0)

    if min_cal_year is None:
        matches["match_date"] = pd.to_datetime(matches["match_date"], errors="coerce")
        matches = matches[
            (matches["week"].astype(int) >= ANALYTICAL_START_WEEK)
            & (matches["match_date"] >= ANALYTICAL_START_DATE)
        ].copy()
    return matches


def _load_match_results_raw_db() -> pd.DataFrame:
    from sqlalchemy import text

    from .database import get_engine

    eng = get_engine()
    min_cal_year = _min_calendar_year_for_recent_load()
    md0 = ANALYTICAL_START_DATE.strftime("%Y-%m-%d")
    if min_cal_year is None:
        q = text(
            """
            SELECT * FROM fr_match_results
            WHERE week >= :aw AND match_date >= :md
            """
        )
        matches = pd.read_sql(q, eng, params={"aw": ANALYTICAL_START_WEEK, "md": md0})
    else:
        q = text(
            """
            SELECT * FROM fr_match_results
            WHERE week >= :aw AND match_date >= :md
              AND EXTRACT(YEAR FROM match_date::timestamp) >= :min_y
            """
        )
        matches = pd.read_sql(
            q,
            eng,
            params={"aw": ANALYTICAL_START_WEEK, "md": md0, "min_y": min_cal_year},
        )
    matches["match_date"] = pd.to_datetime(matches["match_date"], errors="coerce")
    return matches


def _add_match_derived_columns(matches: pd.DataFrame) -> pd.DataFrame:
    if matches.empty:
        return matches
    matches = matches.copy()
    matches["match_date"] = pd.to_datetime(matches["match_date"], errors="coerce")
    expected_home = 1.0 / (
        1.0 + 10 ** ((matches["away_pre_rating"] - matches["home_pre_rating"]) / 400.0)
    )
    actual_home = (matches["home_goals"] > matches["away_goals"]).astype(float)
    actual_home += 0.5 * (matches["home_goals"] == matches["away_goals"]).astype(float)

    matches["expected_home"] = expected_home
    matches["actual_home"] = actual_home
    matches["upset_magnitude"] = (actual_home - expected_home).abs()

    matches["absolute_rating_swing"] = (
        matches["home_rating_change"].abs() + matches["away_rating_change"].abs()
    )
    return matches


@lru_cache(maxsize=1)
def load_match_results() -> pd.DataFrame:
    if _use_database():
        matches = _load_match_results_raw_db()
    else:
        matches = _load_match_results_raw_csv()
    return _add_match_derived_columns(matches)


@lru_cache(maxsize=1)
def _visibility_eligible_db_cached() -> frozenset[int]:
    return _compute_visibility_eligible_team_ids()


def visibility_eligible_team_ids() -> frozenset[int]:
    """Team ids that pass the recent activity gate (see CLUB_VISIBILITY_*). Cached on match-results CSV mtime."""
    if _use_database():
        return _visibility_eligible_db_cached()
    match_csv = OUTPUT_DIR / "europe_match_results.csv"

    def build() -> frozenset[int]:
        return _compute_visibility_eligible_team_ids()

    return _csv_cache.get("visibility_eligible_team_ids", [match_csv], build)


def _compute_visibility_eligible_team_ids() -> frozenset[int]:
    years = club_visibility_calendar_years()
    floor = CLUB_VISIBILITY_MIN_MATCHES_PER_YEAR
    m = load_match_results()
    if m.empty or years == ():
        return frozenset()
    m = m.loc[m["match_date"].notna()].copy()
    if m.empty:
        return frozenset()
    m["cal_year"] = m["match_date"].dt.year.astype(int)

    home_counts = (
        m.groupby(["home_team_id", "cal_year"])
        .size()
        .reset_index(name="n")
        .rename(columns={"home_team_id": "pid"})
    )
    away_counts = (
        m.groupby(["away_team_id", "cal_year"])
        .size()
        .reset_index(name="n")
        .rename(columns={"away_team_id": "pid"})
    )
    counts = (
        pd.concat([home_counts, away_counts], ignore_index=True)
        .groupby(["pid", "cal_year"], as_index=False)["n"]
        .sum()
    )
    pivot = counts.pivot_table(index="pid", columns="cal_year", values="n", fill_value=0)

    eligible: list[int] = []
    for pid in pivot.index.astype(int):
        ok = True
        for y in years:
            n = float(pivot.loc[pid, y]) if y in pivot.columns else 0.0
            if not (n > floor):
                ok = False
                break
        if ok:
            eligible.append(int(pid))
    return frozenset(eligible)


def warm_csv_caches() -> None:
    """Eager-load heavy data frames (CSV files or Postgres tables) so the first heavy API call is faster."""
    load_teams()
    load_weekly_ratings()
    load_match_results()
    visibility_eligible_team_ids()


def list_countries() -> list[str]:
    teams = _strip_international(load_teams())
    countries = sorted(teams["country_name"].dropna().str.lower().unique().tolist())
    return countries


def list_teams(country: str | None = None) -> list[dict[str, Any]]:
    teams = _strip_international(load_teams())
    eligible = visibility_eligible_team_ids()
    teams = teams[teams["pid"].isin(eligible)]
    if country:
        teams = teams[teams["country_name"].str.lower() == country.lower()]
    teams = teams.sort_values(["country_name", "team_name"])
    return teams[["pid", "team_name", "country_name"]].to_dict(orient="records")


def get_team_timeseries(team_id: int) -> list[dict[str, Any]]:
    weekly = load_weekly_ratings()
    team_data = weekly[weekly["pid"] == team_id].sort_values("week")
    if team_data.empty:
        return []
    columns = ["week", "week_date", "rating", "rd", "sigma", "rating_change", "rating_change_pct"]
    for extra in (
        "simple_adjusted_rating",
        "simple_anchor_rating",
        "simple_comparability",
        "simple_heat_generated",
        "simple_cross_weight_sum",
        "simple_n_distinct_cross_opponents",
        "adjusted_rating",
        "total_rd",
        "structural_rd",
        "trust_factor",
        "effective_connectivity",
        "power_score",
        "baseline_rating",
    ):
        if extra in team_data.columns:
            columns.append(extra)
    return team_data[columns].to_dict(orient="records")


def get_country_timeseries(country: str) -> list[dict[str, Any]]:
    weekly = load_weekly_ratings()
    country_data = weekly[weekly["country_name"].str.lower() == country.lower()].copy()
    if country_data.empty:
        return []

    mean_col = strength_chart_column(country_data)
    agg_kw: dict[str, tuple[str, str]] = {
        "average_rating": (mean_col, "mean"),
        "top_rating": (mean_col, "max"),
        "bottom_rating": (mean_col, "min"),
        "active_teams": ("pid", "nunique"),
    }
    aggregated = country_data.groupby(["week", "week_date"], as_index=False).agg(**agg_kw).sort_values("week")
    return aggregated.to_dict(orient="records")


def get_country_top_n_timeseries(country: str, n: int = 5) -> dict[str, Any]:
    """Latest-week top N clubs in this country (by rating), each club's full weekly rating series."""
    weekly = load_weekly_ratings()
    cc = country.lower()
    country_data = weekly[weekly["country_name"].str.lower() == cc].copy()
    country_data = _strip_international(country_data)
    eligible = visibility_eligible_team_ids()
    country_data = country_data[country_data["pid"].isin(eligible)]
    if country_data.empty:
        return {"teams": []}

    rank_col = ladder_sort_column(country_data)
    latest_week = int(country_data["week"].max())
    latest_slice = country_data[country_data["week"] == latest_week].sort_values(
        rank_col, ascending=False
    )
    top_pids = latest_slice.head(n)["pid"].astype(int).tolist()

    teams_df = load_teams()
    chart_col = strength_chart_column(country_data)
    teams_out: list[dict[str, Any]] = []
    for pid in top_pids:
        sub = country_data[country_data["pid"] == pid].sort_values("week")
        name_row = teams_df.loc[teams_df["pid"].astype(int) == pid]
        team_name = (
            str(name_row.iloc[0]["team_name"])
            if not name_row.empty
            else str(sub.iloc[0]["team_name"])
        )
        series = (
            sub[["week_date", chart_col]]
            .rename(columns={chart_col: "rating"})
            .to_dict(orient="records")
        )
        teams_out.append({"pid": int(pid), "team_name": team_name, "series": series})

    return {"teams": teams_out}


def get_team_club_detail(team_id: int, weekly_limit: int = 15) -> dict[str, Any] | None:
    """Full club view: every match (team-centric rows) plus largest per-match rating gains and losses."""
    tid = int(team_id)
    teams_df = load_teams()
    meta = teams_df.loc[teams_df["pid"].astype(int) == tid]
    if meta.empty:
        return None

    team_name = str(meta.iloc[0]["team_name"])
    country_name = str(meta.iloc[0]["country_name"])

    matches_df = load_match_results()
    hid = matches_df["home_team_id"].astype(int)
    aid = matches_df["away_team_id"].astype(int)
    sub = matches_df[(hid == tid) | (aid == tid)].copy()
    sub = sub.sort_values("match_date", ascending=False)

    home_side = sub["home_team_id"].astype(int).to_numpy() == tid
    md = pd.to_datetime(sub["match_date"], errors="coerce")
    match_dates = np.where(md.notna(), md.dt.strftime("%Y-%m-%d"), None)

    opp_id = np.where(home_side, sub["away_team_id"].to_numpy(), sub["home_team_id"].to_numpy())
    opp_name = np.where(
        home_side,
        sub["away_team_name"].astype(str).to_numpy(),
        sub["home_team_name"].astype(str).to_numpy(),
    )
    team_goals = np.where(home_side, sub["home_goals"].to_numpy(), sub["away_goals"].to_numpy())
    opp_goals = np.where(home_side, sub["away_goals"].to_numpy(), sub["home_goals"].to_numpy())
    rating_delta = np.where(
        home_side,
        sub["home_rating_change"].to_numpy(dtype=float),
        sub["away_rating_change"].to_numpy(dtype=float),
    )
    pre_r = np.where(
        home_side,
        sub["home_pre_rating"].to_numpy(dtype=float),
        sub["away_pre_rating"].to_numpy(dtype=float),
    )
    post_r = np.where(
        home_side,
        sub["home_post_rating"].to_numpy(dtype=float),
        sub["away_post_rating"].to_numpy(dtype=float),
    )

    venue = np.where(home_side, "H", "A")
    stacked = np.column_stack(
        [
            match_dates,
            sub["week"].to_numpy(),
            sub["competition"].astype(str).to_numpy(),
            venue,
            opp_id,
            opp_name,
            team_goals,
            opp_goals,
            rating_delta,
            pre_r,
            post_r,
        ]
    )
    cols = [
        "match_date",
        "week",
        "competition",
        "venue",
        "opponent_id",
        "opponent_name",
        "team_goals",
        "opponent_goals",
        "rating_change",
        "pre_rating",
        "post_rating",
    ]
    match_rows = [
        {
            "match_date": row[0],
            "week": int(row[1]),
            "competition": str(row[2]),
            "venue": str(row[3]),
            "opponent_id": int(row[4]),
            "opponent_name": str(row[5]),
            "team_goals": int(row[6]),
            "opponent_goals": int(row[7]),
            "rating_change": float(row[8]),
            "pre_rating": float(row[9]),
            "post_rating": float(row[10]),
        }
        for row in stacked
    ]

    extremes_df = pd.DataFrame(
        {
            "match_date": match_dates,
            "opponent_name": opp_name.astype(str),
            "competition": sub["competition"].astype(str).to_numpy(),
            "rating": post_r.astype(float),
            "rating_change": rating_delta.astype(float),
        }
    )
    pos = extremes_df[extremes_df["rating_change"] > 0]
    neg = extremes_df[extremes_df["rating_change"] < 0]
    gain_cols = ["match_date", "opponent_name", "competition", "rating", "rating_change"]
    rating_gains = (
        pos.nlargest(weekly_limit, "rating_change")[gain_cols].to_dict(orient="records")
        if not pos.empty
        else []
    )
    rating_losses = (
        neg.nsmallest(weekly_limit, "rating_change")[gain_cols].to_dict(orient="records")
        if not neg.empty
        else []
    )

    return {
        "team_id": tid,
        "team_name": team_name,
        "country_name": country_name,
        "matches": match_rows,
        "rating_gains": rating_gains,
        "rating_losses": rating_losses,
    }


def get_team_biggest_matches(team_id: int, limit: int = 10) -> dict[str, list[dict[str, Any]]]:
    matches = load_match_results()
    team_matches = matches[
        (matches["home_team_id"] == team_id) | (matches["away_team_id"] == team_id)
    ].copy()

    if team_matches.empty:
        return {"upsets": [], "swings": []}

    upsets = (
        team_matches.sort_values("upset_magnitude", ascending=False)
        .head(limit)
        .copy()
    )
    swings = (
        team_matches.sort_values("absolute_rating_swing", ascending=False)
        .head(limit)
        .copy()
    )

    result_columns = [
        "match_date",
        "week",
        "competition",
        "home_team_id",
        "home_team_name",
        "away_team_id",
        "away_team_name",
        "home_goals",
        "away_goals",
        "result",
        "home_rating_change",
        "away_rating_change",
        "upset_magnitude",
        "absolute_rating_swing",
    ]
    return {
        "upsets": upsets[result_columns].to_dict(orient="records"),
        "swings": swings[result_columns].to_dict(orient="records"),
    }


def get_latest_snapshot(top_n: int = 25, offset: int = 0) -> list[dict[str, Any]]:
    weekly = load_weekly_ratings()
    latest_week = int(weekly["week"].max())
    latest = weekly[weekly["week"] == latest_week].copy()
    # Exclude synthetic international aggregate identities from the startup club ranking table.
    is_international_country = latest["country_name"].str.lower().eq("international")
    is_international_name = latest["team_name"].str.endswith(" International", na=False)
    latest = latest[~(is_international_country | is_international_name)]
    eligible = visibility_eligible_team_ids()
    latest = latest[latest["pid"].isin(eligible)]
    rank_col = ladder_sort_column(latest)
    latest = latest.sort_values(rank_col, ascending=False).iloc[offset : offset + top_n]
    base_cols = ["pid", "team_name", "country_name", "rating", "rd", "week"]
    extra = [
        c
        for c in (
            "simple_adjusted_rating",
            "simple_comparability",
            "simple_heat_generated",
            "simple_cross_weight_sum",
            "simple_n_distinct_cross_opponents",
            "adjusted_rating",
            "total_rd",
            "trust_factor",
            "effective_connectivity",
            "power_score",
        )
        if c in latest.columns
    ]
    return latest[base_cols + extra].to_dict(orient="records")


def get_country_summaries() -> list[dict[str, Any]]:
    latest_week = None
    latest: pd.DataFrame
    try:
        latest = load_final_ratings().copy()
        if "week" in latest.columns and not latest["week"].isna().all():
            latest_week = int(pd.to_numeric(latest["week"], errors="coerce").dropna().max())
    except Exception:
        weekly = load_weekly_ratings()
        latest_week = int(weekly["week"].max())
        latest = weekly[weekly["week"] == latest_week].copy()

    if latest.empty:
        return []

    eligible = visibility_eligible_team_ids()
    latest = latest[latest["pid"].isin(eligible)]
    is_international_country = latest["country_name"].str.lower().eq("international")
    is_international_name = latest["team_name"].str.endswith(" International", na=False)
    latest = latest[~(is_international_country | is_international_name)]
    if latest.empty:
        return []

    mean_col = ladder_sort_column(latest)
    country_stats = (
        latest.groupby("country_name", as_index=False)
        .agg(
            average_rating=(mean_col, "mean"),
            top_rating=(mean_col, "max"),
            active_teams=("pid", "nunique"),
        )
        .sort_values("average_rating", ascending=False)
    )

    top_team_rows = (
        latest.sort_values(["country_name", mean_col], ascending=[True, False])
        .groupby("country_name", as_index=False)
        .first()[["country_name", "team_name", mean_col]]
        .rename(columns={"team_name": "top_team_name", mean_col: "top_team_rating"})
    )

    merged = country_stats.merge(top_team_rows, on="country_name", how="left")
    merged["country_name"] = merged["country_name"].astype(str)
    merged["week"] = latest_week
    merged = merged[merged["country_name"].str.lower() != "international"]

    return merged[
        [
            "country_name",
            "average_rating",
            "top_rating",
            "active_teams",
            "top_team_name",
            "top_team_rating",
            "week",
        ]
    ].to_dict(orient="records")


def clear_data_caches() -> None:
    """Clear in-memory CSV / DB dataframe caches so next request reloads."""
    _csv_cache.clear()
    load_teams.cache_clear()
    load_weekly_ratings.cache_clear()
    load_final_ratings.cache_clear()
    load_match_results.cache_clear()
    _visibility_eligible_db_cached.cache_clear()
