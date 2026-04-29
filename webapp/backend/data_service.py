from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd


OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output" / "europe"
ANALYTICAL_START_WEEK = 200531
ANALYTICAL_START_DATE = pd.Timestamp("2005-07-01")


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


def week_to_date(week_value: int) -> pd.Timestamp | None:
    """Convert YYYYWW week integer into an ISO date (week start)."""
    if pd.isna(week_value):
        return None
    week_string = str(int(week_value)).zfill(6)
    return pd.to_datetime(f"{week_string}1", format="%G%V%u", errors="coerce")


@lru_cache(maxsize=1)
def load_teams() -> pd.DataFrame:
    teams = pd.read_csv(OUTPUT_DIR / "europe_teams.csv")
    teams = teams.rename(columns={"team_id": "pid"})
    teams["pid"] = teams["pid"].astype(int)
    teams["country_name"] = teams["country_name"].fillna("unknown")
    return teams


@lru_cache(maxsize=1)
def load_weekly_ratings() -> pd.DataFrame:
    weekly = pd.read_csv(
        OUTPUT_DIR / "europe_weekly_ratings.csv",
        dtype={"country_name": "string", "team_name": "string"},
        low_memory=False,
    )
    weekly["week"] = weekly["week"].astype(int)
    weekly["pid"] = weekly["pid"].astype(int)
    weekly = weekly[weekly["week"] >= ANALYTICAL_START_WEEK].copy()
    weekly["week_date"] = weekly["week"].apply(week_to_date)
    weekly["week_date"] = weekly["week_date"].dt.strftime("%Y-%m-%d")
    weekly["country_name"] = weekly["country_name"].fillna("unknown").astype(str)
    weekly["team_name"] = weekly["team_name"].fillna("Unknown Team").astype(str)
    return weekly


@lru_cache(maxsize=1)
def load_final_ratings() -> pd.DataFrame:
    ratings = pd.read_csv(
        OUTPUT_DIR / "europe_ratings.csv",
        dtype={"country_name": "string", "team_name": "string"},
        low_memory=False,
    )
    ratings["country_name"] = ratings["country_name"].fillna("unknown").astype(str)
    ratings["team_name"] = ratings["team_name"].fillna("Unknown Team").astype(str)
    return ratings


@lru_cache(maxsize=1)
def load_match_results() -> pd.DataFrame:
    matches = pd.read_csv(OUTPUT_DIR / "europe_match_results.csv")
    matches["match_date"] = pd.to_datetime(matches["match_date"], errors="coerce")
    matches = matches[
        (matches["week"].astype(int) >= ANALYTICAL_START_WEEK)
        & (matches["match_date"] >= ANALYTICAL_START_DATE)
    ].copy()

    # Approximate expected result from pre-match rating differential.
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


def list_countries() -> list[str]:
    countries = sorted(load_teams()["country_name"].dropna().str.lower().unique().tolist())
    return countries


def list_teams(country: str | None = None) -> list[dict[str, Any]]:
    teams = load_teams()
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
    return team_data[columns].to_dict(orient="records")


def get_country_timeseries(country: str) -> list[dict[str, Any]]:
    weekly = load_weekly_ratings()
    country_data = weekly[weekly["country_name"].str.lower() == country.lower()].copy()
    if country_data.empty:
        return []

    aggregated = (
        country_data.groupby(["week", "week_date"], as_index=False)
        .agg(
            average_rating=("rating", "mean"),
            top_rating=("rating", "max"),
            bottom_rating=("rating", "min"),
            active_teams=("pid", "nunique"),
        )
        .sort_values("week")
    )
    return aggregated.to_dict(orient="records")


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


def get_latest_snapshot(top_n: int = 25) -> list[dict[str, Any]]:
    weekly = load_weekly_ratings()
    latest_week = int(weekly["week"].max())
    latest = weekly[weekly["week"] == latest_week].sort_values("rating", ascending=False).head(top_n)
    return latest[["pid", "team_name", "country_name", "rating", "rd", "week"]].to_dict(orient="records")


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

    country_stats = (
        latest.groupby("country_name", as_index=False)
        .agg(
            average_rating=("rating", "mean"),
            top_rating=("rating", "max"),
            active_teams=("pid", "nunique"),
        )
        .sort_values("average_rating", ascending=False)
    )

    top_team_rows = (
        latest.sort_values(["country_name", "rating"], ascending=[True, False])
        .groupby("country_name", as_index=False)
        .first()[["country_name", "team_name", "rating"]]
        .rename(columns={"team_name": "top_team_name", "rating": "top_team_rating"})
    )

    merged = country_stats.merge(top_team_rows, on="country_name", how="left")
    merged["country_name"] = merged["country_name"].astype(str)
    merged["week"] = latest_week

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
    """Clear in-memory CSV caches so next request reloads files from disk."""
    _csv_cache.clear()
