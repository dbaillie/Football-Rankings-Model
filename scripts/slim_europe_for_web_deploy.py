#!/usr/bin/env python3
"""
Write smaller on-disk copies of `output/europe` CSVs for low-RAM hosting (e.g. Render 512 MiB).

- Rows: keep only the last N calendar years (rating-week year from `week`, match rows from `match_date`).
- Columns: optional `web-minimal` sets that drop unused fields (smaller files + less pandas RAM).

After running, point the API at the output folder with FOOTBALL_OUTPUT_EUROPE_DIR, or replace files in
your deploy artifact. For **full weekly rating history** in Postgres / charts, import from unsliced
``output/europe`` — slimming truncates ``europe_weekly_ratings.csv``.

IMPORTANT - visibility gate: match slicing may remove whole calendar years. If you use fewer years of
matches than `FOOTBALL_CLUB_VISIBILITY_YEARS` expects (default 2024,2025,2026), **no club may pass**
the visibility filter. Set env on the host to match data you kept, e.g.:

    FOOTBALL_CLUB_VISIBILITY_YEARS=2025,2026

Examples:

    python scripts/slim_europe_for_web_deploy.py --last-calendar-years 2 --dest output/europe_slim

    python scripts/slim_europe_for_web_deploy.py --last-calendar-years 2 --weekly-columns web-minimal \\
        --match-columns web-minimal --dest output/europe_slim

"""

from __future__ import annotations

import argparse
import shutil
from datetime import date
from pathlib import Path

import pandas as pd


def _weekly_ratings_src(src_dir: Path) -> Path:
    c = src_dir / "europe_weekly_ratings.csv"
    if c.is_file():
        return c
    t = src_dir / "europe_weekly_ratings.txt"
    if t.is_file():
        return t
    return c


# --- Column sets (must stay aligned with webapp/backend/data_service.py + narratives) ------------

WEEKLY_COLUMNS_WEB_MIN = [
    "week",
    "pid",
    "rating",
    "rd",
    "sigma",
    "last_week_seen",
    "team_name",
    "country_name",
    "rating_change",
    "rating_change_pct",
    "baseline_rating",
    "structural_rd",
    "total_rd",
    "trust_factor",
    "adjusted_rating",
    "power_score",
    "effective_connectivity",
    "simple_cross_weight_sum",
    "simple_heat_generated",
    "simple_n_distinct_cross_opponents",
    "simple_comparability",
    "simple_anchor_rating",
    "simple_adjusted_rating",
]

MATCH_COLUMNS_WEB_MIN = [
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
    "home_pre_rating",
    "home_post_rating",
    "home_rating_change",
    "away_pre_rating",
    "away_post_rating",
    "away_rating_change",
]

# europe_ratings.csv — latest snapshot–style file; same strength signals as weekly for map summaries.
RATINGS_COLUMNS_WEB_MIN = [
    "week",
    "pid",
    "rating",
    "rd",
    "sigma",
    "team_name",
    "country_name",
    "baseline_rating",
    "structural_rd",
    "total_rd",
    "trust_factor",
    "adjusted_rating",
    "power_score",
    "effective_connectivity",
    "simple_cross_weight_sum",
    "simple_heat_generated",
    "simple_n_distinct_cross_opponents",
    "simple_comparability",
    "simple_anchor_rating",
    "simple_adjusted_rating",
]

ANALYTICAL_START_WEEK = 200531


def _min_calendar_year(last_calendar_years: int) -> int:
    return date.today().year - last_calendar_years + 1


def _pick_columns(available: list[str], want: list[str]) -> list[str]:
    s = set(available)
    out = [c for c in want if c in s]
    missing = [c for c in want if c not in s]
    if missing:
        print(f"  [warn] missing columns (skipped): {missing[:12]}{'…' if len(missing) > 12 else ''}")
    return out


def _slim_weekly(src: Path, dest: Path, min_cal_year: int, mode: str) -> None:
    dtype_kw = {"dtype": {"country_name": "string", "team_name": "string"}, "low_memory": False}
    pieces: list[pd.DataFrame] = []
    header_cols: list[str] | None = None

    for chunk in pd.read_csv(src, chunksize=150_000, **dtype_kw):
        if header_cols is None:
            header_cols = list(chunk.columns)
        chunk["week"] = chunk["week"].astype(int)
        wy = chunk["week"] // 100
        chunk = chunk.loc[
            (chunk["week"] >= ANALYTICAL_START_WEEK) & (wy.astype(int) >= min_cal_year)
        ]
        if chunk.empty:
            continue
        if mode == "web-minimal":
            cols = _pick_columns(list(chunk.columns), WEEKLY_COLUMNS_WEB_MIN)
            chunk = chunk[cols]
        pieces.append(chunk)

    if not pieces:
        raise SystemExit("Weekly ratings: no rows after filter — check --last-calendar-years.")
    out = pd.concat(pieces, ignore_index=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dest, index=False)
    print(f"  europe_weekly_ratings.csv -> {len(out):,} rows, {dest.stat().st_size / 1e6:.1f} MB")


def _slim_matches(src: Path, dest: Path, min_cal_year: int, mode: str) -> None:
    pieces: list[pd.DataFrame] = []
    for chunk in pd.read_csv(src, chunksize=120_000):
        chunk["match_date"] = pd.to_datetime(chunk["match_date"], errors="coerce")
        wk = chunk["week"].astype(int)
        md = chunk["match_date"]
        mask = (
            (wk >= ANALYTICAL_START_WEEK)
            & (md >= pd.Timestamp("2005-07-01"))
            & md.notna()
            & (md.dt.year >= min_cal_year)
        )
        chunk = chunk.loc[mask].copy()
        if chunk.empty:
            continue
        if mode == "web-minimal":
            cols = _pick_columns(list(chunk.columns), MATCH_COLUMNS_WEB_MIN)
            chunk = chunk[cols]
        pieces.append(chunk)

    if not pieces:
        raise SystemExit("Match results: no rows after filter.")
    out = pd.concat(pieces, ignore_index=True)
    out.to_csv(dest, index=False)
    print(f"  europe_match_results.csv -> {len(out):,} rows, {dest.stat().st_size / 1e6:.1f} MB")


def _slim_ratings(src: Path, dest: Path, min_cal_year: int, mode: str) -> None:
    """europe_ratings.csv is small — full read OK."""
    df = pd.read_csv(
        src,
        dtype={"country_name": "string", "team_name": "string"},
        low_memory=False,
    )
    if "week" in df.columns:
        df["week"] = pd.to_numeric(df["week"], errors="coerce")
        df = df.dropna(subset=["week"])
        df["week"] = df["week"].astype(int)
        wy = df["week"] // 100
        df = df.loc[wy.astype(int) >= min_cal_year].copy()
    if mode == "web-minimal":
        cols = _pick_columns(list(df.columns), RATINGS_COLUMNS_WEB_MIN)
        df = df[cols]
    df.to_csv(dest, index=False)
    print(f"  europe_ratings.csv -> {len(df):,} rows, {dest.stat().st_size / 1e6:.1f} MB")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", type=Path, default=Path("output/europe"), help="Input directory")
    p.add_argument("--dest", type=Path, required=True, help="Output directory (created if missing)")
    p.add_argument(
        "--last-calendar-years",
        type=int,
        default=2,
        help="Keep rating weeks and matches from this many calendar years (incl. current year).",
    )
    p.add_argument(
        "--weekly-columns",
        choices=("full", "web-minimal"),
        default="web-minimal",
        help="full = all columns from source after row filter; web-minimal = drop unused columns.",
    )
    p.add_argument(
        "--match-columns",
        choices=("full", "web-minimal"),
        default="web-minimal",
    )
    p.add_argument(
        "--ratings-columns",
        choices=("full", "web-minimal"),
        default="web-minimal",
    )
    p.add_argument(
        "--copy-calibration-json",
        action="store_true",
        help="Also copy calibration_summary.json if present.",
    )
    args = p.parse_args()

    src: Path = args.source
    dest: Path = args.dest
    n = args.last_calendar_years
    if n <= 0:
        raise SystemExit("--last-calendar-years must be positive.")
    min_cal_year = _min_calendar_year(n)

    print(f"Slim europe CSVs: last {n} calendar years -> week/match year >= {min_cal_year}")
    print(f"  source: {src.resolve()}")
    print(f"  dest:   {dest.resolve()}")

    dest.mkdir(parents=True, exist_ok=True)

    w_mode = args.weekly_columns
    m_mode = args.match_columns
    r_mode = args.ratings_columns

    _slim_weekly(_weekly_ratings_src(src), dest / "europe_weekly_ratings.csv", min_cal_year, w_mode)
    _slim_matches(src / "europe_match_results.csv", dest / "europe_match_results.csv", min_cal_year, m_mode)
    _slim_ratings(src / "europe_ratings.csv", dest / "europe_ratings.csv", min_cal_year, r_mode)

    t_src = src / "europe_teams.csv"
    if t_src.is_file():
        shutil.copy2(t_src, dest / "europe_teams.csv")
        print(f"  europe_teams.csv -> copied ({t_src.stat().st_size / 1e3:.1f} KB)")
    else:
        print("  [warn] europe_teams.csv missing — skipped.")

    if args.copy_calibration_json:
        c_src = src / "calibration_summary.json"
        if c_src.is_file():
            shutil.copy2(c_src, dest / "calibration_summary.json")
            print("  calibration_summary.json -> copied")
        else:
            print("  [warn] calibration_summary.json not found — skipped.")

    print("\nDone. On Render set FOOTBALL_OUTPUT_EUROPE_DIR to this folder's absolute path,")
    print("or replace output/europe in your repo/build with these files.")
    print("\nIf you sliced matches, align visibility years with data you kept, e.g.:")
    print("  FOOTBALL_CLUB_VISIBILITY_YEARS=2025,2026")


if __name__ == "__main__":
    main()
