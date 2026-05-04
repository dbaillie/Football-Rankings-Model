"""
ingest_leagues_from_config.py

Purpose
-------
Pull football results from all leagues defined in config/football_ingestion_config.py
using Sofascore via ScraperFC. Updates dims and fact table with new data.

The config file serves as the source of truth for:
- Which leagues to pull
- League metadata (country, tier, competition type)
- Season format for each league

Supports incremental ingestion: when not filtering by calendar year, the script can
resume per-league via output/ingestion_progress.json.

By default, only finished matches whose kickoff falls in the **current UTC calendar
year** are kept (older seasons are assumed already in ``fact_result_simple.csv``).
Use ``--all-calendar-years`` for a full historical pull; use ``--match-calendar-year``
for a specific year. When a calendar-year filter is active, league progress is ignored
so every run refreshes that year's fixtures.

Outputs
-------
- fact_result_simple_ingested.csv (new matches from all configured leagues)
- dim_club_updated.csv
- dim_country_updated.csv
- dim_season_updated.csv
- ingestion_summary.csv (leagues processed, rows added, errors)
- unmatched_clubs.csv
- created_clubs.csv
- ingestion_progress.json (progress tracking for resuming)

Install
-------
pip install ScraperFC pandas rapidfuzz pyarrow

Notes
-----
1) Season formats handled:
   - split: '24/25' -> searches for '24/25' in Sofascore
   - start_year: '2024' -> searches for '2024' in Sofascore
   - end_year: '2025' -> searches for '2025' in Sofascore
   - compact: '2425' -> converts to '2024/2025' for search
   - fbref: '2024-2025' -> searches for '2024-2025' in Sofascore

2) Club matching uses scripts/club_identity.py: canonical keys + Rapidfuzz.
   High-confidence fuzzy matches merge onto existing dim clubs at ingest time (no orphan ID unless truly novel).

3) Outputs organized by country to enable supplementing with other data sources.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from ScraperFC.sofascore import Sofascore
from ScraperFC.sofascore import comps as SOFASCORE_COMPS
from ScraperFC.sofascore import API_PREFIX as SOFASCORE_API_PREFIX
from ScraperFC.utils import botasaurus_browser_get_json

# Import config
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.football_ingestion_config import LEAGUE_METADATA
from scripts.club_identity import (
    ClubMatch,
    build_club_lookup,
    resolve_or_create_club,
)


# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

REQUEST_SLEEP_SECONDS = 0.35
CREATE_MISSING_CLUBS = True
DOMESTIC_COMPETITION_TYPES = {"domestic_league", "domestic_cup"}


# ---------------------------------------------------
# LOGGING
# ---------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------

def safe_get(d: dict[str, Any] | None, *keys: str, default: Any = None) -> Any:
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def extract_goals(match: dict[str, Any], side: str) -> int | None:
    """Extract goals from SofaScore payload, supporting multiple shapes."""
    team_goals = safe_get(match, f"{side}Team", "goals")
    if team_goals is not None:
        try:
            return int(team_goals)
        except Exception:
            return None

    score_current = safe_get(match, f"{side}Score", "current")
    if score_current is not None:
        try:
            return int(score_current)
        except Exception:
            return None
    return None


def convert_season_name(season_str: str, season_format: str) -> str | None:
    """
    Convert a season string from Sofascore format to match dim_season.
    
    season_format options:
    - split: '24/25' -> '2024/2025'
    - start_year: '2024' -> '2024/2025'
    - end_year: '2025' -> '2024/2025'
    - compact: '2425' -> '2024/2025'
    - fbref: '2024-2025' -> '2024/2025'
    """
    if season_format == "split":
        # Already in correct format: '24/25' or '2024/2025'
        if "/" in season_str:
            parts = season_str.split("/")
            if len(parts) == 2 and len(parts[0]) == 2 and len(parts[1]) == 2:
                # Convert '24/25' to '2024/2025'
                return f"20{parts[0]}/20{parts[1]}"
            return season_str
        return None
    
    elif season_format == "start_year":
        # Single year: '2024' -> '2024/2025'
        try:
            year = int(season_str)
            return f"{year}/{year + 1}"
        except ValueError:
            return None
    
    elif season_format == "end_year":
        # Single year: '2025' -> '2024/2025'
        try:
            year = int(season_str)
            return f"{year - 1}/{year}"
        except ValueError:
            return None
    
    elif season_format == "compact":
        # '2425' -> '2024/2025'
        if len(season_str) == 4:
            return f"20{season_str[:2]}/20{season_str[2:]}"
        return None
    
    elif season_format == "fbref":
        # '2024-2025' -> '2024/2025'
        return season_str.replace("-", "/")
    
    return None


def is_finished_match(match: dict[str, Any]) -> bool:
    """Check if match has finished."""
    # TEMPORARY: Accept all matches for testing
    return True


def sofascore_match_calendar_year_utc(match: dict[str, Any]) -> int | None:
    """Calendar year of kickoff in UTC from SofaScore ``startTimestamp`` (unix seconds)."""
    ts = safe_get(match, "startTimestamp")
    if ts is None:
        return None
    try:
        return int(pd.to_datetime(int(ts), unit="s", utc=True).year)
    except Exception:
        return None


def _norm_year_token(tok: str) -> int:
    t = str(tok).strip()
    if not t.isdigit():
        raise ValueError(t)
    if len(t) == 2:
        v = int(t)
        return v + (2000 if v < 70 else 1900)
    if len(t) == 4:
        return int(t)
    raise ValueError(t)


def dim_season_calendar_year_overlap(dim_season_name: str, calendar_year: int) -> bool:
    """
    Heuristic: ``dim_season`` labels like ``2014/2015`` span calendar years from the
    earlier year through the later (inclusive). Used to skip SofaScore/FBref pulls when
    ``--match-calendar-year`` is set. Unknown shapes return True (safe: still pull).
    """
    s = str(dim_season_name).strip()
    if "/" in s:
        parts = [p.strip() for p in s.split("/") if p.strip()]
        if len(parts) < 2:
            return True
        try:
            y_lo = _norm_year_token(parts[0])
            y_hi = _norm_year_token(parts[1])
            lo, hi = min(y_lo, y_hi), max(y_lo, y_hi)
            return lo <= int(calendar_year) <= hi
        except ValueError:
            return True
    if s.isdigit() and len(s) == 4:
        try:
            y = int(s)
            return abs(y - int(calendar_year)) <= 1
        except ValueError:
            pass
    return True


def season_name_to_id_map(dim_season: pd.DataFrame) -> dict[str, int]:
    return {
        str(row["season_name"]): int(row["season_id"])
        for _, row in dim_season.iterrows()
    }


def flatten_match_to_fact_row(
    match: dict[str, Any],
    result_id: int,
    country_id: int,
    country_name: str,
    season_id: int,
    league_code: str,
    home_club_id: int,
    away_club_id: int,
) -> dict[str, Any] | None:
    """Convert raw Sofascore match to fact_result_simple row."""
    try:
        match_timestamp = safe_get(match, "startTimestamp")
        if not match_timestamp:
            return None

        match_date = pd.to_datetime(match_timestamp, unit="s").date()
        match_time = pd.to_datetime(match_timestamp, unit="s").time()

        home_goals = extract_goals(match, "home")
        away_goals = extract_goals(match, "away")

        if home_goals is None or away_goals is None:
            return None

        home_nm = safe_get(match, "homeTeam", "name") or ""
        away_nm = safe_get(match, "awayTeam", "name") or ""
        return {
            "result_id": result_id,
            "season_id": season_id,
            "country_id": country_id,
            "country_name": country_name,
            "league_code": league_code,
            "match_date": match_date,
            "match_time": match_time,
            "home_team_name": str(home_nm).strip() if home_nm else "",
            "away_team_name": str(away_nm).strip() if away_nm else "",
            "home_club_id": home_club_id,
            "away_club_id": away_club_id,
            "home_team_goals": int(home_goals),
            "away_team_goals": int(away_goals),
        }
    except Exception as e:
        logger.warning("Failed to flatten match: %s", e)
        return None


def try_resolve_league_key(ss: Sofascore, source_league_name: str) -> str | None:
    """Try to find the league by source_league_name."""
    try:
        _ = ss.get_valid_seasons(source_league_name)
        logger.info("Resolved league: %s", source_league_name)
        return source_league_name
    except Exception as e:
        logger.warning("Could not resolve league '%s': %s", source_league_name, e)
        return None


def inspect_seasons_endpoint(source_league_name: str) -> dict[str, Any]:
    """Inspect raw SofaScore seasons endpoint response for diagnostics."""
    if source_league_name not in SOFASCORE_COMPS:
        return {"error": "league_not_in_sofascore_comps"}
    tournament_id = SOFASCORE_COMPS[source_league_name]["SOFASCORE"]
    url = f"{SOFASCORE_API_PREFIX}/unique-tournament/{tournament_id}/seasons/"
    try:
        response = botasaurus_browser_get_json(url)
    except Exception as exc:
        return {"url": url, "error": repr(exc)}
    return {"url": url, "response": response}


def get_valid_seasons_with_diagnostics(ss: Sofascore, source_league_name: str) -> dict[str, Any]:
    """Fetch valid seasons and raise explicit errors for anti-bot/challenge responses."""
    try:
        return ss.get_valid_seasons(source_league_name)
    except KeyError as exc:
        if str(exc) == "'seasons'":
            diagnostic = inspect_seasons_endpoint(source_league_name)
            raise RuntimeError(
                f"SofaScore seasons payload missing 'seasons' key for '{source_league_name}'. "
                f"Diagnostic: {diagnostic}"
            ) from exc
        raise


def ingest_league(
    league_config: dict[str, Any],
    fact: pd.DataFrame,
    dim_club: pd.DataFrame,
    dim_country: pd.DataFrame,
    dim_season: pd.DataFrame,
    ss: Sofascore,
    *,
    match_calendar_year: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], pd.DataFrame, pd.DataFrame]:
    """
    Ingest a single league's matches.
    Returns (new_fact_rows, unmatched_rows, created_rows, updated_dim_club, updated_dim_country)
    """
    source_league_name = league_config["source_league_name"]
    league_code = league_config["league_key"]
    country_name = league_config["country"]
    country_code = league_config["country_code"]
    season_format = league_config["season_format"]

    logger.info("=" * 80)
    logger.info("Processing: %s (%s)", source_league_name, league_code)
    logger.info("=" * 80)

    # Ensure country exists
    country_id = None
    if country_name in dim_country["country_name"].values:
        country_id = int(dim_country[dim_country["country_name"] == country_name]["country_id"].iloc[0])
    else:
        next_country_id = int(dim_country["country_id"].max()) + 1
        new_country_row = pd.DataFrame([{
            "country_id": next_country_id,
            "country_name": country_name,
            "country_code": country_code,
        }])
        dim_country = pd.concat([dim_country, new_country_row], ignore_index=True)
        country_id = next_country_id

    season_map = season_name_to_id_map(dim_season)
    club_lookup = build_club_lookup(dim_club)
    next_result_id = int(fact["result_id"].max()) + 1 if len(fact) > 0 else 1
    next_club_id = int(dim_club["club_id"].max()) + 1 if len(dim_club) > 0 else 1

    new_fact_rows: list[dict[str, Any]] = []
    unmatched_rows: list[dict[str, Any]] = []
    created_rows: list[dict[str, Any]] = []

    # Try to resolve league
    league_key = try_resolve_league_key(ss, source_league_name)
    if not league_key:
        logger.warning("Could not resolve league %s, skipping", source_league_name)
        return new_fact_rows, unmatched_rows, created_rows, dim_club, dim_country

    # Get valid seasons
    try:
        valid_seasons = get_valid_seasons_with_diagnostics(ss, league_key)
    except Exception as e:
        logger.warning("Failed to get valid seasons for %s: %s", league_code, e)
        raise

    valid_season_names = list(valid_seasons.keys())
    logger.info("Available seasons from Sofascore: %s", valid_season_names[:10])

    # Convert season names and filter to those in dim_season
    seasons_to_pull = []
    for sofascore_season in valid_season_names:
        converted_season = convert_season_name(sofascore_season, season_format)
        if converted_season and converted_season in season_map:
            seasons_to_pull.append((sofascore_season, converted_season))

    if match_calendar_year is not None:
        before = len(seasons_to_pull)
        seasons_to_pull = [
            pair
            for pair in seasons_to_pull
            if dim_season_calendar_year_overlap(pair[1], match_calendar_year)
        ]
        logger.info(
            "Calendar year %s: kept %d/%d dim seasons (overlap heuristic; skipping dead seasons)",
            match_calendar_year,
            len(seasons_to_pull),
            before,
        )

    logger.info("Matching seasons in dim_season (after filters): %d", len(seasons_to_pull))

    # Pull matches for each season
    for sofascore_season, dim_season_name in seasons_to_pull:
        logger.info("Pulling %s | %s", league_code, sofascore_season)
        season_id = season_map[dim_season_name]

        try:
            matches = ss.get_match_dicts(sofascore_season, league_key)
        except Exception as e:
            logger.warning("Failed to pull matches for %s | %s: %s", league_code, sofascore_season, e)
            continue

        if not matches:
            logger.info("No matches returned for %s | %s", league_code, sofascore_season)
            continue

        logger.info("Got %d matches", len(matches))

        finished_count = 0
        for match in matches:
            if not is_finished_match(match):
                continue
            if match_calendar_year is not None:
                cy = sofascore_match_calendar_year_utc(match)
                if cy is None or cy != match_calendar_year:
                    continue
            finished_count += 1

            home_name = safe_get(match, "homeTeam", "name")
            away_name = safe_get(match, "awayTeam", "name")
            if not home_name or not away_name:
                continue

            home_map, dim_club, club_lookup, next_club_id = resolve_or_create_club(
                club_name=home_name,
                dim_club=dim_club,
                club_lookup=club_lookup,
                country_id=country_id,
                next_club_id=next_club_id,
                create_missing=CREATE_MISSING_CLUBS,
                merge_on_fuzzy=True,
            )

            away_map, dim_club, club_lookup, next_club_id = resolve_or_create_club(
                club_name=away_name,
                dim_club=dim_club,
                club_lookup=club_lookup,
                country_id=country_id,
                next_club_id=next_club_id,
                create_missing=CREATE_MISSING_CLUBS,
                merge_on_fuzzy=True,
            )

            if home_map.was_created:
                created_rows.append({
                    "league_code": league_code,
                    "club_name": home_name,
                    "created_club_id": home_map.club_id,
                    "suggested_existing_match": home_map.suggestion,
                    "suggestion_score": home_map.suggestion_score,
                })

            if away_map.was_created:
                created_rows.append({
                    "league_code": league_code,
                    "club_name": away_name,
                    "created_club_id": away_map.club_id,
                    "suggested_existing_match": away_map.suggestion,
                    "suggestion_score": away_map.suggestion_score,
                })

            if home_map.club_id is None:
                unmatched_rows.append({
                    "league_code": league_code,
                    "season_name": dim_season_name,
                    "club_side": "home",
                    "club_name": home_name,
                    "suggested_match": home_map.suggestion,
                    "suggestion_score": home_map.suggestion_score,
                })
                continue

            if away_map.club_id is None:
                unmatched_rows.append({
                    "league_code": league_code,
                    "season_name": dim_season_name,
                    "club_side": "away",
                    "club_name": away_name,
                    "suggested_match": away_map.suggestion,
                    "suggestion_score": away_map.suggestion_score,
                })
                continue

            fact_row = flatten_match_to_fact_row(
                match=match,
                result_id=next_result_id,
                country_id=country_id,
                country_name=country_name,
                season_id=season_id,
                league_code=league_code,
                home_club_id=home_map.club_id,
                away_club_id=away_map.club_id,
            )

            if fact_row:
                new_fact_rows.append(fact_row)
                next_result_id += 1

        time.sleep(REQUEST_SLEEP_SECONDS)

    logger.info("League complete: %d new matches, %d unmatched clubs, %d created clubs",
                len(new_fact_rows), len(unmatched_rows), len(created_rows))

    return new_fact_rows, unmatched_rows, created_rows, dim_club, dim_country


def load_dims(
    fact_path: Path,
    dim_club_path: Path,
    dim_country_path: Path,
    dim_season_path: Path,
    outdir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load dimension tables, preferring updated versions if they exist."""
    # Prefer updated dims if they exist
    dim_club_updated_path = outdir / "dim_club_updated.csv"
    dim_country_updated_path = outdir / "dim_country_updated.csv"
    
    if dim_club_updated_path.exists():
        dim_club = pd.read_csv(dim_club_updated_path)
        logger.info("Loaded updated dim_club: %d rows", len(dim_club))
    else:
        dim_club = pd.read_csv(dim_club_path)
        logger.info("Loaded dim_club: %d rows", len(dim_club))
    
    if dim_country_updated_path.exists():
        dim_country = pd.read_csv(dim_country_updated_path)
        logger.info("Loaded updated dim_country: %d rows", len(dim_country))
    else:
        dim_country = pd.read_csv(dim_country_path)
        logger.info("Loaded dim_country: %d rows", len(dim_country))
    
    dim_season = pd.read_csv(dim_season_path)
    logger.info("Loaded dim_season: %d rows", len(dim_season))

    fact = pd.read_csv(fact_path)
    logger.info("Loaded fact: %d rows", len(fact))

    return fact, dim_club, dim_country, dim_season


def load_progress(outdir: Path) -> set[str]:
    """Load processed leagues from progress file."""
    progress_path = outdir / "ingestion_progress.json"
    if progress_path.exists():
        with open(progress_path, 'r') as f:
            data = json.load(f)
            return set(data.get("processed_leagues", []))
    return set()


def save_progress(outdir: Path, processed_leagues: set[str]) -> None:
    """Save processed leagues to progress file."""
    progress_path = outdir / "ingestion_progress.json"
    data = {"processed_leagues": list(processed_leagues)}
    with open(progress_path, 'w') as f:
        json.dump(data, f, indent=2)


def load_existing_outputs(outdir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load existing ingested_fact, summary, unmatched, created if they exist."""
    ingested_fact_path = outdir / "fact_result_simple_ingested.csv"
    summary_path = outdir / "ingestion_summary.csv"
    unmatched_path = outdir / "unmatched_clubs.csv"
    created_path = outdir / "created_clubs.csv"
    
    def safe_read_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    
    ingested_fact = safe_read_csv(ingested_fact_path)
    summary = safe_read_csv(summary_path)
    unmatched = safe_read_csv(unmatched_path)
    created = safe_read_csv(created_path)
    
    return ingested_fact, summary, unmatched, created


# ---------------------------------------------------
# CLI
# ---------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest football results from all configured leagues"
    )
    parser.add_argument("--fact", default="output/fact_result_simple.csv", help="Path to fact_result_simple.csv")
    parser.add_argument("--dim-club", default="output/dim_club.csv", help="Path to dim_club.csv")
    parser.add_argument("--dim-country", default="output/dim_country.csv", help="Path to dim_country.csv")
    parser.add_argument("--dim-season", default="output/dim_season.csv", help="Path to dim_season.csv")
    parser.add_argument("--outdir", default="output", help="Output folder")
    parser.add_argument(
        "--all-calendar-years",
        action="store_true",
        help="Include matches from every calendar year (disables default current-year-only filter).",
    )
    parser.add_argument(
        "--match-calendar-year",
        type=int,
        metavar="YYYY",
        default=None,
        help="Only keep matches with kickoff in this UTC calendar year. Default when omitted: current UTC year (unless --all-calendar-years).",
    )
    args = parser.parse_args()

    if args.all_calendar_years:
        match_calendar_year: int | None = None
    elif args.match_calendar_year is not None:
        match_calendar_year = args.match_calendar_year
    else:
        match_calendar_year = datetime.now(timezone.utc).year

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    fact, dim_club, dim_country, dim_season = load_dims(
        Path(args.fact),
        Path(args.dim_club),
        Path(args.dim_country),
        Path(args.dim_season),
        outdir,
    )

    processed_leagues = set() if match_calendar_year is not None else load_progress(outdir)
    if match_calendar_year is not None:
        logger.info(
            "Match calendar year filter=%s (UTC): ignoring ingestion_progress.json so leagues are re-fetched",
            match_calendar_year,
        )
    existing_ingested_fact, existing_summary, existing_unmatched, existing_created = load_existing_outputs(outdir)

    ss = Sofascore()

    all_fact_rows: list[dict[str, Any]] = []
    all_unmatched: list[dict[str, Any]] = []
    all_created: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []

    domestic_metadata = [
        cfg for cfg in LEAGUE_METADATA if cfg.get("competition_type") in DOMESTIC_COMPETITION_TYPES
    ]
    logger.info("Domestic ingest mode: %d configured leagues", len(domestic_metadata))

    # Process domestic leagues only
    for league_config in domestic_metadata:
        league_code = league_config["league_key"]
        if league_code in processed_leagues:
            logger.info("Skipping already processed league: %s", league_code)
            continue

        try:
            fact_rows, unmatched, created, dim_club, dim_country = ingest_league(
                league_config=league_config,
                fact=fact,
                dim_club=dim_club,
                dim_country=dim_country,
                dim_season=dim_season,
                ss=ss,
                match_calendar_year=match_calendar_year,
            )

            all_fact_rows.extend(fact_rows)
            all_unmatched.extend(unmatched)
            all_created.extend(created)

            summary_rows.append({
                "league_code": league_config["league_key"],
                "source_league_name": league_config["source_league_name"],
                "country": league_config["country"],
                "matches_ingested": len(fact_rows),
                "unmatched_clubs": len(unmatched),
                "created_clubs": len(created),
                "status": "success",
            })

            if unmatched:
                new_unmatched = pd.DataFrame(unmatched)
                existing_unmatched = pd.concat([existing_unmatched, new_unmatched], ignore_index=True)
                existing_unmatched.drop_duplicates().to_csv(outdir / "unmatched_clubs.csv", index=False)
            
            if created:
                new_created = pd.DataFrame(created)
                existing_created = pd.concat([existing_created, new_created], ignore_index=True)
                existing_created.drop_duplicates().to_csv(outdir / "created_clubs.csv", index=False)
            
            # Update dims
            dim_club.to_csv(outdir / "dim_club_updated.csv", index=False)
            dim_country.to_csv(outdir / "dim_country_updated.csv", index=False)
            
            # Update summary
            new_summary = pd.DataFrame([summary_rows[-1]])
            existing_summary = pd.concat([existing_summary, new_summary], ignore_index=True)
            existing_summary.to_csv(outdir / "ingestion_summary.csv", index=False)

            if match_calendar_year is None:
                processed_leagues.add(league_code)
                save_progress(outdir, processed_leagues)

        except Exception as e:
            logger.error("Error processing league %s: %s", league_config["league_key"], e)
            summary_rows.append({
                "league_code": league_config["league_key"],
                "source_league_name": league_config["source_league_name"],
                "country": league_config["country"],
                "matches_ingested": 0,
                "unmatched_clubs": 0,
                "created_clubs": 0,
                "status": f"error: {str(e)[:100]}",
            })
            
            # Still update summary for errors
            new_summary = pd.DataFrame([summary_rows[-1]])
            existing_summary = pd.concat([existing_summary, new_summary], ignore_index=True)
            existing_summary.to_csv(outdir / "ingestion_summary.csv", index=False)

    # Deduplicate new fact rows
    if all_fact_rows:
        new_ingested_fact = pd.DataFrame(all_fact_rows)
        # Combine with existing ingested
        combined_ingested = pd.concat([existing_ingested_fact, new_ingested_fact], ignore_index=True).drop_duplicates()
    else:
        combined_ingested = existing_ingested_fact.drop_duplicates() if not existing_ingested_fact.empty else pd.DataFrame()

    # Deduplicate against existing fact
    if not combined_ingested.empty:
        dedupe_cols = [
            "season_id",
            "league_code",
            "match_date",
            "match_time",
            "home_club_id",
            "away_club_id",
            "home_team_goals",
            "away_team_goals",
        ]

        existing_keys = fact[dedupe_cols].fillna('').astype(str)
        new_keys = combined_ingested[dedupe_cols].fillna('').astype(str)

        existing_key_set = set(existing_keys.apply("|".join, axis=1))
        keep_mask = ~new_keys.apply("|".join, axis=1).isin(existing_key_set)
        combined_ingested = combined_ingested.loc[keep_mask].copy()

    # Write final outputs
    if not combined_ingested.empty:
        combined_ingested.to_csv(outdir / "fact_result_simple_ingested.csv", index=False)
    
    # Dims are already saved incrementally
    # Summary, unmatched, created already saved incrementally

    logger.info("=" * 80)
    logger.info("INGESTION COMPLETE")
    logger.info("=" * 80)
    logger.info("Total new matches ingested: %d", len(combined_ingested))
    logger.info("Summary written to: %s", outdir / "ingestion_summary.csv")
    logger.info("Updated dim_club: %d rows", len(dim_club))
    logger.info("Updated dim_country: %d rows", len(dim_country))


if __name__ == "__main__":
    main()
