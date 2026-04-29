"""
ingest_euro_comps_from_config.py

UEFA-only ingest path with pluggable providers.

Providers:
- sofascore (existing ScraperFC path)
- football_data_org (alternate API path; requires token)
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from datetime import datetime
import sys

import pandas as pd
import requests
from ScraperFC.sofascore import Sofascore

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.football_ingestion_config import LEAGUE_METADATA
from scripts.ingest_leagues_from_config import (
    ingest_league,
    load_dims,
    logger,
    build_club_lookup,
    resolve_or_create_club,
)

UEFA_LEAGUE_KEYS = {"UCL", "UEL", "UECL", "EURO"}
FOOTBALL_DATA_COMP_CODES = {
    "UCL": "CL",
    "UEL": "EL",
    "UECL": "ECL",
    "EURO": "EC",
}
def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def load_progress(outdir: Path) -> set[str]:
    progress_path = outdir / "ingestion_progress_euro.json"
    if progress_path.exists():
        return set(json.loads(progress_path.read_text(encoding="utf-8")).get("processed_leagues", []))
    return set()


def save_progress(outdir: Path, processed_leagues: set[str]) -> None:
    progress_path = outdir / "ingestion_progress_euro.json"
    progress_path.write_text(
        json.dumps({"processed_leagues": sorted(processed_leagues)}, indent=2),
        encoding="utf-8",
    )


def season_name_to_id_map(dim_season: pd.DataFrame) -> dict[str, int]:
    return {str(row["season_name"]): int(row["season_id"]) for _, row in dim_season.iterrows()}


def season_name_from_date(match_date: datetime, season_format: str) -> str:
    year = int(match_date.year)
    if season_format == "end_year":
        return f"{year - 1}/{year}"
    # default to split-style club season (July rollover)
    start_year = year if match_date.month >= 7 else year - 1
    return f"{start_year}/{start_year + 1}"


def ingest_league_football_data_org(
    league_config: dict,
    fact: pd.DataFrame,
    dim_club: pd.DataFrame,
    dim_country: pd.DataFrame,
    dim_season: pd.DataFrame,
    token: str,
) -> tuple[list[dict], list[dict], list[dict], pd.DataFrame, pd.DataFrame]:
    league_code = league_config["league_key"]
    country_name = league_config["country"]
    country_code = league_config["country_code"]
    season_format = league_config["season_format"]
    comp_code = FOOTBALL_DATA_COMP_CODES.get(league_code)
    if not comp_code:
        raise RuntimeError(f"No football-data.org competition code for league key {league_code}")

    if country_name in dim_country["country_name"].values:
        country_id = int(dim_country[dim_country["country_name"] == country_name]["country_id"].iloc[0])
    else:
        next_country_id = int(dim_country["country_id"].max()) + 1
        dim_country = pd.concat(
            [
                dim_country,
                pd.DataFrame(
                    [{"country_id": next_country_id, "country_name": country_name, "country_code": country_code}]
                ),
            ],
            ignore_index=True,
        )
        country_id = next_country_id

    season_map = season_name_to_id_map(dim_season)
    club_lookup = build_club_lookup(dim_club)
    next_result_id = int(fact["result_id"].max()) + 1 if len(fact) > 0 else 1
    next_club_id = int(dim_club["club_id"].max()) + 1 if len(dim_club) > 0 else 1

    url = f"https://api.football-data.org/v4/competitions/{comp_code}/matches?status=FINISHED"
    response = requests.get(url, headers={"X-Auth-Token": token}, timeout=60)
    if response.status_code != 200:
        raise RuntimeError(f"football-data.org HTTP {response.status_code}: {response.text[:200]}")
    payload = response.json()
    matches = payload.get("matches", [])

    new_fact_rows: list[dict] = []
    unmatched_rows: list[dict] = []
    created_rows: list[dict] = []

    for match in matches:
        utc_date = match.get("utcDate")
        if not utc_date:
            continue
        match_dt = pd.to_datetime(utc_date, utc=True, errors="coerce")
        if pd.isna(match_dt):
            continue

        season_name = season_name_from_date(match_dt.to_pydatetime(), season_format=season_format)
        season_id = season_map.get(season_name)
        if season_id is None:
            continue

        home_team = match.get("homeTeam") or {}
        away_team = match.get("awayTeam") or {}
        home_name = (home_team.get("name") or "").strip()
        away_name = (away_team.get("name") or "").strip()
        home_short = (home_team.get("shortName") or "").strip()
        away_short = (away_team.get("shortName") or "").strip()
        home_api_id = home_team.get("id")
        away_api_id = away_team.get("id")
        if not home_name or not away_name:
            continue

        home_map, dim_club, club_lookup, next_club_id = resolve_or_create_club(
            club_name=home_name,
            dim_club=dim_club,
            club_lookup=club_lookup,
            country_id=country_id,
            next_club_id=next_club_id,
            create_missing=True,
        )
        away_map, dim_club, club_lookup, next_club_id = resolve_or_create_club(
            club_name=away_name,
            dim_club=dim_club,
            club_lookup=club_lookup,
            country_id=country_id,
            next_club_id=next_club_id,
            create_missing=True,
        )

        if home_map.was_created:
            created_rows.append(
                {
                    "league_code": league_code,
                    "club_name": home_name,
                    "created_club_id": home_map.club_id,
                    "suggested_existing_match": home_map.suggestion,
                    "suggestion_score": home_map.suggestion_score,
                }
            )
        if away_map.was_created:
            created_rows.append(
                {
                    "league_code": league_code,
                    "club_name": away_name,
                    "created_club_id": away_map.club_id,
                    "suggested_existing_match": away_map.suggestion,
                    "suggestion_score": away_map.suggestion_score,
                }
            )

        if home_map.club_id is None or away_map.club_id is None:
            unmatched_rows.append(
                {
                    "league_code": league_code,
                    "season_name": season_name,
                    "club_side": "home" if home_map.club_id is None else "away",
                    "club_name": home_name if home_map.club_id is None else away_name,
                }
            )
            continue

        full_time = (match.get("score") or {}).get("fullTime") or {}
        home_goals = full_time.get("home")
        away_goals = full_time.get("away")
        if home_goals is None or away_goals is None:
            continue

        new_fact_rows.append(
            {
                "result_id": next_result_id,
                "season_id": season_id,
                "country_id": country_id,
                "country_name": country_name,
                "league_code": league_code,
                "match_date": match_dt.date(),
                "match_time": match_dt.time(),
                "home_club_id": home_map.club_id,
                "away_club_id": away_map.club_id,
                "home_team_goals": int(home_goals),
                "away_team_goals": int(away_goals),
            }
        )
        next_result_id += 1

    return new_fact_rows, unmatched_rows, created_rows, dim_club, dim_country


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest UEFA competitions only")
    parser.add_argument("--fact", default="output/fact_result_simple.csv", help="Path to fact_result_simple.csv")
    parser.add_argument("--dim-club", default="output/dim_club.csv", help="Path to dim_club.csv")
    parser.add_argument("--dim-country", default="output/dim_country.csv", help="Path to dim_country.csv")
    parser.add_argument("--dim-season", default="output/dim_season.csv", help="Path to dim_season.csv")
    parser.add_argument("--outdir", default="output", help="Output folder")
    parser.add_argument(
        "--provider",
        default="sofascore",
        choices=["sofascore", "football_data_org"],
        help="UEFA ingest provider",
    )
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    fact, dim_club, dim_country, dim_season = load_dims(
        Path(args.fact),
        Path(args.dim_club),
        Path(args.dim_country),
        Path(args.dim_season),
        outdir,
    )

    processed_leagues = load_progress(outdir)
    existing_ingested_fact = _safe_read_csv(outdir / "fact_result_simple_ingested_euro.csv")
    existing_summary = _safe_read_csv(outdir / "ingestion_summary_euro.csv")
    existing_unmatched = _safe_read_csv(outdir / "unmatched_clubs_euro.csv")
    existing_created = _safe_read_csv(outdir / "created_clubs_euro.csv")

    ss = Sofascore() if args.provider == "sofascore" else None
    fd_token = os.getenv("FOOTBALL_DATA_API_TOKEN", "").strip() if args.provider == "football_data_org" else ""
    if args.provider == "football_data_org" and not fd_token:
        raise RuntimeError("FOOTBALL_DATA_API_TOKEN env var is required for provider=football_data_org")
    all_fact_rows: list[dict] = []

    euro_metadata = [cfg for cfg in LEAGUE_METADATA if cfg.get("league_key") in UEFA_LEAGUE_KEYS]
    logger.info("UEFA ingest mode: %d configured leagues", len(euro_metadata))

    for league_config in euro_metadata:
        league_code = league_config["league_key"]
        if league_code in processed_leagues:
            logger.info("Skipping already processed league: %s", league_code)
            continue
        try:
            if args.provider == "sofascore":
                fact_rows, unmatched, created, dim_club, dim_country = ingest_league(
                    league_config=league_config,
                    fact=fact,
                    dim_club=dim_club,
                    dim_country=dim_country,
                    dim_season=dim_season,
                    ss=ss,
                )
            else:
                fact_rows, unmatched, created, dim_club, dim_country = ingest_league_football_data_org(
                    league_config=league_config,
                    fact=fact,
                    dim_club=dim_club,
                    dim_country=dim_country,
                    dim_season=dim_season,
                    token=fd_token,
                )

            all_fact_rows.extend(fact_rows)
            if unmatched:
                existing_unmatched = pd.concat([existing_unmatched, pd.DataFrame(unmatched)], ignore_index=True)
                existing_unmatched.drop_duplicates().to_csv(outdir / "unmatched_clubs_euro.csv", index=False)
            if created:
                existing_created = pd.concat([existing_created, pd.DataFrame(created)], ignore_index=True)
                existing_created.drop_duplicates().to_csv(outdir / "created_clubs_euro.csv", index=False)

            dim_club.to_csv(outdir / "dim_club_updated.csv", index=False)
            dim_country.to_csv(outdir / "dim_country_updated.csv", index=False)

            row = pd.DataFrame(
                [
                    {
                        "league_code": league_code,
                        "source_league_name": league_config["source_league_name"],
                        "country": league_config["country"],
                        "matches_ingested": len(fact_rows),
                        "unmatched_clubs": len(unmatched),
                        "created_clubs": len(created),
                        "status": "success",
                        "provider": args.provider,
                    }
                ]
            )
            existing_summary = pd.concat([existing_summary, row], ignore_index=True)
            existing_summary.to_csv(outdir / "ingestion_summary_euro.csv", index=False)

            processed_leagues.add(league_code)
            save_progress(outdir, processed_leagues)
        except Exception as exc:
            row = pd.DataFrame(
                [
                    {
                        "league_code": league_code,
                        "source_league_name": league_config["source_league_name"],
                        "country": league_config["country"],
                        "matches_ingested": 0,
                        "unmatched_clubs": 0,
                        "created_clubs": 0,
                        "status": f"error: {str(exc)[:100]}",
                        "provider": args.provider,
                    }
                ]
            )
            existing_summary = pd.concat([existing_summary, row], ignore_index=True)
            existing_summary.to_csv(outdir / "ingestion_summary_euro.csv", index=False)
            logger.error("Error processing league %s: %s", league_code, exc)

    if all_fact_rows:
        new_ingested = pd.DataFrame(all_fact_rows)
        combined = pd.concat([existing_ingested_fact, new_ingested], ignore_index=True).drop_duplicates()
    else:
        combined = existing_ingested_fact.drop_duplicates() if not existing_ingested_fact.empty else pd.DataFrame()

    if not combined.empty:
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
        existing_keys = fact[dedupe_cols].fillna("").astype(str)
        new_keys = combined[dedupe_cols].fillna("").astype(str)
        existing_set = set(existing_keys.apply("|".join, axis=1))
        keep_mask = ~new_keys.apply("|".join, axis=1).isin(existing_set)
        combined = combined.loc[keep_mask].copy()
        combined.to_csv(outdir / "fact_result_simple_ingested_euro.csv", index=False)

    logger.info("UEFA INGEST COMPLETE | new rows=%d", len(combined) if not combined.empty else 0)


if __name__ == "__main__":
    main()
