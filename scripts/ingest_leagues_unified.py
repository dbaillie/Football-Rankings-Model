#!/usr/bin/env python3
"""
Domestic + UEFA ingestion in one script: SofaScore and/or FBref (ScraperFC), optional football-data.org API.

Domestic outputs match ``ingest_leagues_from_config.py`` (``fact_result_simple_ingested.csv``, …).
European outputs match ``ingest_euro_comps_from_config.py`` (``fact_result_simple_ingested_euro.csv``,
``ingestion_progress_euro.json``, …).

Backends for **ScraperFC** leagues (see ``comps.yaml`` and
https://scraperfc.readthedocs.io/en/latest/fbref.html ):
  * ``sofascore`` — ``ScraperFC.sofascore.Sofascore``
  * ``fbref`` — ``ScraperFC.fbref.FBref``
  * ``mixed`` — SofaScore when comps lists SOFASCORE, else FBref when only FBREF exists

``--european-provider scraperfc`` applies ``--backend`` to UCL/UEL/UECL/EURO the same way as domestically.
``--european-provider football_data_org`` uses the REST API (``FOOTBALL_DATA_API_TOKEN``).

By default only matches whose kickoff falls in the **current UTC calendar year** are kept; use
``--all-calendar-years`` for full history or ``--match-calendar-year YYYY`` for one year.
When that filter is active, league progress files are ignored and not updated.

Examples:
  python scripts/ingest_leagues_unified.py
  python scripts/ingest_leagues_unified.py --skip-domestic
  python scripts/ingest_leagues_unified.py --backend fbref --fbref-wait-time 8
  python scripts/ingest_leagues_unified.py --european-provider football_data_org
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from ScraperFC.fbref import FBref
from ScraperFC.fbref_match import FBrefMatch
from ScraperFC.utils import get_module_comps

import sys

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config.football_ingestion_config import (  # noqa: E402
    LEAGUE_METADATA,
    SEASON_END_YEAR,
    SEASON_START_YEAR,
)
from ScraperFC.sofascore import Sofascore  # noqa: E402

from scripts.ingest_euro_comps_from_config import (  # noqa: E402
    UEFA_LEAGUE_KEYS,
    ingest_league_football_data_org,
    load_progress as euro_load_progress,
    save_progress as euro_save_progress,
)
from scripts.ingest_leagues_from_config import (  # noqa: E402
    CREATE_MISSING_CLUBS,
    DOMESTIC_COMPETITION_TYPES,
    dim_season_calendar_year_overlap,
    ingest_league,
    load_dims,
    load_existing_outputs,
    load_progress,
    logger,
    save_progress,
)

_FBREF_COMPS = get_module_comps("FBREF")
_SOFACOMPS = get_module_comps("SOFASCORE")


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _dim_season_start_year(season_name: str) -> int | None:
    try:
        return int(str(season_name).strip().split("/")[0])
    except (ValueError, IndexError):
        return None


def _dim_season_in_config_range(season_name: str) -> bool:
    y = _dim_season_start_year(season_name)
    if y is None:
        return True
    return SEASON_START_YEAR <= y <= SEASON_END_YEAR


def fb_year_key_to_dim_season(fb_year: str) -> str | None:
    """
    Map FBref competition-history year label to dim_season ``YYYY/YYYY`` style.
    Handles ``2024-2025``, ``2024-25``, and en-dash separators.
    """
    t = str(fb_year).strip().replace("–", "-").replace("—", "-")
    if "/" in t:
        return t if len(t.split("/")) == 2 else None
    if "-" not in t:
        return None
    left, right = t.split("-", 1)
    left, right = left.strip(), right.strip()
    if len(left) == 4 and len(right) == 4:
        return f"{left}/{right}"
    if len(left) == 4 and len(right) == 2:
        return f"{left}/20{right}"
    return None


def pair_fbref_years_to_dim(
    fb_season_keys: list[str],
    season_map: dict[str, int],
) -> list[tuple[str, str]]:
    """Return (fb_year_key, dim_season_name) pairs present in dim_season."""
    dim_names = set(season_map.keys())
    out: list[tuple[str, str]] = []
    for fb_y in fb_season_keys:
        dim_name = fb_year_key_to_dim_season(fb_y)
        if dim_name and dim_name in dim_names and _dim_season_in_config_range(dim_name):
            out.append((fb_y, dim_name))
    return sorted(out, key=lambda x: x[1])


def _parse_fbref_goals(raw: str | None) -> int | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def flatten_fbref_match_to_fact_row(
    m: FBrefMatch,
    result_id: int,
    country_id: int,
    country_name: str,
    season_id: int,
    league_code: str,
    home_club_id: int,
    away_club_id: int,
) -> dict[str, Any] | None:
    hg = _parse_fbref_goals(m.home_goals)
    ag = _parse_fbref_goals(m.away_goals)
    if hg is None or ag is None:
        return None
    home_nm = str(m.home_team).strip()
    away_nm = str(m.away_team).strip()
    if not home_nm or not away_nm:
        return None
    try:
        match_date = pd.to_datetime(m.date, errors="coerce")
        if pd.isna(match_date):
            return None
        d = match_date.date()
        t = match_date.time()
    except Exception:
        return None

    return {
        "result_id": result_id,
        "season_id": season_id,
        "country_id": country_id,
        "country_name": country_name,
        "league_code": league_code,
        "match_date": d,
        "match_time": t,
        "home_team_name": home_nm,
        "away_team_name": away_nm,
        "home_club_id": home_club_id,
        "away_club_id": away_club_id,
        "home_team_goals": hg,
        "away_team_goals": ag,
    }


def resolve_backend_for_league(source_league_name: str, mode: str) -> str:
    if mode == "sofascore":
        return "sofascore"
    if mode == "fbref":
        return "fbref"
    # mixed: prefer SofaScore when both exist; FBref only when no SOFASCORE entry
    if source_league_name in _SOFACOMPS:
        return "sofascore"
    if source_league_name in _FBREF_COMPS:
        return "fbref"
    return "sofascore"


def ingest_league_fbref(
    league_config: dict[str, Any],
    fact: pd.DataFrame,
    dim_club: pd.DataFrame,
    dim_country: pd.DataFrame,
    dim_season: pd.DataFrame,
    fb: FBref,
    *,
    match_calendar_year: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], pd.DataFrame, pd.DataFrame]:
    from scripts.club_identity import build_club_lookup, resolve_or_create_club

    source_league_name = league_config["source_league_name"]
    league_code = league_config["league_key"]
    country_name = league_config["country"]

    if source_league_name not in _FBREF_COMPS:
        logger.warning("FBref: league %s not in FBREF comps, skipping", source_league_name)
        return [], [], [], dim_club, dim_country

    logger.info("=" * 80)
    logger.info("FBref processing: %s (%s)", source_league_name, league_code)
    logger.info("=" * 80)

    country_code = league_config["country_code"]

    country_id: int | None = None
    if country_name in dim_country["country_name"].values:
        country_id = int(dim_country[dim_country["country_name"] == country_name]["country_id"].iloc[0])
    else:
        next_country_id = int(dim_country["country_id"].max()) + 1
        dim_country = pd.concat(
            [dim_country, pd.DataFrame([{"country_id": next_country_id, "country_name": country_name, "country_code": country_code}])],
            ignore_index=True,
        )
        country_id = next_country_id

    season_map = {str(row["season_name"]): int(row["season_id"]) for _, row in dim_season.iterrows()}
    club_lookup = build_club_lookup(dim_club)
    next_result_id = int(fact["result_id"].max()) + 1 if len(fact) > 0 else 1
    next_club_id = int(dim_club["club_id"].max()) + 1 if len(dim_club) > 0 else 1

    new_fact_rows: list[dict[str, Any]] = []
    unmatched_rows: list[dict[str, Any]] = []
    created_rows: list[dict[str, Any]] = []

    try:
        fb_valid = fb.get_valid_seasons(source_league_name)
    except Exception as e:
        logger.warning("FBref get_valid_seasons failed for %s: %s", source_league_name, e)
        return new_fact_rows, unmatched_rows, created_rows, dim_club, dim_country

    seasons_to_pull = pair_fbref_years_to_dim(list(fb_valid.keys()), season_map)
    if match_calendar_year is not None:
        before = len(seasons_to_pull)
        seasons_to_pull = [
            pair
            for pair in seasons_to_pull
            if dim_season_calendar_year_overlap(pair[1], match_calendar_year)
        ]
        logger.info(
            "FBref calendar year %s: kept %d/%d seasons (dim overlap heuristic)",
            match_calendar_year,
            len(seasons_to_pull),
            before,
        )
    logger.info("FBref seasons matched to dim_season: %d", len(seasons_to_pull))

    for fb_year, dim_season_name in seasons_to_pull:
        season_id = season_map[dim_season_name]
        logger.info("FBref pulling %s | %s", league_code, fb_year)
        try:
            matches = fb.scrape_matches(fb_year, source_league_name)
        except Exception as e:
            logger.warning("FBref scrape_matches failed %s | %s: %s", league_code, fb_year, e)
            continue

        if not matches:
            logger.info("FBref: no matches %s | %s", league_code, fb_year)
            continue

        logger.info("FBref: got %d match records", len(matches))
        for m in matches:
            try:
                md = pd.to_datetime(m.date, errors="coerce")
                if pd.isna(md):
                    continue
                if match_calendar_year is not None and int(md.year) != match_calendar_year:
                    continue
            except Exception:
                continue

            home_nm = str(m.home_team).strip()
            away_nm = str(m.away_team).strip()
            if not home_nm or not away_nm:
                continue

            home_map, dim_club, club_lookup, next_club_id = resolve_or_create_club(
                club_name=home_nm,
                dim_club=dim_club,
                club_lookup=club_lookup,
                country_id=int(country_id),
                next_club_id=next_club_id,
                create_missing=CREATE_MISSING_CLUBS,
                merge_on_fuzzy=True,
            )
            away_map, dim_club, club_lookup, next_club_id = resolve_or_create_club(
                club_name=away_nm,
                dim_club=dim_club,
                club_lookup=club_lookup,
                country_id=int(country_id),
                next_club_id=next_club_id,
                create_missing=CREATE_MISSING_CLUBS,
                merge_on_fuzzy=True,
            )

            if home_map.was_created:
                created_rows.append(
                    {
                        "league_code": league_code,
                        "club_name": home_nm,
                        "created_club_id": home_map.club_id,
                        "suggested_existing_match": home_map.suggestion,
                        "suggestion_score": home_map.suggestion_score,
                    }
                )
            if away_map.was_created:
                created_rows.append(
                    {
                        "league_code": league_code,
                        "club_name": away_nm,
                        "created_club_id": away_map.club_id,
                        "suggested_existing_match": away_map.suggestion,
                        "suggestion_score": away_map.suggestion_score,
                    }
                )

            if home_map.club_id is None:
                unmatched_rows.append(
                    {
                        "league_code": league_code,
                        "season_name": dim_season_name,
                        "club_side": "home",
                        "club_name": home_nm,
                        "suggested_match": home_map.suggestion,
                        "suggestion_score": home_map.suggestion_score,
                    }
                )
                continue
            if away_map.club_id is None:
                unmatched_rows.append(
                    {
                        "league_code": league_code,
                        "season_name": dim_season_name,
                        "club_side": "away",
                        "club_name": away_nm,
                        "suggested_match": away_map.suggestion,
                        "suggestion_score": away_map.suggestion_score,
                    }
                )
                continue

            fact_row = flatten_fbref_match_to_fact_row(
                m=m,
                result_id=next_result_id,
                country_id=int(country_id),
                country_name=country_name,
                season_id=season_id,
                league_code=league_code,
                home_club_id=int(home_map.club_id),
                away_club_id=int(away_map.club_id),
            )
            if fact_row:
                new_fact_rows.append(fact_row)
                next_result_id += 1

        time.sleep(0.35)

    logger.info(
        "FBref league complete: %d new matches, %d unmatched clubs, %d created clubs",
        len(new_fact_rows),
        len(unmatched_rows),
        len(created_rows),
    )
    return new_fact_rows, unmatched_rows, created_rows, dim_club, dim_country


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest domestic and UEFA leagues via SofaScore/FBref (ScraperFC) or football-data.org for UEFA."
    )
    parser.add_argument("--fact", default="output/fact_result_simple.csv")
    parser.add_argument("--dim-club", default="output/dim_club.csv")
    parser.add_argument("--dim-country", default="output/dim_country.csv")
    parser.add_argument("--dim-season", default="output/dim_season.csv")
    parser.add_argument("--outdir", default="output")
    parser.add_argument(
        "--backend",
        choices=("sofascore", "fbref", "mixed"),
        default="sofascore",
        help="mixed: SofaScore when comps has SOFASCORE, else FBref when only FBREF exists",
    )
    parser.add_argument(
        "--fbref-wait-time",
        type=int,
        default=6,
        help="Seconds between FBref requests (ScraperFC default is 6)",
    )
    parser.add_argument(
        "--ignore-progress",
        action="store_true",
        help="Ignore ingestion_progress.json (domestic)",
    )
    parser.add_argument(
        "--ignore-euro-progress",
        action="store_true",
        help="Ignore ingestion_progress_euro.json",
    )
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
        help="Only keep matches in this UTC calendar year. Default: current UTC year unless --all-calendar-years.",
    )
    parser.add_argument("--skip-domestic", action="store_true", help="Only run UEFA bucket")
    parser.add_argument("--skip-european", action="store_true", help="Skip UCL/UEL/UECL/EURO")
    parser.add_argument(
        "--european-provider",
        choices=("scraperfc", "football_data_org"),
        default="scraperfc",
        help="UEFA data source: ScraperFC uses --backend (SofaScore/FBref); API needs FOOTBALL_DATA_API_TOKEN",
    )
    args = parser.parse_args()

    if args.skip_domestic and args.skip_european:
        logger.error("Nothing to ingest: both --skip-domestic and --skip-european")
        return

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

    ss = Sofascore()
    fb = FBref(wait_time=int(args.fbref_wait_time))

    combined_domestic = pd.DataFrame()
    combined_euro = pd.DataFrame()

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

    # ----- Domestic -----
    if not args.skip_domestic:
        processed_leagues = (
            set()
            if match_calendar_year is not None or args.ignore_progress
            else load_progress(outdir)
        )
        if match_calendar_year is not None:
            logger.info(
                "Domestic: match calendar year=%s (UTC); ignoring ingestion_progress.json",
                match_calendar_year,
            )
        existing_ingested_fact, existing_summary, existing_unmatched, existing_created = load_existing_outputs(outdir)

        all_fact_rows: list[dict[str, Any]] = []
        all_unmatched: list[dict[str, Any]] = []
        all_created: list[dict[str, Any]] = []
        summary_rows: list[dict[str, Any]] = []

        domestic_metadata = [cfg for cfg in LEAGUE_METADATA if cfg.get("competition_type") in DOMESTIC_COMPETITION_TYPES]
        logger.info(
            "Domestic ingest | backend=%s | leagues=%d | season years %s-%s",
            args.backend,
            len(domestic_metadata),
            SEASON_START_YEAR,
            SEASON_END_YEAR,
        )

        for league_config in domestic_metadata:
            league_code = league_config["league_key"]
            src = league_config["source_league_name"]

            if league_code in processed_leagues:
                logger.info("Skipping already processed league: %s", league_code)
                continue

            be = resolve_backend_for_league(src, args.backend)
            if be == "fbref" and src not in _FBREF_COMPS:
                logger.warning("%s: FBref requested but league not in FBREF comps — skipping", league_code)
                summary_rows.append(
                    {
                        "league_code": league_code,
                        "source_league_name": src,
                        "country": league_config["country"],
                        "matches_ingested": 0,
                        "unmatched_clubs": 0,
                        "created_clubs": 0,
                        "status": "skipped: no FBREF comp",
                        "backend": "fbref",
                    }
                )
                new_summary = pd.DataFrame([summary_rows[-1]])
                existing_summary = pd.concat([existing_summary, new_summary], ignore_index=True)
                existing_summary.to_csv(outdir / "ingestion_summary.csv", index=False)
                continue

            if be == "sofascore" and src not in _SOFACOMPS:
                logger.warning("%s: SofaScore requested but league not in SOFASCORE comps — skipping", league_code)
                summary_rows.append(
                    {
                        "league_code": league_code,
                        "source_league_name": src,
                        "country": league_config["country"],
                        "matches_ingested": 0,
                        "unmatched_clubs": 0,
                        "created_clubs": 0,
                        "status": "skipped: no SOFASCORE comp",
                        "backend": "sofascore",
                    }
                )
                new_summary = pd.DataFrame([summary_rows[-1]])
                existing_summary = pd.concat([existing_summary, new_summary], ignore_index=True)
                existing_summary.to_csv(outdir / "ingestion_summary.csv", index=False)
                continue

            try:
                if be == "sofascore":
                    fact_rows, unmatched, created, dim_club, dim_country = ingest_league(
                        league_config=league_config,
                        fact=fact,
                        dim_club=dim_club,
                        dim_country=dim_country,
                        dim_season=dim_season,
                        ss=ss,
                        match_calendar_year=match_calendar_year,
                    )
                else:
                    fact_rows, unmatched, created, dim_club, dim_country = ingest_league_fbref(
                        league_config=league_config,
                        fact=fact,
                        dim_club=dim_club,
                        dim_country=dim_country,
                        dim_season=dim_season,
                        fb=fb,
                        match_calendar_year=match_calendar_year,
                    )

                all_fact_rows.extend(fact_rows)
                all_unmatched.extend(unmatched)
                all_created.extend(created)

                summary_rows.append(
                    {
                        "league_code": league_code,
                        "source_league_name": src,
                        "country": league_config["country"],
                        "matches_ingested": len(fact_rows),
                        "unmatched_clubs": len(unmatched),
                        "created_clubs": len(created),
                        "status": "success",
                        "backend": be,
                    }
                )

                if unmatched:
                    existing_unmatched = pd.concat([existing_unmatched, pd.DataFrame(unmatched)], ignore_index=True)
                    existing_unmatched.drop_duplicates().to_csv(outdir / "unmatched_clubs.csv", index=False)

                if created:
                    existing_created = pd.concat([existing_created, pd.DataFrame(created)], ignore_index=True)
                    existing_created.drop_duplicates().to_csv(outdir / "created_clubs.csv", index=False)

                dim_club.to_csv(outdir / "dim_club_updated.csv", index=False)
                dim_country.to_csv(outdir / "dim_country_updated.csv", index=False)

                new_summary = pd.DataFrame([summary_rows[-1]])
                existing_summary = pd.concat([existing_summary, new_summary], ignore_index=True)
                existing_summary.to_csv(outdir / "ingestion_summary.csv", index=False)

                if match_calendar_year is None:
                    processed_leagues.add(league_code)
                    save_progress(outdir, processed_leagues)

            except Exception as e:
                logger.error("Error processing league %s: %s", league_code, e)
                summary_rows.append(
                    {
                        "league_code": league_code,
                        "source_league_name": src,
                        "country": league_config["country"],
                        "matches_ingested": 0,
                        "unmatched_clubs": 0,
                        "created_clubs": 0,
                        "status": f"error: {str(e)[:100]}",
                        "backend": be,
                    }
                )
                new_summary = pd.DataFrame([summary_rows[-1]])
                existing_summary = pd.concat([existing_summary, new_summary], ignore_index=True)
                existing_summary.to_csv(outdir / "ingestion_summary.csv", index=False)

        if all_fact_rows:
            new_ingested_fact = pd.DataFrame(all_fact_rows)
            combined_domestic = pd.concat([existing_ingested_fact, new_ingested_fact], ignore_index=True).drop_duplicates()
        elif not existing_ingested_fact.empty:
            combined_domestic = existing_ingested_fact.drop_duplicates()
        else:
            combined_domestic = pd.DataFrame()

        if not combined_domestic.empty:
            existing_keys = fact[dedupe_cols].fillna("").astype(str)
            new_keys = combined_domestic[dedupe_cols].fillna("").astype(str)
            existing_key_set = set(existing_keys.apply("|".join, axis=1))
            keep_mask = ~new_keys.apply("|".join, axis=1).isin(existing_key_set)
            combined_domestic = combined_domestic.loc[keep_mask].copy()

        if not combined_domestic.empty:
            combined_domestic.to_csv(outdir / "fact_result_simple_ingested.csv", index=False)

        logger.info("Domestic ingest finished | deduped rows written: %d", len(combined_domestic))

    # ----- UEFA: same ``source_league_name`` keys as ScraperFC comps.yaml (SofaScore + FBref both listed) -----
    if not args.skip_european:
        fd_token = os.getenv("FOOTBALL_DATA_API_TOKEN", "").strip()
        if args.european_provider == "football_data_org" and not fd_token:
            logger.error("Skipping UEFA: --european-provider football_data_org requires FOOTBALL_DATA_API_TOKEN")
        else:
            processed_euro = (
                set()
                if match_calendar_year is not None or args.ignore_euro_progress
                else euro_load_progress(outdir)
            )
            if match_calendar_year is not None:
                logger.info(
                    "UEFA: match calendar year=%s (UTC); ignoring ingestion_progress_euro.json",
                    match_calendar_year,
                )
            existing_euro_fact = _safe_read_csv(outdir / "fact_result_simple_ingested_euro.csv")
            existing_euro_summary = _safe_read_csv(outdir / "ingestion_summary_euro.csv")
            existing_euro_unmatched = _safe_read_csv(outdir / "unmatched_clubs_euro.csv")
            existing_euro_created = _safe_read_csv(outdir / "created_clubs_euro.csv")

            euro_metadata = [cfg for cfg in LEAGUE_METADATA if cfg.get("league_key") in UEFA_LEAGUE_KEYS]
            logger.info(
                "UEFA ingest | european_provider=%s backend=%s | competitions=%d",
                args.european_provider,
                args.backend,
                len(euro_metadata),
            )

            all_euro_fact_rows: list[dict[str, Any]] = []

            for league_config in euro_metadata:
                league_code = league_config["league_key"]
                src = league_config["source_league_name"]
                provider = args.european_provider

                if league_code in processed_euro:
                    logger.info("Skipping already processed UEFA league: %s", league_code)
                    continue

                be = resolve_backend_for_league(src, args.backend)
                try:
                    if args.european_provider == "football_data_org":
                        fact_rows, unmatched, created, dim_club, dim_country = ingest_league_football_data_org(
                            league_config=league_config,
                            fact=fact,
                            dim_club=dim_club,
                            dim_country=dim_country,
                            dim_season=dim_season,
                            token=fd_token,
                            match_calendar_year=match_calendar_year,
                        )
                        provider = "football_data_org"
                    else:
                        if be == "fbref" and src not in _FBREF_COMPS:
                            logger.warning("%s: FBref not in comps — skipping UEFA league", league_code)
                            row_e = pd.DataFrame(
                                [
                                    {
                                        "league_code": league_code,
                                        "source_league_name": src,
                                        "country": league_config["country"],
                                        "matches_ingested": 0,
                                        "unmatched_clubs": 0,
                                        "created_clubs": 0,
                                        "status": "skipped: no FBREF comp",
                                        "provider": "fbref",
                                    }
                                ]
                            )
                            existing_euro_summary = pd.concat([existing_euro_summary, row_e], ignore_index=True)
                            existing_euro_summary.to_csv(outdir / "ingestion_summary_euro.csv", index=False)
                            continue
                        if be == "sofascore" and src not in _SOFACOMPS:
                            logger.warning("%s: SofaScore not in comps — skipping UEFA league", league_code)
                            row_e = pd.DataFrame(
                                [
                                    {
                                        "league_code": league_code,
                                        "source_league_name": src,
                                        "country": league_config["country"],
                                        "matches_ingested": 0,
                                        "unmatched_clubs": 0,
                                        "created_clubs": 0,
                                        "status": "skipped: no SOFASCORE comp",
                                        "provider": "sofascore",
                                    }
                                ]
                            )
                            existing_euro_summary = pd.concat([existing_euro_summary, row_e], ignore_index=True)
                            existing_euro_summary.to_csv(outdir / "ingestion_summary_euro.csv", index=False)
                            continue

                        if be == "sofascore":
                            fact_rows, unmatched, created, dim_club, dim_country = ingest_league(
                                league_config=league_config,
                                fact=fact,
                                dim_club=dim_club,
                                dim_country=dim_country,
                                dim_season=dim_season,
                                ss=ss,
                                match_calendar_year=match_calendar_year,
                            )
                        else:
                            fact_rows, unmatched, created, dim_club, dim_country = ingest_league_fbref(
                                league_config=league_config,
                                fact=fact,
                                dim_club=dim_club,
                                dim_country=dim_country,
                                dim_season=dim_season,
                                fb=fb,
                                match_calendar_year=match_calendar_year,
                            )
                        provider = be

                    all_euro_fact_rows.extend(fact_rows)
                    if unmatched:
                        existing_euro_unmatched = pd.concat(
                            [existing_euro_unmatched, pd.DataFrame(unmatched)], ignore_index=True
                        )
                        existing_euro_unmatched.drop_duplicates().to_csv(outdir / "unmatched_clubs_euro.csv", index=False)
                    if created:
                        existing_euro_created = pd.concat(
                            [existing_euro_created, pd.DataFrame(created)], ignore_index=True
                        )
                        existing_euro_created.drop_duplicates().to_csv(outdir / "created_clubs_euro.csv", index=False)

                    dim_club.to_csv(outdir / "dim_club_updated.csv", index=False)
                    dim_country.to_csv(outdir / "dim_country_updated.csv", index=False)

                    row_ok = pd.DataFrame(
                        [
                            {
                                "league_code": league_code,
                                "source_league_name": src,
                                "country": league_config["country"],
                                "matches_ingested": len(fact_rows),
                                "unmatched_clubs": len(unmatched),
                                "created_clubs": len(created),
                                "status": "success",
                                "provider": provider,
                            }
                        ]
                    )
                    existing_euro_summary = pd.concat([existing_euro_summary, row_ok], ignore_index=True)
                    existing_euro_summary.to_csv(outdir / "ingestion_summary_euro.csv", index=False)

                    if match_calendar_year is None:
                        processed_euro.add(league_code)
                        euro_save_progress(outdir, processed_euro)

                except Exception as exc:
                    logger.error("UEFA league error %s: %s", league_code, exc)
                    row_err = pd.DataFrame(
                        [
                            {
                                "league_code": league_code,
                                "source_league_name": src,
                                "country": league_config["country"],
                                "matches_ingested": 0,
                                "unmatched_clubs": 0,
                                "created_clubs": 0,
                                "status": f"error: {str(exc)[:100]}",
                                "provider": provider if args.european_provider == "football_data_org" else be,
                            }
                        ]
                    )
                    existing_euro_summary = pd.concat([existing_euro_summary, row_err], ignore_index=True)
                    existing_euro_summary.to_csv(outdir / "ingestion_summary_euro.csv", index=False)

            if all_euro_fact_rows:
                new_euro = pd.DataFrame(all_euro_fact_rows)
                combined_euro = pd.concat([existing_euro_fact, new_euro], ignore_index=True).drop_duplicates()
            elif not existing_euro_fact.empty:
                combined_euro = existing_euro_fact.drop_duplicates()
            else:
                combined_euro = pd.DataFrame()

            if not combined_euro.empty:
                existing_keys = fact[dedupe_cols].fillna("").astype(str)
                new_keys = combined_euro[dedupe_cols].fillna("").astype(str)
                existing_set = set(existing_keys.apply("|".join, axis=1))
                keep_mask = ~new_keys.apply("|".join, axis=1).isin(existing_set)
                combined_euro = combined_euro.loc[keep_mask].copy()
                combined_euro.to_csv(outdir / "fact_result_simple_ingested_euro.csv", index=False)

            logger.info("UEFA ingest finished | deduped euro rows: %d", len(combined_euro))

    logger.info(
        "UNIFIED INGESTION COMPLETE | domestic_rows=%d euro_rows=%d",
        len(combined_domestic),
        len(combined_euro),
    )


if __name__ == "__main__":
    main()
