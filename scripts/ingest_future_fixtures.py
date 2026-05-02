"""
ingest_future_fixtures.py

For every league in config that is SofaScore-backed and listed in LEAGUES, pull upcoming
fixtures (default: scheduled calendar window), resolve home/away names to club_id using the
same rules as ingest_leagues_from_config, and write output/fact_fixture_upcoming.csv.

Does not append to fact_result; optional writes refresh dim_club_updated / dim_country_updated
when new clubs or countries are created (same layout as domestic ingest).
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from ScraperFC.sofascore import comps as SOFASCORE_COMPS

import sys

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from config.football_ingestion_config import LEAGUE_METADATA, LEAGUES  # noqa: E402
from scripts.club_identity import build_club_lookup, resolve_or_create_club  # noqa: E402
from scripts.sofascore_future_helpers import (  # noqa: E402
    fetch_events_next,
    fetch_rounds,
    fetch_scheduled_window,
    normalize_event_row,
    try_season_id_for_year,
    try_tournament_id_and_slug,
    want_fixture_status,
)

REQUEST_SLEEP_SECONDS = 0.35

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def load_dim_tables(outdir: Path, dim_club_path: Path, dim_country_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    club_p = outdir / "dim_club_updated.csv" if (outdir / "dim_club_updated.csv").exists() else dim_club_path
    country_p = (
        outdir / "dim_country_updated.csv" if (outdir / "dim_country_updated.csv").exists() else dim_country_path
    )
    dim_club = pd.read_csv(club_p)
    dim_country = pd.read_csv(country_p)
    logger.info("dim_club from %s (%d rows)", club_p, len(dim_club))
    logger.info("dim_country from %s (%d rows)", country_p, len(dim_country))
    return dim_club, dim_country


def ensure_country(dim_country: pd.DataFrame, country_name: str, country_code: str) -> tuple[int, pd.DataFrame]:
    if country_name in dim_country["country_name"].values:
        cid = int(dim_country[dim_country["country_name"] == country_name]["country_id"].iloc[0])
        return cid, dim_country
    next_country_id = int(dim_country["country_id"].max()) + 1
    row = pd.DataFrame([{"country_id": next_country_id, "country_name": country_name, "country_code": country_code}])
    dim_country = pd.concat([dim_country, row], ignore_index=True)
    return next_country_id, dim_country


def metadata_rows_for_sofascore_leagues() -> list[dict]:
    """Leagues in LEAGUES with a SOFASCORE tournament id in ScraperFC comps."""
    wanted = set(LEAGUES)
    out: list[dict] = []
    for meta in LEAGUE_METADATA:
        sln = meta["source_league_name"]
        if sln not in wanted:
            continue
        if sln not in SOFASCORE_COMPS or "SOFASCORE" not in SOFASCORE_COMPS[sln]:
            continue
        out.append(meta)
    return out


def fetch_raw_events(
    source_league_name: str,
    *,
    method: str,
    days: int,
    year: str | None,
    max_round: int,
) -> list[dict]:
    resolved = try_tournament_id_and_slug(source_league_name)
    if resolved is None:
        return []
    tid, _slug = resolved
    if method in ("events_next", "rounds"):
        if not year:
            return []
        sid = try_season_id_for_year(source_league_name, year)
        if sid is None:
            return []
        if method == "events_next":
            return fetch_events_next(tid, sid)
        return fetch_rounds(tid, sid, max_round)
    return fetch_scheduled_window(tid, days)


def main() -> None:
    p = argparse.ArgumentParser(description="Ingest upcoming SofaScore fixtures into fact_fixture_upcoming.csv")
    p.add_argument("--outdir", default="output", help="Output directory (repo-relative unless absolute)")
    p.add_argument("--dim-club", default="output/dim_club.csv")
    p.add_argument("--dim-country", default="output/dim_country.csv")
    p.add_argument(
        "--method",
        choices=("scheduled", "events_next", "rounds"),
        default="scheduled",
        help="Fixture discovery method (default scheduled). events_next/rounds use --year for all leagues.",
    )
    p.add_argument("--days", type=int, default=14, help="scheduled: today through N-1 future days")
    p.add_argument("--year", type=str, default=None, help="SofaScore season label for events_next / rounds")
    p.add_argument("--max-round", type=int, default=40, help="rounds: fetch rounds 1..N")
    p.add_argument("--include-live", action="store_true", help='Include status "inprogress"')
    p.add_argument(
        "--no-create-missing-clubs",
        action="store_true",
        help="Do not create dim clubs; leave home_club_id/away_club_id empty when unresolved",
    )
    p.add_argument("--sleep", type=float, default=REQUEST_SLEEP_SECONDS, help="Pause between leagues (seconds)")
    p.add_argument("--dry-run", action="store_true", help="Fetch and resolve only; do not write CSVs")
    args = p.parse_args()

    outdir = Path(args.outdir)
    if not outdir.is_absolute():
        outdir = (_REPO / outdir).resolve()
    dim_club_path = Path(args.dim_club)
    if not dim_club_path.is_absolute():
        dim_club_path = (_REPO / dim_club_path).resolve()
    dim_country_path = Path(args.dim_country)
    if not dim_country_path.is_absolute():
        dim_country_path = (_REPO / dim_country_path).resolve()

    create_missing = not args.no_create_missing_clubs
    meta_rows = metadata_rows_for_sofascore_leagues()
    logger.info("SofaScore fixture ingest: %d leagues from config", len(meta_rows))

    dim_club, dim_country = load_dim_tables(outdir, dim_club_path, dim_country_path)
    dim_club_dirty = False
    dim_country_dirty = False

    out_rows: list[dict] = []
    unmatched_rows: list[dict] = []
    created_rows: list[dict] = []
    summary_rows: list[dict] = []

    ingested_at = datetime.now(timezone.utc).isoformat()

    for meta in meta_rows:
        source_league_name = meta["source_league_name"]
        league_code = meta["league_key"]
        country_name = meta["country"]
        country_code = meta["country_code"]

        if try_tournament_id_and_slug(source_league_name) is None:
            summary_rows.append(
                {
                    "league_code": league_code,
                    "source_league_name": source_league_name,
                    "status": "skip_no_sofascore_id",
                    "raw_events": 0,
                    "upcoming_rows": 0,
                    "matched_pairs": 0,
                    "partial_or_unmatched_fixtures": 0,
                }
            )
            continue

        raw = fetch_raw_events(
            source_league_name,
            method=args.method,
            days=args.days,
            year=args.year,
            max_round=args.max_round,
        )
        time.sleep(max(0.0, args.sleep))

        prev_country_len = len(dim_country)
        country_id, dim_country = ensure_country(dim_country, country_name, country_code)
        if len(dim_country) != prev_country_len:
            dim_country_dirty = True

        club_lookup = build_club_lookup(dim_club)
        next_club_id = int(dim_club["club_id"].max()) + 1 if len(dim_club) > 0 else 1

        normalized: list[dict] = []
        for ev in raw:
            row = normalize_event_row(ev, source=args.method)
            if row is None:
                continue
            if not want_fixture_status(row["status_type"], include_live=args.include_live):
                continue
            normalized.append(row)

        league_matched_pairs = 0
        league_partial = 0

        for row in normalized:
            home_name = row["home_team"]
            away_name = row["away_team"]

            hm, dim_club, club_lookup, next_club_id = resolve_or_create_club(
                club_name=home_name,
                dim_club=dim_club,
                club_lookup=club_lookup,
                country_id=country_id,
                next_club_id=next_club_id,
                create_missing=create_missing,
                merge_on_fuzzy=True,
            )
            am, dim_club, club_lookup, next_club_id = resolve_or_create_club(
                club_name=away_name,
                dim_club=dim_club,
                club_lookup=club_lookup,
                country_id=country_id,
                next_club_id=next_club_id,
                create_missing=create_missing,
                merge_on_fuzzy=True,
            )

            if hm.was_created:
                dim_club_dirty = True
                created_rows.append(
                    {
                        "league_code": league_code,
                        "club_name": home_name,
                        "created_club_id": hm.club_id,
                        "suggested_existing_match": hm.suggestion,
                        "suggestion_score": hm.suggestion_score,
                    }
                )
            if am.was_created:
                dim_club_dirty = True
                created_rows.append(
                    {
                        "league_code": league_code,
                        "club_name": away_name,
                        "created_club_id": am.club_id,
                        "suggested_existing_match": am.suggestion,
                        "suggestion_score": am.suggestion_score,
                    }
                )

            home_id = hm.club_id
            away_id = am.club_id
            if home_id is not None and away_id is not None:
                league_matched_pairs += 1
            else:
                league_partial += 1
                unmatched_rows.append(
                    {
                        "league_code": league_code,
                        "source_league_name": source_league_name,
                        "sofascore_event_id": row["match_id"],
                        "kickoff_utc": row["kickoff_utc"],
                        "home_team": home_name,
                        "away_team": away_name,
                        "home_club_id": home_id,
                        "away_club_id": away_id,
                        "home_suggestion": hm.suggestion,
                        "home_suggestion_score": hm.suggestion_score,
                        "away_suggestion": am.suggestion,
                        "away_suggestion_score": am.suggestion_score,
                    }
                )

            out_rows.append(
                {
                    "sofascore_event_id": row["match_id"],
                    "kickoff_timestamp": row["kickoff_timestamp"],
                    "kickoff_utc": row["kickoff_utc"],
                    "league_code": league_code,
                    "source_league_name": source_league_name,
                    "country_id": country_id,
                    "home_club_id": home_id,
                    "away_club_id": away_id,
                    "home_team_raw": home_name,
                    "away_team_raw": away_name,
                    "status_type": row["status_type"],
                    "status_description": row["status_description"],
                    "fetch_method": row["fetch_method"],
                    "ingested_at": ingested_at,
                }
            )

        summary_rows.append(
            {
                "league_code": league_code,
                "source_league_name": source_league_name,
                "status": "ok",
                "raw_events": len(raw),
                "upcoming_rows": len(normalized),
                "matched_pairs": league_matched_pairs,
                "partial_or_unmatched_fixtures": league_partial,
            }
        )

        logger.info(
            "%s (%s): raw=%d upcoming=%d matched_pairs=%d unmatched_fixture_sides=%d",
            league_code,
            source_league_name,
            len(raw),
            len(normalized),
            league_matched_pairs,
            league_partial,
        )

    df = pd.DataFrame(out_rows)
    if not df.empty:
        df = df.sort_values(["kickoff_timestamp", "sofascore_event_id"], na_position="last").reset_index(drop=True)
        df.insert(0, "fixture_id", range(1, len(df) + 1))

    summary_df = pd.DataFrame(summary_rows)
    unmatched_df = pd.DataFrame(unmatched_rows)
    created_df = pd.DataFrame(created_rows)

    logger.info("Total fixture rows: %d", len(df))

    if args.dry_run:
        return

    outdir.mkdir(parents=True, exist_ok=True)
    fact_path = outdir / "fact_fixture_upcoming.csv"
    df.to_csv(fact_path, index=False, encoding="utf-8")
    logger.info("Wrote %s", fact_path)

    summary_df.to_csv(outdir / "future_fixtures_ingest_summary.csv", index=False, encoding="utf-8")
    if not unmatched_df.empty:
        unmatched_df.to_csv(outdir / "future_fixtures_unmatched.csv", index=False, encoding="utf-8")
    elif (outdir / "future_fixtures_unmatched.csv").exists():
        (outdir / "future_fixtures_unmatched.csv").unlink()

    if not created_df.empty:
        created_df.to_csv(outdir / "future_fixtures_created_clubs.csv", index=False, encoding="utf-8")
    elif (outdir / "future_fixtures_created_clubs.csv").exists():
        (outdir / "future_fixtures_created_clubs.csv").unlink()

    if dim_club_dirty:
        dim_club.to_csv(outdir / "dim_club_updated.csv", index=False, encoding="utf-8")
        logger.info("Updated %s", outdir / "dim_club_updated.csv")
    if dim_country_dirty:
        dim_country.to_csv(outdir / "dim_country_updated.csv", index=False, encoding="utf-8")
        logger.info("Updated %s", outdir / "dim_country_updated.csv")


if __name__ == "__main__":
    main()
