#!/usr/bin/env python3
"""
Rebuild Europe outputs for a calendar year (optional), then push **only that year's slice**
of bulk tables to Supabase while **preserving older rows**.

Uses ``DATABASE_URL`` (same as ``import_europe_to_postgres.py``). Prefer Supabase Session pooler URI.

What runs (after optional pipeline):

1. **fr_teams** — replaced from full ``europe_teams.csv`` (small snapshot).
2. **fr_europe_ratings** — replaced from full ``europe_ratings.csv`` (latest-week leaderboard snapshot).
3. **fr_weekly_ratings** — existing rows for the ISO-year prefix are **merged** with the CSV slice
   (append then dedupe on ``(week, pid)``; CSV wins on conflicts), then that merged year slice replaces
   the previous year rows in the database (no silent loss if the CSV is missing rows still in Postgres).
4. **fr_match_results** — same idea for ``calendar_year`` on ``match_date``: merge DB + CSV on the
   natural key, then replace the year’s rows.

Older calendar years / ISO-year prefixes already in Postgres are left untouched (append-style refresh
for the selected year only).

Examples::

    # 1) Sync DB only (you already ran the pipeline for full europe CSVs)
    python scripts/sync_current_year_append_supabase.py --calendar-year 2026

    # 2) Ingest current-year fixtures only, then resolve + Glicko + sync this year’s slices
    python scripts/sync_current_year_append_supabase.py --calendar-year 2026 --run-pipeline

Typical first-time load of **all** history is still ``scripts/import_europe_to_postgres.py``.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _europe_dir(repo_root: Path) -> Path:
    raw = os.environ.get("FOOTBALL_OUTPUT_EUROPE_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return repo_root / "output" / "europe"


def _resolve_weekly_ratings_path(europe: Path) -> Path:
    csv_p = europe / "europe_weekly_ratings.csv"
    if csv_p.is_file():
        return csv_p
    txt_p = europe / "europe_weekly_ratings.txt"
    if txt_p.is_file():
        return txt_p
    return csv_p


def _load_repo_dotenv(repo_root: Path) -> None:
    path = repo_root / ".env"
    if not path.is_file():
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(path)
        return
    except ImportError:
        pass
    try:
        raw_txt = path.read_text(encoding="utf-8-sig")
    except OSError:
        return
    for ln in raw_txt.splitlines():
        line = ln.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        if key:
            os.environ.setdefault(key, val)


def _postgresql_host_port(database_url: str) -> tuple[str | None, int]:
    u = database_url.strip()
    for prefix in ("postgresql+psycopg2://", "postgres://"):
        if u.startswith(prefix):
            u = "postgresql://" + u[len(prefix) :]
            break
    parsed = urlparse(u)
    port = parsed.port or 5432
    return parsed.hostname, port


def _print_dns_hints(host: str | None) -> None:
    print(
        "\nPython DNS failed for DATABASE_URL host — try Supabase **Session pooler** URI "
        "(host often *.pooler.supabase.com).\n",
        file=sys.stderr,
        flush=True,
    )
    if host:
        print(f"  Host was: {host}\n", file=sys.stderr, flush=True)


def _make_engine(url: str):
    # Explicit UTF-8 avoids psycopg2 "server didn't return client encoding" with some poolers (Supabase).
    return create_engine(
        url,
        poolclass=NullPool,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 60,
            "client_encoding": "utf8",
        },
    )


def _filter_weekly_iso_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    w = df["week"].astype(int)
    return df.loc[w // 100 == year].copy()


def _filter_matches_calendar_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    md = pd.to_datetime(df["match_date"], errors="coerce")
    return df.loc[md.dt.year == year].copy()


def _merge_year_union(existing: pd.DataFrame, incoming: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Stack DB rows then CSV rows; ``drop_duplicates(keep='last')`` so CSV wins — union, no rows dropped before merge."""
    if existing.empty:
        return incoming.copy() if not incoming.empty else existing
    if incoming.empty:
        return existing.copy()
    keys_use = [k for k in keys if k in existing.columns and k in incoming.columns]
    comb = pd.concat([existing, incoming], ignore_index=True, sort=False)
    if keys_use:
        return comb.drop_duplicates(subset=keys_use, keep="last").reset_index(drop=True)
    return comb


def _run_pipeline(repo_root: Path, *, year: int, skip_future_fixtures: bool) -> None:
    py = sys.executable
    cmd = [
        py,
        str(repo_root / "scripts" / "run_europe_ratings_pipeline.py"),
        "--match-calendar-year",
        str(year),
    ]
    if skip_future_fixtures:
        cmd.append("--skip-future-fixtures")
    print("+ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=str(repo_root), check=True)


def main() -> int:
    repo_root = _repo_root()
    _load_repo_dotenv(repo_root)

    p = argparse.ArgumentParser(
        description="Append-style Supabase sync: refresh one calendar year's slices + latest snapshots."
    )
    p.add_argument(
        "--calendar-year",
        type=int,
        default=None,
        help="Calendar year for match_results filter and weekly yyyyww ISO-year prefix (default: current UTC year).",
    )
    p.add_argument(
        "--run-pipeline",
        action="store_true",
        help="Run run_europe_ratings_pipeline.py with --match-calendar-year first (ingest → resolve → Glicko).",
    )
    p.add_argument(
        "--skip-future-fixtures",
        action="store_true",
        help="Forward to pipeline when --run-pipeline is set.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row counts and SQL actions only; do not write to Postgres.",
    )
    args = p.parse_args()

    year = args.calendar_year if args.calendar_year is not None else datetime.now(timezone.utc).year

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url and not args.dry_run:
        print(
            "ERROR: DATABASE_URL is not set.\n"
            f"  Add it to {repo_root / '.env'} or export it.",
            file=sys.stderr,
        )
        return 1

    if args.run_pipeline:
        print(f"Running Europe pipeline (ingest calendar year {year})…", flush=True)
        _run_pipeline(repo_root, year=year, skip_future_fixtures=args.skip_future_fixtures)

    europe = _europe_dir(repo_root)
    teams_path = europe / "europe_teams.csv"
    weekly_path = _resolve_weekly_ratings_path(europe)
    matches_path = europe / "europe_match_results.csv"
    ratings_path = europe / "europe_ratings.csv"

    for path in (teams_path, matches_path, ratings_path):
        if not path.is_file():
            print(f"ERROR: missing {path}", file=sys.stderr)
            return 1
    if not weekly_path.is_file():
        print(
            "ERROR: missing europe_weekly_ratings.csv or europe_weekly_ratings.txt under:\n"
            f"  {europe}",
            file=sys.stderr,
        )
        return 1

    print(f"Europe CSV dir: {europe}", flush=True)

    df_teams = pd.read_csv(teams_path)
    df_rat = pd.read_csv(
        ratings_path,
        dtype={"country_name": "string", "team_name": "string"},
        low_memory=False,
    )

    dtype_weekly = {"dtype": {"country_name": "string", "team_name": "string"}, "low_memory": False}
    df_weekly_full = pd.read_csv(weekly_path, **dtype_weekly)
    df_matches_full = pd.read_csv(matches_path, low_memory=False)

    df_week_y = _filter_weekly_iso_year(df_weekly_full, year)
    df_match_y = _filter_matches_calendar_year(df_matches_full, year)

    print(
        f"Calendar year {year}: weekly slice rows={len(df_week_y):,} | "
        f"match_results slice rows={len(df_match_y):,}",
        flush=True,
    )
    print(
        f"Full CSV rows: teams={len(df_teams):,} | europe_ratings={len(df_rat):,} | "
        f"weekly={len(df_weekly_full):,} | matches={len(df_matches_full):,}",
        flush=True,
    )

    if args.dry_run:
        print("Dry run — no database writes.", flush=True)
        return 0

    sql_batch = max(500, int(os.environ.get("FOOTBALL_PG_INSERT_BATCH", "5000")))
    engine = _make_engine(url)
    host, port = _postgresql_host_port(url)
    if host:
        try:
            socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as dns_exc:
            print(f"WARNING: DNS failed for {host!r}:{port} ({dns_exc}).", file=sys.stderr, flush=True)
            _print_dns_hints(host)

    t0 = time.monotonic()
    print("Postgres: connecting…", flush=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        orig = str(getattr(exc, "orig", exc) or exc).lower()
        if "could not translate host name" in orig or "name or service not known" in orig:
            _print_dns_hints(host)
        raise
    print(f"Postgres: OK ({time.monotonic() - t0:.1f}s)", flush=True)

    # --- snapshot tables (small / leaderboard) ---
    print("fr_teams: replace from full CSV…", flush=True)
    df_teams.to_sql("fr_teams", engine, if_exists="replace", index=False)
    print(f"fr_europe_ratings: replace from full CSV ({len(df_rat):,} rows)…", flush=True)
    df_rat.to_sql("fr_europe_ratings", engine, if_exists="replace", index=False)

    # --- bulk: merge DB year + CSV year (append CSV then dedupe; CSV wins), then replace year slice ---
    print("Postgres: reading existing year slices to merge with CSV…", flush=True)
    with engine.connect() as conn:
        df_db_week = pd.read_sql(
            text("SELECT * FROM fr_weekly_ratings WHERE (week / 100) = :y"),
            conn,
            params={"y": year},
        )
        df_db_match = pd.read_sql(
            text(
                "SELECT * FROM fr_match_results WHERE EXTRACT(YEAR FROM CAST(match_date AS DATE)) = :y"
            ),
            conn,
            params={"y": year},
        )

    db_m = df_db_match.copy()
    csv_m = df_match_y.copy()
    if not db_m.empty and "match_date" in db_m.columns:
        db_m["match_date"] = pd.to_datetime(db_m["match_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if not csv_m.empty and "match_date" in csv_m.columns:
        csv_m["match_date"] = pd.to_datetime(csv_m["match_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df_db_w = df_db_week.copy()
    df_csv_w = df_week_y.copy()
    for d in (df_db_w, df_csv_w):
        if d.empty:
            continue
        if "pid" in d.columns:
            d["pid"] = pd.to_numeric(d["pid"], errors="coerce")
        if "week" in d.columns:
            d["week"] = pd.to_numeric(d["week"], errors="coerce")

    df_match_merged = _merge_year_union(
        db_m,
        csv_m,
        ["match_date", "home_team_id", "away_team_id", "home_goals", "away_goals"],
    )
    df_week_merged = _merge_year_union(df_db_w, df_csv_w, ["week", "pid"])

    print(
        f"Year {year} after merge — weekly: DB {len(df_db_week):,} + CSV {len(df_week_y):,} → {len(df_week_merged):,} | "
        f"matches: DB {len(df_db_match):,} + CSV {len(df_match_y):,} → {len(df_match_merged):,}",
        flush=True,
    )

    with engine.begin() as conn:
        r_week = conn.execute(
            text("DELETE FROM fr_weekly_ratings WHERE (week / 100) = :y"),
            {"y": year},
        )
        print(f"fr_weekly_ratings: deleted rows (week ISO-year prefix {year}): {r_week.rowcount}", flush=True)

        r_match = conn.execute(
            text(
                "DELETE FROM fr_match_results WHERE EXTRACT(YEAR FROM CAST(match_date AS DATE)) = :y"
            ),
            {"y": year},
        )
        print(f"fr_match_results: deleted rows (match_date year {year}): {r_match.rowcount}", flush=True)

    if not df_week_merged.empty:
        print(f"fr_weekly_ratings: inserting {len(df_week_merged):,} merged rows…", flush=True)
        df_week_merged.to_sql(
            "fr_weekly_ratings",
            engine,
            if_exists="append",
            index=False,
            chunksize=sql_batch,
        )
    else:
        print("fr_weekly_ratings: merged slice empty — nothing to insert.", flush=True)

    if not df_match_merged.empty:
        print(f"fr_match_results: inserting {len(df_match_merged):,} merged rows…", flush=True)
        df_match_merged.to_sql(
            "fr_match_results",
            engine,
            if_exists="append",
            index=False,
            chunksize=sql_batch,
        )
    else:
        print("fr_match_results: merged slice empty — nothing to insert.", flush=True)

    ddl = [
        "CREATE INDEX IF NOT EXISTS ix_fr_weekly_week ON fr_weekly_ratings (week)",
        "CREATE INDEX IF NOT EXISTS ix_fr_weekly_pid ON fr_weekly_ratings (pid)",
        "CREATE INDEX IF NOT EXISTS ix_fr_weekly_country ON fr_weekly_ratings (country_name)",
        "CREATE INDEX IF NOT EXISTS ix_fr_match_week ON fr_match_results (week)",
        "CREATE INDEX IF NOT EXISTS ix_fr_match_date ON fr_match_results (match_date)",
        "CREATE INDEX IF NOT EXISTS ix_fr_match_home ON fr_match_results (home_team_id)",
        "CREATE INDEX IF NOT EXISTS ix_fr_match_away ON fr_match_results (away_team_id)",
        "CREATE INDEX IF NOT EXISTS ix_fr_ratings_week_pid ON fr_europe_ratings (week, pid)",
    ]
    with engine.begin() as conn:
        for stmt in ddl:
            conn.execute(text(stmt))
    print("Indexes ensured. Done.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
