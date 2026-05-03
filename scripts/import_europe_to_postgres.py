#!/usr/bin/env python3
"""
Load `output/europe` CSV exports into Postgres tables used when DATABASE_URL is set.

Tables (prefix fr_ = football ratings):
  fr_teams              <- europe_teams.csv
  fr_weekly_ratings     <- europe_weekly_ratings.csv (chunked)
  fr_match_results      <- europe_match_results.csv (chunked)
  fr_europe_ratings     <- europe_ratings.csv

Requires DATABASE_URL (e.g. Supabase session pooler connection string).

Example:

    set DATABASE_URL=postgresql://...
    python scripts/import_europe_to_postgres.py

Optional:

    set FOOTBALL_OUTPUT_EUROPE_DIR=C:\\path\\to\\europe
    set FOOTBALL_PG_INSERT_BATCH=3000   # rows per DB batch if commits still fail (default 5000)
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _europe_dir() -> Path:
    raw = os.environ.get("FOOTBALL_OUTPUT_EUROPE_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _repo_root() / "output" / "europe"


def main() -> int:
    root = _repo_root()
    try:
        from dotenv import load_dotenv

        load_dotenv(root / ".env")
    except ImportError:
        pass

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        print(
            "ERROR: DATABASE_URL is not set. Add it to .env at the repo root or export it in your shell.",
            file=sys.stderr,
        )
        return 1
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    europe = _europe_dir()
    print(f"Europe CSV dir: {europe}")

    import pandas as pd

    # NullPool: no pooled connections — avoids PendingRollbackError after a failed/chunk-aborted
    # INSERT (Ctrl+C, timeout) leaves a connection "dirty"; pool reuse then refuses to proceed.
    # connect_timeout avoids hanging forever if the host is unreachable (seconds).
    engine = create_engine(
        url,
        poolclass=NullPool,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 60},
    )

    teams_path = europe / "europe_teams.csv"
    weekly_path = europe / "europe_weekly_ratings.csv"
    matches_path = europe / "europe_match_results.csv"
    ratings_path = europe / "europe_ratings.csv"

    for p in (teams_path, weekly_path, matches_path, ratings_path):
        if not p.is_file():
            print(f"ERROR: missing {p}", file=sys.stderr)
            return 1

    # --- teams (small — avoid method='multi' first insert quirks on some remote hosts) ---
    t0 = time.monotonic()
    print("Postgres: connecting (first contact can take 30–120s on slow networks)…", flush=True)
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
    print(f"Postgres: OK ({time.monotonic() - t0:.1f}s)", flush=True)

    print(f"Reading {teams_path.name}…", flush=True)
    df_teams = pd.read_csv(teams_path)
    print(f"  loaded {len(df_teams):,} rows from CSV ({time.monotonic() - t0:.1f}s since start)", flush=True)

    print("Uploading fr_teams…", flush=True)
    t_upload = time.monotonic()
    df_teams.to_sql("fr_teams", engine, if_exists="replace", index=False)
    print(
        f"fr_teams: {len(df_teams):,} rows uploaded ({time.monotonic() - t_upload:.1f}s for INSERT)",
        flush=True,
    )

    # --- weekly (chunked) ---
    print(
        "fr_weekly_ratings: starting (largest table — first chunk can take several minutes over the network)…",
        flush=True,
    )
    first = True
    chunk_size = 80_000
    total_w = 0
    chunk_i = 0
    dtype_kw = {"dtype": {"country_name": "string", "team_name": "string"}, "low_memory": False}
    # Small rows-per-commit avoids giant "multi" INSERTs that Supabase/psycopg2 often rejects or
    # drops mid-flight — that leaves SQLAlchemy with PendingRollbackError on commit.
    sql_batch = max(500, int(os.environ.get("FOOTBALL_PG_INSERT_BATCH", "5000")))
    print(f"  (Postgres insert batch size: {sql_batch:,} rows per commit)", flush=True)
    for chunk in pd.read_csv(weekly_path, chunksize=chunk_size, **dtype_kw):
        chunk_i += 1
        chunk.to_sql(
            "fr_weekly_ratings",
            engine,
            if_exists="replace" if first else "append",
            index=False,
            chunksize=sql_batch,
        )
        total_w += len(chunk)
        first = False
        print(f"  fr_weekly_ratings chunk {chunk_i}: {total_w:,} cumulative rows", flush=True)
    print(f"fr_weekly_ratings: {total_w:,} rows total", flush=True)

    # --- matches ---
    print("fr_match_results: starting…", flush=True)
    first = True
    total_m = 0
    m_chunk_i = 0
    for chunk in pd.read_csv(matches_path, chunksize=100_000):
        m_chunk_i += 1
        chunk.to_sql(
            "fr_match_results",
            engine,
            if_exists="replace" if first else "append",
            index=False,
            chunksize=sql_batch,
        )
        total_m += len(chunk)
        first = False
        print(f"  fr_match_results chunk {m_chunk_i}: {total_m:,} cumulative rows", flush=True)
    print(f"fr_match_results: {total_m:,} rows total", flush=True)

    # --- europe_ratings snapshot ---
    df_rat = pd.read_csv(
        ratings_path,
        dtype={"country_name": "string", "team_name": "string"},
        low_memory=False,
    )
    df_rat.to_sql("fr_europe_ratings", engine, if_exists="replace", index=False)
    print(f"fr_europe_ratings: {len(df_rat):,} rows")

    # --- indexes ---
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
    print("Indexes created.")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
