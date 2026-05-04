#!/usr/bin/env python3
"""
Load `output/europe` CSV exports into Postgres tables used when DATABASE_URL is set.

Tables (prefix fr_ = football ratings):
  fr_teams              <- europe_teams.csv
  fr_weekly_ratings     <- europe_weekly_ratings.csv (or .txt; chunked)
  fr_match_results      <- europe_match_results.csv (chunked)
  fr_europe_ratings     <- europe_ratings.csv

Requires DATABASE_URL. Prefer Supabase **Session pooler** URI (`*.pooler.supabase.com`) if direct
`db.<project>.supabase.co` fails DNS from Python on Windows.

Example:

    set DATABASE_URL=postgresql://...
    python scripts/import_europe_to_postgres.py

Optional:

    set FOOTBALL_OUTPUT_EUROPE_DIR=C:\\path\\to\\europe
    set FOOTBALL_PG_INSERT_BATCH=3000   # rows per DB batch if commits still fail (default 5000)
"""

from __future__ import annotations

import os
import socket
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _europe_dir() -> Path:
    raw = os.environ.get("FOOTBALL_OUTPUT_EUROPE_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _repo_root() / "output" / "europe"


def _resolve_weekly_ratings_path(europe: Path) -> Path:
    """Prefer ``europe_weekly_ratings.csv``; accept ``.txt`` (e.g. Excel save-as)."""
    csv_p = europe / "europe_weekly_ratings.csv"
    if csv_p.is_file():
        return csv_p
    txt_p = europe / "europe_weekly_ratings.txt"
    if txt_p.is_file():
        return txt_p
    return csv_p


def _postgresql_host_port(database_url: str) -> tuple[str | None, int]:
    u = database_url.strip()
    for prefix in ("postgresql+psycopg2://", "postgres://"):
        if u.startswith(prefix):
            u = "postgresql://" + u[len(prefix) :]
            break
    parsed = urlparse(u)
    port = parsed.port or 5432
    return parsed.hostname, port


def _print_dns_lookup_hints(host: str | None) -> None:
    print(
        "\n"
        "Hostname lookup failed from Python/psycopg2 (libpq). On Windows, nslookup can still succeed\n"
        "because it uses a different resolver path than Python — especially with IPv6-only DB hosts.\n",
        file=sys.stderr,
        flush=True,
    )
    print(
        "What usually fixes it:\n"
        "  • Supabase → Project Settings → Database → Connection string:\n"
        "    choose Session pooler (URI host is often aws-0-….pooler.supabase.com or similar).\n"
        "    Paste that into DATABASE_URL — avoid raw db.<ref>.supabase.co if Python DNS fails.\n"
        "  • Quick check:  python -c \"import socket; "
        "print(socket.getaddrinfo('YOUR_HOST', 5432))\"\n",
        file=sys.stderr,
        flush=True,
    )
    if host:
        print(f"  • Your URL host was: {host}\n", file=sys.stderr, flush=True)
    print(
        "  • Toggle VPN off, try another network/DNS (1.1.1.1 / 8.8.8.8), or confirm IPv6 routing.\n",
        file=sys.stderr,
        flush=True,
    )


def _load_repo_dotenv(repo_root: Path) -> None:
    """Load repo-root `.env` into os.environ (DATABASE_URL etc.)."""
    path = repo_root / ".env"
    if not path.is_file():
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(path)
        return
    except ImportError:
        print(
            "WARNING: python-dotenv is not installed — using a minimal .env parser.\n"
            "  Install for full compatibility: pip install python-dotenv",
            file=sys.stderr,
            flush=True,
        )
    # Fallback: KEY=VALUE lines only (utf-8-sig strips BOM on first line).
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        if key:
            os.environ.setdefault(key, val)


def main() -> int:
    root = _repo_root()
    _load_repo_dotenv(root)

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        expected = root / ".env"
        print(
            "ERROR: DATABASE_URL is not set.\n"
            f"  Expected repo-root .env at: {expected}\n"
            "  Add DATABASE_URL=postgresql://... there, or export DATABASE_URL in your shell.",
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
        connect_args={
            "connect_timeout": 60,
            "client_encoding": "utf8",
        },
    )

    teams_path = europe / "europe_teams.csv"
    weekly_path = _resolve_weekly_ratings_path(europe)
    matches_path = europe / "europe_match_results.csv"
    ratings_path = europe / "europe_ratings.csv"

    for p in (teams_path, matches_path, ratings_path):
        if not p.is_file():
            print(f"ERROR: missing {p}", file=sys.stderr)
            return 1
    if not weekly_path.is_file():
        print(
            "ERROR: missing europe_weekly_ratings.csv or europe_weekly_ratings.txt under:\n"
            f"  {europe}",
            file=sys.stderr,
        )
        return 1

    # --- teams (small — avoid method='multi' first insert quirks on some remote hosts) ---
    t0 = time.monotonic()
    print("Postgres: connecting (first contact can take 30–120s on slow networks)…", flush=True)
    host, port = _postgresql_host_port(url)
    if host:
        try:
            socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as dns_exc:
            print(
                f"WARNING: Python cannot resolve {host!r}:{port} ({dns_exc}). "
                "Import will likely fail — try Session pooler URI from Supabase.",
                file=sys.stderr,
                flush=True,
            )
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        orig_msg = str(getattr(exc, "orig", exc) or exc).lower()
        if "could not translate host name" in orig_msg or "name or service not known" in orig_msg:
            _print_dns_lookup_hints(host)
        raise
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
    print(
        f"fr_weekly_ratings: {total_w:,} rows total (matches CSV — full weekly history if export was full)",
        flush=True,
    )

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
