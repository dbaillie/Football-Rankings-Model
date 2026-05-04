#!/usr/bin/env python3
"""
Pull the **latest** Europe ratings snapshot for one country from Supabase/Postgres
(``fr_europe_ratings``), using the same connection pattern as other sync scripts.

``country_name`` in the database matches the model (e.g. ``scotland``, ``england``) — compare
case-insensitively.

Examples::

    python scripts/export_country_ratings_from_supabase.py --country scotland
    python scripts/export_country_ratings_from_supabase.py --country scotland --out output/scotland_ratings_latest.csv
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

_REPO = Path(__file__).resolve().parents[1]


def _load_dotenv() -> None:
    path = _REPO / ".env"
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


def main() -> int:
    _load_dotenv()
    p = argparse.ArgumentParser(description="Export latest fr_europe_ratings rows for one country.")
    p.add_argument("--country", type=str, required=True, help="country_name value (e.g. scotland, england)")
    p.add_argument("--out", type=str, default="", help="CSV path (default: print head to stdout)")
    p.add_argument("--sort", type=str, default="rating", help="Sort column (default: rating)")
    p.add_argument("--ascending", action="store_true", help="Sort ascending (default: descending)")
    args = p.parse_args()

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 1

    country = args.country.strip()
    engine = create_engine(
        url,
        poolclass=NullPool,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 60, "client_encoding": "utf8"},
    )

    sql = text(
        """
        WITH latest AS (
            SELECT MAX(week) AS w
            FROM fr_europe_ratings
            WHERE LOWER(TRIM(country_name)) = LOWER(TRIM(:c))
        )
        SELECT r.*
        FROM fr_europe_ratings r
        CROSS JOIN latest l
        WHERE r.week = l.w
          AND LOWER(TRIM(r.country_name)) = LOWER(TRIM(:c))
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"c": country})

    print(f"Rows: {len(df):,} | country={country!r} | latest week in slice: ", end="", flush=True)
    if not df.empty and "week" in df.columns:
        print(f"{int(df['week'].max())}", flush=True)
    else:
        print("(none)", flush=True)

    if df.empty:
        print("No rows — check country spelling matches DB (e.g. scotland not Scotland).", flush=True)
        return 0

    sort_col = args.sort if args.sort in df.columns else "rating"
    if sort_col in df.columns:
        df = df.sort_values(sort_col, ascending=args.ascending).reset_index(drop=True)

    if args.out:
        out = Path(args.out)
        if not out.is_absolute():
            out = _REPO / out
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        print(f"Wrote {out}", flush=True)
    else:
        with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_rows", 40):
            print(df.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
