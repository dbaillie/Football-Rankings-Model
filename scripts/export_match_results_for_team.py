#!/usr/bin/env python3
"""
Export rows from ``fr_match_results`` where home or away team name matches a pattern
(case-insensitive). Uses ``DATABASE_URL`` (load ``.env`` from repo root if present).

In this project, PSG is usually stored as **Paris SG** in ``home_team_name`` / ``away_team_name``.

Examples::

    python scripts/export_match_results_for_team.py --contains "Paris SG" --out psg_matches.csv
    python scripts/export_match_results_for_team.py --contains "Saint-Germain" --out psg.csv
    python scripts/export_match_results_for_team.py --contains "Paris" --contains2 "Germain" --out psg.csv
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
    p = argparse.ArgumentParser(description="Export fr_match_results for a team name pattern.")
    p.add_argument(
        "--contains",
        type=str,
        default="Paris SG",
        help="Substring to match in home_team_name or away_team_name (ILIKE %%pattern%%). PSG is usually 'Paris SG'.",
    )
    p.add_argument(
        "--contains2",
        type=str,
        default=None,
        help="If set, BOTH must appear in the same team name (for Paris + Germain style filters).",
    )
    p.add_argument("--out", type=str, default="", help="Output CSV path (default: stdout summary only).")
    p.add_argument("--limit", type=int, default=0, help="Max rows (0 = no limit).")
    p.add_argument(
        "--team-country",
        type=str,
        default="",
        help=(
            "After the name match, keep rows only where the matched side's country equals this "
            "(case-insensitive), e.g. scotland for Glasgow Rangers vs other clubs containing 'Rangers'."
        ),
    )
    args = p.parse_args()

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 1

    engine = create_engine(
        url,
        poolclass=NullPool,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 60, "client_encoding": "utf8"},
    )

    pat = args.contains.strip()
    if args.contains2:
        c2 = args.contains2.strip()
        sql = text(
            """
            SELECT *
            FROM fr_match_results
            WHERE (
                (home_team_name ILIKE '%' || :p1 || '%' AND home_team_name ILIKE '%' || :p2 || '%')
                OR (away_team_name ILIKE '%' || :p1 || '%' AND away_team_name ILIKE '%' || :p2 || '%')
            )
            ORDER BY match_date, week
            """
        )
        params = {"p1": pat, "p2": c2}
    else:
        sql = text(
            """
            SELECT *
            FROM fr_match_results
            WHERE home_team_name ILIKE '%' || :p || '%'
               OR away_team_name ILIKE '%' || :p || '%'
            ORDER BY match_date, week
            """
        )
        params = {"p": pat}

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    tc = (args.team_country or "").strip().lower()
    if tc and not df.empty and "home_country" in df.columns and "away_country" in df.columns:
        pat_l = pat.lower()

        def _row_matches_team_country(row: pd.Series) -> bool:
            hn = str(row.get("home_team_name", "") or "").lower()
            an = str(row.get("away_team_name", "") or "").lower()
            hc = str(row.get("home_country", "") or "").lower()
            ac = str(row.get("away_country", "") or "").lower()
            if pat_l in hn and hc == tc:
                return True
            if pat_l in an and ac == tc:
                return True
            return False

        before = len(df)
        df = df[df.apply(_row_matches_team_country, axis=1)].reset_index(drop=True)
        print(f"  --team-country {tc!r}: {before:,} -> {len(df):,} rows", flush=True)

    lim = max(0, int(args.limit or 0))
    if lim:
        df = df.iloc[:lim].copy()

    desc = f"{pat!r}"
    if args.contains2:
        desc += f" and {args.contains2.strip()!r} (same name)"
    print(f"Rows: {len(df):,} (filter: {desc})", flush=True)
    if df.empty:
        return 0

    if args.out:
        out = Path(args.out)
        if not out.is_absolute():
            out = (_REPO / out).resolve() if not str(out).startswith(".") else Path.cwd() / out
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        print(f"Wrote {out}", flush=True)
    else:
        with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_rows", 30):
            print(df.to_string(index=False))
        if len(df) > 30:
            print(f"\n... ({len(df) - 30} more rows; use --out FILE.csv)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
