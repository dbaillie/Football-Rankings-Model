#!/usr/bin/env python3
"""
Merge two Europe match edge lists (Glicko input): base + pipeline staging.

Each of ``--base`` and ``--append`` may be ``.csv`` or ``.txt`` (comma-separated); if the path you
pass is missing, the other extension is tried beside it. Output is **always** written as CSV
(``--out`` is forced to ``.csv`` if you pass ``.txt``).

Rows are deduplicated on ``EventId`` (last occurrence wins).

Examples::

    python scripts/merge_europe_matches_csv.py

    python scripts/merge_europe_matches_csv.py \\
        --base output/europe/europe_matches.txt \\
        --append output/pipeline_calendar_append/europe/europe_matches.csv
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def _resolve_matches_file(path: Path) -> Path | None:
    """Return path if it exists; if ``*.csv`` missing try ``*.txt`` and vice versa."""
    if path.is_file():
        return path
    if path.suffix.lower() == ".csv":
        alt = path.with_suffix(".txt")
    elif path.suffix.lower() == ".txt":
        alt = path.with_suffix(".csv")
    else:
        return None
    return alt if alt.is_file() else None


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(
        description="Merge pipeline europe_matches into base (.csv or .txt); dedupe on EventId; write CSV."
    )
    p.add_argument(
        "--base",
        type=Path,
        default=root / "output" / "europe" / "europe_matches.csv",
        help="Base file (.csv or .txt). Default tries .csv then .txt next to it.",
    )
    p.add_argument(
        "--append",
        type=Path,
        default=root / "output" / "pipeline_calendar_append" / "europe" / "europe_matches.csv",
        help="Append file (.csv or .txt). Default tries .csv then .txt.",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=root / "output" / "europe" / "europe_matches.csv",
        help="Output path; always written as CSV (.txt suffix is replaced with .csv).",
    )
    p.add_argument("--backup", action="store_true", help="Copy --out to europe_matches.backup.csv beside it first.")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    base_arg = args.base.resolve()
    append_arg = args.append.resolve()
    out = args.out.resolve()
    if out.suffix.lower() == ".txt":
        out = out.with_suffix(".csv")

    base = _resolve_matches_file(base_arg)
    append = _resolve_matches_file(append_arg)
    if base is None:
        print(f"ERROR: missing base file (tried .csv/.txt): {base_arg}", flush=True)
        return 1
    if append is None:
        print(f"ERROR: missing append file (tried .csv/.txt): {append_arg}", flush=True)
        return 1

    import pandas as pd

    print(f"Reading base:  {base}", flush=True)
    print(f"Reading append: {append}", flush=True)
    df_b = pd.read_csv(base, low_memory=False)
    df_a = pd.read_csv(append, low_memory=False)
    if "EventId" not in df_b.columns or "EventId" not in df_a.columns:
        print("ERROR: both files must have an EventId column.", flush=True)
        return 1

    merged = pd.concat([df_b, df_a], ignore_index=True, sort=False)
    n_before = len(merged)
    merged = merged.drop_duplicates(subset=["EventId"], keep="last").reset_index(drop=True)
    n_after = len(merged)
    print(
        f"base={len(df_b):,} append={len(df_a):,} → merged={n_after:,} "
        f"(dropped {n_before - n_after:,} duplicate EventId)",
        flush=True,
    )

    if args.dry_run:
        print("[dry-run] no write.", flush=True)
        return 0

    out.parent.mkdir(parents=True, exist_ok=True)
    if args.backup and out.is_file():
        bak = out.with_name("europe_matches.backup.csv")
        shutil.copy2(out, bak)
        print(f"Backup: {bak}", flush=True)

    merged.to_csv(out, index=False)
    print(f"Wrote {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
