#!/usr/bin/env python3
"""
Remove cached UEFA-only ingest outputs so the next euro ingest run starts fresh.

Deletes under --outdir (default output/):
  - fact_result_simple_ingested_euro.csv
  - created_clubs_euro.csv
  - unmatched_clubs_euro.csv
  - ingestion_summary_euro.csv
  - ingestion_progress_euro.json

Does not modify domestic fact, dim_club.csv base, or resolved/Glicko outputs.
After reset, typical flow:
  python scripts/ingest_euro_comps_from_config.py ...
  python scripts/resolve_club_identities.py --write ...
  python scripts/run_glicko_europe.py

Or: python scripts/run_europe_ratings_pipeline.py --reset-euro ...
"""

from __future__ import annotations

import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

EURO_ARTIFACTS = (
    "fact_result_simple_ingested_euro.csv",
    "created_clubs_euro.csv",
    "unmatched_clubs_euro.csv",
    "ingestion_summary_euro.csv",
    "ingestion_progress_euro.json",
)


def main() -> None:
    p = argparse.ArgumentParser(description="Delete UEFA ingest cache files for a clean euro re-pull.")
    p.add_argument("--outdir", default="output", help="Output folder (relative to repo unless absolute)")
    p.add_argument("--dry-run", action="store_true", help="List paths only")
    args = p.parse_args()

    outdir = Path(args.outdir)
    if not outdir.is_absolute():
        outdir = (REPO_ROOT / outdir).resolve()

    removed = 0
    for name in EURO_ARTIFACTS:
        path = outdir / name
        if not path.exists():
            print(f"skip (missing): {path}")
            continue
        print(f"remove: {path}")
        if not args.dry_run:
            path.unlink()
            removed += 1

    if args.dry_run:
        print("(dry-run: no files deleted)")
    else:
        print(f"Removed {removed} file(s).")


if __name__ == "__main__":
    main()
