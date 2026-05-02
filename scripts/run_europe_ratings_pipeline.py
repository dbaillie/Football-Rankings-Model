#!/usr/bin/env python3
"""
Single CLI flow: domestic ingest → UEFA ingest → upcoming SofaScore fixtures (separate fact CSV)
→ resolve club identities → Europe Glicko.

Does not change ingest/resolve/Glicko logic; only invokes existing scripts in order.

Examples:
  python scripts/run_europe_ratings_pipeline.py
  python scripts/run_europe_ratings_pipeline.py --dry-run
  python scripts/run_europe_ratings_pipeline.py --skip-domestic --skip-euro
  python scripts/run_europe_ratings_pipeline.py --skip-future-fixtures
  python scripts/run_europe_ratings_pipeline.py --reset-euro --skip-domestic
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _repo_path(p: str | Path) -> Path:
    path = Path(p)
    return path.resolve() if path.is_absolute() else (REPO_ROOT / path).resolve()


def _pick_resolve_fact(outdir: Path, fact_base: Path) -> Path:
    ingested = outdir / "fact_result_simple_ingested.csv"
    if not ingested.exists():
        return fact_base
    try:
        text = ingested.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return fact_base
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) <= 1:
        return fact_base
    return ingested


def _run(cmd: list[str], *, cwd: Path, dry_run: bool) -> None:
    printable = " ".join(cmd)
    print(f"+ {printable}")
    if dry_run:
        return
    subprocess.run(cmd, cwd=str(cwd), check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Orchestrate ingest → euro ingest → resolve → Glicko Europe")
    parser.add_argument("--outdir", default="output", help="Output folder (paths relative to repo root unless absolute)")
    parser.add_argument("--fact-base", default="output/fact_result_simple.csv", help="Base fact CSV")
    parser.add_argument("--dim-club", default="output/dim_club.csv")
    parser.add_argument("--dim-country", default="output/dim_country.csv")
    parser.add_argument("--dim-season", default="output/dim_season.csv")
    parser.add_argument("--skip-domestic", action="store_true", help="Skip ingest_leagues_from_config.py")
    parser.add_argument("--skip-euro", action="store_true", help="Skip ingest_euro_comps_from_config.py")
    parser.add_argument("--skip-resolve", action="store_true", help="Skip resolve_club_identities.py")
    parser.add_argument("--skip-glicko", action="store_true", help="Skip run_glicko_europe.py")
    parser.add_argument(
        "--skip-euro-merge",
        action="store_true",
        help="Forward to resolve_club_identities (--skip-euro-merge)",
    )
    parser.add_argument(
        "--min-suggestion-score",
        type=float,
        default=None,
        help="Forward to resolve_club_identities when set",
    )
    parser.add_argument(
        "--euro-provider",
        choices=["sofascore", "football_data_org"],
        default="sofascore",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands only")
    parser.add_argument(
        "--run-calibration",
        action="store_true",
        help="After Europe Glicko (unless skipped), run scripts/analyse_europe_calibration.py on output/europe",
    )
    parser.add_argument(
        "--reset-euro",
        action="store_true",
        help="Before UEFA ingest, delete euro cache files (see scripts/reset_euro_ingest_artifacts.py)",
    )
    parser.add_argument(
        "--skip-future-fixtures",
        action="store_true",
        help="Skip ingest_future_fixtures.py (writes output/fact_fixture_upcoming.csv)",
    )
    args = parser.parse_args()

    outdir = _repo_path(args.outdir)
    fact_base = _repo_path(args.fact_base)
    dim_club = _repo_path(args.dim_club)
    dim_country = _repo_path(args.dim_country)
    dim_season = _repo_path(args.dim_season)

    py = sys.executable
    scripts_dir = REPO_ROOT / "scripts"

    if args.reset_euro and not args.skip_euro:
        _run(
            [py, str(scripts_dir / "reset_euro_ingest_artifacts.py"), "--outdir", str(outdir)],
            cwd=REPO_ROOT,
            dry_run=args.dry_run,
        )

    if not args.skip_domestic:
        _run(
            [
                py,
                str(scripts_dir / "ingest_leagues_from_config.py"),
                "--outdir",
                str(outdir),
                "--fact",
                str(fact_base),
                "--dim-club",
                str(dim_club),
                "--dim-country",
                str(dim_country),
                "--dim-season",
                str(dim_season),
            ],
            cwd=REPO_ROOT,
            dry_run=args.dry_run,
        )

    if not args.skip_euro:
        _run(
            [
                py,
                str(scripts_dir / "ingest_euro_comps_from_config.py"),
                "--outdir",
                str(outdir),
                "--fact",
                str(fact_base),
                "--dim-club",
                str(dim_club),
                "--dim-country",
                str(dim_country),
                "--dim-season",
                str(dim_season),
                "--provider",
                args.euro_provider,
            ],
            cwd=REPO_ROOT,
            dry_run=args.dry_run,
        )

    if not args.skip_future_fixtures:
        _run(
            [
                py,
                str(scripts_dir / "ingest_future_fixtures.py"),
                "--outdir",
                str(outdir),
                "--dim-club",
                str(dim_club),
                "--dim-country",
                str(dim_country),
            ],
            cwd=REPO_ROOT,
            dry_run=args.dry_run,
        )

    if not args.skip_resolve:
        resolve_fact = _pick_resolve_fact(outdir, fact_base)
        rcmd: list[str] = [
            py,
            str(scripts_dir / "resolve_club_identities.py"),
            "--fact",
            str(resolve_fact),
            "--dim",
            str(dim_club),
            "--dim-updated",
            str(outdir / "dim_club_updated.csv"),
            "--created",
            str(outdir / "created_clubs.csv"),
            "--out-fact",
            str(outdir / "fact_result_simple_resolved.csv"),
            "--out-map",
            str(outdir / "club_id_remap.json"),
            "--write",
        ]
        if args.skip_euro_merge:
            rcmd.append("--skip-euro-merge")
        if args.min_suggestion_score is not None:
            rcmd.extend(["--min-suggestion-score", str(args.min_suggestion_score)])
        _run(rcmd, cwd=REPO_ROOT, dry_run=args.dry_run)

    if not args.skip_glicko:
        _run(
            [
                py,
                str(scripts_dir / "run_glicko_europe.py"),
                "--output-root",
                str(outdir),
            ],
            cwd=REPO_ROOT,
            dry_run=args.dry_run,
        )

    if args.run_calibration:
        _run(
            [
                py,
                str(scripts_dir / "analyse_europe_calibration.py"),
                "--output-root",
                str(outdir),
            ],
            cwd=REPO_ROOT,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
