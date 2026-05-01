#!/usr/bin/env python3
"""
Summarize ScraperFC ``comps.yaml``: which scraper modules exist per competition, and how
that lines up with ``config/football_ingestion_config.LEAGUE_METADATA``.

ScraperFC maps each competition to one or more backends, e.g. ``SOFASCORE: <id>`` or
``FBREF: { history url, finders }``. There is **no** single module that lists all 60
competitions in the shipped ``comps.yaml``; this script quantifies overlap.

Install path: next to ``import ScraperFC`` (typically site-packages/ScraperFC/comps.yaml).

Usage:
  python scripts/report_scraperfc_source_coverage.py
  python scripts/report_scraperfc_source_coverage.py --check-config
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

MODULE_KEYS = frozenset({"SOFASCORE", "FBREF", "TRANSFERMARKT", "UNDERSTAT", "CAPOLOGY"})


def _comps_path() -> Path:
    import ScraperFC

    return Path(ScraperFC.__file__).resolve().parent / "comps.yaml"


def _modules_for(spec: dict | None) -> set[str]:
    if not isinstance(spec, dict):
        return set()
    return {k for k in spec if k in MODULE_KEYS}


def main() -> None:
    p = argparse.ArgumentParser(description="Report ScraperFC comps.yaml coverage by module.")
    p.add_argument("--check-config", action="store_true", help="Cross-check LEAGUE_METADATA names vs comps.yaml keys")
    args = p.parse_args()

    path = _comps_path()
    with open(path, encoding="utf-8") as f:
        comps: dict[str, dict] = yaml.safe_load(f)

    coverage = {m: [] for m in MODULE_KEYS}
    for name, spec in comps.items():
        for m in _modules_for(spec):
            coverage[m].append(name)

    n = len(comps)
    print(f"comps.yaml: {path}")
    print(f"Competitions defined: {n}")
    print()
    for m in sorted(MODULE_KEYS):
        print(f"  {m:14s} {len(coverage[m]):3d} competitions")
    s, f = set(coverage["SOFASCORE"]), set(coverage["FBREF"])
    print()
    print(f"  SOFASCORE-only (no FBREF in yaml): {len(s - f)}")
    print(f"  FBREF-only (no SOFASCORE in yaml):   {len(f - s)}")
    print(f"  Both:                                {len(s & f)}")

    if not args.check_config:
        return

    from config.football_ingestion_config import LEAGUE_METADATA

    print()
    print("LEAGUE_METADATA vs comps.yaml keys")
    missing: list[tuple[str, str]] = []
    no_ss: list[str] = []
    no_fb: list[str] = []
    for meta in LEAGUE_METADATA:
        key = meta["league_key"]
        src = meta["source_league_name"]
        spec = comps.get(src)
        if spec is None:
            missing.append((key, src))
            continue
        mods = _modules_for(spec)
        if "SOFASCORE" not in mods:
            no_ss.append(f"{key} ({src})")
        if "FBREF" not in mods:
            no_fb.append(f"{key} ({src})")

    print(f"  Rows in LEAGUE_METADATA: {len(LEAGUE_METADATA)}")
    print(f"  source_league_name not found as comps.yaml key: {len(missing)}")
    for lk, src in missing:
        print(f"    {lk}: {src!r}")
    print(f"  Found but no SOFASCORE id: {len(no_ss)}")
    for line in no_ss:
        print(f"    {line}")
    print(f"  Found but no FBREF block (cannot use FBREF-only path): {len(no_fb)}")
    for line in no_fb:
        print(f"    {line}")

    if missing:
        print()
        print(
            "Hint: keys must match comps.yaml exactly (e.g. USA USL uses lowercase "
            "'championship'; League 2 key is misspelled 'Leauge' upstream)."
        )


if __name__ == "__main__":
    main()
