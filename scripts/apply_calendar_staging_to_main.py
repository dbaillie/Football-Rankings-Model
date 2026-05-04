#!/usr/bin/env python3
"""
Apply calendar-year staging into main ``output/`` as a **results-only** merge: only
``fact_result_simple_resolved.csv`` is updated for the chosen calendar year (append staging slice,
dedupe; staging wins on duplicate keys). **Club and country dimension tables are not modified** so
identities stay stable mid-season.

Before merging, staging rows for that year are **re-keyed to main club IDs** in the same order as
ingestion-style resolution:

1. **Fixture match** — If main fact already has the same match (``match_date`` + ``league_code`` +
   canonical home/away names from ``norm_club_name`` / ``canonical_match_key``), copy
   ``home_club_id`` / ``away_club_id`` from that main row (same opponent on same date in the same
   competition → same club ids as the rest of main).
2. **Dim lookup** — Else ``(country_id, canonical name)`` then global canonical name, as
   ``build_club_lookup``.
3. **Fuzzy** — Same as ingest: ``suggest_club_match`` / Rapidfuzz to an existing dim row.
4. **Fallback** — keep staging id if still unresolved.

Then ``run_glicko_europe.py`` runs on main (reads existing ``dim_club*.csv`` / ``dim_country*.csv``),
regenerating ``output/europe/*.csv``, then optionally ``sync_current_year_append_supabase.py``.

Prerequisites
-------------
- Main: ``fact_result_simple_resolved.csv`` plus dims as already used by Glicko (unchanged by this script).
- Staging: ``fact_result_simple_resolved.csv`` with the calendar-year match rows to fold in.

Examples::

    python scripts/apply_calendar_staging_to_main.py --calendar-year 2026

    python scripts/apply_calendar_staging_to_main.py \\
        --staging output/pipeline_calendar_append \\
        --main-dir output \\
        --calendar-year 2026 \\
        --backup

    python scripts/apply_calendar_staging_to_main.py --calendar-year 2026 --dry-run

    python scripts/apply_calendar_staging_to_main.py --calendar-year 2026 --skip-supabase
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts.club_identity import build_club_lookup, norm_club_name, suggest_club_match  # noqa: E402
from scripts.club_name_canonical import canonical_match_key  # noqa: E402
from scripts.pipeline_calendar_year_append import (  # noqa: E402
    _FACT_DEDUPE_COLS,
    _merge_append_dedupe,
    _pick_dim_club,
    _safe_read_csv,
    _split_fact_by_calendar_year,
)


def _match_date_iso(val: object) -> str:
    d = pd.to_datetime(val, errors="coerce")
    if pd.isna(d):
        return ""
    return pd.Timestamp(d).strftime("%Y-%m-%d")


def _fixture_identity_key(row: pd.Series) -> tuple[str, str, str, str] | None:
    """
    Same match identity as ingestion uses for a fixture: date + competition + home/away display names
    after ``norm_club_name`` / ``canonical_match_key`` (short-form aliases, Paris SG, etc.).
    """
    d = _match_date_iso(row.get("match_date"))
    if not d:
        return None
    if "home_team_name" not in row.index or "away_team_name" not in row.index:
        return None
    hn, an = row.get("home_team_name"), row.get("away_team_name")
    if hn is None or an is None or (isinstance(hn, float) and pd.isna(hn)) or (isinstance(an, float) and pd.isna(an)):
        return None
    if str(hn).strip() == "" or str(an).strip() == "":
        return None
    lc = ""
    if "league_code" in row.index and pd.notna(row.get("league_code")):
        lc = str(row["league_code"]).strip()
    hk = canonical_match_key(norm_club_name(hn))
    ak = canonical_match_key(norm_club_name(an))
    if not hk or not ak:
        return None
    return (d, lc, hk, ak)


def _build_main_fixture_club_map(main_fact: pd.DataFrame) -> dict[tuple[str, str, str, str], tuple[int, int]]:
    """Map fixture identity → (home_club_id, away_club_id) from existing main fact (last row wins)."""
    out: dict[tuple[str, str, str, str], tuple[int, int]] = {}
    conflicts = 0
    need = {"match_date", "home_team_name", "away_team_name", "home_club_id", "away_club_id"}
    if not need.issubset(main_fact.columns):
        return out
    for _, row in main_fact.iterrows():
        fk = _fixture_identity_key(row)
        if fk is None:
            continue
        try:
            hid = int(row["home_club_id"])
            aid = int(row["away_club_id"])
        except (TypeError, ValueError):
            continue
        pair = (hid, aid)
        if fk in out and out[fk] != pair:
            conflicts += 1
        out[fk] = pair
    if conflicts:
        print(
            f"NOTE: {conflicts} duplicate fixture keys in main fact (same date/league/names; kept last row).",
            flush=True,
        )
    print(f"Built main fixture map: {len(out):,} distinct fixtures → club id pairs.", flush=True)
    return out


def _build_country_canonical_to_club_id(dim_club: pd.DataFrame) -> dict[tuple[int, str], int]:
    """Minimum club_id per (country_id, canonical_key) from main dim."""
    buckets: dict[tuple[int, str], list[int]] = defaultdict(list)
    for _, row in dim_club.iterrows():
        nm = norm_club_name(row["club_name"])
        if not nm:
            continue
        key = canonical_match_key(nm)
        cid = int(row["country_id"])
        buckets[(cid, key)].append(int(row["club_id"]))
    return {k: min(v) for k, v in buckets.items()}


def _resolve_club_id_like_ingest(
    team_name: object,
    league_country_id: object,
    main_dim: pd.DataFrame,
    global_lookup: dict[str, int],
    by_country: dict[tuple[int, str], int],
    fallback_id: object,
) -> int:
    """Match ingestion order: canonical lookup → (country, key) → fuzzy ``suggest_club_match`` → fallback id."""
    nm = norm_club_name(team_name)
    if not nm:
        try:
            return int(fallback_id)
        except (TypeError, ValueError):
            return -1
    key = canonical_match_key(nm)
    if league_country_id is not None and not (isinstance(league_country_id, float) and pd.isna(league_country_id)):
        try:
            cid = int(league_country_id)
            got = by_country.get((cid, key))
            if got is not None:
                return int(got)
        except (TypeError, ValueError):
            pass
    got = global_lookup.get(key)
    if got is not None:
        return int(got)
    if not main_dim.empty and "club_name" in main_dim.columns:
        lc_fuzzy: int | None = None
        if league_country_id is not None and not (isinstance(league_country_id, float) and pd.isna(league_country_id)):
            try:
                lc_fuzzy = int(league_country_id)
            except (TypeError, ValueError):
                lc_fuzzy = None
        suggestion, _score = suggest_club_match(str(team_name), main_dim, league_country_id=lc_fuzzy)
        if suggestion is not None:
            sugg_rows = main_dim[main_dim["club_name"].astype(str) == suggestion]
            if not sugg_rows.empty:
                return int(sugg_rows["club_id"].min())
    try:
        return int(fallback_id)
    except (TypeError, ValueError):
        return -1


def _remap_calendar_year_club_ids_to_main(
    sy: pd.DataFrame,
    main_dim: pd.DataFrame,
    main_fact: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """
    Align staging club ids with main: fixture-level first (same date / league / names as main fact),
    then ingestion-style dim + fuzzy. Returns (updated frame, count of club-id cells changed).
    """
    if sy.empty:
        return sy, 0
    if main_dim.empty:
        print("WARNING: main dim_club empty; skipping club id remap.", flush=True)
        return sy, 0
    required_dim = {"club_name", "country_id", "club_id"}
    if not required_dim.issubset(main_dim.columns):
        print("WARNING: main dim_club missing columns; skipping club id remap.", flush=True)
        return sy, 0

    fixture_map = _build_main_fixture_club_map(main_fact) if not main_fact.empty else {}

    global_lookup = build_club_lookup(main_dim)
    by_country = _build_country_canonical_to_club_id(main_dim)

    out = sy.copy()
    changed = 0
    fixture_rows = 0
    has_country = "country_id" in out.columns
    has_names = (
        "home_team_name" in out.columns
        and "away_team_name" in out.columns
        and "home_club_id" in out.columns
        and "away_club_id" in out.columns
    )

    for idx in out.index:
        row = out.loc[idx]
        used_fixture = False
        if fixture_map and has_names:
            fk = _fixture_identity_key(row)
            if fk is not None and fk in fixture_map:
                hid, aid = fixture_map[fk]
                for id_col, new_id in (("home_club_id", hid), ("away_club_id", aid)):
                    old_raw = row[id_col]
                    try:
                        old_int = int(old_raw)
                    except (TypeError, ValueError):
                        old_int = None
                    out.loc[idx, id_col] = new_id
                    if old_int is not None and old_int != new_id:
                        changed += 1
                used_fixture = True
                fixture_rows += 1
        if used_fixture:
            continue

        for side in ("home", "away"):
            id_col = f"{side}_club_id"
            name_col = f"{side}_team_name"
            if id_col not in out.columns or name_col not in out.columns:
                continue
            old_raw = row[id_col]
            league_cid = row["country_id"] if has_country else None
            new_id = _resolve_club_id_like_ingest(
                row[name_col],
                league_cid,
                main_dim,
                global_lookup,
                by_country,
                old_raw,
            )
            if new_id < 0:
                continue
            try:
                old_int = int(old_raw)
            except (TypeError, ValueError):
                old_int = None
            out.loc[idx, id_col] = new_id
            if old_int is not None and old_int != new_id:
                changed += 1

    print(
        f"Club id remap: {fixture_rows:,} rows matched an existing main fixture; "
        f"{changed} club-id cells changed overall ({len(out):,} calendar-year rows).",
        flush=True,
    )
    return out, changed


def _merge_resolved_facts(
    main_resolved: Path,
    staging_resolved: Path,
    calendar_year: int,
    out_path: Path,
    main_dir: Path,
    *,
    remap_clubs: bool,
) -> tuple[int, int, int]:
    """
    For calendar_year only: append staging slice onto main slice, dedupe (staging wins).
    Other years stay from main unchanged.
    Returns (rows_main_year_before, rows_staging_year, rows_merged_year_after).
    """
    main_df = _safe_read_csv(main_resolved)
    st_df = _safe_read_csv(staging_resolved)
    if main_df.empty:
        raise FileNotFoundError(f"Main resolved fact is empty or missing: {main_resolved}")
    if st_df.empty:
        raise FileNotFoundError(f"Staging resolved fact is empty or missing: {staging_resolved}")

    my, mr = _split_fact_by_calendar_year(main_df, calendar_year)
    sy, _sr = _split_fact_by_calendar_year(st_df, calendar_year)
    if sy.empty:
        print(
            f"WARNING: no rows with match_date in calendar year {calendar_year} in staging; "
            "leaving main resolved fact unchanged.",
            flush=True,
        )
        merged = main_df.copy()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(out_path, index=False)
        return len(my), 0, len(my)

    if remap_clubs:
        main_dim = _safe_read_csv(_pick_dim_club(main_dir))
        sy, _ = _remap_calendar_year_club_ids_to_main(sy, main_dim, main_df)

    keys = [c for c in _FACT_DEDUPE_COLS if c in my.columns and c in sy.columns]
    merged_y = _merge_append_dedupe(my, sy, keys)
    merged = pd.concat([mr, merged_y], ignore_index=True, sort=False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    return len(my), len(sy), len(merged_y)


def _backup_resolved_fact(main_dir: Path, tag: str) -> Path:
    bak = main_dir / f"backup_before_apply_staging_{tag}"
    bak.mkdir(parents=True, exist_ok=True)
    p = main_dir / "fact_result_simple_resolved.csv"
    if p.is_file():
        shutil.copy2(p, bak / p.name)
    print(f"Backed up {p.name} to {bak}", flush=True)
    return bak


def main() -> int:
    p = argparse.ArgumentParser(
        description="Results-only: merge staging resolved fact for one calendar year, then Glicko; dims unchanged."
    )
    p.add_argument("--calendar-year", type=int, required=True, help="Calendar year slice to merge (e.g. 2026).")
    p.add_argument(
        "--staging",
        type=str,
        default="output/pipeline_calendar_append",
        help="Staging folder with fact_result_simple_resolved.csv.",
    )
    p.add_argument("--main-dir", type=str, default="output", help="Main output directory (default: output).")
    p.add_argument(
        "--backup",
        action="store_true",
        help="Copy fact_result_simple_resolved.csv before overwriting (dim files are not touched).",
    )
    p.add_argument("--dry-run", action="store_true", help="Print actions only; no writes or subprocesses.")
    p.add_argument("--skip-glicko", action="store_true", help="Stop after merging resolved fact.")
    p.add_argument(
        "--skip-supabase",
        action="store_true",
        help="Do not run sync_current_year_append_supabase.py after Glicko.",
    )
    p.add_argument(
        "--no-remap-clubs",
        action="store_true",
        help="Do not re-key staging club_ids to main dim before merge (not recommended).",
    )
    args = p.parse_args()

    year = int(args.calendar_year)
    staging = (_REPO / args.staging).resolve() if not Path(args.staging).is_absolute() else Path(args.staging).resolve()
    main_dir = (_REPO / args.main_dir).resolve() if not Path(args.main_dir).is_absolute() else Path(args.main_dir).resolve()

    staging_resolved = staging / "fact_result_simple_resolved.csv"
    main_resolved = main_dir / "fact_result_simple_resolved.csv"

    if not staging_resolved.is_file():
        print(f"ERROR: missing staging resolved fact: {staging_resolved}", file=sys.stderr)
        return 1
    if not main_resolved.is_file():
        print(f"ERROR: missing main resolved fact: {main_resolved}", file=sys.stderr)
        return 1

    py = sys.executable
    scripts_dir = _REPO / "scripts"

    print(f"Staging:  {staging}", flush=True)
    print(f"Main dir: {main_dir}", flush=True)
    print(f"Calendar year: {year} (results-only merge; dim_club / dim_country unchanged)", flush=True)

    if args.dry_run:
        print("[dry-run] Planned steps (dims unchanged):", flush=True)
        if not args.no_remap_clubs:
            print(
                "  • Re-key staging club_ids: same fixture as main fact first, else dim + fuzzy (ingest-style).",
                flush=True,
            )
        print("  • Merge staging resolved fact into main for calendar year (append + dedupe).", flush=True)
        if not args.skip_glicko:
            print(f"  • {py} scripts/run_glicko_europe.py --output-root {main_dir}", flush=True)
        if not args.skip_supabase:
            print(f"  • {py} scripts/sync_current_year_append_supabase.py --calendar-year {year}", flush=True)
        return 0

    if args.backup:
        tag = datetime.now().strftime("%Y%m%d_%H%M%S")
        _backup_resolved_fact(main_dir, tag)

    n_my, n_sy, n_merged = _merge_resolved_facts(
        main_resolved,
        staging_resolved,
        year,
        main_resolved,
        main_dir,
        remap_clubs=not args.no_remap_clubs,
    )
    print(
        f"Merged resolved fact: main Y{year}={n_my:,} + staging Y{year}={n_sy:,} → {n_merged:,} rows (year slice, deduped)",
        flush=True,
    )

    if not args.skip_glicko:
        gcmd = [py, str(scripts_dir / "run_glicko_europe.py"), "--output-root", str(main_dir)]
        print("+ " + " ".join(gcmd), flush=True)
        subprocess.run(gcmd, cwd=str(_REPO), check=True)

    if not args.skip_supabase:
        scmd = [py, str(scripts_dir / "sync_current_year_append_supabase.py"), "--calendar-year", str(year)]
        print("+ " + " ".join(scmd), flush=True)
        subprocess.run(scmd, cwd=str(_REPO), check=True)

    print("Done.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
