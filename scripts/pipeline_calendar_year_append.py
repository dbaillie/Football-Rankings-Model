#!/usr/bin/env python3
"""
Calendar-year delta pipeline: ingest one UTC calendar year into **staging only**, merge with the
main resolved fact, resolve, run Europe Glicko, then **upsert** Europe exports into ``output/europe/``
without touching the main fact CSVs until the final merge step.

Flow
----
1. **Ingest** (``ingest_leagues_unified.py``) with ``--match-calendar-year Y`` → ``{staging}/ingest/``.
   Main ``output/fact_result_simple*.csv`` and dims are read-only inputs; ingest does not write to
   ``output/`` except you can pass ``--main-dir`` if your layout differs.

2. **Artifacts** — copies combined domestic+UEFA ingested rows to ``{staging}/fact_delta_{Y}.csv``
   (audit trail).

3. **Merge fact** — For calendar year ``Y``, rows with ``match_date`` in ``Y`` are merged from
   ``main/fact_result_simple_resolved.csv`` + staging domestic + staging UEFA (ingest wins on
   duplicate keys). All other years are kept unchanged from main (no cross-year dedupe). Written to
   ``{staging}/fact_merged_for_resolve.csv``.

4. **Resolve** — writes ``{staging}/fact_result_simple_resolved.csv`` (still not main).

5. **Glicko** — reads staging resolved + staging merged dims, writes ``{staging}/europe/*.csv``.

6. **Upsert Europe** — merges staging exports into ``{main}/europe/``. For ``europe_match_results``
   and ``europe_weekly_ratings``, only rows for calendar year ``Y`` (match date / ISO week prefix)
   are merged with staging (append then dedupe; staging wins on duplicate keys). Rows for other
   years stay from main. ``europe_ratings`` / ``europe_teams``: staging rows appended, deduped as
   before (last wins).

Optional ``--backup-main-europe`` timestamps copies of the four main Europe CSVs before overwrite.

Examples::

    python scripts/pipeline_calendar_year_append.py --calendar-year 2026 --dry-run

    python scripts/pipeline_calendar_year_append.py --calendar-year 2026 --backup-main-europe

    python scripts/pipeline_calendar_year_append.py --staging output/_2026_staging --skip-ingest   # merge only
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# Match identity (same spirit as ingest dedupe vs base fact).
_FACT_DEDUPE_COLS = [
    "season_id",
    "league_code",
    "match_date",
    "match_time",
    "home_club_id",
    "away_club_id",
    "home_team_goals",
    "away_team_goals",
]

_MATCH_RESULTS_DEDUPE = [
    "match_date",
    "home_team_id",
    "away_team_id",
    "home_goals",
    "away_goals",
]


def _pick_dim_club(main_dir: Path) -> Path:
    u = main_dir / "dim_club_updated.csv"
    if u.is_file():
        return u
    return main_dir / "dim_club.csv"


def _pick_dim_country(main_dir: Path) -> Path:
    u = main_dir / "dim_country_updated.csv"
    if u.is_file():
        return u
    return main_dir / "dim_country.csv"


def _run(cmd: list[str], *, cwd: Path, dry: bool) -> None:
    print("+ " + " ".join(cmd), flush=True)
    if dry:
        return
    subprocess.run(cmd, cwd=str(cwd), check=True)


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _split_fact_by_calendar_year(df: pd.DataFrame, calendar_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rows with match_date in calendar_year vs all other rows."""
    if df.empty or "match_date" not in df.columns:
        return pd.DataFrame(), df.copy()
    md = pd.to_datetime(df["match_date"], errors="coerce")
    in_y = md.dt.year == int(calendar_year)
    return df.loc[in_y].copy(), df.loc[~in_y].copy()


def _split_weekly_by_iso_year(df: pd.DataFrame, calendar_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rows whose rating week encodes calendar_year (yyyyww → yyyy) vs the rest."""
    if df.empty or "week" not in df.columns:
        return pd.DataFrame(), df.copy()
    w = df["week"].astype(int)
    in_y = (w // 100) == int(calendar_year)
    return df.loc[in_y].copy(), df.loc[~in_y].copy()


def _split_match_results_by_calendar_year(df: pd.DataFrame, calendar_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty or "match_date" not in df.columns:
        return pd.DataFrame(), df.copy()
    md = pd.to_datetime(df["match_date"], errors="coerce")
    in_y = md.dt.year == int(calendar_year)
    return df.loc[in_y].copy(), df.loc[~in_y].copy()


def _merge_append_dedupe(
    first: pd.DataFrame,
    second: pd.DataFrame,
    subset: list[str] | None,
) -> pd.DataFrame:
    """Stack first then second; drop_duplicates keep='last' so second wins on duplicate keys (union, no prior delete)."""
    if first.empty:
        return second.copy() if not second.empty else first.copy()
    if second.empty:
        return first.copy()
    keys = [c for c in (subset or []) if c in first.columns and c in second.columns]
    comb = pd.concat([first, second], ignore_index=True, sort=False)
    if keys:
        return comb.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)
    return comb


def _merge_fact_for_resolve(main_dir: Path, ingest_dir: Path, out_path: Path, calendar_year: int) -> None:
    main_res = main_dir / "fact_result_simple_resolved.csv"
    if not main_res.is_file():
        raise FileNotFoundError(f"Missing main resolved fact: {main_res}")

    base = _safe_read_csv(main_res)
    dom = _safe_read_csv(ingest_dir / "fact_result_simple_ingested.csv")
    euro = _safe_read_csv(ingest_dir / "fact_result_simple_ingested_euro.csv")

    base_y, base_rest = _split_fact_by_calendar_year(base, calendar_year)
    if dom.empty:
        dom_y = pd.DataFrame()
    else:
        dom_y, _ = _split_fact_by_calendar_year(dom, calendar_year)
    if euro.empty:
        euro_y = pd.DataFrame()
    else:
        euro_y, _ = _split_fact_by_calendar_year(euro, calendar_year)

    pieces_y = [base_y, dom_y, euro_y]
    merged_y = pd.concat([p for p in pieces_y if not p.empty], ignore_index=True, sort=False)
    cols = [c for c in _FACT_DEDUPE_COLS if c in merged_y.columns]
    if cols and not merged_y.empty:
        merged_y = merged_y.drop_duplicates(subset=cols, keep="last").reset_index(drop=True)

    if merged_y.empty:
        merged = base_rest.copy()
    else:
        merged = pd.concat([base_rest, merged_y], ignore_index=True, sort=False)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(
        f"Wrote merged fact for resolve: {out_path} ({len(merged):,} rows; "
        f"calendar {calendar_year} slice after dedupe: {len(merged_y):,})",
        flush=True,
    )


def _ensure_staging_dims(main_dir: Path, ingest_dir: Path, staging_root: Path) -> None:
    """Merge main + ingest dims into staging for run_glicko_europe."""
    d0 = _safe_read_csv(_pick_dim_club(main_dir))
    d1 = _safe_read_csv(ingest_dir / "dim_club_updated.csv")
    if d0.empty and d1.empty:
        raise FileNotFoundError("No dim_club data")
    club = pd.concat([d0, d1], ignore_index=True).drop_duplicates(subset=["club_id"], keep="last")
    club.to_csv(staging_root / "dim_club_updated.csv", index=False)

    c0 = _safe_read_csv(_pick_dim_country(main_dir))
    c1 = _safe_read_csv(ingest_dir / "dim_country_updated.csv")
    country = pd.concat([c0, c1], ignore_index=True).drop_duplicates(subset=["country_id"], keep="last")
    country.to_csv(staging_root / "dim_country_updated.csv", index=False)
    print(f"Wrote merged dims to {staging_root} (clubs={len(club):,}, countries={len(country):,})", flush=True)


def _copy_main_dims_if_no_ingest(main_dir: Path, staging_root: Path) -> None:
    shutil.copy2(_pick_dim_club(main_dir), staging_root / "dim_club_updated.csv")
    shutil.copy2(_pick_dim_country(main_dir), staging_root / "dim_country_updated.csv")


def _backup_europe(main_europe: Path) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = main_europe.parent / f"europe_backup_{ts}"
    bak.mkdir(parents=True, exist_ok=True)
    for name in (
        "europe_match_results.csv",
        "europe_weekly_ratings.csv",
        "europe_ratings.csv",
        "europe_teams.csv",
    ):
        p = main_europe / name
        if p.is_file():
            shutil.copy2(p, bak / name)
    print(f"Backed up main europe CSVs to {bak}", flush=True)


def _upsert_europe_csvs(staging_europe: Path, main_europe: Path, calendar_year: int) -> None:
    if not staging_europe.is_dir():
        raise FileNotFoundError(f"Missing staging europe dir: {staging_europe}")

    # --- match_results: merge only calendar_year slice (append staging slice, dedupe; rest from main) ---
    st_m = _safe_read_csv(staging_europe / "europe_match_results.csv")
    mn_m = _safe_read_csv(main_europe / "europe_match_results.csv")
    keys_m = [k for k in _MATCH_RESULTS_DEDUPE if k in st_m.columns and k in mn_m.columns]
    if not st_m.empty:
        mn_y, mn_rest = _split_match_results_by_calendar_year(mn_m, calendar_year)
        st_y, _st_rest = _split_match_results_by_calendar_year(st_m, calendar_year)
        merged_y = _merge_append_dedupe(mn_y, st_y, keys_m)
        comb = pd.concat([mn_rest, merged_y], ignore_index=True, sort=False)
        if keys_m:
            comb = comb.copy()
            if "match_date" in comb.columns:
                comb["match_date"] = pd.to_datetime(comb["match_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        comb.to_csv(main_europe / "europe_match_results.csv", index=False)
        print(
            f"Upserted europe_match_results.csv → {len(comb):,} rows "
            f"(calendar {calendar_year} slice merged: main {len(mn_y):,} + staging {len(st_y):,} → {len(merged_y):,})",
            flush=True,
        )

    # --- weekly_ratings: ISO year prefix (week // 100) must match calendar_year ---
    st_w = _safe_read_csv(staging_europe / "europe_weekly_ratings.csv")
    mn_w = _safe_read_csv(main_europe / "europe_weekly_ratings.csv")
    if not st_w.empty:
        if "week" in mn_w.columns and "week" in st_w.columns and "pid" in mn_w.columns and "pid" in st_w.columns:
            mn_y, mn_rest = _split_weekly_by_iso_year(mn_w, calendar_year)
            st_y, _ = _split_weekly_by_iso_year(st_w, calendar_year)
            merged_y = _merge_append_dedupe(mn_y, st_y, ["week", "pid"])
            combw = pd.concat([mn_rest, merged_y], ignore_index=True, sort=False)
            print(
                f"  weekly ISO-{calendar_year}: main {len(mn_y):,} + staging {len(st_y):,} → {len(merged_y):,} rows",
                flush=True,
            )
        else:
            combw = _merge_append_dedupe(
                mn_w,
                st_w,
                ["week", "pid"] if "week" in st_w.columns and "pid" in st_w.columns else None,
            )
        combw.to_csv(main_europe / "europe_weekly_ratings.csv", index=False)
        print(f"Upserted europe_weekly_ratings.csv → {len(combw):,} rows", flush=True)

    # --- europe_ratings (global snapshot; keep full-file merge) ---
    st_r = _safe_read_csv(staging_europe / "europe_ratings.csv")
    mn_r = _safe_read_csv(main_europe / "europe_ratings.csv")
    if not st_r.empty:
        combr = pd.concat([mn_r, st_r], ignore_index=True, sort=False) if not mn_r.empty else st_r.copy()
        if "week" in combr.columns and "pid" in combr.columns:
            combr = combr.sort_values(["pid", "week"]).drop_duplicates(subset=["pid"], keep="last").reset_index(drop=True)
        elif "pid" in combr.columns:
            combr = combr.drop_duplicates(subset=["pid"], keep="last").reset_index(drop=True)
        combr.to_csv(main_europe / "europe_ratings.csv", index=False)
        print(f"Upserted europe_ratings.csv → {len(combr):,} rows", flush=True)

    # --- teams ---
    st_t = _safe_read_csv(staging_europe / "europe_teams.csv")
    mn_t = _safe_read_csv(main_europe / "europe_teams.csv")
    if not st_t.empty:
        idcol = "team_id" if "team_id" in st_t.columns else "pid" if "pid" in st_t.columns else None
        combt = pd.concat([mn_t, st_t], ignore_index=True, sort=False) if not mn_t.empty else st_t.copy()
        if idcol:
            combt = combt.drop_duplicates(subset=[idcol], keep="last").reset_index(drop=True)
        combt.to_csv(main_europe / "europe_teams.csv", index=False)
        print(f"Upserted europe_teams.csv → {len(combt):,} rows", flush=True)


def main() -> int:
    p = argparse.ArgumentParser(description="2026-style ingest in staging → Glicko → upsert into main output/europe.")
    p.add_argument("--calendar-year", type=int, default=2026, help="UTC calendar year for ingest filter.")
    p.add_argument(
        "--staging",
        type=str,
        default="output/pipeline_calendar_append",
        help="Staging root (relative to repo unless absolute). Ingest writes to {staging}/ingest/.",
    )
    p.add_argument("--main-dir", type=str, default="output", help="Main output folder (resolved fact + europe/).")
    p.add_argument(
        "--euro-provider",
        choices=("scraperfc", "football_data_org"),
        default="scraperfc",
        help="Forward to ingest_leagues_unified (UEFA source).",
    )
    p.add_argument("--skip-future-fixtures", action="store_true", help="Forward to unified ingest.")
    p.add_argument("--skip-ingest", action="store_true")
    p.add_argument("--skip-resolve", action="store_true")
    p.add_argument("--skip-glicko", action="store_true")
    p.add_argument("--skip-upsert", action="store_true", help="Do not merge staging europe into main/europe.")
    p.add_argument("--backup-main-europe", action="store_true", help="Copy main europe/*.csv to europe_backup_* before upsert.")
    p.add_argument(
        "--sync-main-resolved",
        action="store_true",
        help="After success, copy staging/fact_result_simple_resolved.csv over main (keeps next pipeline in sync).",
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    year = int(args.calendar_year)
    staging = (_REPO / args.staging).resolve() if not Path(args.staging).is_absolute() else Path(args.staging).resolve()
    main_dir = (_REPO / args.main_dir).resolve() if not Path(args.main_dir).is_absolute() else Path(args.main_dir).resolve()
    ingest_dir = staging / "ingest"
    staging.mkdir(parents=True, exist_ok=True)
    ingest_dir.mkdir(parents=True, exist_ok=True)

    py = sys.executable
    scripts = _REPO / "scripts"
    fact_base = main_dir / "fact_result_simple.csv"
    dim_season = main_dir / "dim_season.csv"
    dim_club = _pick_dim_club(main_dir)
    dim_country = _pick_dim_country(main_dir)

    for req in (fact_base, dim_season, dim_club, dim_country, main_dir / "fact_result_simple_resolved.csv"):
        if not req.is_file():
            print(f"ERROR: missing required file: {req}", file=sys.stderr)
            return 1

    # --- (a) ingest year → staging only ---
    if not args.skip_ingest:
        cmd = [
            py,
            str(scripts / "ingest_leagues_unified.py"),
            "--outdir",
            str(ingest_dir),
            "--fact",
            str(fact_base),
            "--dim-club",
            str(dim_club),
            "--dim-country",
            str(dim_country),
            "--dim-season",
            str(dim_season),
            "--match-calendar-year",
            str(year),
            "--european-provider",
            args.euro_provider,
        ]
        if args.skip_future_fixtures:
            cmd.append("--skip-future-fixtures")
        _run(cmd, cwd=_REPO, dry=args.dry_run)

        # (b) separate artifact file: combined ingested rows only
        if not args.dry_run:
            dom = _safe_read_csv(ingest_dir / "fact_result_simple_ingested.csv")
            euro = _safe_read_csv(ingest_dir / "fact_result_simple_ingested_euro.csv")
            delta = pd.concat([d for d in (dom, euro) if not d.empty], ignore_index=True, sort=False)
            delta_path = staging / f"fact_delta_{year}.csv"
            delta.to_csv(delta_path, index=False)
            print(f"Saved delta-only artifact: {delta_path} ({len(delta):,} rows)", flush=True)

    if args.dry_run:
        print("[dry-run] exiting before merge → resolve → Glicko → upsert (no file changes).", flush=True)
        return 0

    # --- merge fact (main resolved + staging ingested) ---
    merged_fact = staging / "fact_merged_for_resolve.csv"
    if not args.skip_ingest:
        _merge_fact_for_resolve(main_dir, ingest_dir, merged_fact, year)
    elif not args.skip_resolve:
        if not merged_fact.is_file():
            print(f"ERROR: {merged_fact} missing (need prior run without --skip-ingest)", file=sys.stderr)
            return 1

    # --- resolve → staging/fact_result_simple_resolved.csv ---
    if not args.skip_resolve:
        if (ingest_dir / "dim_club_updated.csv").is_file():
            _ensure_staging_dims(main_dir, ingest_dir, staging)
        else:
            _copy_main_dims_if_no_ingest(main_dir, staging)

        dim_up = ingest_dir / "dim_club_updated.csv"
        if not dim_up.is_file():
            dim_up = staging / "dim_club_updated.csv"
        created_p = ingest_dir / "created_clubs.csv"
        if not created_p.is_file():
            created_p = main_dir / "created_clubs.csv"
        if not created_p.is_file():
            created_p = staging / "_empty_created_clubs.csv"
            created_p.write_text(
                "club_name,created_club_id,suggested_existing_match,suggestion_score\n",
                encoding="utf-8",
            )

        rcmd = [
            py,
            str(scripts / "resolve_club_identities.py"),
            "--fact",
            str(merged_fact),
            "--dim",
            str(main_dir / "dim_club.csv"),
            "--dim-updated",
            str(dim_up),
            "--created",
            str(created_p),
            "--out-fact",
            str(staging / "fact_result_simple_resolved.csv"),
            "--out-map",
            str(staging / "club_id_remap.json"),
            "--skip-euro-merge",
            "--write",
        ]
        _run(rcmd, cwd=_REPO, dry=args.dry_run)

    # --- Glicko → staging/europe ---
    if not args.skip_glicko:
        res_path = staging / "fact_result_simple_resolved.csv"
        if args.skip_resolve and not res_path.is_file():
            shutil.copy2(main_dir / "fact_result_simple_resolved.csv", res_path)
            print(f"Copied main resolved fact to {res_path} (--skip-resolve without staging fact)", flush=True)
        if not (staging / "dim_club_updated.csv").is_file():
            _copy_main_dims_if_no_ingest(main_dir, staging)
        gcmd = [py, str(scripts / "run_glicko_europe.py"), "--output-root", str(staging)]
        _run(gcmd, cwd=_REPO, dry=args.dry_run)

    # --- (c) upsert into main europe ---
    if not args.skip_upsert:
        main_europe = main_dir / "europe"
        staging_europe = staging / "europe"
        main_europe.mkdir(parents=True, exist_ok=True)
        if args.backup_main_europe:
            _backup_europe(main_europe)
        _upsert_europe_csvs(staging_europe, main_europe, year)

    if args.sync_main_resolved:
        src = staging / "fact_result_simple_resolved.csv"
        if not src.is_file():
            print(f"WARNING: cannot --sync-main-resolved: missing {src}", file=sys.stderr)
        else:
            dst = main_dir / "fact_result_simple_resolved.csv"
            shutil.copy2(src, dst)
            print(f"Synced merged resolved fact → {dst}", flush=True)

    print("Done.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
