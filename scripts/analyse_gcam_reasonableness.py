"""
GCAM comparability diagnostics (“reasonableness proof pack”).

Reads latest-week rows from output/europe/europe_ratings.csv and optionally aggregates
from output/europe/europe_gcam_community_weekly.csv at the same rating period.

Outputs (under output/europe/):
  gcam_reasonableness_summary.json   — headline metrics and correlations
  gcam_reasonableness_by_ec_decile.csv
  gcam_reasonableness_by_community.csv  — requires primary_community column
  gcam_reasonableness_largest_shifts.csv
  gcam_reasonableness_high_raw_low_trust.csv
  gcam_reasonableness_community_latest_week.csv — slice of community weekly at latest period

Usage:
  python scripts/analyse_gcam_reasonableness.py
  python scripts/analyse_gcam_reasonableness.py --output-root output
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd


def _rel_under(root: Path, path: Path) -> str:
    """Portable relative path for JSON (Python <3.9 has no Path.is_relative_to)."""
    try:
        return os.path.relpath(path, root)
    except ValueError:
        return str(path)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GCAM comparability diagnostics from europe_ratings.csv.")
    p.add_argument("--output-root", type=str, default="output", help="Root containing europe/")
    p.add_argument(
        "--week",
        type=int,
        default=None,
        help="Rating period (yyyyww). Default: max week in europe_ratings.csv.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    root = Path(args.output_root)
    if not root.is_absolute():
        root = (Path.cwd() / root).resolve()
    euro = root / "europe"
    ratings_path = euro / "europe_ratings.csv"
    comm_path = euro / "europe_gcam_community_weekly.csv"

    if not ratings_path.exists():
        raise FileNotFoundError(f"Missing {ratings_path}. Run scripts/run_glicko_europe.py first.")

    df = pd.read_csv(ratings_path, low_memory=False)
    need = {"rating", "adjusted_rating", "effective_connectivity", "trust_factor"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"europe_ratings.csv missing columns: {sorted(miss)}")

    df["week"] = pd.to_numeric(df["week"], errors="coerce")
    df = df.dropna(subset=["week"])
    df["week"] = df["week"].astype(int)

    week = int(args.week) if args.week is not None else int(df["week"].max())
    snap = df.loc[df["week"] == week].copy()
    if snap.empty:
        raise ValueError(f"No rows for week={week}")

    snap["delta_adjusted_minus_raw"] = snap["adjusted_rating"].astype(float) - snap["rating"].astype(float)

    ec = snap["effective_connectivity"].astype(float)
    tr = snap["trust_factor"].astype(float)
    delta = snap["delta_adjusted_minus_raw"].astype(float)

    # Spearman-style rank correlation via pandas (Pearson on ranks)
    ec_rank = ec.rank(method="average")
    delta_rank = delta.rank(method="average")
    corr_ec_delta = float(ec_rank.corr(delta_rank)) if len(snap) > 2 else float("nan")

    tr_rank = tr.rank(method="average")
    corr_tr_delta = float(tr_rank.corr(delta_rank)) if len(snap) > 2 else float("nan")

    q_labels = [f"decile_{i}" for i in range(1, 11)]
    try:
        snap["_ec_decile"] = pd.qcut(ec, q=10, labels=q_labels, duplicates="drop")
    except ValueError:
        snap["_ec_decile"] = "all"

    dec_rows = []
    for lab in q_labels:
        sub = snap.loc[snap["_ec_decile"] == lab]
        if sub.empty:
            continue
        dec_rows.append(
            {
                "effective_connectivity_decile": lab,
                "n_teams": int(len(sub)),
                "mean_effective_connectivity": float(sub["effective_connectivity"].mean()),
                "mean_delta_adjusted_minus_raw": float(sub["delta_adjusted_minus_raw"].mean()),
                "mean_trust_factor": float(sub["trust_factor"].mean()),
                "mean_rating_raw": float(sub["rating"].mean()),
                "mean_adjusted_rating": float(sub["adjusted_rating"].mean()),
            }
        )
    if not dec_rows and len(snap):
        dec_rows.append(
            {
                "effective_connectivity_decile": "all",
                "n_teams": int(len(snap)),
                "mean_effective_connectivity": float(snap["effective_connectivity"].mean()),
                "mean_delta_adjusted_minus_raw": float(snap["delta_adjusted_minus_raw"].mean()),
                "mean_trust_factor": float(snap["trust_factor"].mean()),
                "mean_rating_raw": float(snap["rating"].mean()),
                "mean_adjusted_rating": float(snap["adjusted_rating"].mean()),
            }
        )
    dec_df = pd.DataFrame(dec_rows)

    # Largest shifts
    base_cols = ["pid", "team_name", "country_name", "rating", "adjusted_rating", "delta_adjusted_minus_raw",
                 "effective_connectivity", "trust_factor"]
    if "primary_community" in snap.columns:
        base_cols.append("primary_community")
    largest_up = snap.nlargest(25, "delta_adjusted_minus_raw")[
        [c for c in base_cols if c in snap.columns]
    ].copy()
    largest_down = snap.nsmallest(25, "delta_adjusted_minus_raw")[
        [c for c in base_cols if c in snap.columns]
    ].copy()

    # High raw + low trust (intuition: inflated-looking raw with weak global evidence)
    snap["_high_raw_low_trust"] = (snap["rating"] >= snap["rating"].quantile(0.9)) & (
        snap["trust_factor"] <= snap["trust_factor"].quantile(0.25)
    )
    hl = snap.loc[snap["_high_raw_low_trust"]].sort_values("rating", ascending=False)

    summary = {
        "rating_period_week": week,
        "n_teams": int(len(snap)),
        "mean_delta_adjusted_minus_raw": float(delta.mean()),
        "std_delta_adjusted_minus_raw": float(delta.std(ddof=0)),
        "mean_effective_connectivity": float(ec.mean()),
        "mean_trust_factor": float(tr.mean()),
        "spearman_rank_corr_effective_connectivity_vs_delta": corr_ec_delta,
        "spearman_rank_corr_trust_vs_delta": corr_tr_delta,
        "interpretation_notes": [
            "Negative delta means adjusted rating below raw (shrink toward baseline).",
            "If comparability goals hold, teams with lower effective_connectivity often show more negative delta.",
            "High raw + low trust highlights clubs whose strength estimate is least globally comparable.",
        ],
    }

    comm_agg_path = euro / "gcam_reasonableness_by_community.csv"
    if "primary_community" in snap.columns:
        agg = (
            snap.groupby("primary_community", dropna=False)
            .agg(
                n=("pid", "count"),
                mean_rating=("rating", "mean"),
                mean_adjusted=("adjusted_rating", "mean"),
                mean_delta=("delta_adjusted_minus_raw", "mean"),
                mean_ec=("effective_connectivity", "mean"),
                mean_trust=("trust_factor", "mean"),
            )
            .reset_index()
            .sort_values("mean_delta")
        )
        agg.to_csv(comm_agg_path, index=False)
    else:
        comm_agg_path = None

    dec_path = euro / "gcam_reasonableness_by_ec_decile.csv"
    dec_df.to_csv(dec_path, index=False)

    largest_path = euro / "gcam_reasonableness_largest_shifts.csv"
    pd.concat(
        [
            largest_up.assign(shift_direction="largest_upward_delta"),
            largest_down.assign(shift_direction="largest_downward_delta"),
        ],
        ignore_index=True,
    ).to_csv(largest_path, index=False)

    hl_path = euro / "gcam_reasonableness_high_raw_low_trust.csv"
    cols = [c for c in ["pid", "team_name", "country_name", "rating", "adjusted_rating",
                        "delta_adjusted_minus_raw", "trust_factor", "effective_connectivity", "primary_community"]
            if c in hl.columns]
    hl[cols].to_csv(hl_path, index=False)

    comm_week_path = euro / "gcam_reasonableness_community_latest_week.csv"
    if comm_path.exists():
        cw = pd.read_csv(comm_path, low_memory=False)
        if "rating_period" in cw.columns:
            cw_last = cw.loc[cw["rating_period"].astype(int) == week].copy()
            cw_last.to_csv(comm_week_path, index=False)
        else:
            comm_week_path = None
    else:
        comm_week_path = None

    out_json = euro / "gcam_reasonableness_summary.json"
    payload = {
        **summary,
        "outputs": {
            "by_ec_decile_csv": _rel_under(root, dec_path),
            "largest_shifts_csv": _rel_under(root, largest_path),
            "high_raw_low_trust_csv": _rel_under(root, hl_path),
            "by_community_csv": _rel_under(root, comm_agg_path) if comm_agg_path else None,
            "community_week_slice_csv": _rel_under(root, comm_week_path) if comm_week_path else None,
        },
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Rating week {week}: {len(snap)} teams")
    print(f"Wrote {out_json}")
    print(f"Wrote {dec_path}")
    print(f"Wrote {largest_path}")
    print(f"Wrote {hl_path}")
    if comm_agg_path:
        print(f"Wrote {comm_agg_path}")
    if comm_week_path:
        print(f"Wrote {comm_week_path}")
    print(
        f"Mean delta (adj - raw): {summary['mean_delta_adjusted_minus_raw']:.3f} | "
        f"corr(rank EC, rank delta): {corr_ec_delta:.4f}"
    )


if __name__ == "__main__":
    main()
