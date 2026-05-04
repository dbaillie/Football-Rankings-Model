"""
Empirical calibration of Europe Glicko match predictions vs outcomes.

Joins engine row-level predictions with match-level pre-ratings from the same run,
bins by home minus away pre-match rating difference, and summarises how well
``pred_pA`` (the Glicko expectation for the home side) matches realised results.

Inputs (from ``run_glicko_europe.py``):
  {output_root}/europe/europe_predictions.csv
  {output_root}/europe/europe_match_results.csv

Outputs (for dashboards / a future web tab):
  {output_root}/europe/calibration_bins.csv
  {output_root}/europe/calibration_summary.json   # GET /api/calibration

Usage:
  python scripts/run_glicko_europe.py
  python scripts/analyse_europe_calibration.py
  python scripts/analyse_europe_calibration.py --output-root output --bin-width 50
  python scripts/analyse_europe_calibration.py --last-weeks 104   # ~2 years of rating weeks
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

UEFA_COMP_CODES = frozenset({"UCL", "UEL", "UECL", "EURO"})


def _rel_under(root: Path, p: Path) -> str:
    try:
        return str(p.relative_to(root))
    except ValueError:
        return str(p)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Calibration analysis for europe_predictions vs match results.")
    p.add_argument(
        "--output-root",
        type=str,
        default="output",
        help="Root containing europe/ (default: output)",
    )
    p.add_argument(
        "--bin-width",
        type=float,
        default=50.0,
        help="Fixed-width bins for home_pre_rating - away_pre_rating (default: 50)",
    )
    p.add_argument(
        "--min-bin-count",
        type=int,
        default=25,
        help="Bins with fewer rows are still listed but flagged with low_n=true (default: 25)",
    )
    p.add_argument(
        "--last-weeks",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Only include matches whose rating week (yyyyww) falls in the last N distinct weeks "
            "in the merged data. Omit for full history."
        ),
    )
    return p.parse_args()


def _filter_last_distinct_weeks(df: pd.DataFrame, last_weeks: int | None) -> tuple[pd.DataFrame, dict]:
    """
    Keep rows whose ``week`` is among the last ``last_weeks`` distinct values when sorted.

    ``yyyyww`` integers sort in chronological order for typical football-era ISO weeks.
    Returns (filtered_df, metadata dict).
    """
    if last_weeks is None or int(last_weeks) <= 0:
        return df, {"applied": False, "last_weeks_requested": None}

    n_req = int(last_weeks)
    wcol = df["week"].astype(int)
    distinct = sorted(pd.unique(wcol))
    if len(distinct) <= n_req:
        return df.copy(), {
            "applied": True,
            "last_weeks_requested": n_req,
            "distinct_weeks_available": len(distinct),
            "distinct_weeks_used": len(distinct),
            "week_id_min": int(distinct[0]),
            "week_id_max": int(distinct[-1]),
            "truncated_to_all_available": True,
        }

    keep = set(distinct[-n_req:])
    out = df.loc[wcol.isin(keep)].copy()
    return out, {
        "applied": True,
        "last_weeks_requested": n_req,
        "distinct_weeks_available": len(distinct),
        "distinct_weeks_used": len(keep),
        "week_id_min": int(min(keep)),
        "week_id_max": int(max(keep)),
        "truncated_to_all_available": False,
    }


def _elo_expected_home(rating_diff_home_minus_away: np.ndarray) -> np.ndarray:
    """Classic Elo logistic on rating difference (home - away), same shape as webapp data_service."""
    d = np.asarray(rating_diff_home_minus_away, dtype=float)
    return 1.0 / (1.0 + 10.0 ** ((-d) / 400.0))


def _expected_score_metrics(y: np.ndarray, p: np.ndarray) -> dict[str, float]:
    resid = y - p
    return {
        "mae": float(np.mean(np.abs(resid))),
        "rmse": float(np.sqrt(np.mean(resid**2))),
        "mean_actual_score": float(np.mean(y)),
        "mean_pred_score": float(np.mean(p)),
    }


def _attach_pre_match_adjusted_ratings(
    matches: pd.DataFrame,
    weekly: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each fixture row, attach latest available adjusted rating strictly before match week
    for home and away teams. Falls back to raw pre-ratings when no adjusted history exists.
    """
    req_w = {"pid", "week", "adjusted_rating"}
    if not req_w.issubset(weekly.columns):
        out = matches.copy()
        out["home_pre_adjusted_rating"] = out["home_pre_rating"].astype(float)
        out["away_pre_adjusted_rating"] = out["away_pre_rating"].astype(float)
        return out

    wk = weekly[["pid", "week", "adjusted_rating"]].copy()
    wk = wk.dropna(subset=["pid", "week", "adjusted_rating"])
    if wk.empty:
        out = matches.copy()
        out["home_pre_adjusted_rating"] = out["home_pre_rating"].astype(float)
        out["away_pre_adjusted_rating"] = out["away_pre_rating"].astype(float)
        return out
    wk["pid"] = wk["pid"].astype(int)
    wk["week"] = wk["week"].astype(int)
    wk = wk.sort_values(["pid", "week"]).reset_index(drop=True)

    base = matches.copy()
    home_id_col = "home_team_id" if "home_team_id" in base.columns else "PlayerA"
    away_id_col = "away_team_id" if "away_team_id" in base.columns else "PlayerB"
    base["match_week"] = base["week"].astype(int)
    base = base.sort_values([home_id_col, "match_week"]).reset_index(drop=False)

    # Home side: last adjusted rating from same pid at week < match_week
    left_home = base[["index", home_id_col, "match_week"]].rename(columns={home_id_col: "pid"})
    left_home["pid"] = left_home["pid"].astype(int)
    left_home = left_home.sort_values(["match_week", "pid"]).reset_index(drop=True)
    right = wk.rename(columns={"week": "wk_week"})
    right = right.sort_values(["wk_week", "pid"]).reset_index(drop=True)
    home_asof = pd.merge_asof(
        left_home,
        right,
        left_on="match_week",
        right_on="wk_week",
        by="pid",
        direction="backward",
        allow_exact_matches=False,
    )
    home_adj = home_asof[["index", "adjusted_rating"]].rename(columns={"adjusted_rating": "home_pre_adjusted_rating"})

    # Away side
    left_away = base[["index", away_id_col, "match_week"]].rename(columns={away_id_col: "pid"})
    left_away["pid"] = left_away["pid"].astype(int)
    left_away = left_away.sort_values(["match_week", "pid"]).reset_index(drop=True)
    away_asof = pd.merge_asof(
        left_away,
        right,
        left_on="match_week",
        right_on="wk_week",
        by="pid",
        direction="backward",
        allow_exact_matches=False,
    )
    away_adj = away_asof[["index", "adjusted_rating"]].rename(columns={"adjusted_rating": "away_pre_adjusted_rating"})

    out = base.merge(home_adj, on="index", how="left").merge(away_adj, on="index", how="left")
    out["home_pre_adjusted_rating"] = out["home_pre_adjusted_rating"].fillna(out["home_pre_rating"].astype(float))
    out["away_pre_adjusted_rating"] = out["away_pre_adjusted_rating"].fillna(out["away_pre_rating"].astype(float))
    out = out.sort_values("index").drop(columns=["index"])
    return out


def _predictor_head_to_head_block(
    frame: pd.DataFrame,
    weekly: pd.DataFrame,
    label: str,
) -> dict[str, object]:
    """Compare raw pred_pA vs adjusted-rating logistic expectation on a given match slice."""
    out: dict[str, object] = {
        "scope": label,
        "rows_used": int(len(frame)),
    }
    if frame.empty:
        return out

    comp = _attach_pre_match_adjusted_ratings(frame, weekly)
    rd_adj = (
        comp["home_pre_adjusted_rating"].astype(float)
        - comp["away_pre_adjusted_rating"].astype(float)
    ).to_numpy()
    p_adj = _elo_expected_home(rd_adj)
    yy = comp["actual_scoreA"].astype(float).to_numpy()
    p_raw = comp["pred_pA"].astype(float).to_numpy()
    raw_m = _expected_score_metrics(yy, p_raw)
    adj_m = _expected_score_metrics(yy, p_adj)
    winner = "gcam_adjusted_pre_ratings" if adj_m["mae"] < raw_m["mae"] else "raw_glicko_pred_pA"
    out.update(
        {
            "raw_glicko_pred_pA": raw_m,
            "gcam_adjusted_pre_ratings": adj_m,
            "delta_mae_adjusted_minus_raw": float(adj_m["mae"] - raw_m["mae"]),
            "delta_rmse_adjusted_minus_raw": float(adj_m["rmse"] - raw_m["rmse"]),
            "better_by_mae": winner,
            "notes": [
                "Both predictors are compared on expected-score targets (1/0.5/0) for home side.",
                "GCAM predictor uses Elo-400 logistic on pre-match adjusted rating difference.",
                "When no prior adjusted weekly row exists for a team, raw pre-rating fallback is used.",
            ],
        }
    )
    return out


def main() -> None:
    args = _parse_args()
    root = Path(args.output_root)
    if not root.is_absolute():
        root = (Path.cwd() / root).resolve()
    euro = root / "europe"
    pred_path = euro / "europe_predictions.csv"
    match_path = euro / "europe_match_results.csv"
    weekly_path = euro / "europe_weekly_ratings.csv"
    if not weekly_path.is_file():
        wt = euro / "europe_weekly_ratings.txt"
        if wt.is_file():
            weekly_path = wt

    if not pred_path.exists():
        print(f"Missing {pred_path}. Run scripts/run_glicko_europe.py first.", file=sys.stderr)
        sys.exit(1)
    if not match_path.exists():
        print(f"Missing {match_path}. Run scripts/run_glicko_europe.py first.", file=sys.stderr)
        sys.exit(1)

    pred = pd.read_csv(pred_path)
    mat = pd.read_csv(match_path)
    weekly = pd.read_csv(weekly_path) if weekly_path.is_file() else pd.DataFrame()

    required_p = {"week", "PlayerA", "PlayerB", "actual_scoreA", "pred_pA"}
    required_m = {"week", "home_team_id", "away_team_id", "home_pre_rating", "away_pre_rating", "result"}
    if not required_p.issubset(pred.columns):
        print(f"europe_predictions.csv missing columns: {required_p - set(pred.columns)}", file=sys.stderr)
        sys.exit(1)
    if not required_m.issubset(mat.columns):
        print(f"europe_match_results.csv missing columns: {required_m - set(mat.columns)}", file=sys.stderr)
        sys.exit(1)

    pred = pred.copy()
    pred["week"] = pred["week"].astype(int)
    pred["PlayerA"] = pred["PlayerA"].astype(int)
    pred["PlayerB"] = pred["PlayerB"].astype(int)

    mat = mat.copy()
    mat["week"] = mat["week"].astype(int)
    mat["home_team_id"] = mat["home_team_id"].astype(int)
    mat["away_team_id"] = mat["away_team_id"].astype(int)

    key_cols = ["week", "PlayerA", "PlayerB"]
    mat_for_join = mat.rename(
        columns={
            "home_team_id": "PlayerA",
            "away_team_id": "PlayerB",
        }
    )

    merged = pred.merge(
        mat_for_join,
        on=key_cols,
        how="inner",
        suffixes=("_pred", ""),
    )

    n_pred = len(pred)
    n_mat = len(mat)
    n_merge_inner = len(merged)

    if n_merge_inner == 0:
        print("No rows merged — check week/home/away IDs align between predictions and match results.", file=sys.stderr)
        sys.exit(1)

    if n_merge_inner < n_pred:
        print(
            f"Warning: merged {n_merge_inner} of {n_pred} prediction rows ({n_pred - n_merge_inner} unmatched)."
        )

    merged = merged.dropna(subset=["home_pre_rating", "away_pre_rating", "pred_pA", "actual_scoreA"])
    merged = merged[np.isfinite(merged["home_pre_rating"].to_numpy(dtype=float))].copy()
    n_merge_after_dropna = len(merged)
    if n_merge_after_dropna == 0:
        print("No rows left after dropping NaN / non-finite pre-ratings.", file=sys.stderr)
        sys.exit(1)

    merged, week_filter_meta = _filter_last_distinct_weeks(merged, args.last_weeks)
    n_merge = len(merged)
    if n_merge == 0:
        print("No rows left after --last-weeks filter.", file=sys.stderr)
        sys.exit(1)

    if week_filter_meta.get("applied"):
        print(
            f"Week filter: last {week_filter_meta['distinct_weeks_used']} distinct rating weeks "
            f"(yyyyww {week_filter_meta['week_id_min']}–{week_filter_meta['week_id_max']}), "
            f"{n_merge:,} matches (was {n_merge_after_dropna:,} before filter)."
        )

    y = merged["actual_scoreA"].astype(float).to_numpy()
    p_hat = merged["pred_pA"].astype(float).to_numpy()
    rd = (merged["home_pre_rating"] - merged["away_pre_rating"]).astype(float).to_numpy()

    merged["rating_diff_home_minus_away"] = rd
    merged["elo_expected_home"] = _elo_expected_home(rd)

    base_metrics = _expected_score_metrics(y, p_hat)
    mae = base_metrics["mae"]
    rmse = base_metrics["rmse"]
    mean_y = base_metrics["mean_actual_score"]
    mean_p = base_metrics["mean_pred_score"]

    elo_res = y - merged["elo_expected_home"].to_numpy(dtype=float)
    mae_elo = float(np.mean(np.abs(elo_res)))
    rmse_elo = float(np.sqrt(np.mean(elo_res**2)))

    bin_w = float(args.bin_width)
    low = np.floor(np.nanmin(rd) / bin_w) * bin_w
    high = np.ceil(np.nanmax(rd) / bin_w) * bin_w
    edges = np.arange(low, high + bin_w + 1e-9, bin_w)
    merged["rating_bin"] = pd.cut(merged["rating_diff_home_minus_away"], bins=edges, right=False, include_lowest=True)

    rows_out: list[dict] = []
    bin_summaries: list[dict] = []

    min_n = int(args.min_bin_count)

    for interval in merged["rating_bin"].cat.categories:
        sub = merged.loc[merged["rating_bin"] == interval]
        if sub.empty:
            continue
        nn = int(len(sub))
        left = float(interval.left)
        right = float(interval.right)
        mid = (left + right) / 2.0
        yy = sub["actual_scoreA"].astype(float).to_numpy()
        pp = sub["pred_pA"].astype(float).to_numpy()
        rdd = sub["rating_diff_home_minus_away"].astype(float).to_numpy()

        h = np.isclose(yy, 1.0)
        d = np.isclose(yy, 0.5)
        aw = np.isclose(yy, 0.0)

        row_csv = {
            "rating_diff_bin_low": left,
            "rating_diff_bin_high": right,
            "rating_diff_bin_mid": mid,
            "n_matches": nn,
            "mean_rating_diff": float(np.mean(rdd)),
            "mean_pred_pA": float(np.mean(pp)),
            "mean_actual_score": float(np.mean(yy)),
            "calibration_gap_expected_score": float(np.mean(yy) - np.mean(pp)),
            "empirical_p_home_win": float(np.mean(h)),
            "empirical_p_draw": float(np.mean(d)),
            "empirical_p_away_win": float(np.mean(aw)),
            "mean_elo_expected_home": float(np.mean(sub["elo_expected_home"])),
            "low_n": nn < min_n,
        }
        rows_out.append(row_csv)

        bin_summaries.append(
            {
                "rating_diff_low": left,
                "rating_diff_high": right,
                "rating_diff_mid": mid,
                "n": nn,
                "low_n": nn < min_n,
                "mean_rating_diff": row_csv["mean_rating_diff"],
                "mean_pred_pA": row_csv["mean_pred_pA"],
                "mean_actual_score": row_csv["mean_actual_score"],
                "calibration_gap_expected_score": row_csv["calibration_gap_expected_score"],
                "empirical_p_home_win": row_csv["empirical_p_home_win"],
                "empirical_p_draw": row_csv["empirical_p_draw"],
                "empirical_p_away_win": row_csv["empirical_p_away_win"],
                "mean_elo_expected_home": row_csv["mean_elo_expected_home"],
            }
        )

    bins_df = pd.DataFrame(rows_out)
    bins_path = euro / "calibration_bins.csv"
    bins_df.to_csv(bins_path, index=False)

    # Predictor comparison blocks: all matches + UEFA/EURO subset.
    all_matches_compare = _predictor_head_to_head_block(merged, weekly, label="all_matches")
    uefa_mask = merged.get("competition", pd.Series("", index=merged.index)).astype(str).str.upper().isin(UEFA_COMP_CODES)
    uefa_comp = merged.loc[uefa_mask].copy()
    uefa_compare = _predictor_head_to_head_block(uefa_comp, weekly, label="uefa_only")
    uefa_compare["competition_codes"] = sorted(UEFA_COMP_CODES)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "output_root": str(root),
        "source_files": {
            "predictions": _rel_under(root, pred_path),
            "match_results": _rel_under(root, match_path),
        },
        "bin_width": bin_w,
        "min_bin_count_flag": min_n,
        "filters": week_filter_meta,
        "counts": {
            "prediction_rows": n_pred,
            "match_rows": n_mat,
            "merged_inner_join_rows": n_merge_inner,
            "merged_rows_after_dropna": n_merge_after_dropna,
            "merged_rows_used_after_week_filter": n_merge,
        },
        "global_metrics": {
            "mae_expected_score_glicko_pred": mae,
            "rmse_expected_score_glicko_pred": rmse,
            "mean_actual_score": mean_y,
            "mean_pred_pA": mean_p,
            "mae_expected_score_elo400_baseline": mae_elo,
            "rmse_expected_score_elo400_baseline": rmse_elo,
        },
        "predictor_comparisons": {
            "all_matches": all_matches_compare,
            "uefa_only": uefa_compare,
        },
        # Backward-compatible alias for existing consumers.
        "uefa_only_predictor_comparison": uefa_compare,
        "bins": bin_summaries,
        "notes": [
            "actual_scoreA is 1 (home win), 0.5 (draw), 0 (away win); pred_pA is the engine pre-match Glicko expectation E(PlayerA).",
            "It is not a full win/draw/loss distribution — calibration compares mean realised score to mean pred within bins.",
            "elo400 baseline uses home_pre_rating - away_pre_rating with the same 400-divisor logistic as the webapp upset heuristic.",
            "Calibration tab plots rating_diff_mid vs mean_actual_score and mean_pred_pA; empirical_p_home_win can be overlaid on the same axes (0–1 scale).",
        ],
    }

    json_path = euro / "calibration_summary.json"
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Wrote {bins_path} ({len(bins_df)} bins)")
    print(f"Wrote {json_path}")
    lf = f" | last_weeks={args.last_weeks}" if args.last_weeks else ""
    print(
        f"Global MAE (score): Glicko pred {mae:.4f} | Elo-400 on pre-ratings {mae_elo:.4f} "
        f"| rows_used={n_merge}{lf}"
    )
    if all_matches_compare.get("rows_used", 0):
        rg_all = all_matches_compare["raw_glicko_pred_pA"]["mae"]  # type: ignore[index]
        ag_all = all_matches_compare["gcam_adjusted_pre_ratings"]["mae"]  # type: ignore[index]
        win_all = all_matches_compare.get("better_by_mae")
        print(
            "All-matches MAE (score): "
            f"raw pred_pA {float(rg_all):.4f} | adjusted-rating logistic {float(ag_all):.4f} "
            f"| winner={win_all} | rows={all_matches_compare['rows_used']}"
        )
    if uefa_compare.get("rows_used", 0):
        rg = uefa_compare["raw_glicko_pred_pA"]["mae"]  # type: ignore[index]
        ag = uefa_compare["gcam_adjusted_pre_ratings"]["mae"]  # type: ignore[index]
        winner = uefa_compare.get("better_by_mae")
        print(
            "UEFA-only MAE (score): "
            f"raw pred_pA {float(rg):.4f} | adjusted-rating logistic {float(ag):.4f} "
            f"| winner={winner} | rows={uefa_compare['rows_used']}"
        )


if __name__ == "__main__":
    main()
