"""
Random-search optimisation of GCAM post-hoc parameters against expected-score prediction quality.

This script does NOT rerun raw Glicko. It keeps the existing raw weekly ratings fixed and
recomputes GCAM post-hoc outputs for candidate GCAM parameter sets.

Objective options:
  - all_mae   : minimise MAE on all matches
  - uefa_mae  : minimise MAE on UEFA/EURO subset
  - blend_mae : weighted mean of all + UEFA MAE

Outputs:
  - output/europe/gcam_optim_trials.csv
  - output/europe/gcam_optim_best.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT / "libs"))

from gcam.config import GCAMConfig  # noqa: E402
from gcam.football import DEFAULT_UEFA_CODES, fact_table_to_weighted_matches  # noqa: E402
from gcam.pipeline import run_posthoc_gcam  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Optimise GCAM params against expected-score MAE.")
    p.add_argument("--output-root", type=str, default="output")
    p.add_argument("--trials", type=int, default=30, help="Number of random parameter sets.")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--objective",
        choices=("all_mae", "uefa_mae", "blend_mae"),
        default="blend_mae",
        help="Objective used for selecting best config.",
    )
    p.add_argument(
        "--uefa-weight",
        type=float,
        default=0.5,
        help="Only for blend_mae: objective = (1-w)*all_mae + w*uefa_mae",
    )
    p.add_argument(
        "--last-weeks",
        type=int,
        default=None,
        help="Optional speed filter: only evaluate matches from the last N distinct weeks.",
    )
    p.add_argument(
        "--out-prefix",
        type=str,
        default="gcam_optim",
        help="Output file prefix under output/europe/",
    )
    return p.parse_args()


def _elo_expected_home(rating_diff_home_minus_away: np.ndarray) -> np.ndarray:
    d = np.asarray(rating_diff_home_minus_away, dtype=float)
    return 1.0 / (1.0 + 10.0 ** ((-d) / 400.0))


def _mae_rmse(y: np.ndarray, p: np.ndarray) -> tuple[float, float]:
    r = y - p
    return float(np.mean(np.abs(r))), float(np.sqrt(np.mean(r**2)))


def _filter_last_distinct_weeks(df: pd.DataFrame, last_weeks: int | None) -> pd.DataFrame:
    if last_weeks is None or int(last_weeks) <= 0:
        return df.copy()
    wk = df["week"].astype(int)
    distinct = sorted(pd.unique(wk))
    if len(distinct) <= int(last_weeks):
        return df.copy()
    keep = set(distinct[-int(last_weeks) :])
    return df.loc[wk.isin(keep)].copy()


def _load_inputs(output_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    euro = output_root / "europe"
    weekly_path = euro / "europe_weekly_ratings.csv"
    match_path = euro / "europe_match_results.csv"
    pred_path = euro / "europe_predictions.csv"
    for p in (weekly_path, match_path, pred_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing required file: {p}")
    weekly = pd.read_csv(weekly_path)
    matches = pd.read_csv(match_path)
    pred = pd.read_csv(pred_path)
    return weekly, matches, pred


def _build_merged_eval_frame(matches: pd.DataFrame, pred: pd.DataFrame) -> pd.DataFrame:
    pred2 = pred.copy()
    pred2["week"] = pred2["week"].astype(int)
    pred2["PlayerA"] = pred2["PlayerA"].astype(int)
    pred2["PlayerB"] = pred2["PlayerB"].astype(int)

    m2 = matches.copy()
    m2["week"] = m2["week"].astype(int)
    m2["home_team_id"] = m2["home_team_id"].astype(int)
    m2["away_team_id"] = m2["away_team_id"].astype(int)
    m2j = m2.rename(columns={"home_team_id": "PlayerA", "away_team_id": "PlayerB"})

    merged = pred2.merge(
        m2j,
        on=["week", "PlayerA", "PlayerB"],
        how="inner",
        suffixes=("_pred", ""),
    )
    merged = merged.dropna(subset=["pred_pA", "actual_scoreA", "home_pre_rating", "away_pre_rating"])
    return merged.reset_index(drop=True)


def _trim_for_eval_window(
    weekly: pd.DataFrame,
    fact_like: pd.DataFrame,
    eval_matches: pd.DataFrame,
    rolling_weeks_max: int = 156,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reduce GCAM compute workload to only what can affect the evaluation slice.
    Keeps weekly rows from (min_eval_week - rolling_weeks_max - 1) onward and
    fact rows from corresponding dates onward.
    """
    if eval_matches.empty:
        return weekly.copy(), fact_like.copy()
    eval_weeks = sorted(pd.unique(eval_matches["week"].astype(int)))
    min_eval_week = int(eval_weeks[0])
    max_eval_week = int(eval_weeks[-1])
    min_keep_week = max(190001, int(min_eval_week - rolling_weeks_max - 2))
    wk_trim = weekly.loc[weekly["week"].astype(int).between(min_keep_week, max_eval_week)].copy()
    fact_trim = fact_like.loc[fact_like["yyyyww"].astype(int).between(min_keep_week, max_eval_week)].copy()
    return wk_trim, fact_trim


def _build_country_map_from_matches(matches: pd.DataFrame) -> dict[int, str]:
    parts = []
    if {"home_team_id", "home_country"}.issubset(matches.columns):
        h = matches[["home_team_id", "home_country"]].rename(columns={"home_team_id": "pid", "home_country": "country"})
        parts.append(h)
    if {"away_team_id", "away_country"}.issubset(matches.columns):
        a = matches[["away_team_id", "away_country"]].rename(columns={"away_team_id": "pid", "away_country": "country"})
        parts.append(a)
    if not parts:
        return {}
    cc = pd.concat(parts, ignore_index=True)
    cc["pid"] = cc["pid"].astype(int)
    cc["country"] = cc["country"].fillna("unknown").astype(str)
    cc = cc.sort_values(["pid", "country"]).drop_duplicates(subset=["pid"], keep="first")
    return {int(r["pid"]): str(r["country"]) for _, r in cc.iterrows()}


def _attach_pre_match_adjusted(merged_eval: pd.DataFrame, weekly_gcam: pd.DataFrame) -> pd.DataFrame:
    wk = weekly_gcam[["pid", "week", "adjusted_rating"]].copy()
    wk = wk.dropna(subset=["pid", "week", "adjusted_rating"])
    wk["pid"] = wk["pid"].astype(int)
    wk["week"] = wk["week"].astype(int)
    wk = wk.rename(columns={"week": "wk_week"}).sort_values(["wk_week", "pid"]).reset_index(drop=True)

    base = merged_eval.copy()
    base["match_week"] = base["week"].astype(int)
    base = base.reset_index(drop=False).rename(columns={"index": "_idx"})

    h_left = base[["_idx", "PlayerA", "match_week"]].rename(columns={"PlayerA": "pid"})
    h_left = h_left.sort_values(["match_week", "pid"]).reset_index(drop=True)
    h_asof = pd.merge_asof(
        h_left,
        wk,
        left_on="match_week",
        right_on="wk_week",
        by="pid",
        direction="backward",
        allow_exact_matches=False,
    )
    h_adj = h_asof[["_idx", "adjusted_rating"]].rename(columns={"adjusted_rating": "home_pre_adjusted_rating"})

    a_left = base[["_idx", "PlayerB", "match_week"]].rename(columns={"PlayerB": "pid"})
    a_left = a_left.sort_values(["match_week", "pid"]).reset_index(drop=True)
    a_asof = pd.merge_asof(
        a_left,
        wk,
        left_on="match_week",
        right_on="wk_week",
        by="pid",
        direction="backward",
        allow_exact_matches=False,
    )
    a_adj = a_asof[["_idx", "adjusted_rating"]].rename(columns={"adjusted_rating": "away_pre_adjusted_rating"})

    out = base.merge(h_adj, on="_idx", how="left").merge(a_adj, on="_idx", how="left")
    out["home_pre_adjusted_rating"] = out["home_pre_adjusted_rating"].fillna(out["home_pre_rating"].astype(float))
    out["away_pre_adjusted_rating"] = out["away_pre_adjusted_rating"].fillna(out["away_pre_rating"].astype(float))
    out = out.sort_values("_idx").drop(columns=["_idx"])
    return out


def _make_cfg_from_sample(base_cfg: GCAMConfig, u: dict[str, float]) -> GCAMConfig:
    cfg = GCAMConfig(**asdict(base_cfg))
    cfg.rolling_weeks = int(round(u["rolling_weeks"]))
    cfg.connectivity_floor = float(u["connectivity_floor"])
    cfg.community_connectivity_floor = float(u["community_connectivity_floor"])
    cfg.volume_trust_half_life = float(u["volume_trust_half_life"])
    cfg.direct_vs_community_blend = float(u["direct_vs_community_blend"])
    cfg.structural_rd_scale = float(u["structural_rd_scale"])
    cfg.structural_rd_gamma = float(u["structural_rd_gamma"])
    cfg.trust_floor = float(u["trust_floor"])
    cfg.trust_rd_scale = float(u["trust_rd_scale"])
    cfg.baseline_global_weight = float(u["baseline_global_weight"])
    cfg.baseline_mode = str(u["baseline_mode"])
    return cfg


def _sample_params(rng: np.random.Generator) -> dict[str, float | str]:
    return {
        "rolling_weeks": float(rng.integers(52, 157)),
        "connectivity_floor": float(rng.uniform(0.01, 0.25)),
        "community_connectivity_floor": float(rng.uniform(0.01, 0.25)),
        "volume_trust_half_life": float(rng.uniform(8.0, 80.0)),
        "direct_vs_community_blend": float(rng.uniform(0.25, 0.9)),
        "structural_rd_scale": float(rng.uniform(8.0, 120.0)),
        "structural_rd_gamma": float(rng.uniform(0.7, 2.0)),
        "trust_floor": float(rng.uniform(0.05, 0.45)),
        "trust_rd_scale": float(rng.uniform(80.0, 320.0)),
        "baseline_global_weight": float(rng.uniform(0.35, 1.0)),
        "baseline_mode": str(rng.choice(np.array(["blend", "global"], dtype=object))),
    }


def _objective_value(all_mae: float, uefa_mae: float, objective: str, uefa_weight: float) -> float:
    if objective == "all_mae":
        return float(all_mae)
    if objective == "uefa_mae":
        return float(uefa_mae)
    w = float(max(0.0, min(1.0, uefa_weight)))
    return float((1.0 - w) * all_mae + w * uefa_mae)


def main() -> None:
    args = _parse_args()
    out_root = Path(args.output_root)
    if not out_root.is_absolute():
        out_root = (Path.cwd() / out_root).resolve()
    euro = out_root / "europe"
    euro.mkdir(parents=True, exist_ok=True)

    weekly, matches, pred = _load_inputs(out_root)
    merged_eval = _build_merged_eval_frame(matches, pred)
    merged_eval = _filter_last_distinct_weeks(merged_eval, args.last_weeks)
    if merged_eval.empty:
        raise RuntimeError("No merged evaluation rows after filtering.")

    # Raw benchmark from pred_pA (constant across trials).
    y_all = merged_eval["actual_scoreA"].astype(float).to_numpy()
    p_raw_all = merged_eval["pred_pA"].astype(float).to_numpy()
    raw_all_mae, raw_all_rmse = _mae_rmse(y_all, p_raw_all)
    uefa_mask = merged_eval["competition"].astype(str).str.upper().isin(DEFAULT_UEFA_CODES)
    if not np.any(uefa_mask):
        raise RuntimeError("No UEFA/EURO rows in evaluation slice.")
    y_u = merged_eval.loc[uefa_mask, "actual_scoreA"].astype(float).to_numpy()
    p_raw_u = merged_eval.loc[uefa_mask, "pred_pA"].astype(float).to_numpy()
    raw_uefa_mae, raw_uefa_rmse = _mae_rmse(y_u, p_raw_u)

    # Build fixed weighted fixture base inputs.
    country_map = _build_country_map_from_matches(matches)
    if not country_map:
        raise RuntimeError("Could not derive team->country mapping from match results.")
    fact_like = matches.rename(
        columns={
            "home_team_id": "home_club_id",
            "away_team_id": "away_club_id",
            "week": "yyyyww",
            "competition": "league_code",
            "home_country": "country_name",
        }
    ).copy()
    need_cols = ["home_club_id", "away_club_id", "home_goals", "away_goals", "match_date", "yyyyww", "league_code"]
    miss = [c for c in need_cols if c not in fact_like.columns]
    if miss:
        raise RuntimeError(f"Missing columns in europe_match_results for optimisation: {miss}")
    fact_like = fact_like.rename(columns={"home_goals": "home_team_goals", "away_goals": "away_team_goals"})
    weekly_base_full = weekly.copy()
    # Ensure post-hoc GCAM always starts from raw weekly Glicko fields only.
    drop_existing_gcam = [
        "primary_community",
        "entropy",
        "normalized_entropy",
        "volume_trust",
        "direct_connectivity",
        "community_connectivity",
        "effective_connectivity",
        "n_weighted_interactions",
        "n_interactions",
        "n_distinct_opponent_communities",
        "n_distinct_opponents",
        "global_mean_rating",
        "community_mean_rating",
        "baseline_rating",
        "structural_rd",
        "total_rd",
        "trust_factor",
        "adjusted_rating",
        "power_score",
    ]
    weekly_base_full = weekly_base_full.drop(
        columns=[c for c in drop_existing_gcam if c in weekly_base_full.columns],
        errors="ignore",
    )
    weekly_base_full["week"] = weekly_base_full["week"].astype(int)
    fact_like, weekly_base = fact_like.copy(), weekly_base_full.copy()
    if args.last_weeks is not None and int(args.last_weeks) > 0:
        weekly_base, fact_like = _trim_for_eval_window(
            weekly=weekly_base_full,
            fact_like=fact_like,
            eval_matches=merged_eval,
            rolling_weeks_max=160,
        )
    weighted_matches = fact_table_to_weighted_matches(fact_like, country_map, GCAMConfig(), DEFAULT_UEFA_CODES)

    weekly_base = weekly_base.copy()
    for c in ("pid", "week"):
        weekly_base[c] = weekly_base[c].astype(int)
    if "rating" not in weekly_base.columns or "rd" not in weekly_base.columns:
        raise RuntimeError("Weekly ratings must contain rating and rd columns.")

    rng = np.random.default_rng(int(args.seed))
    base_cfg = GCAMConfig()
    trials_out: list[dict[str, Any]] = []
    best_row: dict[str, Any] | None = None

    for i in range(int(args.trials)):
        sample = _sample_params(rng)
        cfg = _make_cfg_from_sample(base_cfg, sample)

        weekly_gcam, _ = run_posthoc_gcam(weekly_base, weighted_matches, cfg)
        eval_aug = _attach_pre_match_adjusted(merged_eval, weekly_gcam)
        p_adj_all = _elo_expected_home(
            (
                eval_aug["home_pre_adjusted_rating"].astype(float)
                - eval_aug["away_pre_adjusted_rating"].astype(float)
            ).to_numpy()
        )
        all_mae, all_rmse = _mae_rmse(y_all, p_adj_all)
        p_adj_u = p_adj_all[uefa_mask.to_numpy()]
        u_mae, u_rmse = _mae_rmse(y_u, p_adj_u)
        obj = _objective_value(all_mae, u_mae, args.objective, args.uefa_weight)

        row = {
            "trial": i + 1,
            "objective": obj,
            "all_mae_adjusted": all_mae,
            "all_rmse_adjusted": all_rmse,
            "uefa_mae_adjusted": u_mae,
            "uefa_rmse_adjusted": u_rmse,
            "all_mae_raw": raw_all_mae,
            "all_rmse_raw": raw_all_rmse,
            "uefa_mae_raw": raw_uefa_mae,
            "uefa_rmse_raw": raw_uefa_rmse,
            "delta_all_mae_adjusted_minus_raw": all_mae - raw_all_mae,
            "delta_uefa_mae_adjusted_minus_raw": u_mae - raw_uefa_mae,
            **sample,
        }
        trials_out.append(row)
        if best_row is None or float(row["objective"]) < float(best_row["objective"]):
            best_row = row
            print(
                f"[trial {i+1:03d}] new best objective={obj:.6f} "
                f"(all_mae={all_mae:.6f}, uefa_mae={u_mae:.6f})"
            )

    if best_row is None:
        raise RuntimeError("No trials executed.")

    trials_df = pd.DataFrame(trials_out).sort_values("objective").reset_index(drop=True)
    trials_path = euro / f"{args.out_prefix}_trials.csv"
    trials_df.to_csv(trials_path, index=False)

    best_cfg = _make_cfg_from_sample(base_cfg, best_row)
    out_doc = {
        "objective": args.objective,
        "uefa_weight": float(args.uefa_weight),
        "seed": int(args.seed),
        "trials": int(args.trials),
        "last_weeks": args.last_weeks,
        "rows_used_all": int(len(merged_eval)),
        "rows_used_uefa": int(np.sum(uefa_mask.to_numpy())),
        "raw_benchmark": {
            "all_mae": raw_all_mae,
            "all_rmse": raw_all_rmse,
            "uefa_mae": raw_uefa_mae,
            "uefa_rmse": raw_uefa_rmse,
        },
        "best_trial": best_row,
        "best_gcam_config": asdict(best_cfg),
        "trials_csv": str(trials_path),
    }
    best_path = euro / f"{args.out_prefix}_best.json"
    best_path.write_text(json.dumps(out_doc, indent=2), encoding="utf-8")

    print(f"Wrote {trials_path} ({len(trials_df)} trials)")
    print(f"Wrote {best_path}")
    print(
        "Best deltas vs raw: "
        f"all_mae {best_row['delta_all_mae_adjusted_minus_raw']:+.6f}, "
        f"uefa_mae {best_row['delta_uefa_mae_adjusted_minus_raw']:+.6f}"
    )


if __name__ == "__main__":
    main()

