"""
Optimize Glicko-2 for Europe on **initial conditions + tau only**.

All other `run` keys (decay, reseed, max_sigma, gates, etc.) are taken from
`create_config()` in `run_glicko_europe.py` and held fixed.

Optimized (4 parameters, u in (0,1)^4 via sigmoid from z):
  best_init_rating, best_init_rd, best_init_sigma, best_tau

Objective (default): mean Brier score on trinomial outcomes
  E[(pred_pA - actual_scoreA)^2]  with actual in {0, 0.5, 1}

Alternative: mean log loss on non-draw matches only (binary home win).

Optimizer: Nelder-Mead (scipy).

Example:
  python scripts/optimise_glicko_europe.py --max-weeks 200 --maxiter 40
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "libs"))

from glicko_engine.core import run_glicko2  # noqa: E402

# Reuse loader + config mapping from Europe runner
from run_glicko_europe import (  # noqa: E402
    ANALYTICAL_START_WEEK,
    create_config,
    glicko_run_kwargs,
    load_europe_data,
)


def merge_inits_and_tau(base_run: dict, u: np.ndarray) -> dict:
    """
    Map u in (0,1)^4 to bounded inits + tau, centered on current base_run values.
    u = 0.5 reproduces create_config() defaults for these four keys.
    """
    u = np.clip(np.asarray(u, dtype=float), 1e-6, 1.0 - 1e-6)
    r = dict(base_run)

    ic = float(r["best_init_rating"])
    span_ic = 400.0
    r["best_init_rating"] = float(np.clip(ic - span_ic + u[0] * (2.0 * span_ic), 1000.0, 2800.0))

    ird = float(r["best_init_rd"])
    span_rd = 150.0
    r["best_init_rd"] = float(np.clip(ird - span_rd + u[1] * (2.0 * span_rd), 50.0, 550.0))

    isig = float(r["best_init_sigma"])
    span_sig = 0.04
    r["best_init_sigma"] = float(np.clip(isig - span_sig + u[2] * (2.0 * span_sig), 0.02, 0.12))

    t0 = float(r["best_tau"])
    span_t = 0.35
    r["best_tau"] = float(np.clip(t0 - span_t + u[3] * (2.0 * span_t), 0.05, 1.5))

    return r


def z_to_u(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -40.0, 40.0)))


def evaluate_predictions(
    pred_df: pd.DataFrame,
    metric: str,
    burn_in_weeks: int,
) -> float:
    """Lower is better."""
    if pred_df is None or pred_df.empty:
        return 1e6
    wk = pred_df["week"].astype(int)
    w_sorted = sorted(wk.unique())
    if len(w_sorted) <= burn_in_weeks:
        return 1e6
    cut = w_sorted[burn_in_weeks]
    df = pred_df[wk >= cut].copy()
    if df.empty:
        return 1e6
    y = df["actual_scoreA"].to_numpy(dtype=float)
    p = df["pred_pA"].to_numpy(dtype=float)
    p = np.clip(p, 1e-9, 1.0 - 1e-9)
    if metric == "brier":
        return float(np.mean((p - y) ** 2))
    if metric == "logloss":
        # Bernoulli log loss on win vs not-win; draws excluded
        mask = np.abs(y - 0.5) > 1e-9
        if not np.any(mask):
            return 1e6
        yb = (y[mask] > 0.5).astype(float)
        pp = p[mask]
        return float(-np.mean(yb * np.log(pp) + (1.0 - yb) * np.log(1.0 - pp)))
    raise ValueError(f"Unknown metric: {metric}")


def run_objective(
    z: np.ndarray,
    matches_pdf: pd.DataFrame,
    weeks: list[int],
    metric: str,
    burn_in_weeks: int,
    base_run: dict,
    use_two_phase: bool,
) -> float:
    u = z_to_u(np.asarray(z, dtype=float))
    run_block = merge_inits_and_tau(base_run, u)
    cfg = {"run": run_block}
    kw = glicko_run_kwargs(cfg)
    try:
        if use_two_phase:
            w_all = weeks
            warm = [w for w in w_all if int(w) < ANALYTICAL_START_WEEK]
            ana = [w for w in w_all if int(w) >= ANALYTICAL_START_WEEK]
            if not ana:
                return 1e6
            m = matches_pdf[matches_pdf["week"].isin(w_all)]
            if warm:
                wm = m[m["week"] < ANALYTICAL_START_WEEK]
                warm_state, _, _, _ = run_glicko2(
                    matches_pdf=wm,
                    weeks=warm,
                    **kw,
                    seed_from_wagr=False,
                    snapshot_weeks=[warm[-1]],
                    diag_every=0,
                )
            else:
                warm_state = None
            am = m[m["week"] >= ANALYTICAL_START_WEEK]
            _, pred_df, _, _ = run_glicko2(
                matches_pdf=am,
                weeks=ana,
                **kw,
                seed_from_wagr=False,
                initial_state=warm_state,
                snapshot_weeks=ana,
                diag_every=0,
            )
        else:
            _, pred_df, _, _ = run_glicko2(
                matches_pdf=matches_pdf,
                weeks=weeks,
                **kw,
                seed_from_wagr=False,
                snapshot_weeks=weeks,
                diag_every=0,
            )
    except Exception:
        return 1e6

    return evaluate_predictions(pred_df, metric, burn_in_weeks)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--max-weeks", type=int, default=None, help="Use only the last N calendar weeks (speed).")
    p.add_argument("--burn-in-weeks", type=int, default=52, help="Ignore first N distinct weeks in loss.")
    p.add_argument("--metric", choices=("brier", "logloss"), default="brier")
    p.add_argument("--maxiter", type=int, default=50)
    p.add_argument("--two-phase", action="store_true", help="Match run_glicko_europe warm-up + analytical split.")
    p.add_argument(
        "--out",
        type=Path,
        default=ROOT / "output" / "europe" / "optim_best_config.json",
    )
    args = p.parse_args()

    base_cfg = create_config()
    base_run = base_cfg["run"]

    print("Loading Europe matches (this may take a while)...", flush=True)
    matches_glicko, _teams, _ = load_europe_data()
    m = matches_glicko[["week", "EventId", "PlayerA", "PlayerB", "scoreA"]].copy()
    w_sorted = sorted(m["week"].unique())
    if args.max_weeks is not None and len(w_sorted) > args.max_weeks:
        keep = set(w_sorted[-args.max_weeks :])
        m = m[m["week"].isin(keep)]
        w_sorted = sorted(keep)
        print(f"  Using last {args.max_weeks} weeks: {len(m)} matches", flush=True)
    else:
        print(f"  Using {len(w_sorted)} weeks, {len(m)} matches", flush=True)

    n_weeks = len(w_sorted)
    # Loss uses weeks with index >= burn_in; need strictly fewer burn weeks than distinct weeks.
    # If burn_in >= n_weeks, every eval returns 1e6 (flat objective) — looks "fast" but is meaningless.
    min_tail = max(4, min(8, n_weeks // 4))
    effective_burn = min(args.burn_in_weeks, max(0, n_weeks - min_tail))
    if effective_burn != args.burn_in_weeks:
        print(
            f"  Note: burn-in reduced from {args.burn_in_weeks} to {effective_burn} "
            f"({n_weeks} distinct weeks in slice; need tail for loss).",
            flush=True,
        )

    # Nelder-Mead start: z=0 -> u=0.5 -> merge_inits_and_tau matches create_config() for the four keys
    x0 = np.zeros(4, dtype=float)

    def fun(z: np.ndarray) -> float:
        return run_objective(
            z,
            m,
            w_sorted,
            args.metric,
            effective_burn,
            base_run,
            args.two_phase,
        )

    print(
        f"Optimizing (metric={args.metric}, burn_in_weeks={effective_burn}, "
        f"two_phase={args.two_phase}, maxiter={args.maxiter})...",
        flush=True,
    )
    res = minimize(
        fun,
        x0,
        method="Nelder-Mead",
        options={"maxiter": args.maxiter, "disp": True, "adaptive": True},
    )

    u_best = z_to_u(res.x)
    run_best = merge_inits_and_tau(base_run, u_best)
    loss_best = float(res.fun)
    out_full_cfg = create_config()
    out_full_cfg["run"] = {**out_full_cfg["run"], **run_best}
    out_doc = {
        "success": bool(res.success),
        "message": str(res.message),
        "nfev": int(res.nfev),
        "metric": args.metric,
        "loss": loss_best,
        "burn_in_weeks": effective_burn,
        "burn_in_weeks_requested": args.burn_in_weeks,
        "max_weeks": args.max_weeks,
        "two_phase": args.two_phase,
        "u_best": u_best.tolist(),
        "config": out_full_cfg,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out_doc, f, indent=2)
    print(json.dumps({k: out_doc[k] for k in ("success", "loss", "metric", "nfev")}, indent=2))
    print("Best run block:", json.dumps(run_best, indent=2))
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
