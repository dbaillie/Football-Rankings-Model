
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
from scipy.optimize import minimize

from .core import (
    log_loss_binary,
    brier_score_binary,
    accuracy_binary,
    run_glicko2,
    score_predictions_elite_only,
)
from .data import prepare_inputs, load_json, save_json


class CycleDetector:
    def __init__(self, pattern_len: int = 5, max_repeats: int = 2, tol: float = 1e-8):
        self.pattern_len = pattern_len
        self.max_repeats = max_repeats
        self.tol = tol
        self.history = []
        self.cycle_count = 0

    def record(self, params, loss):
        params_key = tuple(round(float(p), 6) for p in params)
        self.history.append((params_key, float(loss)))
        if len(self.history) < 2 * self.pattern_len:
            return
        recent = self.history[-self.pattern_len:]
        previous = self.history[-2 * self.pattern_len:-self.pattern_len]
        is_cycle = True
        for (p1, l1), (p2, l2) in zip(recent, previous):
            if p1 != p2 or abs(l1 - l2) > self.tol:
                is_cycle = False
                break
        if is_cycle:
            self.cycle_count += 1
            if self.cycle_count >= self.max_repeats:
                raise StopIteration('Cycle detected in optimiser.')
        else:
            self.cycle_count = 0


def estimate_parameters(config: dict) -> dict:
    matches_pdf, weeks_run, rank_map, rankings_weekly, _ = prepare_inputs(config)

    n_weeks = len(weeks_run)
    train_cut = max(1, int(n_weeks * float(config['estimation']['train_frac'])))
    train_weeks = weeks_run[:train_cut]
    valid_weeks = weeks_run[train_cut:]

    fixed_init_rating = float(config['run']['best_init_rating'])
    burn_in = int(config['estimation'].get('pred_burn_in_weeks', 0))
    pred_top_n = int(config['estimation'].get('pred_top_n', 500))
    cycle_detector = CycleDetector(
        pattern_len=int(config['estimation'].get('cycle_pattern_len', 5)),
        max_repeats=int(config['estimation'].get('cycle_max_repeats', 2)),
    )

    bounds = config['estimation']['param_bounds']
    start_points = config['estimation']['start_points']
    fd_eps = np.array(config['estimation'].get('fd_eps', [1.0, 0.001, 0.01, 0.001]), dtype=float)
    eval_log = []

    def objective(params):
        init_rd, init_sigma, tau, inactivity_drift = [float(x) for x in params]
        train_state, _, _, _ = run_glicko2(
            matches_pdf=matches_pdf[matches_pdf['week'].isin(train_weeks)].copy(),
            weeks=train_weeks,
            init_rating=fixed_init_rating,
            init_rd=init_rd,
            init_sigma=init_sigma,
            tau=tau,
            inactivity_drift=inactivity_drift,
            upset_gate_max=float(config['run'].get('upset_gate_max', 0.0)),
            upset_gate_k=float(config['run'].get('upset_gate_k', 0.0)),
            info_gate_scale=float(config['run'].get('info_gate_scale', 0.0)),
            inactivity_decay_pts=float(config['run'].get('inactivity_decay_pts_per_week', 0.0)),
            inactivity_decay_grace=int(config['run'].get('inactivity_decay_grace_weeks', 8)),
            reseed_after_weeks=int(config['run'].get('reseed_after_weeks', 0)),
            seed_from_wagr=bool(config['seeding'].get('seed_from_rankings', False)),
            wagr_rank_map=rank_map,
            snapshot_weeks=None,
            diag_every=0,
        )
        _, valid_pred_df, valid_snapshots, _ = run_glicko2(
            matches_pdf=matches_pdf[matches_pdf['week'].isin(valid_weeks)].copy(),
            weeks=valid_weeks,
            init_rating=fixed_init_rating,
            init_rd=init_rd,
            init_sigma=init_sigma,
            tau=tau,
            inactivity_drift=inactivity_drift,
            upset_gate_max=float(config['run'].get('upset_gate_max', 0.0)),
            upset_gate_k=float(config['run'].get('upset_gate_k', 0.0)),
            info_gate_scale=float(config['run'].get('info_gate_scale', 0.0)),
            inactivity_decay_pts=float(config['run'].get('inactivity_decay_pts_per_week', 0.0)),
            inactivity_decay_grace=int(config['run'].get('inactivity_decay_grace_weeks', 8)),
            reseed_after_weeks=int(config['run'].get('reseed_after_weeks', 0)),
            seed_from_wagr=bool(config['seeding'].get('seed_from_rankings', False)),
            wagr_rank_map=rank_map,
            initial_state=train_state,
            snapshot_weeks=set(int(w) for w in valid_weeks),
            diag_every=0,
        )

        ll = log_loss_binary(valid_pred_df['actual_scoreA'].values, valid_pred_df['pred_pA'].values)
        br = brier_score_binary(valid_pred_df['actual_scoreA'].values, valid_pred_df['pred_pA'].values)
        acc = accuracy_binary(valid_pred_df['actual_scoreA'].values, valid_pred_df['pred_pA'].values)
        elite_scores = score_predictions_elite_only(
            pred_df=valid_pred_df,
            valid_weeks=valid_weeks,
            valid_state_snapshots=valid_snapshots,
            wagr_weekly=rankings_weekly,
            init_rating_centre=fixed_init_rating,
            pred_top_n=pred_top_n,
            burn_in_weeks=burn_in,
        )
        elite_ll = float(elite_scores['model_log_loss'].mean()) if not elite_scores.empty else np.nan
        obj = elite_ll if np.isfinite(elite_ll) else ll
        row = {
            'init_rd': init_rd, 'init_sigma': init_sigma, 'tau': tau, 'inactivity_drift': inactivity_drift,
            'log_loss': ll, 'brier': br, 'accuracy': acc, 'elite_log_loss': elite_ll, 'objective': obj,
        }
        eval_log.append(row)
        cycle_detector.record(params, obj)
        return obj

    best = None
    for start in start_points:
        x0 = np.array(start, dtype=float)
        try:
            res = minimize(
                objective,
                x0=x0,
                method='L-BFGS-B',
                bounds=bounds,
                options={'maxiter': int(config['estimation'].get('maxiter', 50)), 'eps': fd_eps},
            )
            candidate = {
                'x': res.x.tolist(),
                'fun': float(res.fun),
                'success': bool(res.success),
                'message': str(res.message),
            }
        except StopIteration as e:
            candidate = {'x': x0.tolist(), 'fun': float('inf'), 'success': False, 'message': str(e)}
        if best is None or candidate['fun'] < best['fun']:
            best = candidate

    out = {
        'weeks_run': weeks_run,
        'best_params': {
            'best_init_rd': float(best['x'][0]),
            'best_init_sigma': float(best['x'][1]),
            'best_tau': float(best['x'][2]),
            'best_inactivity_drift': float(best['x'][3]),
        },
        'optimiser': best,
        'evaluation_log': pd.DataFrame(eval_log),
    }
    return out


def update_run_params_in_config(config: dict, best_params: dict) -> dict:
    cfg = dict(config)
    cfg.setdefault('run', {})
    cfg['run'].update(best_params)
    return cfg


def estimate_parameters_from_config_path(config_path: str | Path, save: bool = True) -> dict:
    config = load_json(config_path)
    result = estimate_parameters(config)
    updated = update_run_params_in_config(config, result['best_params'])
    if save:
        save_json(updated, config_path)
    return result
