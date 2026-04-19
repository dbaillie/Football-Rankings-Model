
from __future__ import annotations

from pathlib import Path

from .core import run_glicko2
from .data import prepare_inputs, load_table, load_json
from .outputs import state_to_ratings_df, snapshots_to_df


def run_pipeline_from_config(config: dict) -> dict:
    matches, weeks_run, rank_map, _, rankings = prepare_inputs(config)
    players = None
    players_path = config.get('paths', {}).get('players')
    if players_path and Path(players_path).exists():
        players = load_table(players_path)

    current_rankings = rankings[rankings['week'].astype(int) == int(weeks_run[-1])][['pid', 'rank']].copy()
    state, pred_df, snapshots, sof_df = run_glicko2(
        matches_pdf=matches.copy(),
        weeks=weeks_run,
        init_rating=float(config['run']['best_init_rating']),
        init_rd=float(config['run']['best_init_rd']),
        init_sigma=float(config['run']['best_init_sigma']),
        tau=float(config['run']['best_tau']),
        inactivity_drift=float(config['run']['best_inactivity_drift']),
        upset_gate_max=float(config['run'].get('upset_gate_max', 0.0)),
        upset_gate_k=float(config['run'].get('upset_gate_k', 0.0)),
        info_gate_scale=float(config['run'].get('info_gate_scale', 0.0)),
        inactivity_decay_pts=float(config['run'].get('inactivity_decay_pts_per_week', 0.0)),
        inactivity_decay_grace=int(config['run'].get('inactivity_decay_grace_weeks', 8)),
        reseed_after_weeks=int(config['run'].get('reseed_after_weeks', 0)),
        sof_pos_sigma=float(config['run'].get('sof_pos_sigma', 50)),
        sof_norm_top_n=int(config['run'].get('sof_norm_top_n', 150)),
        sof_norm_target=float(config['run'].get('sof_norm_target', 1000.0)),
        seed_from_wagr=bool(config['seeding'].get('seed_from_rankings', False)),
        wagr_rank_map=rank_map,
        snapshot_weeks=set(int(w) for w in weeks_run),
        diag_every=int(config['run'].get('diag_every', 10)),
    )

    final_df = state_to_ratings_df(
        state=state,
        week=int(weeks_run[-1]),
        init_rating_centre=float(config['run']['best_init_rating']),
        players_df=players,
        current_rankings=current_rankings,
    )
    weekly_df = snapshots_to_df(snapshots, init_rating_centre=float(config['run']['best_init_rating']))

    return {
        'state': state,
        'predictions': pred_df,
        'weekly_snapshots': weekly_df,
        'final_ratings': final_df,
        'sof': sof_df,
        'weeks_run': weeks_run,
    }


def run_pipeline_from_config_path(config_path: str | Path) -> dict:
    return run_pipeline_from_config(load_json(config_path))
