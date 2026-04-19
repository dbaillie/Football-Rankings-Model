
from __future__ import annotations

import pandas as pd

from .core import mu_to_rating, phi_to_rd


def state_to_ratings_df(
    state: dict[int, dict],
    week: int,
    init_rating_centre: float,
    players_df: pd.DataFrame | None = None,
    current_rankings: pd.DataFrame | None = None,
    player_id_col: str = 'pid',
) -> pd.DataFrame:
    rows = []
    for pid, st in state.items():
        rows.append({
            'week': int(week),
            player_id_col: int(pid),
            'rating': mu_to_rating(st['mu'], init_rating_centre),
            'rd': phi_to_rd(st['phi']),
            'sigma': st['sigma'],
            'last_week_seen': st.get('last_week_seen'),
        })
    out = pd.DataFrame(rows)

    if current_rankings is not None and not current_rankings.empty:
        cur = current_rankings.copy()
        cur.columns = [player_id_col if c == 'pid' else c for c in cur.columns]
        out = out.merge(cur[[player_id_col, 'rank']], on=player_id_col, how='left')

    if players_df is not None and not players_df.empty:
        pdf = players_df.copy()
        if 'PlayerId' in pdf.columns and player_id_col not in pdf.columns:
            pdf = pdf.rename(columns={'PlayerId': player_id_col})
        if player_id_col in pdf.columns:
            extra_cols = [c for c in pdf.columns if c != player_id_col]
            out = out.merge(pdf[[player_id_col] + extra_cols], on=player_id_col, how='left')

    return out.sort_values('rating', ascending=False).reset_index(drop=True)


def snapshots_to_df(snapshots: dict[int, dict[int, dict]], init_rating_centre: float) -> pd.DataFrame:
    rows = []
    for week, week_state in snapshots.items():
        for pid, st in week_state.items():
            rows.append({
                'week': int(week),
                'pid': int(pid),
                'rating': mu_to_rating(st['mu'], init_rating_centre),
                'rd': phi_to_rd(st['phi']),
                'sigma': float(st['sigma']),
                'last_week_seen': int(st['last_week_seen']),
            })
    if not rows:
        return pd.DataFrame(columns=['week', 'pid', 'rating', 'rd', 'sigma', 'last_week_seen'])
    return pd.DataFrame(rows).sort_values(['week', 'rating'], ascending=[True, False]).reset_index(drop=True)
