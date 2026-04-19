
from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import pandas as pd

from .core import iso_week_to_sunday


def load_table(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == '.csv':
        return pd.read_csv(path)
    if suffix in {'.parquet', '.pq'}:
        return pd.read_parquet(path)
    raise ValueError(f'Unsupported file type for {path}. Use CSV or parquet.')


def load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def save_json(obj: dict[str, Any], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))


def prepare_inputs(config: dict) -> tuple[pd.DataFrame, list[int], dict[int, int], dict[int, dict[int, int]], pd.DataFrame]:
    matches = load_table(config['paths']['matches'])
    rankings = load_table(config['paths']['rankings'])

    req_match_cols = {'week', 'EventId', 'PlayerA', 'PlayerB', 'scoreA'}
    req_rank_cols = {'week', 'pid', 'rank'}
    missing_m = req_match_cols - set(matches.columns)
    missing_r = req_rank_cols - set(rankings.columns)
    if missing_m:
        raise ValueError(f'Matches file missing columns: {sorted(missing_m)}')
    if missing_r:
        raise ValueError(f'Rankings file missing columns: {sorted(missing_r)}')

    matches = matches.copy()
    rankings = rankings.copy()

    if config.get('data', {}).get('run_id_column') and config.get('data', {}).get('run_id_value') is not None:
        col = config['data']['run_id_column']
        val = config['data']['run_id_value']
        matches = matches[matches[col].astype(str) == str(val)].copy()

    last_week = int(config['window']['last_week'])
    matches = matches[
        matches['week'].notna()
        & matches['PlayerA'].notna()
        & matches['PlayerB'].notna()
        & matches['scoreA'].notna()
        & (matches['week'].astype(int) <= last_week)
    ].copy()

    matches['week'] = matches['week'].astype(int)
    matches['PlayerA'] = matches['PlayerA'].astype(int)
    matches['PlayerB'] = matches['PlayerB'].astype(int)
    matches['scoreA'] = matches['scoreA'].astype(float)

    weeks_all = sorted(matches['week'].dropna().astype(int).unique().tolist(), key=iso_week_to_sunday)
    n_back = int(config['window']['run_last_n_weeks'])
    weeks_run = weeks_all[-n_back:] if len(weeks_all) > n_back else weeks_all
    if not weeks_run:
        raise ValueError('No weeks found after filtering.')

    if config['window'].get('use_last_week_rank_filter', False):
        ranked_last = set(rankings.loc[rankings['week'].astype(int) == last_week, 'pid'].dropna().astype(int))
        matches = matches[matches['PlayerA'].isin(ranked_last) & matches['PlayerB'].isin(ranked_last)].copy()

    matches = matches[matches['week'].isin(weeks_run)].sort_values(['week', 'EventId', 'PlayerA', 'PlayerB'])
    first_week = int(weeks_run[0])

    first_week_rankings = rankings[rankings['week'].astype(int) == first_week][['pid', 'rank']].dropna().copy()
    first_week_rankings['pid'] = first_week_rankings['pid'].astype(int)
    first_week_rankings['rank'] = first_week_rankings['rank'].astype(int)
    rank_map = dict(zip(first_week_rankings['pid'], first_week_rankings['rank']))

    rankings = rankings[rankings['week'].isin(weeks_run)][['week', 'pid', 'rank']].dropna().copy()
    rankings['week'] = rankings['week'].astype(int)
    rankings['pid'] = rankings['pid'].astype(int)
    rankings['rank'] = rankings['rank'].astype(int)

    rankings_weekly = {}
    for wk in weeks_run:
        wk_df = rankings[rankings['week'] == int(wk)]
        rankings_weekly[int(wk)] = dict(zip(wk_df['pid'], wk_df['rank']))

    return matches, weeks_run, rank_map, rankings_weekly, rankings
