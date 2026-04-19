"""
Test script to check if run_glicko2 generates predictions.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[0] / 'libs'))

from glicko_engine.core import run_glicko2
from glicko_engine.data import prepare_inputs, load_json

def test_predictions():
    print("Testing prediction generation...")

    # Simple test data
    matches_df = pd.DataFrame({
        'week': [202510, 202510, 202511],
        'EventId': ['20251001_1_2', '20251002_3_4', '20251003_1_3'],
        'PlayerA': [1, 3, 1],
        'PlayerB': [2, 4, 3],
        'scoreA': [1.0, 0.5, 0.0]
    })

    rankings_df = pd.DataFrame({
        'pid': [1, 2, 3, 4],
        'rank': [100, 100, 100, 100],
        'week': [202614, 202614, 202614, 202614]
    })

    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_match_path = Path(tmpdir) / "matches.csv"
        temp_rankings_path = Path(tmpdir) / "rankings.csv"

        matches_df.to_csv(temp_match_path, index=False)
        rankings_df.to_csv(temp_rankings_path, index=False)

        config_temp = {
            'paths': {
                'matches': str(temp_match_path),
                'rankings': str(temp_rankings_path)
            },
            'window': {'last_week': 202614, 'run_last_n_weeks': 10},
            'data': {},
            'seeding': {'seed_from_rankings': False},
            'run': {'best_init_rating': 1500.0}
        }

        matches_pdf, weeks_run, rank_map, _, _ = prepare_inputs(config_temp)
        print(f"Prepared {len(matches_pdf)} matches for weeks {weeks_run}")

        # Run glicko
        _, pred_df, _, _ = run_glicko2(
            matches_pdf=matches_pdf,
            weeks=weeks_run,
            init_rating=1500.0,
            init_rd=350.0,
            init_sigma=0.06,
            tau=0.4,
            inactivity_drift=0.0,
            snapshot_weeks=set(weeks_run),
            diag_every=0,
        )

        print(f"Predictions shape: {pred_df.shape}")
        print(f"Predictions columns: {list(pred_df.columns)}")
        if not pred_df.empty:
            print(f"Sample: {pred_df.iloc[0].to_dict()}")

if __name__ == "__main__":
    test_predictions()