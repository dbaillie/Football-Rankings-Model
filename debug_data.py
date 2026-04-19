"""
Debug script for match-only optimization data processing.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import sys
sys.path.append(str(Path(__file__).resolve().parents[0] / 'libs'))

from glicko_engine.data import prepare_inputs, load_json

def debug_data_processing():
    # Load config
    config_path = Path('config/config.example.json')
    config = load_json(config_path)
    print(f"Config loaded from {config_path}")

    # Simulate fact_result_simple processing
    fact_path = Path('output/fact_result_simple.csv')
    fact_df = pd.read_csv(fact_path)
    fact_df = fact_df[fact_df['country_name'].str.lower() == 'scotland'].copy()
    print(f"Scotland matches: {len(fact_df)}")

    fact_df['match_date'] = pd.to_datetime(fact_df['match_date'], dayfirst=True, errors='coerce')
    fact_df = fact_df.dropna(subset=['match_date'])
    print(f"After date parsing: {len(fact_df)}")

    iso = fact_df['match_date'].dt.isocalendar()
    fact_df['week'] = iso['year'] * 100 + iso['week']
    print(f"Week range: {fact_df['week'].min()} to {fact_df['week'].max()}")

    matches_df = pd.DataFrame({
        "week": fact_df["week"].astype(int),
        "EventId": fact_df["match_date"].dt.strftime("%Y%m%d")
            + "_"
            + fact_df["home_club_id"].astype(str)
            + "_"
            + fact_df["away_club_id"].astype(str),
        "PlayerA": fact_df["home_club_id"].astype(int),
        "PlayerB": fact_df["away_club_id"].astype(int),
        "scoreA": np.where(
            fact_df["home_team_goals"] > fact_df["away_team_goals"],
            1.0,
            np.where(
                fact_df["home_team_goals"] == fact_df["away_team_goals"],
                0.5,
                0.0,
            ),
        ),
    })

    print(f"Matches df shape: {matches_df.shape}")
    print(f"Matches week range: {matches_df['week'].min()} to {matches_df['week'].max()}")

    # Create dummy rankings
    rankings_path = Path('output/scotland/scotland_teams.csv')
    rankings_df = pd.read_csv(rankings_path)
    last_week = int(config.get('window', {}).get('last_week', 202614))
    dummy_rankings = pd.DataFrame({
        'pid': rankings_df['team_id'].astype(int),
        'rank': 100,
        'week': last_week,
    })

    print(f"Dummy rankings for week {last_week}: {len(dummy_rankings)} teams")

    # Save to temp files
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_match_path = Path(tmpdir) / "matches.csv"
        temp_rankings_path = Path(tmpdir) / "rankings.csv"

        matches_df.to_csv(temp_match_path, index=False)
        dummy_rankings.to_csv(temp_rankings_path, index=False)

        print(f"Saved matches to {temp_match_path}")
        print(f"Saved rankings to {temp_rankings_path}")

        # Update config
        config_temp = dict(config)
        config_temp["paths"] = dict(config_temp.get("paths", {}))
        config_temp["paths"]["matches"] = str(temp_match_path)
        config_temp["paths"]["rankings"] = str(temp_rankings_path)

        # Test prepare_inputs
        try:
            matches_pdf, weeks_run, rank_map, _, _ = prepare_inputs(config_temp)
            print(f"prepare_inputs successful!")
            print(f"weeks_run length: {len(weeks_run)}")
            print(f"weeks_run: {weeks_run[:5]}...{weeks_run[-5:]}")
            print(f"matches_pdf shape: {matches_pdf.shape}")
            print(f"Filtered matches week range: {matches_pdf['week'].min()} to {matches_pdf['week'].max()}")

            # Check train/validation split
            n_weeks = len(weeks_run)
            train_cut = max(1, int(n_weeks * float(config_temp['estimation']['train_frac'])))
            train_weeks = weeks_run[:train_cut]
            valid_weeks = weeks_run[train_cut:]

            print(f"Train weeks: {len(train_weeks)} ({train_weeks[0]} to {train_weeks[-1]})")
            print(f"Valid weeks: {len(valid_weeks)} ({valid_weeks[0]} to {valid_weeks[-1]})")

            train_matches = matches_pdf[matches_pdf['week'].isin(train_weeks)]
            valid_matches = matches_pdf[matches_pdf['week'].isin(valid_weeks)]
            print(f"Train matches: {len(train_matches)}")
            print(f"Valid matches: {len(valid_matches)}")

        except Exception as e:
            print(f"prepare_inputs failed: {e}")

if __name__ == "__main__":
    debug_data_processing()