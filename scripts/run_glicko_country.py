"""
Generalized Glicko-2 rating system for football data by country.

Usage: python scripts/run_glicko_country.py <country_name>

This script:
1. Loads processed data from fact_result_simple.csv
2. Filters by specified country
3. Transforms data to Glicko-2 format
4. Runs the rating algorithm
5. Saves results in output/{country_name}/
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import sys

# Import the Glicko engine
sys.path.append('libs')
from glicko_engine.core import run_glicko2
from glicko_engine.outputs import state_to_ratings_df, snapshots_to_df


def load_country_data(country_name: str):
    """Load and process country data from fact_result_simple.csv."""
    # Load the processed data
    fact_df = pd.read_csv('output/fact_result_simple.csv')
    clubs_df = pd.read_csv('output/dim_club.csv')

    # Filter by country and drop NaN values
    country_data = fact_df[fact_df['country_name'].str.lower() == country_name.lower()].copy()
    country_data = country_data.dropna(subset=['home_club_id', 'away_club_id', 'home_team_goals', 'away_team_goals'])

    if country_data.empty:
        raise ValueError(f"No valid data found for country: {country_name}")

    print(f"Loaded {len(country_data)} matches for {country_name}")

    # Get clubs for this country
    country_clubs = clubs_df[clubs_df['country_id'] == country_data['country_id'].iloc[0]].copy()
    club_id_to_name = dict(zip(country_clubs['club_id'], country_clubs['club_name']))

    # Convert date and create week
    country_data['match_date'] = pd.to_datetime(country_data['match_date'], format='mixed', dayfirst=True)
    country_data['year'] = country_data['match_date'].dt.year
    country_data['week'] = country_data['match_date'].dt.isocalendar().week
    country_data['week'] = country_data['week'].clip(upper=52)  # Handle invalid weeks
    country_data['yyyyww'] = country_data['year'] * 100 + country_data['week']

    # Transform to Glicko format
    glicko_matches = []

    for _, row in country_data.iterrows():
        home_club_id = int(row['home_club_id'])
        away_club_id = int(row['away_club_id'])
        home_goals = row['home_team_goals']
        away_goals = row['away_team_goals']

        # Determine scoreA (1 for win, 0.5 for draw, 0 for loss)
        if home_goals > away_goals:
            score_a = 1.0  # home win
        elif home_goals == away_goals:
            score_a = 0.5  # draw
        else:
            score_a = 0.0  # away win

        glicko_matches.append({
            'week': int(row['yyyyww']),
            'EventId': f"{row['match_date'].strftime('%Y%m%d')}_{home_club_id}_{away_club_id}",
            'PlayerA': home_club_id,  # home team
            'PlayerB': away_club_id,  # away team
            'scoreA': score_a
        })

    matches_glicko = pd.DataFrame(glicko_matches)

    # Create team info
    unique_club_ids = sorted(set(matches_glicko['PlayerA']).union(set(matches_glicko['PlayerB'])))
    team_info = pd.DataFrame([
        {'team_id': tid, 'team_name': club_id_to_name.get(tid, f'Club_{tid}')}
        for tid in unique_club_ids
    ])

    return matches_glicko, team_info, unique_club_ids


def create_config(country_name: str):
    """Create a basic config for the country."""
    return {
        "paths": {
            "matches": f"{country_name}_matches.csv",
            "rankings": None,
            "players": f"{country_name}_teams.csv",
            "output_dir": f"output/{country_name}"
        },
        "data": {
            "run_id_column": None,
            "run_id_value": None
        },
        "window": {
            "last_week": 202652,  # Future week to include all data
            "run_last_n_weeks": 520,  # About 10 years
            "use_last_week_rank_filter": False
        },
        "seeding": {
            "seed_from_rankings": False
        },
        "run": {
            "best_init_rating": 1500.0,
            "best_init_rd": 350.0,
            "best_init_sigma": 0.06,
            "best_tau": 0.4,
            "best_inactivity_drift": 0.0,
            "upset_gate_max": 0.0,
            "upset_gate_k": 0.0,
            "info_gate_scale": 0.0,
            "inactivity_decay_pts_per_week": 2.5,
            "inactivity_decay_grace_weeks": 12,
            "reseed_after_weeks": 52
        }
    }


def run_data_model():
    """Ensure data model exists by running create_data_model.py if needed."""
    output_dir = Path('output')
    fact_file = output_dir / 'fact_result_simple.csv'
    clubs_file = output_dir / 'dim_club.csv'

    if not fact_file.exists() or not clubs_file.exists():
        print("Data model not found. Running create_data_model.py...")
        import subprocess
        result = subprocess.run([sys.executable, 'scripts/create_data_model.py'],
                              capture_output=True, text=True)
        if result.returncode != 0:
            print("Error running create_data_model.py:")
            print(result.stderr)
            sys.exit(1)
        print("Data model created successfully.")
    else:
        print("Data model found.")


def main():
    parser = argparse.ArgumentParser(description="Run Glicko-2 ratings for a specific country")
    parser.add_argument("country", help="Country name (e.g., 'scotland', 'england', 'germany')")
    args = parser.parse_args()

    country_name = args.country.lower()

    # Ensure data model exists
    run_data_model()

    print(f"Processing Glicko-2 ratings for {country_name}...")

    try:
        # Load country data
        matches_df, teams_df, club_ids = load_country_data(country_name)

        if matches_df.empty:
            print(f"No matches found for {country_name}")
            return

        print(f"Loaded {len(matches_df)} matches from {len(teams_df)} teams")
        print(f"Date range: {matches_df['week'].min()} to {matches_df['week'].max()}")

        # Create output directory
        output_dir = Path(f"output/{country_name}")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save processed data
        matches_df.to_csv(output_dir / f"{country_name}_matches.csv", index=False)
        teams_df.to_csv(output_dir / f"{country_name}_teams.csv", index=False)

        # Create config
        config = create_config(country_name)
        with open(output_dir / "config.json", "w") as f:
            json.dump(config, f, indent=2)

        print("Running Glicko-2 ratings...")

        # Get unique weeks
        weeks = sorted(matches_df['week'].unique())

        # Run Glicko-2
        state, pred_df, week_snapshots, sof_df = run_glicko2(
            matches_pdf=matches_df,
            weeks=weeks,
            init_rating=1500.0,
            init_rd=350.0,
            init_sigma=0.06,
            tau=0.4,
            inactivity_drift=0.0,
            max_sigma=0.1,
            upset_gate_max=0.0,
            upset_gate_k=0.0,
            info_gate_scale=0.0,
            inactivity_decay_pts=2.5,
            inactivity_decay_grace=12,
            reseed_after_weeks=52,
            seed_from_wagr=False,
            snapshot_weeks=weeks,  # Capture every week for movement charts
            diag_every=10
        )

        # Convert final ratings
        final_ratings = state_to_ratings_df(
            state=state,
            week=weeks[-1],
            init_rating_centre=1500.0,
            players_df=teams_df.rename(columns={'team_id': 'pid', 'team_name': 'name'}),
            current_rankings=None
        )

        # Add team names
        id_to_team = {tid: name for tid, name in zip(teams_df['team_id'], teams_df['team_name'])}
        final_ratings['team_name'] = final_ratings['pid'].map(id_to_team)

        # Create weekly rating snapshots and compute movement per team
        weekly_ratings = snapshots_to_df(week_snapshots, init_rating_centre=1500.0)
        weekly_ratings['team_name'] = weekly_ratings['pid'].map(id_to_team)
        weekly_ratings = weekly_ratings.sort_values(['team_name', 'week']).reset_index(drop=True)
        weekly_ratings['rating_change'] = weekly_ratings.groupby('pid')['rating'].diff().fillna(0.0)
        weekly_ratings['rating_change_pct'] = weekly_ratings.groupby('pid')['rating'].pct_change().fillna(0.0)

        # Save results
        final_ratings.to_csv(output_dir / f"{country_name}_ratings.csv", index=False)
        pred_df.to_csv(output_dir / f"{country_name}_predictions.csv", index=False)
        weekly_ratings.to_csv(output_dir / f"{country_name}_weekly_ratings.csv", index=False)

        print("\nTop 10 teams by rating:")
        top_10 = final_ratings.sort_values('rating', ascending=False).head(10)
        for i, (_, row) in enumerate(top_10.iterrows(), 1):
            print("2d")

        print(f"\nResults saved to {output_dir}")

    except Exception as e:
        print(f"Error processing {country_name}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()