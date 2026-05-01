"""
Europe-wide Glicko-2 rating system using continental data.

Usage: python scripts/run_glicko_europe.py [--output-root DIR]

This script:
1. Loads processed data from fact_result_simple_resolved.csv
2. Uses global club IDs for cross-country matches
3. Runs the rating algorithm across all European leagues
4. Saves results to output/europe/
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import sys

# Import the Glicko engisne
sys.path.append('libs')
from glicko_engine.core import run_glicko2, GLICKO2_SCALE, weeks_between
from glicko_engine.outputs import state_to_ratings_df, snapshots_to_df

# Analytical boundary:
# Weeks before this are used as warm-up only (run-in) and are excluded from output files.
ANALYTICAL_START_WEEK = 200531
# Safety date boundary for 05/06 season start (prevents any calendar/week conversion leakage).
ANALYTICAL_START_DATE = pd.Timestamp("2005-07-01")
# If a team is absent this long, treat return as a new entrant and drop prior history segment.
RESET_AFTER_INACTIVE_WEEKS = 104
UEFA_LEAGUE_CODES = frozenset({"UCL", "UEL", "UECL", "EURO"})


def parse_match_dates(series: pd.Series) -> pd.Series:
    """Parse ISO dates safely, then fall back to mixed/day-first legacy formats."""
    raw = series.astype(str)
    iso = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
    fallback = pd.to_datetime(raw, format="mixed", dayfirst=True, errors="coerce")
    return iso.fillna(fallback)


def load_europe_data(output_root: Path):
    """Load and process all European data from the resolved fact table."""
    resolved_fact = output_root / "fact_result_simple_resolved.csv"
    # Load the processed data
    fact_df = pd.read_csv(resolved_fact)
    clubs_df = pd.read_csv(output_root / "dim_club_updated.csv")

    # Drop rows with missing club IDs or goals
    fact_df = fact_df.dropna(subset=['home_club_id', 'away_club_id', 'home_team_goals', 'away_team_goals'])

    print(f"Loaded {len(fact_df)} total matches")

    # Create club_id to name mapping (global)
    club_id_to_name = dict(zip(clubs_df['club_id'], clubs_df['club_name']))
    club_id_to_country = dict(zip(clubs_df['club_id'], clubs_df['country_id']))

    # Convert date and create week
    fact_df['match_date'] = parse_match_dates(fact_df['match_date'])
    iso_calendar = fact_df['match_date'].dt.isocalendar()
    fact_df['year'] = iso_calendar.year.astype(int)
    fact_df['week'] = iso_calendar.week.astype(int)
    fact_df['week'] = fact_df['week'].clip(upper=52)  # Handle invalid weeks
    fact_df['yyyyww'] = fact_df['year'] * 100 + fact_df['week']

    # Transform to Glicko format
    glicko_matches = []

    for _, row in fact_df.iterrows():
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
            'scoreA': score_a,
            'country_name': row['country_name']  # Keep for analysis
        })

    matches_glicko = pd.DataFrame(glicko_matches)

    # Create team info with global club IDs
    unique_club_ids = sorted(set(matches_glicko['PlayerA']).union(set(matches_glicko['PlayerB'])))
    team_info = pd.DataFrame([
        {
            'team_id': tid,
            'team_name': club_id_to_name.get(tid, f'Club_{tid}'),
            'country_id': club_id_to_country.get(tid, None)
        }
        for tid in unique_club_ids
    ])

    # Add country info from clubs
    country_df = pd.read_csv(output_root / "dim_country_updated.csv")
    country_id_to_name = dict(zip(country_df['country_id'], country_df['country_name']))
    team_info['country_name'] = team_info['country_id'].map(country_id_to_name)

    print(f"Loaded {len(matches_glicko)} matches from {len(team_info)} teams")
    print(f"Date range: {matches_glicko['week'].min()} to {matches_glicko['week'].max()}")

    # Breakdown: UEFA rows use country_name "International" from config, not "europe"
    if "league_code" in fact_df.columns:
        uefa_mask = fact_df["league_code"].astype(str).isin(UEFA_LEAGUE_CODES)
        print(f"  Domestic / league matches: {(~uefa_mask).sum()}")
        print(f"  UEFA competition matches: {uefa_mask.sum()}")
    else:
        domestic = matches_glicko[matches_glicko["country_name"].astype(str).str.lower() != "europe"]
        european = matches_glicko[matches_glicko["country_name"].astype(str).str.lower() == "europe"]
        print(f"  Domestic matches: {len(domestic)}")
        print(f"  European matches (country_name=='europe'): {len(european)}")

    return matches_glicko, team_info, unique_club_ids


def create_config(europe_output_dir: Path):
    """Create a config for Europe-wide ratings."""
    out_s = str(europe_output_dir).replace("\\", "/")
    return {
        "paths": {
            "matches": "europe_matches.csv",
            "rankings": None,
            "players": "europe_teams.csv",
            "output_dir": out_s,
        },
        "data": {
            "run_id_column": None,
            "run_id_value": None
        },
        "window": {
            "last_week": 202652,  # Future week to include all data
            "run_last_n_weeks": 520,  # About 10 years
            "analytical_start_week": ANALYTICAL_START_WEEK,
            "use_last_week_rank_filter": False
        },
        "seeding": {
            "seed_from_rankings": False
        },
        "run": {
            "best_init_rating": 1500.0,
            "best_init_rd": 350.0,
            "best_init_sigma": 0.06,
            "best_tau": .4,
            "best_inactivity_drift": 0.0,
            "max_sigma": 0.1,
            "upset_gate_max": 0.0,
            "upset_gate_k": 0.0,
            "info_gate_scale": 0.0,
            "inactivity_decay_pts_per_week": 1.0,
            "inactivity_decay_grace_weeks": 52,
            "reseed_after_weeks": RESET_AFTER_INACTIVE_WEEKS,
        },
    }


def glicko_run_kwargs(config: dict) -> dict:
    """Map create_config()['run'] keys to run_glicko2 keyword arguments (single source of truth)."""
    r = config["run"]
    return {
        "init_rating": float(r["best_init_rating"]),
        "init_rd": float(r["best_init_rd"]),
        "init_sigma": float(r["best_init_sigma"]),
        "tau": float(r["best_tau"]),
        "inactivity_drift": float(r["best_inactivity_drift"]),
        "max_sigma": float(r.get("max_sigma", 0.1)),
        "upset_gate_max": float(r.get("upset_gate_max", 0.0)),
        "upset_gate_k": float(r.get("upset_gate_k", 0.0)),
        "info_gate_scale": float(r.get("info_gate_scale", 0.0)),
        "inactivity_decay_pts": float(r.get("inactivity_decay_pts_per_week", 0.0)),
        "inactivity_decay_grace": int(r.get("inactivity_decay_grace_weeks", 12)),
        "reseed_after_weeks": int(r.get("reseed_after_weeks", RESET_AFTER_INACTIVE_WEEKS)),
    }


def trim_history_after_long_absence(weekly_ratings: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the latest continuous history segment per team.
    A new segment starts when the team is absent for >= RESET_AFTER_INACTIVE_WEEKS.
    """
    if weekly_ratings.empty:
        return weekly_ratings

    cleaned_parts = []
    for pid, grp in weekly_ratings.sort_values(["pid", "week"]).groupby("pid", sort=False):
        grp = grp.copy()
        weeks = grp["week"].astype(int).tolist()
        segment_ids = []
        current_segment = 0
        previous_week = None

        for wk in weeks:
            if previous_week is not None:
                gap = weeks_between(previous_week, wk)
                if gap >= RESET_AFTER_INACTIVE_WEEKS:
                    current_segment += 1
            segment_ids.append(current_segment)
            previous_week = wk

        grp["history_segment"] = segment_ids
        latest_segment = int(grp["history_segment"].max())
        cleaned_parts.append(grp[grp["history_segment"] == latest_segment].drop(columns=["history_segment"]))

    return pd.concat(cleaned_parts, ignore_index=True)


def run_data_model(output_root: Path):
    """Ensure prerequisite data exists before running Europe ratings."""
    fact_file = output_root / "fact_result_simple_resolved.csv"
    clubs_file = output_root / "dim_club.csv"

    if not fact_file.exists() or not clubs_file.exists():
        raise FileNotFoundError(
            f"Missing prerequisite files. Expected {fact_file} and {clubs_file}. "
            "Run pipeline first: create_data_model.py -> ingest_leagues_from_config.py -> resolve_club_identities.py --write"
        )
    else:
        print("Prerequisite data found.")


def _parse_cli_args():
    p = argparse.ArgumentParser(description="Europe-wide Glicko-2 ratings from resolved fact table.")
    p.add_argument(
        "--output-root",
        type=str,
        default="output",
        help="Directory containing fact_result_simple_resolved.csv, dim_club_updated.csv, dim_country_updated.csv (default: output)",
    )
    return p.parse_args()


def main():
    """Run Europe-wide Glicko-2 ratings."""
    args = _parse_cli_args()
    output_root = Path(args.output_root)
    if not output_root.is_absolute():
        output_root = (Path.cwd() / output_root).resolve()

    # Ensure data model exists
    run_data_model(output_root)

    print("Processing Europe-wide Glicko-2 ratings...")

    try:
        resolved_fact_path = output_root / "fact_result_simple_resolved.csv"
        # Load all European data
        matches_df, teams_df, club_ids = load_europe_data(output_root)

        if matches_df.empty:
            print("No matches found")
            return

        # Create output directory
        output_dir = output_root / "europe"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save processed data (without country_name column for Glicko)
        matches_for_glicko = matches_df[['week', 'EventId', 'PlayerA', 'PlayerB', 'scoreA']]
        matches_for_glicko.to_csv(output_dir / "europe_matches.csv", index=False)
        teams_df.to_csv(output_dir / "europe_teams.csv", index=False)

        # Create config
        config = create_config(output_dir)
        with open(output_dir / "config.json", "w") as f:
            json.dump(config, f, indent=2)

        print("\nRunning Glicko-2 ratings...")

        # Get unique weeks
        weeks = sorted(matches_df['week'].unique())
        warmup_weeks = [w for w in weeks if int(w) < ANALYTICAL_START_WEEK]
        analytical_weeks = [w for w in weeks if int(w) >= ANALYTICAL_START_WEEK]

        if not analytical_weeks:
            raise ValueError(
                f"No analytical weeks found at or after {ANALYTICAL_START_WEEK}. "
                "Check ANALYTICAL_START_WEEK and source data."
            )

        print(f"  Total weeks: {len(weeks)}")
        print(f"  Warm-up weeks (< {ANALYTICAL_START_WEEK}): {len(warmup_weeks)}")
        print(f"  Analytical weeks (>= {ANALYTICAL_START_WEEK}): {len(analytical_weeks)}")

        run_kw = glicko_run_kwargs(config)
        init_centre = float(config["run"]["best_init_rating"])

        # Phase 1: Warm-up run (no analytical output). This stabilizes ratings before 05/06.
        if warmup_weeks:
            warmup_matches = matches_for_glicko[matches_for_glicko["week"] < ANALYTICAL_START_WEEK]
            warm_state, _, warm_snapshots, _ = run_glicko2(
                matches_pdf=warmup_matches,
                weeks=warmup_weeks,
                **run_kw,
                seed_from_wagr=False,
                snapshot_weeks=[warmup_weeks[-1]],  # Keep only final warm-up state for pre-week lookups.
                diag_every=10,
            )
        else:
            warm_state = None
            warm_snapshots = {}

        # Phase 2: Analytical run from 05/06 onward, initialized from warm-up state.
        analytical_matches = matches_for_glicko[matches_for_glicko["week"] >= ANALYTICAL_START_WEEK]
        state, pred_df, analytical_snapshots, sof_df = run_glicko2(
            matches_pdf=analytical_matches,
            weeks=analytical_weeks,
            **run_kw,
            seed_from_wagr=False,
            initial_state=warm_state,
            snapshot_weeks=analytical_weeks,  # Capture every analytical week for movement charts
            diag_every=10,
        )
        week_snapshots = {**warm_snapshots, **analytical_snapshots}

        # Convert final ratings
        final_ratings = state_to_ratings_df(
            state=state,
            week=analytical_weeks[-1],
            init_rating_centre=init_centre,
            players_df=teams_df.rename(columns={'team_id': 'pid', 'team_name': 'name'}),
            current_rankings=None
        )

        # Add team names and country info
        id_to_team = {tid: name for tid, name in zip(teams_df['team_id'], teams_df['team_name'])}
        id_to_country = {tid: country for tid, country in zip(teams_df['team_id'], teams_df['country_name'])}
        final_ratings['team_name'] = final_ratings['pid'].map(id_to_team)
        final_ratings['country_name'] = final_ratings['pid'].map(id_to_country)

        # Create weekly rating snapshots and compute movement per team
        weekly_ratings = snapshots_to_df(analytical_snapshots, init_rating_centre=init_centre)
        weekly_ratings['team_name'] = weekly_ratings['pid'].map(id_to_team)
        weekly_ratings['country_name'] = weekly_ratings['pid'].map(id_to_country)
        weekly_ratings = weekly_ratings.sort_values(['team_name', 'week']).reset_index(drop=True)
        weekly_ratings['rating_change'] = weekly_ratings.groupby('pid')['rating'].diff().fillna(0.0)
        weekly_ratings['rating_change_pct'] = weekly_ratings.groupby('pid')['rating'].pct_change().fillna(0.0)
        weekly_ratings = trim_history_after_long_absence(weekly_ratings)
        weekly_ratings = weekly_ratings.sort_values(['team_name', 'week']).reset_index(drop=True)
        weekly_ratings['rating_change'] = weekly_ratings.groupby('pid')['rating'].diff().fillna(0.0)
        weekly_ratings['rating_change_pct'] = weekly_ratings.groupby('pid')['rating'].pct_change().fillna(0.0)

        # Create detailed match results with pre/post ratings
        print("\nCreating detailed match results...")
        
        # Reload original match data with all details
        fact_df = pd.read_csv(resolved_fact_path)
        fact_df = fact_df.dropna(subset=['home_club_id', 'away_club_id', 'home_team_goals', 'away_team_goals'])
        fact_df['match_date'] = parse_match_dates(fact_df['match_date'])
        iso_calendar = fact_df['match_date'].dt.isocalendar()
        fact_df['year'] = iso_calendar.year.astype(int)
        fact_df['week'] = iso_calendar.week.astype(int)
        fact_df['week'] = fact_df['week'].clip(upper=52)
        fact_df['yyyyww'] = fact_df['year'] * 100 + fact_df['week']
        fact_df = fact_df[
            (fact_df['yyyyww'] >= ANALYTICAL_START_WEEK) &
            (fact_df['match_date'] >= ANALYTICAL_START_DATE)
        ].copy()
        
        # Build pre/post rating lookup from snapshots
        # week_snapshots contains state after each week's games
        # We need pre-ratings (before game) and post-ratings (after game)
        
        # Create a mapping of week -> state (post-rating for that week)
        week_to_state = {}
        for wk, snap in week_snapshots.items():
            week_to_state[int(wk)] = {
                int(pid): {
                    'mu': v['mu'],
                    'phi': v['phi'],
                    'sigma': v['sigma']
                }
                for pid, v in snap.items()
            }
        
        # Get sorted weeks for pre-rating lookup
        sorted_weeks = sorted(week_to_state.keys())
        
        # Build match results with ratings
        match_results = []
        
        for _, row in fact_df.iterrows():
            wk = int(row['yyyyww'])
            home_id = int(row['home_club_id'])
            away_id = int(row['away_club_id'])
            
            # Get pre-rating (from previous week or init if first appearance)
            if wk in week_to_state:
                post_state = week_to_state[wk]
            else:
                post_state = {}
            
            # Find pre-week (previous week in the data)
            pre_week = None
            for w in sorted_weeks:
                if w < wk:
                    pre_week = w
                else:
                    break
            
            if pre_week and pre_week in week_to_state:
                pre_state = week_to_state[pre_week]
            else:
                pre_state = {}
            
            # Get ratings (using configured init centre)
            def get_rating(state_dict, pid, default=None):
                d = init_centre if default is None else default
                if pid in state_dict:
                    return init_centre + GLICKO2_SCALE * state_dict[pid]['mu']
                return d

            def get_rd(state_dict, pid, default=None):
                d = float(config["run"]["best_init_rd"]) if default is None else default
                if pid in state_dict:
                    return GLICKO2_SCALE * state_dict[pid]["phi"]
                return d
            
            home_pre_rating = get_rating(pre_state, home_id)
            home_post_rating = get_rating(post_state, home_id)
            home_pre_rd = get_rd(pre_state, home_id)
            home_post_rd = get_rd(post_state, home_id)
            
            away_pre_rating = get_rating(pre_state, away_id)
            away_post_rating = get_rating(post_state, away_id)
            away_pre_rd = get_rd(pre_state, away_id)
            away_post_rd = get_rd(post_state, away_id)
            
            # Determine result
            if row['home_team_goals'] > row['away_team_goals']:
                result = 'H'
            elif row['home_team_goals'] < row['away_team_goals']:
                result = 'A'
            else:
                result = 'D'
            
            league_label = row["country_name"]
            if "league_code" in fact_df.columns:
                lc = row.get("league_code")
                if pd.notna(lc) and str(lc).strip():
                    league_label = str(lc).strip()

            match_results.append({
                'match_date': row['match_date'].strftime('%Y-%m-%d'),
                'week': wk,
                'competition': league_label,
                'home_team_id': home_id,
                'home_team_name': id_to_team.get(home_id, f'Club_{home_id}'),
                'home_country': id_to_country.get(home_id),
                'away_team_id': away_id,
                'away_team_name': id_to_team.get(away_id, f'Club_{away_id}'),
                'away_country': id_to_country.get(away_id),
                'home_goals': int(row['home_team_goals']),
                'away_goals': int(row['away_team_goals']),
                'result': result,
                'home_pre_rating': round(home_pre_rating, 1),
                'home_post_rating': round(home_post_rating, 1),
                'home_rating_change': round(home_post_rating - home_pre_rating, 1),
                'home_pre_rd': round(home_pre_rd, 1),
                'home_post_rd': round(home_post_rd, 1),
                'away_pre_rating': round(away_pre_rating, 1),
                'away_post_rating': round(away_post_rating, 1),
                'away_rating_change': round(away_post_rating - away_pre_rating, 1),
                'away_pre_rd': round(away_pre_rd, 1),
                'away_post_rd': round(away_post_rd, 1),
            })
        
        results_df = pd.DataFrame(match_results)
        
        # Save results
        final_ratings.to_csv(output_dir / "europe_ratings.csv", index=False)
        pred_df.to_csv(output_dir / "europe_predictions.csv", index=False)
        weekly_ratings.to_csv(output_dir / "europe_weekly_ratings.csv", index=False)
        results_df.to_csv(output_dir / "europe_match_results.csv", index=False)
        
        print(f"  Match results: {len(results_df)} matches saved")

        print("\n" + "="*60)
        print("TOP 20 EUROPEAN TEAMS")
        print("="*60)
        top_20 = final_ratings.sort_values('rating', ascending=False).head(20)
        for i, (_, row) in enumerate(top_20.iterrows(), 1):
            country = row.get('country_name', 'N/A')
            print(f"{i:2d}. {row['team_name']:<30} {row['rating']:7.1f} ({country})")

        print("\n" + "="*60)
        print("RATINGS BY COUNTRY (Top 5 per country)")
        print("="*60)
        for country in final_ratings['country_name'].dropna().unique():
            country_teams = final_ratings[final_ratings['country_name'] == country].sort_values('rating', ascending=False).head(5)
            print(f"\n{country.upper()}:")
            for _, row in country_teams.iterrows():
                print(f"  {row['team_name']:<30} {row['rating']:7.1f}")

        print(f"\nResults saved to {output_dir}")

    except Exception as e:
        import traceback
        print(f"Error processing Europe data: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()