import sys
sys.path.append('libs')

import pandas as pd
from glicko_engine.h2h_optimisation import optimise_glicko2_from_history, run_best_model_from_history

history = pd.read_csv("output/fact_result_simple.csv")

# Drop rows with NaN in club IDs
history = history.dropna(subset=['home_club_id', 'away_club_id'])

# Convert club IDs to int
history['home_club_id'] = history['home_club_id'].astype(int)
history['away_club_id'] = history['away_club_id'].astype(int)

# Map match_result to score from home's perspective: H=1, A=0, D=0.5
history['score'] = history['match_result'].map({'H': 1, 'A': 0, 'D': 0.5})

# Filter to one country and year to check optimisation works on a smaller dataset first
history = history[(history['country_id'] == 4)]

best_params, results_df, matches, meta = optimise_glicko2_from_history(
    history=history,
    entity_a_col="home_club_id",
    entity_b_col="away_club_id",
    score_col="score",
    period_col="match_date",
    objective="log_loss"
)
print(results_df)
state, pred_df, week_snapshots, sof_df, matches, meta = run_best_model_from_history(
    history=history,
    entity_a_col="home_club_id",
    entity_b_col="away_club_id",
    score_col="score",
    period_col="match_date",
    best_params=best_params,
)