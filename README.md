# Football Rankings Model

This project implements Glicko-2 rating system for football teams across different countries.

## Web dashboard hosting

The interactive map and API live under `webapp/`. For local run, Vite build, and deploy steps (Render + Vercel, environment variables, CSV data on the server), see [webapp/README.md](webapp/README.md). A Render Blueprint template is in [render.yaml](render.yaml).

## Data Model

The project uses a star schema data model:

- `dim_country.csv`: Country information
- `dim_season.csv`: Season information
- `dim_club.csv`: Club/team information
- `fact_result_simple.csv`: Match results (simplified)
- `fact_result.csv`: Match results (full details)

## Canonical Europe pipeline

The repository now uses one club-identity flow:

1. `python scripts/create_data_model.py`
2. `python scripts/ingest_leagues_from_config.py` (optional enrichment)
3. `python scripts/resolve_club_identities.py --write`
4. `python scripts/run_glicko_europe.py`

Notes:
- `scripts/resolve_club_identities.py` is the single source of truth for club ID remapping + duplicate fixture dedupe.
- `scripts/european_results.py` has been removed as a legacy matching path.
- `scripts/run_glicko_europe.py` expects `output/fact_result_simple_resolved.csv`.

## Scripts

### `scripts/create_data_model.py`
Processes raw CSV football data files and creates the data model.

### `scripts/optimize_glicko_match_only.py`
Optimizes Glicko-2 parameters based purely on match prediction accuracy (log loss, brier score, accuracy), without ranking comparisons.

**Usage:**
```bash
# Optimize parameters using config file
python scripts/optimize_glicko_match_only.py --config config/config.example.json

# Optimize for specific country using processed data
python scripts/optimize_glicko_match_only.py \
  --fact-result-simple output/fact_result_simple.csv \
  --country scotland \
  --rankings-file output/scotland/scotland_teams.csv \
  --output-dir output/scotland
```

**Output:**
- `parameter_search_results.csv`: All parameter combinations tested
- `best_params.json`: Optimal parameters found
- `optimiser.json`: Optimization summary

### `scripts/optimize_glicko_params.py`
Optimizes Glicko-2 parameters comparing against external rankings (WAGR) for validation.

## Glicko-2 Parameters

- **Rating Scale**: 1500 ± 350 (initial)
- **Time-based Decay**: Teams lose 2.5 points per week after 12 weeks inactive
- **Re-seeding**: Teams reset after 52 weeks inactive
- **Volatility**: τ = 0.4, σ = 0.06

## Available Countries

Based on the data model, available countries include:
- Austria, Belgium, Denmark, England, Finland, France, Germany, Greece, Ireland, Italy, Netherlands, Norway, Poland, Portugal, Romania, Russia, Scotland, Spain, Sweden

## Example Results

**England Premier League (2026):**
1. Arsenal: 2045.2
2. Man City: 2031.6
3. Liverpool: 1931.0

**Scotland Premiership (2026):**
1. Rangers: 1875.4
2. Celtic: 1858.1
3. Hearts: 1792.6