# Football Rankings Model

This project implements Glicko-2 rating system for football teams across different countries.

## Data Model

The project uses a star schema data model:

- `dim_country.csv`: Country information
- `dim_season.csv`: Season information
- `dim_club.csv`: Club/team information
- `fact_result_simple.csv`: Match results (simplified)
- `fact_result.csv`: Match results (full details)

## Scripts

### `scripts/create_data_model.py`
Processes raw CSV football data files and creates the data model.

### `scripts/run_glicko_country.py <country_name>`
Runs Glicko-2 ratings for a specific country.

**Usage:**
```bash
# Create data model (run once)
python scripts/create_data_model.py

# Run ratings for a country
python scripts/run_glicko_country.py scotland
python scripts/run_glicko_country.py england
python scripts/run_glicko_country.py germany
```

**Output Structure:**
```
output/
├── dim_*.csv                    # Data model dimensions
├── fact_result*.csv            # Data model facts
└── {country_name}/              # Country-specific results
    ├── config.json             # Glicko configuration
    ├── {country}_matches.csv   # Processed matches
    ├── {country}_teams.csv     # Team information
    ├── {country}_ratings.csv   # Final Glicko ratings
    └── {country}_predictions.csv # Match predictions
```

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