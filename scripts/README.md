# Local sport-agnostic Glicko-2 scripts

Files:
- `glicko2_local.py` — shared engine and helpers
- `estimate_params.py` — local parameter estimation / tuning
- `run_ratings.py` — local fixed-parameter run
- `config.example.json` — starting config

## Expected input files

### matches.csv
Required columns:
- `week` — integer rating period in `YYYYWW`
- `EventId` — event identifier
- `PlayerA`
- `PlayerB`
- `scoreA` — 1.0 win, 0.0 loss, 0.5 draw

Optional:
- run identifier column, if you want to filter to one run via config

### rankings.csv
Required columns:
- `week`
- `pid`
- `rank`

This is used for:
- optional initial seeding
- elite-only validation comparison

### players.csv
Optional; if provided it is merged into the final ratings output.
Use `pid` as the player id column.

## Usage

1. Copy `config.example.json` to `config.json`
2. Point `paths.matches`, `paths.rankings`, and optionally `paths.players` to local CSV/parquet files
3. Estimate parameters:

```bash
python estimate_params.py --config config.json
```

That script writes:
- `output/parameter_search_results.csv`
- `output/best_params.json`

It also updates the `run` block inside `config.json` so the run script uses the newly estimated parameters.

### Library-based optimisation

A newer wrapper is available that uses the `libs/glicko_engine` parameter-optimisation helpers directly:

```bash
python scripts/optimize_glicko_params.py --config config.json
```

Additional options:
- `--output-dir <dir>` to override where result files are saved
- `--save-config` to write the best parameters back into the input config
- `--fact-result-simple <path>` to convert `fact_result_simple.csv` into Glicko matches directly
- `--country <name>` to filter the fact_result_simple file by country
- `--rankings-file <path>` to provide a country-specific rankings file

If you use `--fact-result-simple`, you must also provide `--rankings-file`.

This script saves:
- `parameter_search_results.csv`
- `best_params.json`
- `optimiser.json`

4. Run the model:

```bash
python run_ratings.py --config config.json
```

Outputs:
- `output/final_ratings.csv`
- `output/weekly_snapshots.csv`
- `output/match_predictions.csv`
