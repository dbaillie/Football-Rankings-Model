# FastAPI + React Football Dashboard

This web app serves your `output/europe` Glicko-2 results through a FastAPI API and a React-based frontend.

## Data prerequisite

Generate data before starting the web app:

```bash
python scripts/create_data_model.py
python scripts/resolve_club_identities.py --write
python scripts/run_glicko_europe.py
```

UEFA-only ingest can be run independently with a pluggable provider:

```bash
# Existing path (SofaScore via ScraperFC)
python scripts/ingest_euro_comps_from_config.py --provider sofascore

# Alternate source (football-data.org API; requires token)
set FOOTBALL_DATA_API_TOKEN=your_token_here
python scripts/ingest_euro_comps_from_config.py --provider football_data_org
```

## Features

- Hoverable Europe map with per-country summary tooltip
- Click-to-drill from map into country and team detail
- Country selector with rating trend (average + top team over time)
- Team selector with weekly rating movement
- Biggest matches for a team:
  - Biggest upsets (based on expected result vs actual result)
  - Largest rating swings
- Latest top-25 snapshot table

## Run locally

From the repository root:

```bash
pip install -r requirements.txt
python run_server.py
```

(or `python -m uvicorn webapp.backend.main:app --reload` — must be that module, not another `main:app`).

Wait until the terminal prints **`CSV preload complete`** (first boot can take 1–2 minutes while match results load). If you reload code often and want an instant listening socket, set `FOOTBALL_RANKINGS_SKIP_PRELOAD=1` (first club request may then hang while CSVs load).

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

After pulling changes that add API routes, **stop uvicorn completely** (Ctrl+C so both reloader and worker exit) and start it again. If `--reload` misses a change, club endpoints can 404 while older routes still work—check [http://127.0.0.1:8000/openapi.json](http://127.0.0.1:8000/openapi.json) for **`/api/club/{team_id}`**.

## API endpoints

- `GET /api/health`
- `GET /api/countries`
- `GET /api/country-summaries`
- `GET /api/teams?country=england`
- `GET /api/snapshot?top_n=25`
- `GET /api/country/{country}/timeseries`
- `GET /api/team/{team_id}/timeseries`
- `GET /api/team/{team_id}/biggest-matches?limit=12`
- `GET /api/club/{team_id}` — **canonical** club payload (all matches + weekly rating gains/losses)
- `GET /api/team/{team_id}/club-detail` — same payload (back-compat alias)
