# FastAPI + React Football Dashboard

This web app serves your `output/europe` Glicko-2 results through a FastAPI API and a React-based frontend.

## Data prerequisite

Generate data before starting the web app:

```bash
python scripts/create_data_model.py
python scripts/resolve_club_identities.py --write
python scripts/run_glicko_europe.py
# Optional: empirical calibration tables + JSON for the API / future UI tab
python scripts/analyse_europe_calibration.py
# Recent window only (last N distinct yyyyww rating weeks):
python scripts/analyse_europe_calibration.py --last-weeks 104
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
- **Calibration** tab (`#/calibration`) — charts + summary table from `GET /api/calibration` (run `scripts/analyse_europe_calibration.py` after Glicko).

## Run locally

### Backend (always)

From the repository root:

```bash
pip install -r requirements.txt
python run_server.py
```

(or `python -m uvicorn webapp.backend.main:app --reload` — must be that module, not another `main:app`).

Wait until the terminal prints **`CSV preload complete`** (first boot can take 1–2 minutes while match results load). If you reload code often and want an instant listening socket, set `FOOTBALL_RANKINGS_SKIP_PRELOAD=1` (first club request may then hang while CSVs load).

### Frontend options

The dashboard UI is built with **Vite + React** under `webapp/frontend/` (recommended for parity with hosted deploys):

1. **Production-style (FastAPI serves `dist/`):** Install Node/npm, then:

   ```bash
   cd webapp/frontend
   npm install
   npm run build
   ```

   Restart `python run_server.py` and open [http://127.0.0.1:8000](http://127.0.0.1:8000).

2. **Vite dev + API on 8000:** In one terminal run `python run_server.py`. In another:

   ```bash
   cd webapp/frontend
   npm install
   npm run dev
   ```

   Open the URL Vite prints (port **5173**). `/api/*` is proxied to `127.0.0.1:8000` — no `VITE_API_BASE_URL` needed.

3. **Legacy in-browser Babel (no npm):** If `webapp/frontend/dist/index.html` is absent, the API serves `index.legacy.html`, which loads React from CDNs and `/assets/app.jsx`. Use this only when you cannot run Node.

### Environment

- **Hosted SPA (Vercel):** set `VITE_API_BASE_URL` to your Render (or other) API origin, no trailing slash. See `webapp/frontend/.env.local.example`.
- **API CORS:** after you know the Vercel URL, set `FOOTBALL_CORS_ORIGINS=https://your-site.vercel.app` (comma-separated). If unset, the API allows all origins (`*`).

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000) when using option (1) or (3).

After pulling changes that add API routes, **stop uvicorn completely** (Ctrl+C so both reloader and worker exit) and start it again. If `--reload` misses a change, club endpoints can 404 while older routes still work—check [http://127.0.0.1:8000/openapi.json](http://127.0.0.1:8000/openapi.json) for **`/api/club/{team_id}`**.

## Contact form (Info page)

The **Contact me** form POSTs to `/api/contact` and sends mail over SMTP. Install deps including `email-validator` (`pip install -r requirements.txt`). Configure before use:

| Variable | Required | Purpose |
|----------|----------|---------|
| `FOOTBALL_CONTACT_TO_EMAIL` | Yes | Your inbox — where submissions are delivered |
| `FOOTBALL_SMTP_HOST` | Yes | SMTP server (e.g. Outlook/Gmail relay host) |
| `FOOTBALL_SMTP_PORT` | No | Default `587` (STARTTLS). Use `465` with SSL — see below |
| `FOOTBALL_SMTP_USER` | Yes | SMTP login username (often your email) |
| `FOOTBALL_SMTP_PASSWORD` | Yes | SMTP password or app password |
| `FOOTBALL_SMTP_FROM` | No | From address if different from `FOOTBALL_SMTP_USER` |
| `FOOTBALL_SMTP_SSL` | No | Set to `1` for implicit TLS on port 465 (`SMTP_SSL`) |
| `FOOTBALL_CONTACT_SUBJECT_PREFIX` | No | Email subject prefix (default `[Football rankings]`) |

`GET /api/health` includes `contact_email`: `configured` or `not_configured`. `GET /api/contact/status` returns `{ "enabled": true/false }`.

**Gmail / Google Workspace:** use an [App Password](https://support.google.com/accounts/answer/185833) on port 587 with STARTTLS (leave `FOOTBALL_SMTP_SSL` unset).

## API endpoints

- `GET /health` — minimal `{ "status": "ok" }` (Render / uptime checks)
- `GET /ratings` — latest-week snapshot rows (CSV-backed; query `top_n`, default 500)
- `GET /api/health`
- `GET /api/contact/status` — whether the contact form can send mail
- `POST /api/contact` — JSON body `{ "name", "email", "message", "company" }` (`company` is a honeypot; leave empty)
- `GET /api/countries`
- `GET /api/country-summaries`
- `GET /api/teams?country=england`
- `GET /api/snapshot?top_n=25`
- `GET /api/country/{country}/timeseries`
- `GET /api/team/{team_id}/timeseries`
- `GET /api/team/{team_id}/biggest-matches?limit=12`
- `GET /api/club/{team_id}` — **canonical** club payload (all matches + weekly rating gains/losses)
- `GET /api/team/{team_id}/club-detail` — same payload (back-compat alias)

## Hosting (Render + Vercel)

- **Data (Render will crash without this):** The API expects these files on the server — same filenames as locally under `output/europe/`:

  `europe_teams.csv`, `europe_weekly_ratings.csv`, `europe_ratings.csv`, `europe_match_results.csv`

  Optional tab: `calibration_summary.json` (from `scripts/analyse_europe_calibration.py`).

  `.gitignore` excludes `output/`, so **GitHub clones do not include them** unless you deliberately add them. Easiest MVP: from repo root,

  ```bash
  git add -f output/europe/europe_teams.csv output/europe/europe_weekly_ratings.csv output/europe/europe_ratings.csv output/europe/europe_match_results.csv
  ```

  (add `calibration_summary.json` the same way if you want that page live), commit, push, redeploy Render. If GitHub rejects a file for being **too large** (~100 MB limit per file), use a different strategy (persistent disk + upload, Private asset URL in build, etc.) instead.

  Alternate: set **`FOOTBALL_OUTPUT_EUROPE_DIR`** on Render to an **absolute path** where you placed those CSVs (e.g. a mounted persistent disk directory).
- **Backend (Render):** Use the repository root as the service root. Start command:

  `uvicorn webapp.backend.main:app --host 0.0.0.0 --port $PORT`

  Build must include `pip install -r requirements.txt` and a Vite build if you want FastAPI to serve the SPA from `webapp/frontend/dist/`. See `render.yaml` for a template.
- **Frontend (Vercel):** Root directory `webapp/frontend`, framework Vite, build `npm run build`, output `dist`. Set `VITE_API_BASE_URL` to the public API origin (no trailing slash).
- **CORS:** Set `FOOTBALL_CORS_ORIGINS` on Render to your Vercel URL after the first frontend deploy.
