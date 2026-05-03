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
- **Low RAM (e.g. Render free ~512 MiB):** set **`FOOTBALL_LOAD_LAST_CALENDAR_YEARS=10`** (or `8`, etc.) on the API service. That keeps only the last *N* calendar years of **`europe_weekly_ratings.csv`** and **`europe_match_results.csv`** in memory (loaded in chunks so the giant weekly file is not fully materialised). Older history disappears from the hosted site until you raise RAM or remove this setting.

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000) when using option (1) or (3).

After pulling changes that add API routes, **stop uvicorn completely** (Ctrl+C so both reloader and worker exit) and start it again. If `--reload` misses a change, club endpoints can 404 while older routes still work—check [http://127.0.0.1:8000/openapi.json](http://127.0.0.1:8000/openapi.json) for **`/api/club/{team_id}`**.

## Postgres (Supabase) data backend

For hosted deployments, reading multi‑hundred‑MB CSVs from disk on every cold start is fragile. Optional **`DATABASE_URL`** switches the API to load **`fr_*`** tables from Postgres instead of **`output/europe/*.csv`**.

### Supabase setup

1. Create a Supabase project (free tier is fine).
2. Open **Project Settings → Database → Connection string** and copy the **URI** (postgres driver). Use the **Session pooler** connection if Supabase offers both poolers; keep port consistent with the string they show.
3. Locally, copy [`webapp/backend/.env.example`](webapp/backend/.env.example) concepts into a **repo-root `.env`** (same place `python-dotenv` loads from when you run `run_server.py`) or export **`DATABASE_URL`** in your shell. Never commit real credentials.
4. On **Render**, add **`DATABASE_URL`** under Environment for the API service (same URI). Redeploy after saving.
5. From repo root, after generating Europe CSVs under `output/europe/`:

   ```bash
   pip install -r requirements.txt
   set DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/postgres
   python scripts/import_europe_to_postgres.py
   ```

   The script replaces **`fr_teams`**, **`fr_weekly_ratings`**, **`fr_match_results`**, **`fr_europe_ratings`** and creates indexes. **`FOOTBALL_OUTPUT_EUROPE_DIR`** can point at a non-default CSV folder.
6. In Supabase **Table Editor**, confirm row counts look sane.
7. Start the API locally and verify **`GET http://127.0.0.1:8000/api/health`** shows **`data_backend":"postgres"`** and **`postgres_reachable":"yes`**, then **`GET http://127.0.0.1:8000/ratings?top_n=10`** (same rows as **`GET /api/snapshot?top_n=10`**).
8. Deploy the backend to Render with **`DATABASE_URL`** set.

### Behaviour notes

- **`GET /api/health`** includes **`data_backend`** (`postgres` vs `csv_files`) and **`postgres_reachable`** when Postgres is configured.
- **`GET /ratings`** and **`GET /api/snapshot`** accept **`top_n`** (page size) and **`offset`** (pagination). Defaults are conservative (**`/ratings`** default **`top_n=100`**, max **500**).
- CSV files remain useful as **import sources** and for offline analytics; they do not need to exist on the server when **`DATABASE_URL`** is set.
- Startup **`warm_csv_caches`** still loads large frames into memory once per process (from Postgres or CSV). **`FOOTBALL_LOAD_LAST_CALENDAR_YEARS`** still trims weekly ratings and matches when set.

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
- `GET /ratings` — latest-week snapshot rows (`top_n`, `offset`; default `top_n=100`, max 500)
- `GET /api/health`
- `GET /api/contact/status` — whether the contact form can send mail
- `POST /api/contact` — JSON body `{ "name", "email", "message", "company" }` (`company` is a honeypot; leave empty)
- `GET /api/countries`
- `GET /api/country-summaries`
- `GET /api/teams?country=england`
- `GET /api/snapshot?top_n=25&offset=0`
- `GET /api/country/{country}/timeseries`
- `GET /api/team/{team_id}/timeseries`
- `GET /api/team/{team_id}/biggest-matches?limit=12`
- `GET /api/club/{team_id}` — **canonical** club payload (all matches + weekly rating gains/losses)
- `GET /api/team/{team_id}/club-detail` — same payload (back-compat alias)

## Hosting (Render + Vercel)

- **Data:** Prefer **`DATABASE_URL`** (Supabase Postgres) plus **`python scripts/import_europe_to_postgres.py`** so the API does not rely on huge CSVs on the host. Alternatively, ship CSVs:

- **Data (CSV mode — Render needs files on disk):** The API expects these files on the server — same filenames as locally under `output/europe/`:

  `europe_teams.csv`, `europe_weekly_ratings.csv`, `europe_ratings.csv`, `europe_match_results.csv`

  Optional tab: `calibration_summary.json` (from `scripts/analyse_europe_calibration.py`).

  `.gitignore` excludes `output/`, so **GitHub clones do not include them** unless you deliberately add them. Easiest MVP: from repo root,

  ```bash
  git add -f output/europe/europe_teams.csv output/europe/europe_weekly_ratings.csv output/europe/europe_ratings.csv output/europe/europe_match_results.csv
  ```

  (add `calibration_summary.json` the same way if you want that page live), commit, push, redeploy Render. If GitHub rejects a file for being **too large** (~100 MB limit per file), use a different strategy (persistent disk + upload, Private asset URL in build, etc.) instead.

  Alternate: set **`FOOTBALL_OUTPUT_EUROPE_DIR`** on Render to an **absolute path** where you placed those CSVs (e.g. a mounted persistent disk directory).

  **Pre-shrink CSVs on disk (recommended with ~512 MiB RAM):** run locally:

  ```bash
  python scripts/slim_europe_for_web_deploy.py --last-calendar-years 2 --dest output/europe_slim --copy-calibration-json
  ```

  That writes row- and column-trimmed **`europe_weekly_ratings.csv`**, **`europe_match_results.csv`**, **`europe_ratings.csv`**, copies **`europe_teams.csv`**, optionally **`calibration_summary.json`**. Commit **`output/europe_slim/`** (or replace `output/europe` after backup) and point **`FOOTBALL_OUTPUT_EUROPE_DIR`** at that folder on the server — smaller Git + lower pandas RAM than trimming only at runtime.

  If you trim matches to fewer calendar years than **`FOOTBALL_CLUB_VISIBILITY_YEARS`** (default `2024,2025,2026`), set env to years that still exist in the sliced file, e.g. **`FOOTBALL_CLUB_VISIBILITY_YEARS=2025,2026`**, or many clubs disappear from the map.
- **Backend (Render):** Use the repository root as the service root. Start command:

  `uvicorn webapp.backend.main:app --host 0.0.0.0 --port $PORT`

  Build must include `pip install -r requirements.txt` and a Vite build if you want FastAPI to serve the SPA from `webapp/frontend/dist/`. See `render.yaml` for a template.
- **Frontend (Vercel):** Root directory `webapp/frontend`, framework Vite, build `npm run build`, output `dist`. Set `VITE_API_BASE_URL` to the public API origin (no trailing slash).
- **CORS:** Set `FOOTBALL_CORS_ORIGINS` on Render to your Vercel URL after the first frontend deploy.
