from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .data_service import (
    OUTPUT_DIR,
    clear_data_caches,
    get_country_summaries,
    get_country_timeseries,
    get_country_top_n_timeseries,
    get_latest_snapshot,
    get_team_biggest_matches,
    get_team_club_detail,
    get_team_timeseries,
    list_countries,
    list_teams,
    load_teams,
    warm_csv_caches,
)

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    paths = sorted({p for r in app.routes if (p := getattr(r, "path", None))})
    probe = [p for p in paths if "ping-club" in p or "clubdata" in p or p == "/api/health"]
    print(f"[Football Rankings] loaded main.py from:\n  {Path(__file__).resolve()}", flush=True)
    print(f"[Football Rankings] probe routes (should include /api/ping-club):\n  {probe}", flush=True)

    if os.environ.get("FOOTBALL_RANKINGS_SKIP_PRELOAD") == "1":
        print(
            "Football Rankings API: FOOTBALL_RANKINGS_SKIP_PRELOAD=1 — CSV preload skipped; "
            "first club request may stall while ~190k match rows load.",
            flush=True,
        )
        yield
        return
    print(
        "Football Rankings API: preloading CSV caches (may take 1–2 min). "
        "Browser requests will wait until this completes.",
        flush=True,
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, warm_csv_caches)
    print("Football Rankings API: CSV preload complete.", flush=True)
    yield


app = FastAPI(title="Football Rankings API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _club_detail_payload(team_id: int) -> dict:
    detail = get_team_club_detail(team_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Team not found.")
    return detail


@app.get("/api/ping-club")
def ping_club() -> dict[str, str]:
    """If this returns 404, the running process is not serving webapp.backend.main from this repo."""
    return {"ping_club": "ok", "module": "webapp.backend.main"}


@app.get("/api/clubdata")
def api_club_data(
    team_id: int = Query(..., ge=1, description="Team pid (europe_teams.team_id)"),
) -> dict:
    """Same payload as path-based club routes, but query-only — survives setups where extra path segments 404."""
    return _club_detail_payload(team_id)


@app.get("/api/club/{team_id}")
def api_club_detail(team_id: int) -> dict:
    return _club_detail_payload(team_id)


@app.get("/api/team/{team_id}/club-detail")
def api_team_club_detail_compat(team_id: int) -> dict:
    return _club_detail_payload(team_id)


@app.get("/api/teams/{team_id}/club")
def api_team_club_nested(team_id: int) -> dict:
    """Preferred URL — sits beside GET /api/teams so proxies/path quirks avoid bare /api/club/…."""
    return _club_detail_payload(team_id)


@app.get("/api/teams/{team_id}/identity")
def team_identity(team_id: int) -> dict:
    """Tiny payload — only reads europe_teams.csv. Use to verify team id + routing before heavy club JSON."""
    tid = int(team_id)
    teams_df = load_teams()
    row = teams_df.loc[teams_df["pid"].astype(int) == tid]
    if row.empty:
        raise HTTPException(status_code=404, detail="Team id not in europe_teams.csv.")
    return {
        "team_id": tid,
        "team_name": str(row.iloc[0]["team_name"]),
        "country_name": str(row.iloc[0]["country_name"]),
    }


@app.get("/api/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "europe_data_dir": str(OUTPUT_DIR.resolve()),
        "csv_preload_at_startup": "no"
        if os.environ.get("FOOTBALL_RANKINGS_SKIP_PRELOAD") == "1"
        else "yes",
        "club_json_try_get": "/api/clubdata?team_id=498",
        "routing_probe": "/api/ping-club",
    }


@app.post("/api/reload")
def reload_data() -> dict[str, str]:
    clear_data_caches()
    return {"status": "reloaded"}


@app.get("/api/countries")
def countries() -> list[str]:
    return list_countries()


@app.get("/api/teams")
def teams(country: str | None = Query(default=None)) -> list[dict]:
    return list_teams(country=country)


@app.get("/api/snapshot")
def snapshot(top_n: int = Query(default=25, ge=1, le=100)) -> list[dict]:
    return get_latest_snapshot(top_n=top_n)


@app.get("/api/country-summaries")
def country_summaries() -> list[dict]:
    return get_country_summaries()


@app.get("/api/team/{team_id}/timeseries")
def team_timeseries(team_id: int) -> list[dict]:
    series = get_team_timeseries(team_id)
    if not series:
        raise HTTPException(status_code=404, detail="Team not found in weekly ratings data.")
    return series


@app.get("/api/country/{country}/timeseries")
def country_timeseries(country: str) -> list[dict]:
    series = get_country_timeseries(country)
    if not series:
        raise HTTPException(status_code=404, detail="Country not found in weekly ratings data.")
    return series


@app.get("/api/country/{country}/top-timeseries")
def country_top_timeseries(
    country: str, n: int = Query(default=5, ge=1, le=20)
) -> dict:
    payload = get_country_top_n_timeseries(country, n=n)
    if not payload.get("teams"):
        raise HTTPException(status_code=404, detail="Country not found in weekly ratings data.")
    return payload


@app.get("/api/team/{team_id}/biggest-matches")
def biggest_matches(team_id: int, limit: int = Query(default=10, ge=1, le=50)) -> dict:
    result = get_team_biggest_matches(team_id, limit=limit)
    if not result["upsets"] and not result["swings"]:
        raise HTTPException(status_code=404, detail="Team not found in match results data.")
    return result


@app.get("/")
def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")
