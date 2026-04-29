from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .data_service import (
    clear_data_caches,
    get_country_summaries,
    get_country_timeseries,
    get_latest_snapshot,
    get_team_biggest_matches,
    get_team_timeseries,
    list_countries,
    list_teams,
)


app = FastAPI(title="Football Rankings API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"
app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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


@app.get("/api/team/{team_id}/biggest-matches")
def biggest_matches(team_id: int, limit: int = Query(default=10, ge=1, le=50)) -> dict:
    result = get_team_biggest_matches(team_id, limit=limit)
    if not result["upsets"] and not result["swings"]:
        raise HTTPException(status_code=404, detail="Team not found in match results data.")
    return result


@app.get("/")
def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")
