from __future__ import annotations

import asyncio
import os

try:
    from dotenv import load_dotenv as _load_dotenv
except ImportError:
    def _load_dotenv() -> None:  # pragma: no cover
        return None


_load_dotenv()
from contextlib import asynccontextmanager
from pathlib import Path

from pydantic import BaseModel, EmailStr, Field

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .contact_email import contact_smtp_configured, send_contact_submission
from .country_narrative import build_country_narrative
from .team_narrative import build_team_narrative
from .calibration_service import clear_calibration_summary_cache, load_calibration_summary
from .database import ping_database, use_database
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
DIST_DIR = FRONTEND_DIR / "dist"
DIST_INDEX = DIST_DIR / "index.html"
INDEX_LEGACY = FRONTEND_DIR / "index.legacy.html"


def _marble_mark_svg_path() -> Path | None:
    """Favicon / header mark from Vite `public/` (copied to dist root); only `/assets` is mounted as static."""
    for candidate in (DIST_DIR / "marble-mark.svg", FRONTEND_DIR / "public" / "marble-mark.svg"):
        if candidate.is_file():
            return candidate
    return None


def _cors_allow_origins() -> list[str]:
    """Local dev hosts plus optional deployed origins via FOOTBALL_CORS_ORIGINS (comma-separated)."""
    raw = (os.environ.get("FOOTBALL_CORS_ORIGINS") or "").strip()
    base = (
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
    )
    if raw == "*" or raw == "":
        return ["*"]
    origins = list(dict.fromkeys([*(p.rstrip("/") for p in base)]))
    for part in raw.split(","):
        p = part.strip().rstrip("/")
        if p and p not in origins:
            origins.append(p)
    return origins


def _spa_index_file() -> Path | None:
    if DIST_INDEX.is_file():
        return DIST_INDEX
    if INDEX_LEGACY.is_file():
        return INDEX_LEGACY
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    paths = sorted({p for r in app.routes if (p := getattr(r, "path", None))})
    probe = [p for p in paths if "ping-club" in p or "clubdata" in p or p == "/api/health"]
    print(f"[Football Rankings] loaded main.py from:\n  {Path(__file__).resolve()}", flush=True)
    print(f"[Football Rankings] probe routes (should include /api/ping-club):\n  {probe}", flush=True)

    if os.environ.get("FOOTBALL_RANKINGS_SKIP_PRELOAD") == "1":
        print(
            "Football Rankings API: FOOTBALL_RANKINGS_SKIP_PRELOAD=1 — startup preload skipped; "
            "first heavy request may stall while large tables load.",
            flush=True,
        )
        yield
        return
    backend = "Postgres (DATABASE_URL)" if use_database() else "CSV files under europe_data_dir"
    print(
        f"Football Rankings API: preloading data caches from {backend} (may take 1–2 min). "
        "Browser requests will wait until this completes.",
        flush=True,
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, warm_csv_caches)
    print("Football Rankings API: data preload complete.", flush=True)
    yield


app = FastAPI(title="Football Rankings API", version="0.1.0", lifespan=lifespan)

_origins = _cors_allow_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    # Browsers disallow credentials with Access-Control-Allow-Origin: *
    allow_credentials=_origins != ["*"],
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


@app.get("/health")
def health_minimal() -> dict[str, str]:
    """Render / portfolio health probes (minimal JSON); see `/api/health` for richer diagnostics."""
    return {"status": "ok"}


@app.get("/ratings")
def ratings_csv_public(
    top_n: int = Query(default=100, ge=1, le=500, description="Rows from latest rating week."),
    offset: int = Query(default=0, ge=0, description="Skip this many rows after ranking."),
) -> list[dict]:
    """Latest-week snapshot rows (paginated). Same backing data as `/api/snapshot`."""
    try:
        return get_latest_snapshot(top_n=top_n, offset=offset)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Ratings data file not found") from None
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to load ratings data") from None


@app.get("/api/health")
def health() -> dict[str, str]:
    pg_ok = not use_database() or ping_database()
    return {
        "status": "ok",
        "data_backend": "postgres" if use_database() else "csv_files",
        "postgres_reachable": "yes" if pg_ok else "no",
        "europe_data_dir": str(OUTPUT_DIR.resolve()),
        "csv_preload_at_startup": "no"
        if os.environ.get("FOOTBALL_RANKINGS_SKIP_PRELOAD") == "1"
        else "yes",
        "club_json_try_get": "/api/clubdata?team_id=498",
        "routing_probe": "/api/ping-club",
        "calibration_summary": "/api/calibration",
        "contact_email": "configured" if contact_smtp_configured() else "not_configured",
    }


class ContactBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    email: EmailStr
    message: str = Field(..., min_length=1, max_length=8000)
    company: str = Field(default="", max_length=400)


@app.get("/api/contact/status")
def contact_status() -> dict[str, bool]:
    """Whether POST /api/contact can deliver mail (SMTP env vars set)."""
    return {"enabled": contact_smtp_configured()}


@app.post("/api/contact")
async def contact_submit(body: ContactBody) -> dict[str, bool]:
    """Submit Info-page contact form; delivers to FOOTBALL_CONTACT_TO_EMAIL via SMTP."""
    if body.company.strip():
        return {"ok": True}
    if not contact_smtp_configured():
        raise HTTPException(
            status_code=503,
            detail=(
                "Contact email is not configured. Set FOOTBALL_CONTACT_TO_EMAIL, FOOTBALL_SMTP_HOST, "
                "FOOTBALL_SMTP_USER, and FOOTBALL_SMTP_PASSWORD (see webapp README)."
            ),
        )
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: send_contact_submission(
                name=body.name.strip(),
                reply_email=str(body.email),
                message=body.message.strip(),
            ),
        )
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Could not send your message. Try again later.",
        ) from None
    return {"ok": True}


@app.post("/api/reload")
def reload_data() -> dict[str, str]:
    clear_data_caches()
    clear_calibration_summary_cache()
    return {"status": "reloaded"}


@app.get("/api/countries")
def countries() -> list[str]:
    return list_countries()


@app.get("/api/teams")
def teams(country: str | None = Query(default=None)) -> list[dict]:
    return list_teams(country=country)


@app.get("/api/snapshot")
def snapshot(
    top_n: int = Query(default=25, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    return get_latest_snapshot(top_n=top_n, offset=offset)


@app.get("/api/calibration")
def calibration() -> dict:
    """Calibration bins / globals from output/europe/calibration_summary.json (run analyse_europe_calibration.py)."""
    payload = load_calibration_summary()
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "calibration_summary.json not found or unreadable. "
                "Run: python scripts/analyse_europe_calibration.py"
            ),
        )
    return payload


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


def _parse_int_cutoffs(
    raw: str | None, default: tuple[int, ...], lo: int, hi: int, max_n: int
) -> tuple[int, ...]:
    if raw is None or not str(raw).strip():
        return default
    out: list[int] = []
    for part in str(raw).split(","):
        p = part.strip()
        if not p.isdigit():
            continue
        z = int(p)
        if lo <= z <= hi:
            out.append(z)
    return tuple(sorted(set(out)))[:max_n] if out else default


def _parse_continental_z(raw: str | None) -> tuple[int, ...]:
    return _parse_int_cutoffs(raw, (25, 50, 100), 5, 500, 8)


def _parse_domestic_z(raw: str | None) -> tuple[int, ...]:
    return _parse_int_cutoffs(raw, (5, 10, 25), 1, 80, 8)


@app.get("/api/country/{country}/narrative")
def country_narrative(
    country: str,
    top_n: int = Query(default=5, ge=1, le=15),
    continental_z: str | None = Query(
        default=None,
        description="Comma-separated continental ladder sizes (e.g. 25,50,100). Default 25,50,100.",
    ),
) -> dict:
    payload = build_country_narrative(
        country,
        top_n=top_n,
        continental_cutoffs=_parse_continental_z(continental_z),
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="Country not found in weekly ratings data.")
    return payload


@app.get("/api/team/{team_id}/narrative")
def team_narrative(
    team_id: int,
    domestic_z: str | None = Query(
        default=None,
        description="Comma-separated domestic ladder sizes (e.g. 5,10,25). Default 5,10,25.",
    ),
    continental_z: str | None = Query(
        default=None,
        description="Comma-separated continental ladder sizes (e.g. 25,50,100). Default 25,50,100.",
    ),
) -> dict:
    payload = build_team_narrative(
        team_id,
        domestic_cutoffs=_parse_domestic_z(domestic_z),
        continental_cutoffs=_parse_continental_z(continental_z),
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="Team not found in weekly ratings data.")
    return payload


@app.get("/api/team/{team_id}/biggest-matches")
def biggest_matches(team_id: int, limit: int = Query(default=10, ge=1, le=50)) -> dict:
    result = get_team_biggest_matches(team_id, limit=limit)
    if not result["upsets"] and not result["swings"]:
        raise HTTPException(status_code=404, detail="Team not found in match results data.")
    return result


@app.get("/marble-mark.svg")
def marble_mark_svg() -> FileResponse:
    path = _marble_mark_svg_path()
    if path is None:
        raise HTTPException(status_code=404, detail="marble-mark.svg not found (build the frontend).")
    return FileResponse(
        path,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/")
def index() -> FileResponse:
    spa = _spa_index_file()
    if spa is None:
        raise HTTPException(
            status_code=500,
            detail=(
                "Frontend not built. Run "
                "`cd webapp/frontend && npm install && npm run build` "
                "or rely on webapp/frontend/index.legacy.html for Babel/React-CDN loading."
            ),
        )
    return FileResponse(spa, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


def _frontend_static_headers(*, path: str) -> dict[str, str]:
    """Dev-friendly: JSX/CSS change often — avoid stale UMD bundles when ?v= is forgotten."""
    suf = Path(path).suffix.lower()
    if suf in {".jsx", ".js", ".html", ".css"}:
        return {"Cache-Control": "no-store, max-age=0"}
    return {}


class _DevStaticFiles(StaticFiles):
    def __init__(self, directory: Path, **kw: object) -> None:
        super().__init__(directory=str(directory.resolve()), **kw)

    async def get_response(self, path: str, scope: dict) -> object:
        resp = await super().get_response(path, scope)
        for k, v in _frontend_static_headers(path=path).items():
            resp.headers[k] = v
        return resp


_dist_assets = DIST_DIR / "assets"
if _dist_assets.is_dir():
    app.mount(
        "/assets",
        StaticFiles(directory=str(_dist_assets.resolve()), html=False),
        name="assets",
    )
else:
    app.mount(
        "/assets",
        _DevStaticFiles(directory=FRONTEND_DIR.resolve(), html=False, check_dir=True),
        name="assets",
    )
