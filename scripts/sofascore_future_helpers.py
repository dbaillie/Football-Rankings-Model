"""
Shared SofaScore helpers for upcoming fixtures (scheduled / events_next / rounds).

Used by pull_future_fixtures_sofascore.py and ingest_future_fixtures.py.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone

from ScraperFC.sofascore import API_PREFIX
from ScraperFC.sofascore import Sofascore
from ScraperFC.sofascore import comps as SOFASCORE_COMPS
from ScraperFC.utils import botasaurus_browser_get_json


def try_tournament_id_and_slug(league: str) -> tuple[int, str] | None:
    """Resolve comps key to SofaScore tournament id, or None if missing / not SOFASCORE-backed."""
    if league not in SOFASCORE_COMPS:
        return None
    meta = SOFASCORE_COMPS[league]
    if "SOFASCORE" not in meta:
        return None
    tid = int(meta["SOFASCORE"])
    slug = league.replace(" ", "_").replace("/", "-")[:48]
    return tid, slug


def tournament_id_and_slug(league: str) -> tuple[int, str]:
    """Resolve ScraperFC comps key to SofaScore unique tournament id (CLI: exit on error)."""
    out = try_tournament_id_and_slug(league)
    if out is None:
        if league not in SOFASCORE_COMPS:
            print(f"Unknown league for SofaScore comps: {league!r}", file=sys.stderr)
            print(
                "Tip: keys must match ScraperFC comps.yaml "
                "(see scripts/report_scraperfc_source_coverage.py).",
                file=sys.stderr,
            )
        else:
            print(f"League {league!r} has no SOFASCORE id in comps.", file=sys.stderr)
        sys.exit(1)
    return out


def try_season_id_for_year(league: str, year: str) -> int | None:
    try:
        ss = Sofascore().get_valid_seasons(league)
        if year not in ss:
            return None
        return int(ss[year])
    except Exception:
        return None


def season_id_for_year(league: str, year: str) -> int:
    sid = try_season_id_for_year(league, year)
    if sid is None:
        ss = Sofascore().get_valid_seasons(league)
        print(
            f"Year {year!r} not valid for {league}. Options (sample): {list(ss.keys())[:12]} ...",
            file=sys.stderr,
        )
        sys.exit(1)
    return sid


def dedupe_events_by_id(events: list[dict]) -> list[dict]:
    by_id: dict[int, dict] = {}
    for ev in events:
        eid = ev.get("id")
        if eid is None:
            continue
        by_id[int(eid)] = ev
    return list(by_id.values())


def fetch_events_next(unique_tournament_id: int, season_id: int) -> list[dict]:
    out: list[dict] = []
    page = 0
    while True:
        url = (
            f"{API_PREFIX}/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}"
        )
        try:
            response = botasaurus_browser_get_json(url)
        except Exception as e:
            print(f"events/next page {page} failed: {e}", file=sys.stderr)
            break
        if not response or "events" not in response:
            break
        chunk = response["events"]
        if not chunk:
            break
        out.extend(chunk)
        page += 1
    return dedupe_events_by_id(out)


def fetch_rounds(unique_tournament_id: int, season_id: int, max_round: int) -> list[dict]:
    seen: set[int] = set()
    merged: list[dict] = []
    for r in range(1, max_round + 1):
        url = f"{API_PREFIX}/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{r}"
        try:
            response = botasaurus_browser_get_json(url)
        except Exception as e:
            print(f"events/round/{r} failed: {e}", file=sys.stderr)
            continue
        if not response or "events" not in response:
            continue
        for ev in response["events"]:
            eid = ev.get("id")
            if eid is None or eid in seen:
                continue
            seen.add(int(eid))
            merged.append(ev)
    return dedupe_events_by_id(merged)


def fetch_scheduled_window(unique_tournament_id: int, days: int) -> list[dict]:
    out: list[dict] = []
    today = date.today()
    for i in range(max(1, days)):
        d = today + timedelta(days=i)
        ds = d.isoformat()
        url = f"{API_PREFIX}/sport/football/scheduled-events/{ds}"
        try:
            response = botasaurus_browser_get_json(url)
        except Exception as e:
            print(f"scheduled-events {ds} failed: {e}", file=sys.stderr)
            continue
        for ev in response.get("events") or []:
            ut = ((ev.get("tournament") or {}).get("uniqueTournament")) or {}
            if int(ut.get("id") or -1) != unique_tournament_id:
                continue
            out.append(ev)
    return dedupe_events_by_id(out)


def normalize_event_row(ev: dict, *, source: str) -> dict | None:
    st = ev.get("status") or {}
    typ = str(st.get("type") or "")
    home = (ev.get("homeTeam") or {}).get("name")
    away = (ev.get("awayTeam") or {}).get("name")
    mid = ev.get("id")
    ts = ev.get("startTimestamp")
    if mid is None or home is None or away is None:
        return None
    kickoff_iso = ""
    if ts is not None:
        try:
            kickoff_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
        except Exception:
            kickoff_iso = str(ts)
    return {
        "match_id": int(mid),
        "kickoff_timestamp": ts,
        "kickoff_utc": kickoff_iso,
        "home_team": str(home),
        "away_team": str(away),
        "status_type": typ,
        "status_description": str(st.get("description") or ""),
        "fetch_method": source,
    }


def want_fixture_status(typ: str, *, include_live: bool) -> bool:
    if typ == "notstarted":
        return True
    if include_live and typ == "inprogress":
        return True
    return False
