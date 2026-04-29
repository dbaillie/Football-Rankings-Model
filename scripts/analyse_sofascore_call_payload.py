from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ScraperFC.sofascore import Sofascore
from ScraperFC.sofascore import comps as SOFASCORE_COMPS
from ScraperFC.sofascore import API_PREFIX as SOFASCORE_API_PREFIX
from ScraperFC.utils import botasaurus_browser_get_json, botasaurus_request_get_json


def keys_of(obj: Any) -> list[str]:
    if isinstance(obj, dict):
        return sorted(obj.keys())
    return []


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect SofaScore payload structure for one league/season.")
    parser.add_argument("--league", default="UEFA Champions League")
    parser.add_argument("--season", default=None, help="Optional season label. If omitted, first returned season is used.")
    parser.add_argument("--out", default="output/sofascore_payload_analysis.json")
    args = parser.parse_args()

    ss = Sofascore()
    report: dict[str, Any] = {"league": args.league}
    if args.league in SOFASCORE_COMPS:
        tournament_id = SOFASCORE_COMPS[args.league]["SOFASCORE"]
        seasons_url = f"{SOFASCORE_API_PREFIX}/unique-tournament/{tournament_id}/seasons/"
        report["seasons_url"] = seasons_url
        try:
            report["raw_seasons_response_browser"] = botasaurus_browser_get_json(seasons_url)
        except Exception as exc:
            report["raw_seasons_response_browser_error"] = repr(exc)
        try:
            report["raw_seasons_response_request"] = botasaurus_request_get_json(seasons_url)
        except Exception as exc:
            report["raw_seasons_response_request_error"] = repr(exc)
    else:
        report["league_in_comps"] = False

    try:
        seasons = ss.get_valid_seasons(args.league)
        season_names = list(seasons.keys())
        report["valid_seasons_count"] = len(season_names)
        report["valid_seasons_preview"] = season_names[:10]
    except Exception as exc:
        report["valid_seasons_error"] = repr(exc)
        Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        return

    season = args.season or season_names[0]
    report["selected_season"] = season

    try:
        matches = ss.get_match_dicts(season, args.league)
        report["match_count"] = len(matches)
    except Exception as exc:
        report["get_match_dicts_error"] = repr(exc)
        Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        return

    if not matches:
        Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        return

    m = matches[0]
    report["sample_top_keys"] = keys_of(m)
    report["sample_status"] = m.get("status")
    report["sample_homeTeam_keys"] = keys_of(m.get("homeTeam"))
    report["sample_awayTeam_keys"] = keys_of(m.get("awayTeam"))
    report["sample_homeTeam_goals"] = (m.get("homeTeam") or {}).get("goals")
    report["sample_awayTeam_goals"] = (m.get("awayTeam") or {}).get("goals")
    report["sample_homeScore"] = m.get("homeScore")
    report["sample_awayScore"] = m.get("awayScore")

    Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
