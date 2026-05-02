"""
Pull upcoming (not started) football fixtures from SofaScore via the same stack as ScraperFC.

ScraperFC docs (review summary)
--------------------------------
Official docs: https://scraperfc.readthedocs.io/en/latest/index.html

• **Sofascore module** — ``ScraperFC.sofascore.Sofascore`` exposes season discovery and
  ``get_match_dicts(year, league)``, which paginates only::

      GET .../unique-tournament/{id}/season/{seasonId}/events/last/{page}

  That endpoint is oriented toward **already played** matches; it does **not** expose
  a documented ``events/next`` helper in ScraperFC itself.

• **FBref module** — match lists from HTML pages; no first-class “fixtures only” API like
  SofaScore JSON.

• **ClubElo / others** — separate sources; not used here.

This script therefore uses ``ScraperFC.utils.botasaurus_browser_get_json`` (same TLS/browser
fingerprint path as ScraperFC) and calls **additional** undocumented SofaScore routes that
community docs describe (see e.g. https://github.com/apdmatos/sofascore-api):

• ``GET /sport/football/scheduled-events/{YYYY-MM-DD}`` — all football events that calendar day.
• ``GET .../unique-tournament/{tid}/season/{sid}/events/next/{page}`` — try pagination
  symmetric to ``events/last`` (works on many tournaments; may return empty if the API changes).
• ``GET .../unique-tournament/{tid}/season/{sid}/events/round/{n}`` — fixtures + results for a
  round; upcoming rows often have ``status.type == "notstarted"``.

Outputs a CSV of future fixtures (filtering configurable). Does **not** write into the main
fact table — exploratory / scheduling use only.

Examples::

  python scripts/pull_future_fixtures_sofascore.py --league "England Premier League" --days 14 --dry-run
  python scripts/pull_future_fixtures_sofascore.py --league "Spain La Liga" --method rounds --year "24/25" --max-round 38 --output output/laliga_rounds.csv
  python scripts/pull_future_fixtures_sofascore.py --league "England Premier League" --method events_next --year "25/26"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.sofascore_future_helpers import (
    fetch_events_next,
    fetch_rounds,
    fetch_scheduled_window,
    normalize_event_row,
    season_id_for_year,
    tournament_id_and_slug,
    want_fixture_status,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upcoming SofaScore fixtures (browser-backed HTTP, ScraperFC utils).")
    p.add_argument(
        "--league",
        type=str,
        required=True,
        help='ScraperFC comps key with SOFASCORE backend, e.g. "England Premier League"',
    )
    p.add_argument(
        "--year",
        type=str,
        default=None,
        help="SofaScore season label (e.g. '25/26'). Required for methods that need season id: events_next, rounds.",
    )
    p.add_argument(
        "--method",
        choices=("events_next", "scheduled", "rounds"),
        default="scheduled",
        help="How to discover fixtures (default: scheduled — most reliable for not-started games).",
    )
    p.add_argument(
        "--days",
        type=int,
        default=14,
        help="For --method scheduled: scan today + N-1 days (default: 14).",
    )
    p.add_argument(
        "--max-round",
        type=int,
        default=40,
        help="For --method rounds: fetch rounds 1..N (default: 40).",
    )
    p.add_argument(
        "--include-live",
        action="store_true",
        help='Include status type "inprogress" as well as "notstarted".',
    )
    p.add_argument(
        "--output",
        type=str,
        default=None,
        help="CSV path (default: output/sofascore_upcoming_{slug}.csv)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print counts only; no CSV.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    league = args.league.strip()
    tid, slug = tournament_id_and_slug(league)

    raw: list[dict] = []
    source_tag = args.method

    if args.method in ("events_next", "rounds"):
        if not args.year:
            print("--year is required for --method events_next and rounds.", file=sys.stderr)
            sys.exit(1)
        sid = season_id_for_year(league, args.year)
        if args.method == "events_next":
            raw = fetch_events_next(tid, sid)
            if not raw:
                print(
                    "events_next returned no rows; try --method scheduled (default) or --method rounds.",
                    file=sys.stderr,
                )
        else:
            raw = fetch_rounds(tid, sid, args.max_round)
    else:
        raw = fetch_scheduled_window(tid, args.days)

    rows: list[dict] = []
    for ev in raw:
        row = normalize_event_row(ev, source=source_tag)
        if row is None:
            continue
        if not want_fixture_status(row["status_type"], include_live=args.include_live):
            continue
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["kickoff_timestamp", "match_id"], na_position="last")

    print(f"League={league!r} tournament_id={tid} method={args.method} upcoming_rows={len(df)}")

    if args.dry_run:
        if not df.empty:
            print(df.head(15).to_string(index=False))
        return

    out_path = Path(args.output) if args.output else _REPO_ROOT / "output" / f"sofascore_upcoming_{slug}.csv"
    if not out_path.is_absolute():
        out_path = (_REPO_ROOT / out_path).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
