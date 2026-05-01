"""
Resolve duplicate/unknown club IDs in fact data by:
1) created_clubs.csv: map created_club_id -> existing dim_club id via suggested_existing_match
   (ingestion now applies fuzzy merges live via scripts/club_identity.py — this mainly catches legacy rows.)
   Optional --min-suggestion-score to skip weak fuzzy suggestions from older ingest logs.
2) Canonical name collisions in dim (e.g. Man City vs Manchester City): map duplicate ids -> min(club_id)
3) Fixtures: same (date, score) and same home (or same away) with two different partner ids —
   if one partner is a created id and one is not, map created -> canonical (never overrides (1)-(2))
4) Deduplicate rows after remapping (same match identity columns, keep first result_id)

Club_1111 example: created_clubs lists "FC Bayern München" -> suggested "Bayern Munich" -> dim club_id 349.

Usage:
  python scripts/resolve_club_identities.py
  python scripts/resolve_club_identities.py --write --min-suggestion-score 80
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.club_name_canonical import canonical_match_key
from scripts.club_identity import norm_club_name


def norm_name(s: str) -> str:
    """Backward-compatible alias for resolve-stage lookups (same rules as ingest)."""
    return norm_club_name(s)


def build_dim_lookups(dim: pd.DataFrame) -> dict[str, list[int]]:
    name_to_ids: dict[str, list[int]] = defaultdict(list)
    for _, row in dim.iterrows():
        cid = int(row["club_id"])
        key = canonical_match_key(norm_name(str(row["club_name"])))
        name_to_ids[key].append(cid)
    return dict(name_to_ids)


def from_canonical_key_collisions(dim: pd.DataFrame) -> dict[int, int]:
    """Map duplicate club_ids that share the same canonical name key (e.g. Man City vs Manchester City)."""
    key_to_ids: dict[str, list[int]] = defaultdict(list)
    for _, row in dim.iterrows():
        key = canonical_match_key(norm_name(str(row["club_name"])))
        key_to_ids[key].append(int(row["club_id"]))
    out: dict[int, int] = {}
    for ids in key_to_ids.values():
        if len(ids) < 2:
            continue
        canonical_id = min(ids)
        for x in ids:
            if x != canonical_id:
                out[x] = canonical_id
    return out


def best_dim_id(
    name: str,
    name_to_ids: dict[str, list[int]],
    extra_hints: set[str] | None = None,
) -> int | None:
    """Pick single club_id if unique best match, else None."""
    n = canonical_match_key(norm_name(name))
    if not n:
        return None
    if n in name_to_ids and len(name_to_ids[n]) == 1:
        return name_to_ids[n][0]
    if n in name_to_ids and len(name_to_ids[n]) > 1:
        return min(name_to_ids[n])  # stable tie-break
    if extra_hints:
        for h in extra_hints:
            hn = canonical_match_key(norm_name(h))
            if hn in name_to_ids:
                return min(name_to_ids[hn])
    return None


def parse_match_date(s: str) -> pd.Timestamp | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    return pd.to_datetime(str(s), format="mixed", dayfirst=True, errors="coerce")


def from_created_clubs(
    created: pd.DataFrame,
    dim: pd.DataFrame,
    min_suggestion_score: float | None,
) -> dict[int, int]:
    """Map created_club_id -> existing dim club_id when suggestion matches unambiguously."""
    name_to_ids = build_dim_lookups(dim)
    out: dict[int, int] = {}
    for _, row in created.iterrows():
        if min_suggestion_score is not None and "suggestion_score" in created.columns:
            sc = row.get("suggestion_score")
            if pd.notna(sc) and float(sc) < min_suggestion_score:
                continue
        cid = int(row["created_club_id"])
        cname = str(row.get("club_name", ""))
        sugg = str(row.get("suggested_existing_match", "")) if pd.notna(row.get("suggested_existing_match")) else ""
        if not cname and not sugg:
            continue
        hints = {s for s in (cname, sugg) if s and str(s).lower() not in ("nan", "none")}
        # Prefer suggested_existing_match (from fuzzy table) over raw Sofascore name
        tid = best_dim_id(sugg or cname, name_to_ids, extra_hints=hints)
        if tid is not None and tid != cid:
            out[cid] = tid
    return out


def from_fixtures(
    fact: pd.DataFrame,
    created_ids: set[int],
) -> dict[int, int]:
    """
    Duplicate rows for the same real fixture: same (date, score) and the same
    home team but different away id (or same away, different home). If exactly
    one of the two ids is a Sofascore-created id, map that id to the other.
    """
    out: dict[int, int] = {}
    gcols = [c for c in fact.columns if c in ("match_date", "home_club_id", "away_club_id", "home_team_goals", "away_team_goals")]
    work = fact[gcols].copy()
    for col in ("home_club_id", "away_club_id"):
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0).astype(int)
    work["d"] = work["match_date"].map(parse_match_date)
    work = work.dropna(subset=["d"])
    work["hg"] = work["home_team_goals"]
    work["ag"] = work["away_team_goals"]

    for _grp, part in work.groupby(["d", "hg", "ag"]):
        if len(part) < 2:
            continue
        for _home, sub in part.groupby("home_club_id"):
            aw = sub["away_club_id"].drop_duplicates().tolist()
            if len(aw) < 2:
                continue
            c_in = [x for x in aw if x in created_ids]
            c_out = [x for x in aw if x not in created_ids]
            if len(c_in) == 1 and len(c_out) == 1:
                out[c_in[0]] = c_out[0]
        for _aw, sub in part.groupby("away_club_id"):
            hw = sub["home_club_id"].drop_duplicates().tolist()
            if len(hw) < 2:
                continue
            c_in = [x for x in hw if x in created_ids]
            c_out = [x for x in hw if x not in created_ids]
            if len(c_in) == 1 and len(c_out) == 1:
                out[c_in[0]] = c_out[0]
    return out


def merge_remappings(*maps: dict[int, int]) -> dict[int, int]:
    """
    Merge remap dicts in priority order (first wins per source id).

    `main()` passes (created_clubs suggestions, canonical collisions, fixture dedupe).
    Fixture overlap can guess wrong when unrelated leagues share (date, score, side);
    never let it override explicit suggestions or canonical name merges.
    """
    combined: dict[int, int] = {}
    for m in maps:
        for k, v in m.items():
            if k not in combined:
                combined[k] = v
    # Transitive: follow chains to canonical
    changed = True
    while changed:
        changed = False
        for k, v in list(combined.items()):
            if v in combined and combined[v] != v:
                combined[k] = combined[v]
                changed = True
    # No self-maps
    return {k: v for k, v in combined.items() if k != v}


def apply_remap(
    fact: pd.DataFrame,
    remap: dict[int, int],
) -> pd.DataFrame:
    out = fact.copy()
    for col in ("home_club_id", "away_club_id"):
        s = pd.to_numeric(out[col], errors="coerce")
        s = s.map(lambda x: int(x) if pd.notna(x) else x)
        out[col] = s.map(lambda x: remap.get(int(x), x) if isinstance(x, (int, float)) and not (isinstance(x, float) and pd.isna(x)) else x)
    return out


def dedupe_fact(fact: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate real matches: same date, home, away, goals, league+country (if present)."""
    cols = [c for c in fact.columns if c not in ("result_id",)]
    base = [c for c in cols if c in fact.columns]
    return fact.drop_duplicates(subset=base, keep="first").reset_index(drop=True)


def dedupe_uefa_remerge_twins(fact: pd.DataFrame) -> pd.DataFrame:
    """
    When resolve is run with `--fact` pointing at an already-resolved table, euro rows are
    concatenated again. Identity fixes then remap new euro IDs correctly while stale copies keep
    wrong club IDs, producing two rows for the same fixture (same date/score/home or away).

    Collapse UEFA club phases (UCL/UEL/UECL only): keep the last row per scoreline key so the
    freshly appended remapped slice wins over older stale IDs.
    """
    codes = {"UCL", "UEL", "UECL"}
    if "league_code" not in fact.columns:
        return fact
    lc = fact["league_code"].astype(str)
    mask = lc.isin(codes)
    if not mask.any():
        return fact

    dom = fact[~mask].copy()
    uefa = fact[mask].copy()
    keys_home = ["match_date", "league_code", "home_club_id", "home_team_goals", "away_team_goals"]
    keys_away = ["match_date", "league_code", "away_club_id", "home_team_goals", "away_team_goals"]
    if len([c for c in keys_home if c in uefa.columns]) < len(keys_home):
        return fact

    uefa = uefa.drop_duplicates(subset=keys_home, keep="last")
    uefa = uefa.drop_duplicates(subset=keys_away, keep="last")
    out = pd.concat([dom, uefa], ignore_index=True)
    sort_cols = [c for c in ("country_name", "league_code", "match_date", "result_id") if c in out.columns]
    return out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    root = Path(__file__).resolve().parents[1]
    parser.add_argument("--fact", type=Path, default=root / "output" / "fact_result_simple_ingested.csv")
    parser.add_argument("--dim", type=Path, default=root / "output" / "dim_club.csv")
    parser.add_argument("--dim-updated", type=Path, default=root / "output" / "dim_club_updated.csv")
    parser.add_argument("--created", type=Path, default=root / "output" / "created_clubs.csv")
    parser.add_argument("--out-fact", type=Path, default=root / "output" / "fact_result_simple_resolved.csv")
    parser.add_argument("--out-map", type=Path, default=root / "output" / "club_id_remap.json")
    parser.add_argument("--write", action="store_true", help="Write out-fact, out-map, report")
    parser.add_argument(
        "--skip-euro-merge",
        action="store_true",
        help="Do not merge output/fact_result_simple_ingested_euro.csv or created_clubs_euro.csv even if present.",
    )
    parser.add_argument(
        "--min-suggestion-score",
        type=float,
        default=None,
        help="If set, only use created_clubs row when suggestion_score >= this (0-100).",
    )
    args = parser.parse_args()

    fact = pd.read_csv(args.fact, low_memory=False)

    out_root = args.out_fact.resolve().parent
    euro_fact_path = out_root / "fact_result_simple_ingested_euro.csv"
    if not args.skip_euro_merge and euro_fact_path.exists():
        euro_fact = pd.read_csv(euro_fact_path, low_memory=False)
        if not euro_fact.empty:
            print(f"Merging {len(euro_fact)} rows from {euro_fact_path.name} into fact input")
            fact = pd.concat([fact, euro_fact], ignore_index=True, sort=False)
            if "result_id" in fact.columns and fact["result_id"].duplicated().any():
                fact = fact.copy()
                fact["result_id"] = pd.RangeIndex(start=1, stop=len(fact) + 1, dtype="int64")
    dim = pd.read_csv(args.dim, low_memory=False)
    if args.dim_updated.exists():
        # Prefer updated dim (has created names) for lookups; merge unique ids
        du = pd.read_csv(args.dim_updated, low_memory=False)
        dim = pd.concat([dim, du], ignore_index=True).drop_duplicates(subset=["club_id"], keep="last")

    created: pd.DataFrame
    if args.created.exists():
        created = pd.read_csv(args.created, low_memory=False)
    else:
        created = pd.DataFrame(
            columns=["club_name", "created_club_id", "suggested_existing_match", "suggestion_score"]
        )

    if not created.empty and "suggested_existing_match" not in created.columns and "suggested_match" in created.columns:
        created = created.rename(columns={"suggested_match": "suggested_existing_match"})

    euro_created_path = out_root / "created_clubs_euro.csv"
    if not args.skip_euro_merge and euro_created_path.exists():
        euro_created = pd.read_csv(euro_created_path, low_memory=False)
        if not euro_created.empty:
            if "suggested_existing_match" not in euro_created.columns and "suggested_match" in euro_created.columns:
                euro_created = euro_created.rename(columns={"suggested_match": "suggested_existing_match"})
            print(f"Merging {len(euro_created)} rows from {euro_created_path.name} into created-clubs input")
            created = pd.concat([created, euro_created], ignore_index=True)
            if "created_club_id" in created.columns:
                created = created.drop_duplicates(subset=["created_club_id"], keep="last")

    max_dim_id = int(dim["club_id"].max()) if not dim.empty else 0
    created_ids: set[int] = set()
    if not created.empty and "created_club_id" in created.columns:
        created_ids = set(created["created_club_id"].dropna().astype(int).tolist())
    for col in ("home_club_id", "away_club_id"):
        if col in fact.columns:
            s = pd.to_numeric(fact[col], errors="coerce")
            for v in s.dropna().unique():
                vi = int(v)
                if vi > max_dim_id and vi >= 1100:  # typical Sofascore-created id block
                    created_ids.add(vi)

    map_from_suggest = (
        from_created_clubs(created, dim, args.min_suggestion_score) if not created.empty else {}
    )
    map_from_canonical = from_canonical_key_collisions(dim)
    map_from_fixtures = from_fixtures(fact, created_ids)
    remap = merge_remappings(map_from_suggest, map_from_canonical, map_from_fixtures)

    before_dedup = len(fact)
    dsub = [c for c in fact.columns if c != "result_id" and c in fact.columns]
    n_exact_dup = before_dedup - len(fact.drop_duplicates(subset=dsub, keep="first"))

    resolved = apply_remap(fact, remap)
    resolved = dedupe_fact(resolved)
    resolved = dedupe_uefa_remerge_twins(resolved)
    n_rows_before = before_dedup
    n_rows_after = len(resolved)

    report = {
        "from_created_suggestions": len(map_from_suggest),
        "from_canonical_name_collisions": len(map_from_canonical),
        "from_fixtures_merged": len(map_from_fixtures),
        "total_remap": len(remap),
        "approx_duplicate_rows_before_dedup": int(n_exact_dup),
        "rows_input": n_rows_before,
        "rows_after_remap_dedup": n_rows_after,
        "remap": {str(k): v for k, v in sorted(remap.items())},
    }

    print(json.dumps({k: v for k, v in report.items() if k != "remap"}, indent=2))
    if len(remap) <= 80:
        print("remap:", json.dumps(report["remap"], indent=2))
    else:
        print("remap (first 40):", json.dumps({k: report["remap"][k] for k in list(report["remap"])[:40]}))

    if args.write:
        resolved.to_csv(args.out_fact, index=False)
        with open(args.out_map, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        rpath = args.out_fact.parent / "resolve_club_identities_report.txt"
        with open(rpath, "w", encoding="utf-8") as f:
            f.write(
                f"rows_input={n_rows_before} rows_out={n_rows_after} remapped_ids={len(remap)}\n"
            )
        print(f"Wrote {args.out_fact}")
        print(f"Wrote {args.out_map}")


if __name__ == "__main__":
    main()
