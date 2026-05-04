"""
Shared club identity rules used during ingestion (and referenced post-ingestion).

Goals:
- One normalization pipeline aligned with resolve_club_identities aliases/canonical keys.
- Apply Rapidfuzz suggestions **at ingest time**: merge onto existing dim clubs instead of only
  logging suggestions for a later resolve step — fewer orphan Sofascore IDs and simpler pipelines.

Post-ingest ``resolve_club_identities.py`` remains useful for fixture-collision hints on legacy data
and for replaying CSV-driven remap batches without re-pulling sources.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import pandas as pd
from rapidfuzz import fuzz, process

from scripts.club_name_canonical import canonical_match_key

logger = logging.getLogger(__name__)

FUZZY_SUGGESTION_THRESHOLD = 88


def norm_club_name(s: Any) -> str:
    """Lower-level club-string normaliser aligned with resolve_club_identities / ingestion."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""

    t = str(s).strip().lower()
    t = t.translate(str.maketrans({"ø": "o", "æ": "ae", "å": "a", "ð": "d"}))
    t = unicodedata.normalize("NFKD", t)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))

    t = t.replace("\ufffd", "")

    replacements = {
        "&": " and ",
        "'": "",
        "'": "",
        ".": " ",
        ",": " ",
        "-": " ",
        "/": " ",
    }
    for old, new in replacements.items():
        t = t.replace(old, new)

    t = re.sub(
        r"\b(fc|cf|sc|ac|afc|sv|kv|uk|fk|sk)\b",
        "",
        t,
    )
    t = re.sub(r"\s+", " ", t).strip()
    return t


@dataclass
class ClubMatch:
    club_id: int | None
    was_created: bool
    suggestion: str | None
    suggestion_score: float | None


def suggest_club_match(
    name: str,
    dim_club: pd.DataFrame,
    league_country_id: int | None = None,
) -> tuple[str | None, float | None]:
    """
    Rapidfuzz best dim ``club_name`` for ``name``.

    When ``league_country_id`` is set (domestic leagues), search **that country's clubs first**
    so e.g. ``Queens Park Rangers`` matches English ``QPR`` instead of Scottish ``Rangers``
    (token_set_ratio scores 100 against ``Rangers``).
    International / empty subset falls back to the full dim.
    """

    def _extract(subset: pd.DataFrame) -> tuple[str | None, float | None]:
        choices = subset["club_name"].astype(str).tolist()
        if not choices:
            return None, None
        result = process.extractOne(
            name,
            choices,
            scorer=fuzz.token_set_ratio,
            score_cutoff=FUZZY_SUGGESTION_THRESHOLD,
        )
        if result:
            return result[0], float(result[1])
        return None, None

    if league_country_id is not None and "country_id" in dim_club.columns:
        sub = dim_club[dim_club["country_id"] == int(league_country_id)]
        hit = _extract(sub)
        if hit[0] is not None:
            return hit
    return _extract(dim_club)


def build_club_lookup(dim_club: pd.DataFrame) -> dict[str, int]:
    """Map canonical_match_key -> minimum club_id (stable anchor per spelling bucket)."""
    buckets: dict[str, list[int]] = defaultdict(list)
    for _, row in dim_club.iterrows():
        nm = norm_club_name(row["club_name"])
        if not nm:
            continue
        key = canonical_match_key(nm)
        buckets[key].append(int(row["club_id"]))
    return {key: min(ids) for key, ids in buckets.items()}


def resolve_or_create_club(
    club_name: str,
    dim_club: pd.DataFrame,
    club_lookup: dict[str, int],
    country_id: int,
    next_club_id: int,
    *,
    create_missing: bool = False,
    merge_on_fuzzy: bool = True,
) -> tuple[ClubMatch, pd.DataFrame, dict[str, int], int]:
    """
    Resolve club_name to club_id.

    Order:
    1. Exact canonical key hit on lookup / alias keys seeded during this ingest run.
    2. If merge_on_fuzzy: Rapidfuzz suggestion at or above threshold -> reuse matching dim row id,
       cache incoming canon_key -> id so aliases converge within one run.
    3. Else create new dim row when create_missing else unresolved (ClubMatch.club_id None).
    """
    nm = norm_club_name(club_name)
    if not nm:
        return ClubMatch(None, False, None, None), dim_club, club_lookup, next_club_id

    canon_key = canonical_match_key(nm)
    if canon_key in club_lookup:
        club_id = club_lookup[canon_key]
        return ClubMatch(club_id, False, None, None), dim_club, club_lookup, next_club_id

    suggestion: str | None = None
    suggestion_score: float | None = None

    if merge_on_fuzzy:
        suggestion, suggestion_score = suggest_club_match(club_name, dim_club, league_country_id=country_id)
        if suggestion is not None and suggestion_score is not None:
            sugg_rows = dim_club[dim_club["club_name"].astype(str) == suggestion]
            if not sugg_rows.empty:
                resolved_id = int(sugg_rows["club_id"].min())
                club_lookup[canon_key] = resolved_id
                logger.debug(
                    "Merged ingest club '%s' -> existing '%s' (id=%s, fuzzy=%s)",
                    club_name,
                    suggestion,
                    resolved_id,
                    suggestion_score,
                )
                return (
                    ClubMatch(resolved_id, False, suggestion, suggestion_score),
                    dim_club,
                    club_lookup,
                    next_club_id,
                )

    if create_missing:
        new_club_id = next_club_id
        new_row = pd.DataFrame(
            [
                {
                    "club_id": new_club_id,
                    "club_name": club_name,
                    "country_id": country_id,
                }
            ]
        )
        dim_club = pd.concat([dim_club, new_row], ignore_index=True)
        club_lookup[canon_key] = new_club_id
        return (
            ClubMatch(new_club_id, True, suggestion, suggestion_score),
            dim_club,
            club_lookup,
            next_club_id + 1,
        )

    return (
        ClubMatch(None, False, suggestion, suggestion_score),
        dim_club,
        club_lookup,
        next_club_id,
    )
