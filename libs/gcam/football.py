"""Football-specific adapter: fixtures to generic weighted community interactions."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .config import GCAMConfig

DEFAULT_UEFA_CODES = frozenset({"UCL", "UEL", "UECL", "EURO"})


def _series_weights(league_code: pd.Series, cfg: GCAMConfig, uefa_codes: frozenset[str]) -> np.ndarray:
    lc = league_code.astype(str).str.strip().str.upper().fillna("")
    n = len(lc)
    w = np.full(n, cfg.domestic_weight, dtype=float)
    uefa_mask = lc.isin(set(uefa_codes))
    cup_mask = lc.str.contains("CUP", na=False) | lc.str.endswith("_CUP")
    friend_mask = lc.str.contains("FRIEND", na=False)
    w[uefa_mask] = cfg.uefa_weight
    w[cup_mask & ~uefa_mask] = cfg.cup_weight
    w[friend_mask] = cfg.friendly_weight
    return w


def iter_weighted_match_pairs(matches: pd.DataFrame):
    """Memory-light iterator for community connectivity over large match subsets."""
    if matches.empty:
        return iter(())
    return zip(
        matches["home_community"].astype(str),
        matches["away_community"].astype(str),
        matches["weight"].astype(float),
    )


def competition_community_suffix(
    league_code: pd.Series,
    cfg: GCAMConfig,
    uefa_codes: frozenset[str],
) -> np.ndarray:
    """
    Map row league_code to GCAM community suffix (paired with ``{country}|suffix``).

    - Domestic league tiers share one bucket per nation (``domestic``) so SC0/SC1/SC2 do not inflate entropy.
    - UEFA/EURO codes stay distinct (UCL, UEL, …) for cross-calibration.
    - Domestic cups → ``cup``; friendlies → ``friendly``.
    """
    lc_u = league_code.astype(str).str.strip().str.upper().fillna("")
    n = len(lc_u)
    uefa_set = set(uefa_codes)
    friend = lc_u.str.contains("FRIEND", na=False).to_numpy()
    uefa_m = lc_u.isin(uefa_set).to_numpy()
    raw_cup = lc_u.str.contains("CUP", na=False) | lc_u.str.endswith("_CUP")
    cup_m = raw_cup.to_numpy() & ~uefa_m

    codes = lc_u.astype(str).to_numpy()
    dom = np.full(n, cfg.football_domestic_community_suffix, dtype=object)
    cup_s = np.full(n, cfg.football_cup_community_suffix, dtype=object)
    fri = np.full(n, cfg.football_friendly_community_suffix, dtype=object)

    return np.where(
        friend,
        fri,
        np.where(uefa_m, codes, np.where(cup_m, cup_s, dom)),
    )


def fact_table_to_weighted_matches(
    fact_df: pd.DataFrame,
    club_id_to_country_name: dict[int, str],
    cfg: GCAMConfig | None = None,
    uefa_codes: frozenset[str] | None = None,
) -> pd.DataFrame:
    """
    Build one row per fixture with home/away club ids, calendar date, rating week,
    community labels, and interaction weight.

    Community label: ``{club_country}|suffix`` where suffix is ``domestic`` for normal leagues
    (all tiers in one national bucket), ``cup`` / ``friendly`` as detected, or the UEFA code
    (UCL, UEL, …) for continental football.
    """
    cfg = cfg or GCAMConfig()
    uefa = uefa_codes if uefa_codes is not None else DEFAULT_UEFA_CODES

    req = ["home_club_id", "away_club_id", "home_team_goals", "away_team_goals", "match_date", "yyyyww"]
    for c in req:
        if c not in fact_df.columns:
            raise ValueError(f"fact_df must contain column {c!r} for GCAM football adapter")

    df = fact_df.dropna(subset=["home_club_id", "away_club_id", "home_team_goals", "away_team_goals", "match_date"]).copy()
    df["home_club_id"] = df["home_club_id"].astype(int)
    df["away_club_id"] = df["away_club_id"].astype(int)

    md = pd.to_datetime(df["match_date"], errors="coerce")
    df = df.loc[md.notna()].copy()
    df["match_date"] = md.dt.normalize()

    if "league_code" in df.columns:
        lc = df["league_code"]
    else:
        lc = pd.Series("", index=df.index)

    cc_map = {int(k): str(v).strip() if pd.notna(v) else "unknown" for k, v in club_id_to_country_name.items()}
    hc = df["home_club_id"].map(cc_map).fillna("unknown").astype(str)
    ac = df["away_club_id"].map(cc_map).fillna("unknown").astype(str)
    hc = hc.where(hc.str.len() > 0, "unknown")
    ac = ac.where(ac.str.len() > 0, "unknown")

    wts = _series_weights(lc.fillna(""), cfg, uefa)
    suffix = competition_community_suffix(lc.fillna(""), cfg, uefa).astype(str)
    suf_ser = pd.Series(suffix, index=df.index, dtype=str)
    df["home_community"] = hc.astype(str) + "|" + suf_ser
    df["away_community"] = ac.astype(str) + "|" + suf_ser
    df["weight"] = wts
    df["yyyyww"] = df["yyyyww"].astype(int)
    df = df.loc[df["weight"] > 0, ["match_date", "yyyyww", "home_club_id", "away_club_id", "home_community", "away_community", "weight"]]
    return df.reset_index(drop=True)


def weighted_match_pairs_for_communities(matches: pd.DataFrame) -> list[tuple[str, str, float]]:
    """Materialise pairs as a list (prefer ``iter_weighted_match_pairs`` for large frames)."""
    return list(iter_weighted_match_pairs(matches))
