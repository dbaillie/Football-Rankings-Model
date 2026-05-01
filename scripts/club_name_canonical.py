"""Merge equivalent club name spellings used across data sources (e.g. Sofascore vs full names)."""


def canonical_match_key(normalized_name: str) -> str:
    """
    Map normalized club strings to a single key so aliases collide.

    `normalized_name` should already be lowercased / punctuation-stripped (use the same
    normalizer as the caller: norm_text or resolve's norm_name).
    """
    n = normalized_name.strip()
    return _SHORT_FORM_TO_CANONICAL.get(n, n)


_SHORT_FORM_TO_CANONICAL: dict[str, str] = {
    "man city": "manchester city",
    "man utd": "manchester united",
    "man united": "manchester united",
    # UEFA/source variants that should collapse to existing domestic identities.
    "bayern munchen": "bayern munich",
    "fc bayern munchen": "bayern munich",
    "atletico de madrid": "ath madrid",
    "club atletico de madrid": "ath madrid",
    "kobenhavn": "copenhagen",
    "fc kobenhavn": "fc copenhagen",
    "royale union saint gilloise": "st gilloise",
    "eintracht frankfurt": "ein frankfurt",
    "internazionale milano": "inter",
    "fc internazionale milano": "inter",
    # Domestic Sofascore abbrev vs UEFA / full official names (token_set fuzzy often < 88).
    "paris saint germain": "paris sg",
    "paris st germain": "paris sg",
    "paris sg fc": "paris sg",
    "bayer 04 leverkusen": "leverkusen",
    "1 koln": "koln",
}
