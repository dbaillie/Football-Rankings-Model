# =========================================================
# FOOTBALL INGESTION CONFIG
# =========================================================

SOURCE_SYSTEM = "SofaScore"

SEASON_START_YEAR = 2020
SEASON_END_YEAR = 2025


# =========================================================
# LEAGUES TO RUN
# =========================================================

LEAGUES = [
    "Argentina Liga Profesional",
    "Argentina Copa de la Liga Profesional",
    "Bulgaria Parva Liga",
    "CONCACAF Gold Cup",
    "CONMEBOL Copa Libertadores",
    "England Premier League",
    "England EFL Championship",
    "England WSL",
    "England WSL 2",
    "FIFA World Cup",
    "FIFA Womens World Cup",
    "France Ligue 1",
    "France Ligue 2",
    "France National 1",
    "Germany Bundesliga",
    "Germany 2.Bundesliga",
    "Italy Serie A",
    "Italy Serie B",
    "Mexico Liga MX Apertura",
    "Mexico Liga MX Clausura",
    "Netherlands Eredivisie",
    "Peru Liga 1",
    "Portugal Primeira Liga",
    "Portugal Liga Portugal 2",
    "Saudi Arabia Pro League",
    "Spain La Liga",
    "Spain La Liga 2",
    "Turkiye Super Lig",
    "UEFA Champions League",
    "UEFA Europa League",
    "UEFA Conference League",
    "UEFA European Championship",
    "Ukraine Premier League",
    "USA MLS",
    "USA USL Championship",
    "USA USL League 1",
    "USA USL League 2",
]


# =========================================================
# LEAGUE METADATA
# season_format options:
#   split       -> '24/25'
#   start_year  -> '2024'
#   end_year    -> '2025'
#   compact     -> '2425'
#   fbref       -> '2024-2025'
# =========================================================

LEAGUE_METADATA = [

    # ARGENTINA
    {
        "source_league_name": "Argentina Liga Profesional",
        "league_key": "ARG1",
        "league_name": "Liga Profesional",
        "country": "Argentina",
        "country_code": "ARG",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "start_year",
    },
    {
        "source_league_name": "Argentina Copa de la Liga Profesional",
        "league_key": "ARGCUP",
        "league_name": "Copa de la Liga Profesional",
        "country": "Argentina",
        "country_code": "ARG",
        "tier": None,
        "competition_type": "domestic_cup",
        "season_format": "start_year",
    },

    # BULGARIA
    {
        "source_league_name": "Bulgaria Parva Liga",
        "league_key": "BUL1",
        "league_name": "Parva Liga",
        "country": "Bulgaria",
        "country_code": "BUL",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # CONCACAF / CONMEBOL
    {
        "source_league_name": "CONCACAF Gold Cup",
        "league_key": "GOLD",
        "league_name": "Gold Cup",
        "country": "International",
        "country_code": "INT",
        "tier": 1,
        "competition_type": "international",
        "season_format": "end_year",
    },
    {
        "source_league_name": "CONMEBOL Copa Libertadores",
        "league_key": "LIB",
        "league_name": "Copa Libertadores",
        "country": "International",
        "country_code": "INT",
        "tier": 1,
        "competition_type": "continental",
        "season_format": "start_year",
    },

    # ENGLAND
    {
        "source_league_name": "England Premier League",
        "league_key": "ENG1",
        "league_name": "Premier League",
        "country": "England",
        "country_code": "ENG",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "England EFL Championship",
        "league_key": "ENG2",
        "league_name": "Championship",
        "country": "England",
        "country_code": "ENG",
        "tier": 2,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "England WSL",
        "league_key": "ENGW1",
        "league_name": "WSL",
        "country": "England",
        "country_code": "ENG",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "England WSL 2",
        "league_key": "ENGW2",
        "league_name": "WSL 2",
        "country": "England",
        "country_code": "ENG",
        "tier": 2,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # FIFA
    {
        "source_league_name": "FIFA World Cup",
        "league_key": "WC",
        "league_name": "World Cup",
        "country": "International",
        "country_code": "INT",
        "tier": 1,
        "competition_type": "international",
        "season_format": "end_year",
    },
    {
        "source_league_name": "FIFA Womens World Cup",
        "league_key": "WWC",
        "league_name": "Women's World Cup",
        "country": "International",
        "country_code": "INT",
        "tier": 1,
        "competition_type": "international",
        "season_format": "end_year",
    },

    # FRANCE
    {
        "source_league_name": "France Ligue 1",
        "league_key": "FRA1",
        "league_name": "Ligue 1",
        "country": "France",
        "country_code": "FRA",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "France Ligue 2",
        "league_key": "FRA2",
        "league_name": "Ligue 2",
        "country": "France",
        "country_code": "FRA",
        "tier": 2,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "France National 1",
        "league_key": "FRA3",
        "league_name": "National 1",
        "country": "France",
        "country_code": "FRA",
        "tier": 3,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # GERMANY
    {
        "source_league_name": "Germany Bundesliga",
        "league_key": "GER1",
        "league_name": "Bundesliga",
        "country": "Germany",
        "country_code": "GER",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "Germany 2.Bundesliga",
        "league_key": "GER2",
        "league_name": "2. Bundesliga",
        "country": "Germany",
        "country_code": "GER",
        "tier": 2,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # ITALY
    {
        "source_league_name": "Italy Serie A",
        "league_key": "ITA1",
        "league_name": "Serie A",
        "country": "Italy",
        "country_code": "ITA",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "Italy Serie B",
        "league_key": "ITA2",
        "league_name": "Serie B",
        "country": "Italy",
        "country_code": "ITA",
        "tier": 2,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # MEXICO
    {
        "source_league_name": "Mexico Liga MX Apertura",
        "league_key": "MEX1A",
        "league_name": "Liga MX Apertura",
        "country": "Mexico",
        "country_code": "MEX",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "start_year",
    },
    {
        "source_league_name": "Mexico Liga MX Clausura",
        "league_key": "MEX1C",
        "league_name": "Liga MX Clausura",
        "country": "Mexico",
        "country_code": "MEX",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "start_year",
    },

    # NETHERLANDS
    {
        "source_league_name": "Netherlands Eredivisie",
        "league_key": "NED1",
        "league_name": "Eredivisie",
        "country": "Netherlands",
        "country_code": "NED",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # PERU
    {
        "source_league_name": "Peru Liga 1",
        "league_key": "PER1",
        "league_name": "Liga 1",
        "country": "Peru",
        "country_code": "PER",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "start_year",
    },

    # PORTUGAL
    {
        "source_league_name": "Portugal Primeira Liga",
        "league_key": "POR1",
        "league_name": "Primeira Liga",
        "country": "Portugal",
        "country_code": "POR",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "Portugal Liga Portugal 2",
        "league_key": "POR2",
        "league_name": "Liga Portugal 2",
        "country": "Portugal",
        "country_code": "POR",
        "tier": 2,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # SAUDI ARABIA
    {
        "source_league_name": "Saudi Arabia Pro League",
        "league_key": "KSA1",
        "league_name": "Saudi Pro League",
        "country": "Saudi Arabia",
        "country_code": "KSA",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # SPAIN
    {
        "source_league_name": "Spain La Liga",
        "league_key": "ESP1",
        "league_name": "La Liga",
        "country": "Spain",
        "country_code": "ESP",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },
    {
        "source_league_name": "Spain La Liga 2",
        "league_key": "ESP2",
        "league_name": "La Liga 2",
        "country": "Spain",
        "country_code": "ESP",
        "tier": 2,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # TURKIYE
    {
        "source_league_name": "Turkiye Super Lig",
        "league_key": "TUR1",
        "league_name": "Super Lig",
        "country": "Turkiye",
        "country_code": "TUR",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # UEFA
    {
        "source_league_name": "UEFA Champions League",
        "league_key": "UCL",
        "league_name": "Champions League",
        "country": "International",
        "country_code": "INT",
        "tier": 1,
        "competition_type": "continental",
        "season_format": "split",
    },
    {
        "source_league_name": "UEFA Europa League",
        "league_key": "UEL",
        "league_name": "Europa League",
        "country": "International",
        "country_code": "INT",
        "tier": 2,
        "competition_type": "continental",
        "season_format": "split",
    },
    {
        "source_league_name": "UEFA Conference League",
        "league_key": "UECL",
        "league_name": "Conference League",
        "country": "International",
        "country_code": "INT",
        "tier": 3,
        "competition_type": "continental",
        "season_format": "split",
    },
    {
        "source_league_name": "UEFA European Championship",
        "league_key": "EURO",
        "league_name": "European Championship",
        "country": "International",
        "country_code": "INT",
        "tier": 1,
        "competition_type": "international",
        "season_format": "end_year",
    },

    # UKRAINE
    {
        "source_league_name": "Ukraine Premier League",
        "league_key": "UKR1",
        "league_name": "Premier League",
        "country": "Ukraine",
        "country_code": "UKR",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "split",
    },

    # USA
    {
        "source_league_name": "USA MLS",
        "league_key": "USA1",
        "league_name": "MLS",
        "country": "USA",
        "country_code": "USA",
        "tier": 1,
        "competition_type": "domestic_league",
        "season_format": "start_year",
    },
    {
        "source_league_name": "USA USL Championship",
        "league_key": "USA2",
        "league_name": "USL Championship",
        "country": "USA",
        "country_code": "USA",
        "tier": 2,
        "competition_type": "domestic_league",
        "season_format": "start_year",
    },
    {
        "source_league_name": "USA USL League 1",
        "league_key": "USA3",
        "league_name": "USL League 1",
        "country": "USA",
        "country_code": "USA",
        "tier": 3,
        "competition_type": "domestic_league",
        "season_format": "start_year",
    },
    {
        "source_league_name": "USA USL League 2",
        "league_key": "USA4",
        "league_name": "USL League 2",
        "country": "USA",
        "country_code": "USA",
        "tier": 4,
        "competition_type": "domestic_league",
        "season_format": "start_year",
    },
]