import pandas as pd
import os
from pathlib import Path
import duckdb

# Path to the data directory
data_dir = Path('data/football')

# List to hold all dataframes
all_dfs = []

# Traverse the directory structure
for country_dir in data_dir.iterdir():
    if not country_dir.is_dir():
        continue
    country = country_dir.name
    print(f"Processing country: {country}")
    
    for sub_dir in country_dir.iterdir():
        if sub_dir.is_dir():
            if sub_dir.name == 'misc':
                # Misc structure: country/misc/*.csv
                for csv_file in sub_dir.glob('*.csv'):
                    print(f"  Loading misc file: {csv_file}")
                    try:
                        df = pd.read_csv(csv_file, on_bad_lines='skip')
                        df['Country'] = country
                        df['League'] = csv_file.stem
                        df['country'] = country
                        df['season'] = df.get('Season')
                        df['league'] = csv_file.stem
                        # Standardize columns
                        df['home_team'] = df.get('HomeTeam', df.get('Home'))
                        df['away_team'] = df.get('AwayTeam', df.get('Away'))
                        df['home_goals'] = df.get('FTHG', df.get('HG'))
                        df['away_goals'] = df.get('FTAG', df.get('AG'))
                        df['result'] = df.get('FTR', df.get('Res'))
                        df['date'] = df.get('Date')
                        df['time'] = df.get('Time')
                        all_dfs.append(df)
                    except Exception as e:
                        print(f"    Error loading {csv_file}: {e}")
                        continue
            else:
                # Seasonal structure: country/season/*.csv
                season = sub_dir.name
                for csv_file in sub_dir.glob('*.csv'):
                    print(f"  Loading seasonal file: {csv_file} for season {season}")
                    try:
                        df = pd.read_csv(csv_file, on_bad_lines='skip')
                        df['Country'] = country
                        df['League'] = csv_file.stem
                        df['country'] = country
                        df['season'] = season
                        df['league'] = csv_file.stem  # e.g., E0, AUT
                        # Standardize columns
                        df['home_team'] = df.get('HomeTeam', df.get('Home'))
                        df['away_team'] = df.get('AwayTeam', df.get('Away'))
                        df['home_goals'] = df.get('FTHG', df.get('HG'))
                        df['away_goals'] = df.get('FTAG', df.get('AG'))
                        df['result'] = df.get('FTR', df.get('Res'))
                        df['date'] = df.get('Date')
                        df['time'] = df.get('Time')
                        all_dfs.append(df)
                    except Exception as e:
                        print(f"    Error loading {csv_file}: {e}")
                        continue

# Concatenate all dataframes
if all_dfs:
    big_df = pd.concat(all_dfs, ignore_index=True, sort=False)
    print(f"Total rows loaded: {len(big_df)}")
else:
    print("No data found.")
    exit()

# big_df['home_team'] = big_df.get('HomeTeam', big_df.get('Home'))
# big_df['away_team'] = big_df.get('AwayTeam', big_df.get('Away'))
# big_df['home_goals'] = big_df.get('FTHG', big_df.get('HG'))
# big_df['away_goals'] = big_df.get('FTAG', big_df.get('AG'))
# big_df['result'] = big_df.get('FTR', big_df.get('Res'))
# big_df['date'] = big_df.get('Date')
# big_df['time'] = big_df.get('Time')

# Create dim_country
unique_countries = big_df['country'].dropna().unique()
dim_country = pd.DataFrame({
    'country_id': range(1, len(unique_countries) + 1),
    'country_name': unique_countries
})
country_map = dict(zip(dim_country['country_name'], dim_country['country_id']))
big_df['country_id'] = big_df['country'].map(country_map)

# Create dim_season
unique_seasons = big_df['season'].dropna().unique()
dim_season = pd.DataFrame({
    'season_id': range(1, len(unique_seasons) + 1),
    'season_name': unique_seasons
})
season_map = dict(zip(dim_season['season_name'], dim_season['season_id']))
big_df['season_id'] = big_df['season'].map(season_map)

# Create dim_club
club_country_pairs = []
for _, row in big_df.iterrows():
    if pd.notna(row['home_team']):
        club_country_pairs.append((row['home_team'], row['country_id']))
    if pd.notna(row['away_team']):
        club_country_pairs.append((row['away_team'], row['country_id']))

club_country_df = pd.DataFrame(club_country_pairs, columns=['club_name', 'country_id']).drop_duplicates()
dim_club = club_country_df.reset_index(drop=True)
dim_club['club_id'] = range(1, len(dim_club) + 1)

# Create club map
club_map = dict(zip(zip(dim_club['club_name'], dim_club['country_id']), dim_club['club_id']))

# Add club ids to big_df
big_df['home_club_key'] = list(zip(big_df['home_team'], big_df['country_id']))
big_df['away_club_key'] = list(zip(big_df['away_team'], big_df['country_id']))
big_df['home_club_id'] = big_df['home_club_key'].map(club_map)
big_df['away_club_id'] = big_df['away_club_key'].map(club_map)

# Create fact_result
# Include all original columns plus the new ids, exclude redundant ones
exclude_cols = {'country', 'season', 'Home', 'Away', 'HomeTeam', 'AwayTeam', 'HG', 'AG', 'Res', 'FTHG', 'FTAG', 'FTR', 'home_team', 'away_team', 'home_club_key', 'away_club_key'}
fact_columns = [col for col in big_df.columns if col not in exclude_cols]
fact_result = big_df[fact_columns].copy()
fact_result['result_id'] = range(1, len(fact_result) + 1)

# Rename columns for verbosity
fact_result = fact_result.rename(columns={
    'Country': 'country_name',
    'League': 'league_code',
    'league': 'league_code_duplicate',  # in case
    'date': 'match_date',
    'time': 'match_time',
    'result': 'match_result',
    'home_goals': 'home_team_goals',
    'away_goals': 'away_team_goals'
})

# Use DuckDB to create the data model
con = duckdb.connect(':memory:')

# Register dataframes as tables
con.register('dim_country', dim_country)
con.register('dim_season', dim_season)
con.register('dim_club', dim_club)
con.register('fact_result', fact_result)

# Print schemas
print("\n=== DIM_COUNTRY ===")
print(con.execute("DESCRIBE dim_country").fetchall())
print(dim_country.head())

print("\n=== DIM_SEASON ===")
print(con.execute("DESCRIBE dim_season").fetchall())
print(dim_season.head())

print("\n=== DIM_CLUB ===")
print(con.execute("DESCRIBE dim_club").fetchall())
print(dim_club.head())

print("\n=== FACT_RESULT ===")
print(con.execute("DESCRIBE fact_result").fetchall())
print(fact_result.head())

# Optionally, save to CSV files
output_dir = Path('output')
output_dir.mkdir(exist_ok=True)
dim_country.to_csv(output_dir / 'dim_country.csv', index=False)
dim_season.to_csv(output_dir / 'dim_season.csv', index=False)
dim_club.to_csv(output_dir / 'dim_club.csv', index=False)

# Create and save simplified fact_result
fact_result_simple = fact_result[['result_id', 'country_id', 'season_id', 'home_club_id', 'away_club_id', 'country_name', 'league_code', 'match_date', 'match_time', 'match_result', 'home_team_goals', 'away_team_goals']]
fact_result_simple.to_csv(output_dir / 'fact_result_simple.csv', index=False)

fact_result.to_csv(output_dir / 'fact_result.csv', index=False)

print(f"\nData model created and saved to {output_dir}")