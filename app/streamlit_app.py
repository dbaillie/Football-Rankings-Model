import streamlit as st
import pandas as pd
import os
from pathlib import Path

# Set page config
st.set_page_config(
    page_title="Football Rankings Dashboard",
    layout="wide"
)

# Title
st.title("⚽ Football Rankings Dashboard")
st.markdown("Display Glicko-2 ratings for football teams by country")

# Function to get available countries
def get_available_countries():
    """Get list of countries that have been processed"""
    # Use absolute path to ensure we're looking in the right place
    output_dir = Path(__file__).parent.parent / "output"
    countries = []

    if output_dir.exists():
        for item in output_dir.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                ratings_file = item / f"{item.name}_ratings.csv"
                if ratings_file.exists():
                    countries.append(item.name.title())

    return sorted(countries)

# Function to load ratings for a country
def load_country_ratings(country_name):
    """Load ratings data for a specific country"""
    # Use absolute path
    output_dir = Path(__file__).parent.parent / "output"
    country_lower = country_name.lower()
    ratings_file = output_dir / country_lower / f"{country_lower}_ratings.csv"

    if not ratings_file.exists():
        st.error(f"No ratings data found for {country_name}")
        return None

    try:
        df = pd.read_csv(ratings_file)
        # Sort by rating descending
        df = df.sort_values('rating', ascending=False).reset_index(drop=True)
        return df
    except Exception as e:
        st.error(f"Error loading data for {country_name}: {e}")
        return None


# Function to load weekly ratings for a country
def load_country_weekly_ratings(country_name):
    """Load weekly ratings data for a specific country"""
    output_dir = Path(__file__).parent.parent / "output"
    country_lower = country_name.lower()
    weekly_file = output_dir / country_lower / f"{country_lower}_weekly_ratings.csv"

    if not weekly_file.exists():
        st.warning(f"Weekly ratings file not found for {country_name}. Run run_glicko_country.py again to generate weekly snapshots.")
        return None

    try:
        df = pd.read_csv(weekly_file)
        return df
    except Exception as e:
        st.error(f"Error loading weekly data for {country_name}: {e}")
        return None

# Sidebar for country selection
st.sidebar.header("Select Country")
available_countries = get_available_countries()

if not available_countries:
    st.error("No processed country data found. Please run the Glicko calculations first.")
    st.stop()

selected_country = st.sidebar.selectbox(
    "Choose a country:",
    available_countries,
    index=0 if available_countries else None
)

# Main content
if selected_country:
    st.header(f"{selected_country} Team Ratings")

    # Load data
    ratings_df = load_country_ratings(selected_country)

    if ratings_df is not None:
        # Display key metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Teams", len(ratings_df))

        with col2:
            top_rating = ratings_df['rating'].max()
            st.metric("Highest Rating", f"{top_rating:.1f}")

        with col3:
            avg_rating = ratings_df['rating'].mean()
            st.metric("Average Rating", f"{avg_rating:.1f}")

        # Display ratings table
        st.subheader("Current Ratings")

        # Format the dataframe for display
        display_df = ratings_df[['team_name', 'rating', 'rd', 'sigma']].copy()
        display_df.columns = ['Team', 'Rating', 'Rating Deviation', 'Volatility']
        display_df['Rating'] = display_df['Rating'].round(1)
        display_df['Rating Deviation'] = display_df['Rating Deviation'].round(1)
        display_df['Volatility'] = display_df['Volatility'].round(4)

        st.dataframe(display_df, use_container_width=True)

        # Rating movement chart
        st.subheader("Ratings Over Time")
        weekly_df = load_country_weekly_ratings(selected_country)

        if weekly_df is not None and not weekly_df.empty:
            # Convert YYYYWW to a date at the start of the ISO week
            weekly_df['week'] = weekly_df['week'].astype(int)
            weekly_df['week_str'] = weekly_df['week'].astype(str).str.zfill(6)
            weekly_df['week_date'] = pd.to_datetime(
                weekly_df['week_str'] + '1',
                format='%G%V%u',
                errors='coerce'
            )

            teams = weekly_df['team_name'].unique().tolist()
            top_teams = ratings_df.head(10)['team_name'].tolist()
            default_teams = [team for team in top_teams if team in teams][:10]

            selected_teams = st.multiselect(
                "Select teams to display:",
                teams,
                default=default_teams
            )

            if not selected_teams:
                selected_teams = default_teams or teams[:10]

            chart_df = weekly_df[weekly_df['team_name'].isin(selected_teams)].copy()

            import plotly.express as px

            fig = px.line(
                chart_df,
                x='week_date',
                y='rating',
                color='team_name',
                markers=True,
                title=f"Rating Movement Over Time in {selected_country}",
                labels={
                    'week_date': 'Date',
                    'rating': 'Glicko-2 Rating',
                    'team_name': 'Team'
                }
            )
            fig.update_layout(legend_title_text='Team')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Weekly ratings are not available yet. Run the country data generation again to create weekly snapshots.")

        # Additional info
        st.subheader("Data Info")
        latest_week = ratings_df['week'].max()
        st.info(f"Data as of week {latest_week}")

        # Raw data (collapsible)
        with st.expander("View Raw Data"):
            st.dataframe(ratings_df)

# Footer
st.markdown("---")
st.markdown("Built with Streamlit | Glicko-2 Rating System")
st.markdown("© 2026 Douglas Baillie. Feedback: [douglasbaillie@live.co.uk](mailto:douglasbaillie@live.co.uk)")