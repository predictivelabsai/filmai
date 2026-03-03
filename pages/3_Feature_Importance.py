import streamlit as st
import os
from datetime import datetime
from dotenv import load_dotenv
import requests
import pandas as pd
import numpy as np
import plotly.express as px
from utils.regression_util import run_random_forest_importance

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Feature Importance - Film AI",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Box Office Feature Importance")
st.markdown("Fetch real data from TMDb/OMDb, run a RandomForest regression, and analyze feature importances.")

# Ensure state
if 'movies_df' not in st.session_state:
    st.session_state.movies_df = None
if 'feature_importances_' not in st.session_state:
    st.session_state.feature_importances_ = None
if 'feature_columns_' not in st.session_state:
    st.session_state.feature_columns_ = None

# Sidebar - Data sources and filters
with st.sidebar:
    st.markdown("### 🔌 Data Sources")
    use_tmdb = st.checkbox("TMDb (The Movie Database)", value=True)
    use_omdb = st.checkbox("OMDb (Open Movie Database)", value=True)

    tmdb_key_present = bool(os.getenv("TMDB_API_KEY"))
    omdb_key_present = bool(os.getenv("OMDB_API_KEY"))
    st.caption(f"TMDb key: {'✅' if tmdb_key_present else '❌'} | OMDb key: {'✅' if omdb_key_present else '❌'}")

    st.markdown("---")
    st.markdown("### 🔎 Filters")
    # Basic presets; dynamic genre list will be loaded once we have TMDb genres
    selected_genres = st.multiselect("Genres (filter after fetching)", [], help="Will populate after fetching genres")
    selected_regions = st.multiselect("Regions (ISO-3166 Country Code)", ["US", "GB", "FR", "DE", "IN", "JP", "CN"], default=["US"])
    year_min, year_max = st.slider("Release Year Range", min_value=1980, max_value=datetime.now().year, value=(2000, datetime.now().year), step=1)
    pages_to_fetch = st.slider("Pages to Fetch (TMDb Discover)", min_value=1, max_value=5, value=2, help="Each page ~20 movies")
    min_vote_count = st.number_input("Min TMDb vote_count", min_value=0, value=100, step=50)

    st.markdown("---")
    # Model settings are intentionally hidden for simplicity; defaults are used in the backend utility.

@st.cache_data(show_spinner=False)
def fetch_tmdb_genres(api_key: str) -> dict:
    url = f"https://api.themoviedb.org/3/genre/movie/list"
    params = {"api_key": api_key}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json().get("genres", [])
    return {g["id"]: g["name"] for g in data}

@st.cache_data(show_spinner=False)
def tmdb_discover_page(api_key: str, page: int, year_min: int, year_max: int, min_votes: int) -> list:
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": api_key,
        "page": page,
        "sort_by": "popularity.desc",
        "vote_count.gte": min_votes,
        "primary_release_date.gte": f"{year_min}-01-01",
        "primary_release_date.lte": f"{year_max}-12-31",
        "include_adult": "false",
        "include_video": "false",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

@st.cache_data(show_spinner=False)
def tmdb_movie_details(api_key: str, movie_id: int) -> dict:
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": api_key, "append_to_response": "credits,external_ids"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(show_spinner=False)
def omdb_by_imdb(api_key: str, imdb_id: str) -> dict:
    url = "http://www.omdbapi.com/"
    params = {"apikey": api_key, "i": imdb_id}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_box_office(value: str) -> float:
    if not value or value == "N/A":
        return np.nan
    digits = "".join(ch for ch in value if ch.isdigit())
    try:
        return float(digits)
    except Exception:
        return np.nan

def extract_features(row: dict, genre_map: dict) -> dict:
    genres = [genre_map.get(gid, str(gid)) for gid in row.get("genre_ids", [])] if "genre_ids" in row else [g.get("name") for g in row.get("genres", [])]
    title = row.get("title") or row.get("original_title") or ""
    original_language = row.get("original_language", "unk")
    popularity = row.get("popularity", np.nan)
    vote_average = row.get("vote_average", np.nan)
    vote_count = row.get("vote_count", np.nan)
    release_date = row.get("release_date") or row.get("release_date", "")
    release_year = int(release_date.split("-")[0]) if release_date and "-" in release_date else np.nan
    runtime = row.get("runtime", np.nan)
    budget = row.get("budget", np.nan)
    # primary region from production_countries
    prod_countries = row.get("production_countries", [])
    primary_region = prod_countries[0]["iso_3166_1"] if prod_countries else "UNK"
    # credits
    credits = row.get("credits", {})
    cast = credits.get("cast", []) if credits else []
    crew = credits.get("crew", []) if credits else []
    cast_pop = sum([c.get("popularity", 0) or 0 for c in cast[:3]])
    director_pop = 0.0
    for c in crew:
        if c.get("job") == "Director":
            director_pop = c.get("popularity", 0) or 0
            break
    # genres list to string list
    return {
        "title": title,
        "genres": genres,
        "primary_genre": genres[0] if genres else "Unknown",
        "original_language": original_language,
        "popularity": popularity,
        "vote_average": vote_average,
        "vote_count": vote_count,
        "release_year": release_year,
        "runtime": runtime,
        "budget": budget,
        "region": primary_region,
        "cast_popularity_top3": cast_pop,
        "director_popularity": director_pop,
    }

st.markdown("---")
st.markdown("## 1) 📥 Get Data")
col_fetch, col_info = st.columns([2, 1])
with col_fetch:
    if st.button("🔄 Fetch Movies", type="primary", use_container_width=True, disabled=not (use_tmdb and tmdb_key_present)):
        try:
            with st.spinner("Fetching genres..."):
                genre_map = fetch_tmdb_genres(os.getenv("TMDB_API_KEY"))
            # Update sidebar genres options
            st.session_state['available_genres_list'] = sorted(set(genre_map.values()))
            # Discover movies
            movies = []
            with st.spinner("Discovering movies from TMDb..."):
                for page in range(1, pages_to_fetch + 1):
                    movies.extend(tmdb_discover_page(os.getenv("TMDB_API_KEY"), page, year_min, year_max, min_vote_count))
            # Enrich each movie
            rows = []
            missing_omdb = 0
            with st.spinner("Loading details and OMDb box office..."):
                for m in movies:
                    try:
                        details = tmdb_movie_details(os.getenv("TMDB_API_KEY"), m["id"])
                        feat = extract_features(details, genre_map)
                        imdb_id = (details.get("external_ids") or {}).get("imdb_id")
                        tmdb_revenue = details.get("revenue") or np.nan
                        box_office = np.nan
                        if use_omdb and omdb_key_present and imdb_id:
                            om = omdb_by_imdb(os.getenv("OMDB_API_KEY"), imdb_id)
                            box_office = parse_box_office(om.get("BoxOffice"))
                        if np.isnan(box_office) and isinstance(tmdb_revenue, (int, float)) and tmdb_revenue > 0:
                            box_office = float(tmdb_revenue)
                        row = {
                            **feat,
                            "tmdb_id": m["id"],
                            "imdb_id": imdb_id,
                            "box_office": box_office
                        }
                        rows.append(row)
                    except Exception:
                        missing_omdb += 1
                        continue
            df = pd.DataFrame(rows)
            # Apply filters post-hoc
            if selected_regions:
                df = df[df["region"].isin(selected_regions)]
            if selected_genres:
                df = df[df["genres"].apply(lambda gs: any(g in selected_genres for g in (gs or [])))]
            # Drop rows without label
            df = df.dropna(subset=["box_office"])
            st.session_state.movies_df = df
            st.success(f"✅ Fetched {len(df)} movies with box office data. Missing OMDb: {missing_omdb}")
        except Exception as e:
            st.error(f"❌ Fetch error: {str(e)}")
with col_info:
    if 'available_genres_list' in st.session_state:
        st.markdown("#### Available Genres")
        st.write(", ".join(st.session_state['available_genres_list'][:20]) + (" ..." if len(st.session_state['available_genres_list']) > 20 else ""))

if st.session_state.movies_df is not None and not st.session_state.movies_df.empty:
    st.dataframe(st.session_state.movies_df[["title", "region", "primary_genre", "release_year", "box_office", "vote_average", "vote_count"]].sort_values("box_office", ascending=False), use_container_width=True, height=300)
else:
    st.info("Fetch data to proceed.")

st.markdown("---")
st.markdown("## 2) 🧠 Run Analysis (RandomForest)")
if st.button("🚀 Train Model", type="primary", use_container_width=True, disabled=st.session_state.movies_df is None or st.session_state.movies_df.empty):
    df = st.session_state.movies_df.copy()
    try:
        with st.spinner("Training RandomForest..."):
            result = run_random_forest_importance(df)
        st.session_state.feature_importances_ = result["feature_importances"]
        st.session_state.feature_columns_ = result["feature_columns"]
        st.success("✅ Model trained. Feature importances computed.")
        st.markdown(f"R² (train): {result['r2_train']:.3f} — R² (test): {result['r2_test']:.3f}")
    except Exception as e:
        st.error(f"❌ Training error: {str(e)}")

st.markdown("---")
st.markdown("## 3) 🔍 Feature Importance")
if st.session_state.feature_importances_ is not None:
    top_k = st.slider("Top K features", min_value=5, max_value=30, value=15)
    top_feats = st.session_state.feature_importances_.head(top_k).iloc[::-1]
    fig = px.bar(
        x=top_feats.values,
        y=top_feats.index,
        orientation="h",
        labels={"x": "Importance", "y": "Feature"},
        title="Top Feature Importances"
    )
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("📘 Methodology: How to interpret feature importance"):
        st.markdown("""
        - **Model**: We train a RandomForestRegressor on engineered features:
          - Numeric: runtime, vote_average, vote_count, popularity, budget, release_year, cast_popularity_top3, director_popularity
          - Categorical: region, original_language, primary_genre (one‑hot encoded)
          - Genres: multi‑hot columns per genre (e.g., `genre_Action`)
        - **Importance Metric**: Bars show impurity‑based feature importance (mean decrease in variance) aggregated across trees.
        - **Interpretation**:
          - Higher bars suggest stronger association with box office variance in this dataset.
          - Importances are relative and do not imply causation.
          - Groups of correlated features can split importance among themselves.
        - **Caveats**:
          - One‑hot and multi‑hot features appear as separate columns; consider summing related columns (e.g., all `genre_*`) for a category‑level view.
          - If many features are highly correlated (e.g., popularity and votes), importances may be diluted across them.
          - Impurity importances can be biased toward high‑cardinality features; consider permutation importance for confirmatory analysis.
        - **Recommendations**:
          - Use this graph to shortlist drivers, then validate with permutation tests or simpler models.
          - Inspect top features and cross‑check in the treemap to see how effects aggregate across Region → Genre → Title.
        """)
else:
    st.info("Train the model to view feature importances.")

st.markdown("---")
st.markdown("## 🌳 Aggregate Treemap (Region → Genre → Title)")
if st.session_state.movies_df is not None and not st.session_state.movies_df.empty:
    dft = st.session_state.movies_df.copy()
    dft["box_office_millions"] = dft["box_office"] / 1_000_000.0
    fig = px.treemap(
        dft,
        path=["region", "primary_genre", "title"],
        values="box_office_millions",
        color="box_office_millions",
        color_continuous_scale="Blues",
        title="Box Office Aggregation (Millions USD)"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Fetch data to render treemap.")


