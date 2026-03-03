import streamlit as st
import os
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from tmdbv3api import TMDb, Movie, Person, Genre
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="API Exploration - Film AI",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 API Exploration")
st.markdown("Explore and query OMDB and TMDB databases with advanced filtering, visualization, and data analysis.")

# Initialize API keys
tmdb_key = os.getenv("TMDB_API_KEY")
omdb_key = os.getenv("OMDB_API_KEY")

# Sidebar for API status
with st.sidebar:
    st.markdown("### 🔌 API Status")
    
    tmdb_status = "✅" if tmdb_key else "❌"
    omdb_status = "✅" if omdb_key else "❌"
    
    st.markdown(f"{tmdb_status} **TMDB**")
    st.markdown(f"{omdb_status} **OMDB**")
    
    if not tmdb_key and not omdb_key:
        st.error("⚠️ No API keys configured. Please set TMDB_API_KEY and/or OMDB_API_KEY in your .env file.")
    
    st.markdown("---")
    st.markdown("### 📊 View Options")
    show_json = st.checkbox("Show Raw JSON", value=False)
    show_table = st.checkbox("Show Table View", value=True)
    show_visualizations = st.checkbox("Show Visualizations", value=True)

# Main tabs
tab1, tab2, tab3 = st.tabs(["🎬 TMDB Explorer", "🎥 OMDB Explorer", "📊 Combined Analysis"])

# ==================== TMDB EXPLORER ====================
with tab1:
    if not tmdb_key:
        st.error("❌ TMDB API key not configured. Please set TMDB_API_KEY in your .env file.")
    else:
        st.markdown("### 🎬 TMDB Database Explorer")
        
        # Query type selection
        query_type = st.selectbox(
            "Select Query Type",
            [
                "Search Movies",
                "Search Actors",
                "Discover Movies (Advanced Filters)",
                "Popular Movies",
                "Top Rated Movies",
                "Trending Movies",
                "Movie Details by ID",
                "Actor Details by ID",
                "Movies by Genre"
            ],
            key="tmdb_query_type"
        )
        
        # Initialize TMDB
        tmdb = TMDb()
        tmdb.api_key = tmdb_key
        tmdb.language = 'en'
        
        results_data = None
        results_df = None
        
        # Search Movies
        if query_type == "Search Movies":
            col1, col2 = st.columns([3, 1])
            with col1:
                search_query = st.text_input("Movie Title", placeholder="e.g., Inception", key="tmdb_movie_search")
            with col2:
                max_results = st.number_input("Max Results", min_value=1, max_value=20, value=10, key="tmdb_movie_max")
            
            if st.button("🔍 Search Movies", key="tmdb_search_movies"):
                if search_query:
                    try:
                        with st.spinner("Searching TMDB..."):
                            movie_api = Movie()
                            results = movie_api.search(search_query)
                            
                            if results:
                                results_list = []
                                results_to_store = []
                                for idx, movie in enumerate(results):
                                    if idx >= max_results:
                                        break
                                    try:
                                        movie_dict = {
                                            "ID": movie.id,
                                            "Title": getattr(movie, 'title', 'N/A'),
                                            "Release Date": getattr(movie, 'release_date', 'N/A'),
                                            "Rating": getattr(movie, 'vote_average', 0),
                                            "Vote Count": getattr(movie, 'vote_count', 0),
                                            "Popularity": getattr(movie, 'popularity', 0),
                                            "Overview": (getattr(movie, 'overview', '') or '')[:100] + "..." if getattr(movie, 'overview', '') else "N/A"
                                        }
                                        results_list.append(movie_dict)
                                        results_to_store.append(movie)
                                    except Exception as e:
                                        st.warning(f"Error processing movie {idx}: {str(e)}")
                                        continue
                                
                                if results_list:
                                    results_data = results_to_store
                                    results_df = pd.DataFrame(results_list)
                                    st.session_state.tmdb_results = results_data
                                    st.session_state.tmdb_df = results_df
                                else:
                                    st.warning("No valid results found.")
                            else:
                                st.warning("No results found.")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())
                else:
                    st.warning("Please enter a movie title.")
        
        # Search Actors
        elif query_type == "Search Actors":
            col1, col2 = st.columns([3, 1])
            with col1:
                search_query = st.text_input("Actor Name", placeholder="e.g., Leonardo DiCaprio", key="tmdb_actor_search")
            with col2:
                max_results = st.number_input("Max Results", min_value=1, max_value=20, value=10, key="tmdb_actor_max")
            
            if st.button("🔍 Search Actors", key="tmdb_search_actors"):
                if search_query:
                    try:
                        with st.spinner("Searching TMDB..."):
                            person_api = Person()
                            results = person_api.search(search_query)
                            
                            if results:
                                results_list = []
                                results_to_store = []
                                for idx, person in enumerate(results):
                                    if idx >= max_results:
                                        break
                                    try:
                                        # Safely get known_for
                                        known_for_str = "N/A"
                                        if hasattr(person, 'known_for') and person.known_for:
                                            try:
                                                known_for_list = list(person.known_for) if person.known_for else []
                                                known_for_items = []
                                                for item in known_for_list[:3]:
                                                    if isinstance(item, dict):
                                                        title = item.get('title') or item.get('name', 'N/A')
                                                    else:
                                                        title = getattr(item, 'title', getattr(item, 'name', 'N/A'))
                                                    known_for_items.append(str(title))
                                                known_for_str = ", ".join(known_for_items) if known_for_items else "N/A"
                                            except:
                                                known_for_str = "N/A"
                                        
                                        person_dict = {
                                            "ID": person.id,
                                            "Name": getattr(person, 'name', 'N/A'),
                                            "Popularity": getattr(person, 'popularity', 0),
                                            "Known For": known_for_str
                                        }
                                        results_list.append(person_dict)
                                        results_to_store.append(person)
                                    except Exception as e:
                                        st.warning(f"Error processing person {idx}: {str(e)}")
                                        continue
                                
                                if results_list:
                                    results_data = results_to_store
                                    results_df = pd.DataFrame(results_list)
                                    st.session_state.tmdb_results = results_data
                                    st.session_state.tmdb_df = results_df
                                else:
                                    st.warning("No valid results found.")
                            else:
                                st.warning("No results found.")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())
                else:
                    st.warning("Please enter an actor name.")
        
        # Discover Movies with Advanced Filters
        elif query_type == "Discover Movies (Advanced Filters)":
            col1, col2, col3 = st.columns(3)
            
            with col1:
                year_min = st.number_input("Year From", min_value=1900, max_value=datetime.now().year, value=2000, key="discover_year_min")
                year_max = st.number_input("Year To", min_value=1900, max_value=datetime.now().year, value=datetime.now().year, key="discover_year_max")
                min_rating = st.slider("Min Rating", min_value=0.0, max_value=10.0, value=0.0, step=0.1, key="discover_min_rating")
            
            with col2:
                min_vote_count = st.number_input("Min Vote Count", min_value=0, value=100, key="discover_min_votes")
                sort_by = st.selectbox(
                    "Sort By",
                    ["popularity.desc", "popularity.asc", "release_date.desc", "release_date.asc", "vote_average.desc", "vote_average.asc"],
                    key="discover_sort"
                )
                max_results = st.number_input("Max Results", min_value=1, max_value=100, value=20, key="discover_max")
            
            with col3:
                # Fetch genres
                try:
                    genre_api = Genre()
                    genres_list = genre_api.movie_list()
                    genre_dict = {g.name: g.id for g in genres_list}
                    selected_genres = st.multiselect("Genres", list(genre_dict.keys()), key="discover_genres")
                    genre_ids = [genre_dict[g] for g in selected_genres] if selected_genres else None
                except:
                    genre_ids = None
                    st.info("Could not load genres")
            
            if st.button("🔍 Discover Movies", key="tmdb_discover"):
                try:
                    with st.spinner("Discovering movies..."):
                        url = "https://api.themoviedb.org/3/discover/movie"
                        params = {
                            "api_key": tmdb_key,
                            "primary_release_date.gte": f"{year_min}-01-01",
                            "primary_release_date.lte": f"{year_max}-12-31",
                            "vote_average.gte": min_rating,
                            "vote_count.gte": min_vote_count,
                            "sort_by": sort_by,
                            "page": 1
                        }
                        
                        if genre_ids:
                            params["with_genres"] = ",".join(map(str, genre_ids))
                        
                        response = requests.get(url, params=params, timeout=30)
                        response.raise_for_status()
                        data = response.json()
                        
                        movies = data.get("results", [])[:max_results]
                        
                        if movies:
                            results_list = []
                            for movie in movies:
                                results_list.append({
                                    "ID": movie.get("id"),
                                    "Title": movie.get("title"),
                                    "Release Date": movie.get("release_date", "N/A"),
                                    "Rating": movie.get("vote_average", 0),
                                    "Vote Count": movie.get("vote_count", 0),
                                    "Popularity": movie.get("popularity", 0),
                                    "Overview": movie.get("overview", "N/A")[:100] + "..." if movie.get("overview") else "N/A"
                                })
                            
                            results_data = movies
                            results_df = pd.DataFrame(results_list)
                            st.session_state.tmdb_results = results_data
                            st.session_state.tmdb_df = results_df
                        else:
                            st.warning("No movies found with these filters.")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # Popular Movies
        elif query_type == "Popular Movies":
            max_results = st.number_input("Number of Results", min_value=1, max_value=50, value=20, key="popular_max")
            
            if st.button("📊 Get Popular Movies", key="tmdb_popular"):
                try:
                    with st.spinner("Fetching popular movies..."):
                        movie = Movie()
                        results = movie.popular()
                        
                        if results:
                            results_list = []
                            for m in results[:max_results]:
                                results_list.append({
                                    "ID": m.id,
                                    "Title": m.title,
                                    "Release Date": m.release_date if hasattr(m, 'release_date') else "N/A",
                                    "Rating": m.vote_average if hasattr(m, 'vote_average') else 0,
                                    "Vote Count": m.vote_count if hasattr(m, 'vote_count') else 0,
                                    "Popularity": m.popularity if hasattr(m, 'popularity') else 0,
                                    "Overview": m.overview[:100] + "..." if hasattr(m, 'overview') and m.overview else "N/A"
                                })
                            
                            results_data = results[:max_results]
                            results_df = pd.DataFrame(results_list)
                            st.session_state.tmdb_results = results_data
                            st.session_state.tmdb_df = results_df
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # Top Rated Movies
        elif query_type == "Top Rated Movies":
            max_results = st.number_input("Number of Results", min_value=1, max_value=50, value=20, key="toprated_max")
            
            if st.button("🏆 Get Top Rated Movies", key="tmdb_toprated"):
                try:
                    with st.spinner("Fetching top rated movies..."):
                        movie = Movie()
                        results = movie.top_rated()
                        
                        if results:
                            results_list = []
                            for m in results[:max_results]:
                                results_list.append({
                                    "ID": m.id,
                                    "Title": m.title,
                                    "Release Date": m.release_date if hasattr(m, 'release_date') else "N/A",
                                    "Rating": m.vote_average if hasattr(m, 'vote_average') else 0,
                                    "Vote Count": m.vote_count if hasattr(m, 'vote_count') else 0,
                                    "Popularity": m.popularity if hasattr(m, 'popularity') else 0,
                                    "Overview": m.overview[:100] + "..." if hasattr(m, 'overview') and m.overview else "N/A"
                                })
                            
                            results_data = results[:max_results]
                            results_df = pd.DataFrame(results_list)
                            st.session_state.tmdb_results = results_data
                            st.session_state.tmdb_df = results_df
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # Trending Movies
        elif query_type == "Trending Movies":
            time_window = st.selectbox("Time Window", ["day", "week"], key="trending_window")
            max_results = st.number_input("Number of Results", min_value=1, max_value=50, value=20, key="trending_max")
            
            if st.button("🔥 Get Trending Movies", key="tmdb_trending"):
                try:
                    with st.spinner("Fetching trending movies..."):
                        url = f"https://api.themoviedb.org/3/trending/movie/{time_window}"
                        params = {"api_key": tmdb_key}
                        response = requests.get(url, params=params, timeout=30)
                        response.raise_for_status()
                        data = response.json()
                        
                        movies = data.get("results", [])[:max_results]
                        
                        if movies:
                            results_list = []
                            for movie in movies:
                                results_list.append({
                                    "ID": movie.get("id"),
                                    "Title": movie.get("title"),
                                    "Release Date": movie.get("release_date", "N/A"),
                                    "Rating": movie.get("vote_average", 0),
                                    "Vote Count": movie.get("vote_count", 0),
                                    "Popularity": movie.get("popularity", 0),
                                    "Overview": movie.get("overview", "N/A")[:100] + "..." if movie.get("overview") else "N/A"
                                })
                            
                            results_data = movies
                            results_df = pd.DataFrame(results_list)
                            st.session_state.tmdb_results = results_data
                            st.session_state.tmdb_df = results_df
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # Movie Details by ID
        elif query_type == "Movie Details by ID":
            movie_id = st.number_input("Movie ID", min_value=1, value=550, key="tmdb_movie_id")
            
            if st.button("📋 Get Movie Details", key="tmdb_movie_details"):
                try:
                    with st.spinner("Fetching movie details..."):
                        movie = Movie()
                        details = movie.details(movie_id)
                        
                        if details:
                            results_data = details
                            # Convert to dict for display
                            movie_dict = {
                                "ID": details.id,
                                "Title": details.title,
                                "Release Date": details.release_date if hasattr(details, 'release_date') else "N/A",
                                "Rating": details.vote_average if hasattr(details, 'vote_average') else 0,
                                "Vote Count": details.vote_count if hasattr(details, 'vote_count') else 0,
                                "Popularity": details.popularity if hasattr(details, 'popularity') else 0,
                                "Budget": f"${details.budget:,}" if hasattr(details, 'budget') and details.budget else "N/A",
                                "Revenue": f"${details.revenue:,}" if hasattr(details, 'revenue') and details.revenue else "N/A",
                                "Runtime": f"{details.runtime} min" if hasattr(details, 'runtime') and details.runtime else "N/A",
                                "Overview": details.overview if hasattr(details, 'overview') else "N/A"
                            }
                            results_df = pd.DataFrame([movie_dict])
                            st.session_state.tmdb_results = details
                            st.session_state.tmdb_df = results_df
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # Actor Details by ID
        elif query_type == "Actor Details by ID":
            actor_id = st.number_input("Actor ID", min_value=1, value=6193, key="tmdb_actor_id")
            
            if st.button("👤 Get Actor Details", key="tmdb_actor_details"):
                try:
                    with st.spinner("Fetching actor details..."):
                        person = Person()
                        details = person.details(actor_id)
                        
                        if details:
                            results_data = details
                            person_dict = {
                                "ID": details.id,
                                "Name": details.name,
                                "Popularity": details.popularity if hasattr(details, 'popularity') else 0,
                                "Birthday": details.birthday if hasattr(details, 'birthday') else "N/A",
                                "Place of Birth": details.place_of_birth if hasattr(details, 'place_of_birth') else "N/A",
                                "Biography": details.biography[:200] + "..." if hasattr(details, 'biography') and details.biography else "N/A"
                            }
                            results_df = pd.DataFrame([person_dict])
                            st.session_state.tmdb_results = details
                            st.session_state.tmdb_df = results_df
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # Movies by Genre
        elif query_type == "Movies by Genre":
            try:
                genre_api = Genre()
                genres_list = genre_api.movie_list()
                genre_dict = {g.name: g.id for g in genres_list}
                selected_genre = st.selectbox("Select Genre", list(genre_dict.keys()), key="genre_select")
                max_results = st.number_input("Number of Results", min_value=1, max_value=50, value=20, key="genre_max")
                
                if st.button("🎭 Get Movies by Genre", key="tmdb_genre"):
                    try:
                        with st.spinner("Fetching movies..."):
                            url = "https://api.themoviedb.org/3/discover/movie"
                            params = {
                                "api_key": tmdb_key,
                                "with_genres": genre_dict[selected_genre],
                                "sort_by": "popularity.desc",
                                "page": 1
                            }
                            response = requests.get(url, params=params, timeout=30)
                            response.raise_for_status()
                            data = response.json()
                            
                            movies = data.get("results", [])[:max_results]
                            
                            if movies:
                                results_list = []
                                for movie in movies:
                                    results_list.append({
                                        "ID": movie.get("id"),
                                        "Title": movie.get("title"),
                                        "Release Date": movie.get("release_date", "N/A"),
                                        "Rating": movie.get("vote_average", 0),
                                        "Vote Count": movie.get("vote_count", 0),
                                        "Popularity": movie.get("popularity", 0),
                                        "Overview": movie.get("overview", "N/A")[:100] + "..." if movie.get("overview") else "N/A"
                                    })
                                
                                results_data = movies
                                results_df = pd.DataFrame(results_list)
                                st.session_state.tmdb_results = results_data
                                st.session_state.tmdb_df = results_df
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            except Exception as e:
                st.error(f"Error loading genres: {str(e)}")
        
        # Display Results
        if 'tmdb_results' in st.session_state and st.session_state.tmdb_results:
            st.markdown("---")
            st.markdown("### 📊 Results")
            
            # JSON View
            if show_json:
                with st.expander("📄 Raw JSON Data", expanded=False):
                    # Convert to serializable format
                    results = st.session_state.tmdb_results
                    
                    # Handle single object vs list
                    if not isinstance(results, list):
                        results = [results]
                    
                    json_data = []
                    for item in results:
                        try:
                            if hasattr(item, '__dict__'):
                                # Convert object to dict, filtering out private attributes
                                item_dict = {}
                                for k, v in item.__dict__.items():
                                    if not k.startswith('_'):
                                        # Convert nested objects/dicts
                                        if hasattr(v, '__dict__'):
                                            item_dict[k] = {k2: v2 for k2, v2 in v.__dict__.items() if not k2.startswith('_')}
                                        elif isinstance(v, (list, tuple)):
                                            item_dict[k] = [str(x) if not isinstance(x, (str, int, float, bool, type(None))) else x for x in v]
                                        else:
                                            item_dict[k] = v
                                json_data.append(item_dict)
                            elif isinstance(item, dict):
                                json_data.append(item)
                            else:
                                json_data.append(str(item))
                        except Exception as e:
                            json_data.append({"error": f"Could not serialize: {str(e)}", "item": str(item)})
                    
                    # Display JSON
                    if len(json_data) == 1:
                        st.json(json_data[0])
                    elif len(json_data) > 1:
                        st.json(json_data)
                    else:
                        st.json({})
            
            # Table View
            if show_table and 'tmdb_df' in st.session_state and st.session_state.tmdb_df is not None:
                st.markdown("#### 📋 Table View")
                st.dataframe(st.session_state.tmdb_df, use_container_width=True)
            
            # Visualizations
            if show_visualizations and 'tmdb_df' in st.session_state and st.session_state.tmdb_df is not None:
                st.markdown("#### 📈 Visualizations")
                
                df = st.session_state.tmdb_df
                
                # Rating Distribution
                if 'Rating' in df.columns:
                    try:
                        ratings = pd.to_numeric(df['Rating'], errors='coerce').dropna()
                        if len(ratings) > 0:
                            fig_ratings = px.histogram(
                                x=ratings,
                                nbins=20,
                                title="Rating Distribution",
                                labels={"x": "Rating", "y": "Count"}
                            )
                            st.plotly_chart(fig_ratings, use_container_width=True)
                    except:
                        pass
                
                # Popularity vs Rating Scatter
                if 'Rating' in df.columns and 'Popularity' in df.columns:
                    try:
                        df_viz = df.copy()
                        df_viz['Rating'] = pd.to_numeric(df_viz['Rating'], errors='coerce')
                        df_viz['Popularity'] = pd.to_numeric(df_viz['Popularity'], errors='coerce')
                        df_viz = df_viz.dropna(subset=['Rating', 'Popularity'])
                        
                        if len(df_viz) > 0:
                            fig_scatter = px.scatter(
                                df_viz,
                                x='Rating',
                                y='Popularity',
                                hover_data=['Title'] if 'Title' in df_viz.columns else None,
                                title="Popularity vs Rating",
                                labels={"Rating": "Rating", "Popularity": "Popularity"}
                            )
                            st.plotly_chart(fig_scatter, use_container_width=True)
                    except:
                        pass
                
                # Release Year Distribution
                if 'Release Date' in df.columns:
                    try:
                        df_viz = df.copy()
                        df_viz['Year'] = pd.to_datetime(df_viz['Release Date'], errors='coerce').dt.year
                        years = df_viz['Year'].dropna()
                        if len(years) > 0:
                            fig_years = px.histogram(
                                x=years,
                                nbins=30,
                                title="Release Year Distribution",
                                labels={"x": "Year", "y": "Count"}
                            )
                            st.plotly_chart(fig_years, use_container_width=True)
                    except:
                        pass

# ==================== OMDB EXPLORER ====================
with tab2:
    if not omdb_key:
        st.error("❌ OMDB API key not configured. Please set OMDB_API_KEY in your .env file.")
    else:
        st.markdown("### 🎥 OMDB Database Explorer")
        
        # Query type selection
        query_type = st.selectbox(
            "Select Query Type",
            [
                "Search by Title",
                "Search by IMDb ID",
                "Search with Year Filter"
            ],
            key="omdb_query_type"
        )
        
        results_data = None
        results_df = None
        
        # Search by Title
        if query_type == "Search by Title":
            search_title = st.text_input("Movie Title", placeholder="e.g., Inception", key="omdb_title")
            
            if st.button("🔍 Search OMDB", key="omdb_search_title"):
                if search_title:
                    try:
                        with st.spinner("Searching OMDB..."):
                            url = f"http://www.omdbapi.com/?apikey={omdb_key}&t={search_title}"
                            response = requests.get(url, timeout=15)
                            
                            if response.status_code == 200:
                                data = response.json()
                                
                                if data.get('Response') == 'True':
                                    results_data = data
                                    # Convert to DataFrame
                                    movie_dict = {
                                        "Title": data.get('Title', 'N/A'),
                                        "Year": data.get('Year', 'N/A'),
                                        "Rated": data.get('Rated', 'N/A'),
                                        "Released": data.get('Released', 'N/A'),
                                        "Runtime": data.get('Runtime', 'N/A'),
                                        "Genre": data.get('Genre', 'N/A'),
                                        "Director": data.get('Director', 'N/A'),
                                        "Actors": data.get('Actors', 'N/A'),
                                        "Plot": data.get('Plot', 'N/A'),
                                        "Language": data.get('Language', 'N/A'),
                                        "Country": data.get('Country', 'N/A'),
                                        "Awards": data.get('Awards', 'N/A'),
                                        "IMDb Rating": data.get('imdbRating', 'N/A'),
                                        "IMDb Votes": data.get('imdbVotes', 'N/A'),
                                        "Box Office": data.get('BoxOffice', 'N/A'),
                                        "Metascore": data.get('Metascore', 'N/A'),
                                        "Rotten Tomatoes": data.get('Ratings', [{}])[0].get('Value', 'N/A') if data.get('Ratings') else 'N/A'
                                    }
                                    results_df = pd.DataFrame([movie_dict])
                                    st.session_state.omdb_results = results_data
                                    st.session_state.omdb_df = results_df
                                else:
                                    st.error(f"Movie not found: {data.get('Error', 'Unknown error')}")
                            else:
                                st.error(f"HTTP Error: {response.status_code}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                else:
                    st.warning("Please enter a movie title.")
        
        # Search by IMDb ID
        elif query_type == "Search by IMDb ID":
            imdb_id = st.text_input("IMDb ID", placeholder="e.g., tt1375666", key="omdb_imdb_id")
            
            if st.button("🔍 Search by IMDb ID", key="omdb_search_imdb"):
                if imdb_id:
                    try:
                        with st.spinner("Searching OMDB..."):
                            url = f"http://www.omdbapi.com/?apikey={omdb_key}&i={imdb_id}"
                            response = requests.get(url, timeout=15)
                            
                            if response.status_code == 200:
                                data = response.json()
                                
                                if data.get('Response') == 'True':
                                    results_data = data
                                    movie_dict = {
                                        "Title": data.get('Title', 'N/A'),
                                        "Year": data.get('Year', 'N/A'),
                                        "Rated": data.get('Rated', 'N/A'),
                                        "Released": data.get('Released', 'N/A'),
                                        "Runtime": data.get('Runtime', 'N/A'),
                                        "Genre": data.get('Genre', 'N/A'),
                                        "Director": data.get('Director', 'N/A'),
                                        "Actors": data.get('Actors', 'N/A'),
                                        "Plot": data.get('Plot', 'N/A'),
                                        "Language": data.get('Language', 'N/A'),
                                        "Country": data.get('Country', 'N/A'),
                                        "Awards": data.get('Awards', 'N/A'),
                                        "IMDb Rating": data.get('imdbRating', 'N/A'),
                                        "IMDb Votes": data.get('imdbVotes', 'N/A'),
                                        "Box Office": data.get('BoxOffice', 'N/A'),
                                        "Metascore": data.get('Metascore', 'N/A')
                                    }
                                    results_df = pd.DataFrame([movie_dict])
                                    st.session_state.omdb_results = results_data
                                    st.session_state.omdb_df = results_df
                                else:
                                    st.error(f"Movie not found: {data.get('Error', 'Unknown error')}")
                            else:
                                st.error(f"HTTP Error: {response.status_code}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                else:
                    st.warning("Please enter an IMDb ID.")
        
        # Search with Year Filter
        elif query_type == "Search with Year Filter":
            col1, col2 = st.columns(2)
            with col1:
                search_title = st.text_input("Movie Title", placeholder="e.g., Batman", key="omdb_title_year")
            with col2:
                year = st.number_input("Year", min_value=1900, max_value=datetime.now().year, value=None, key="omdb_year")
            
            if st.button("🔍 Search with Year", key="omdb_search_year"):
                if search_title:
                    try:
                        with st.spinner("Searching OMDB..."):
                            url = f"http://www.omdbapi.com/?apikey={omdb_key}&t={search_title}"
                            if year:
                                url += f"&y={year}"
                            
                            response = requests.get(url, timeout=15)
                            
                            if response.status_code == 200:
                                data = response.json()
                                
                                if data.get('Response') == 'True':
                                    results_data = data
                                    movie_dict = {
                                        "Title": data.get('Title', 'N/A'),
                                        "Year": data.get('Year', 'N/A'),
                                        "Rated": data.get('Rated', 'N/A'),
                                        "Released": data.get('Released', 'N/A'),
                                        "Runtime": data.get('Runtime', 'N/A'),
                                        "Genre": data.get('Genre', 'N/A'),
                                        "Director": data.get('Director', 'N/A'),
                                        "Actors": data.get('Actors', 'N/A'),
                                        "Plot": data.get('Plot', 'N/A'),
                                        "Language": data.get('Language', 'N/A'),
                                        "Country": data.get('Country', 'N/A'),
                                        "Awards": data.get('Awards', 'N/A'),
                                        "IMDb Rating": data.get('imdbRating', 'N/A'),
                                        "IMDb Votes": data.get('imdbVotes', 'N/A'),
                                        "Box Office": data.get('BoxOffice', 'N/A'),
                                        "Metascore": data.get('Metascore', 'N/A')
                                    }
                                    results_df = pd.DataFrame([movie_dict])
                                    st.session_state.omdb_results = results_data
                                    st.session_state.omdb_df = results_df
                                else:
                                    st.error(f"Movie not found: {data.get('Error', 'Unknown error')}")
                            else:
                                st.error(f"HTTP Error: {response.status_code}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                else:
                    st.warning("Please enter a movie title.")
        
        # Display Results
        if 'omdb_results' in st.session_state and st.session_state.omdb_results:
            st.markdown("---")
            st.markdown("### 📊 Results")
            
            # Show poster if available
            if isinstance(st.session_state.omdb_results, dict) and st.session_state.omdb_results.get('Poster') and st.session_state.omdb_results.get('Poster') != 'N/A':
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.image(st.session_state.omdb_results.get('Poster'), use_container_width=True)
                with col2:
                    st.markdown(f"### {st.session_state.omdb_results.get('Title', 'N/A')} ({st.session_state.omdb_results.get('Year', 'N/A')})")
                    st.markdown(f"**Director:** {st.session_state.omdb_results.get('Director', 'N/A')}")
                    st.markdown(f"**Cast:** {st.session_state.omdb_results.get('Actors', 'N/A')}")
                    st.markdown(f"**Genre:** {st.session_state.omdb_results.get('Genre', 'N/A')}")
                    st.markdown(f"**IMDb Rating:** {st.session_state.omdb_results.get('imdbRating', 'N/A')}/10")
                    st.markdown(f"**Box Office:** {st.session_state.omdb_results.get('BoxOffice', 'N/A')}")
            
            # JSON View
            if show_json:
                with st.expander("📄 Raw JSON Data", expanded=False):
                    st.json(st.session_state.omdb_results)
            
            # Table View
            if show_table and 'omdb_df' in st.session_state and st.session_state.omdb_df is not None:
                st.markdown("#### 📋 Table View")
                st.dataframe(st.session_state.omdb_df, use_container_width=True)
            
            # Visualizations
            if show_visualizations and 'omdb_df' in st.session_state and st.session_state.omdb_df is not None:
                st.markdown("#### 📈 Visualizations")
                
                df = st.session_state.omdb_df
                
                # IMDb Rating visualization
                if 'IMDb Rating' in df.columns:
                    try:
                        rating_str = df['IMDb Rating'].iloc[0] if len(df) > 0 else None
                        if rating_str and rating_str != 'N/A':
                            rating = float(rating_str)
                            fig_rating = go.Figure(go.Indicator(
                                mode="gauge+number",
                                value=rating,
                                domain={'x': [0, 1], 'y': [0, 1]},
                                title={'text': "IMDb Rating"},
                                gauge={'axis': {'range': [None, 10]},
                                       'bar': {'color': "darkblue"},
                                       'steps': [
                                           {'range': [0, 5], 'color': "lightgray"},
                                           {'range': [5, 7], 'color': "gray"},
                                           {'range': [7, 10], 'color': "lightgreen"}],
                                       'threshold': {'line': {'color': "red", 'width': 4},
                                                     'thickness': 0.75, 'value': 8}}))
                            st.plotly_chart(fig_rating, use_container_width=True)
                    except:
                        pass
                
                # Box Office visualization
                if 'Box Office' in df.columns:
                    try:
                        box_office_str = df['Box Office'].iloc[0] if len(df) > 0 else None
                        if box_office_str and box_office_str != 'N/A':
                            # Parse box office (remove $ and commas)
                            box_office = float(box_office_str.replace('$', '').replace(',', ''))
                            fig_box = go.Figure(go.Bar(
                                x=['Box Office'],
                                y=[box_office],
                                text=[f"${box_office:,.0f}"],
                                textposition='auto',
                                marker_color='green'
                            ))
                            fig_box.update_layout(
                                title="Box Office Revenue",
                                yaxis_title="Revenue ($)",
                                showlegend=False
                            )
                            st.plotly_chart(fig_box, use_container_width=True)
                    except:
                        pass

# ==================== COMBINED ANALYSIS ====================
with tab3:
    st.markdown("### 📊 Combined Analysis")
    st.markdown("Compare and analyze data from both TMDB and OMDB databases.")
    
    if not tmdb_key or not omdb_key:
        st.warning("⚠️ Both TMDB and OMDB API keys are required for combined analysis.")
    else:
        st.info("💡 Use the TMDB and OMDB explorers to fetch data, then come back here to see combined visualizations.")
        
        # Check if we have data from both sources
        has_tmdb = 'tmdb_df' in st.session_state and st.session_state.tmdb_df is not None
        has_omdb = 'omdb_df' in st.session_state and st.session_state.omdb_df is not None
        
        if has_tmdb:
            st.markdown("#### 🎬 TMDB Data Available")
            st.dataframe(st.session_state.tmdb_df.head(), use_container_width=True)
        
        if has_omdb:
            st.markdown("#### 🎥 OMDB Data Available")
            st.dataframe(st.session_state.omdb_df, use_container_width=True)
        
        if has_tmdb and has_omdb:
            st.markdown("---")
            st.markdown("#### 📈 Combined Visualizations")
            
            # Compare ratings if available
            try:
                tmdb_df = st.session_state.tmdb_df
                omdb_df = st.session_state.omdb_df
                
                if 'Rating' in tmdb_df.columns and 'IMDb Rating' in omdb_df.columns:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        tmdb_rating = pd.to_numeric(tmdb_df['Rating'], errors='coerce').dropna()
                        if len(tmdb_rating) > 0:
                            fig_tmdb = px.histogram(
                                x=tmdb_rating,
                                nbins=20,
                                title="TMDB Rating Distribution",
                                labels={"x": "Rating", "y": "Count"}
                            )
                            st.plotly_chart(fig_tmdb, use_container_width=True)
                    
                    with col2:
                        omdb_rating_str = omdb_df['IMDb Rating'].iloc[0] if len(omdb_df) > 0 else None
                        if omdb_rating_str and omdb_rating_str != 'N/A':
                            try:
                                omdb_rating = float(omdb_rating_str)
                                fig_omdb = go.Figure(go.Bar(
                                    x=['IMDb Rating'],
                                    y=[omdb_rating],
                                    text=[f"{omdb_rating:.1f}/10"],
                                    textposition='auto',
                                    marker_color='orange'
                                ))
                                fig_omdb.update_layout(
                                    title="OMDB IMDb Rating",
                                    yaxis_title="Rating",
                                    yaxis_range=[0, 10],
                                    showlegend=False
                                )
                                st.plotly_chart(fig_omdb, use_container_width=True)
                            except:
                                pass
            except Exception as e:
                st.error(f"Error creating comparison: {str(e)}")
        
        elif not has_tmdb and not has_omdb:
            st.info("👆 Fetch data from TMDB and OMDB explorers above to enable combined analysis.")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666;">
    <p>💡 Tip: Use the sidebar to toggle JSON, table, and visualization views.</p>
</div>
""", unsafe_allow_html=True)

