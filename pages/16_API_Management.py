import streamlit as st
import os
from dotenv import load_dotenv
from tmdbv3api import TMDb, Movie, Person
import requests
from tavily import TavilyClient

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="API Management - Film AI",
    page_icon="🔌",
    layout="wide"
)

st.title("🔌 API Management")
st.markdown("Configure, test, and manage external API integrations.")

# Sidebar
with st.sidebar:
    st.markdown("### 🔧 Quick Actions")
    
    if st.button("🔄 Refresh All Status", use_container_width=True):
        st.rerun()
    
    if st.button("📋 View API Docs", use_container_width=True):
        st.info("API documentation links available in each section below.")
    
    st.markdown("---")
    st.markdown("### 📊 API Status Summary")
    
    api_keys = {
        "OpenAI": os.getenv("OPENAI_API_KEY"),
        "TMDB": os.getenv("TMDB_API_KEY"),
        "OMDB": os.getenv("OMDB_API_KEY"),
        "Tavily": os.getenv("TAVILY_API_KEY")
    }
    
    configured_count = sum(1 for v in api_keys.values() if v)
    total_count = len(api_keys)
    
    st.metric("Configured APIs", f"{configured_count}/{total_count}")
    
    for api, key in api_keys.items():
        status = "✅" if key else "❌"
        st.markdown(f"{status} {api}")

# Main content
tab1, tab2, tab3, tab4 = st.tabs(["🤖 OpenAI", "🎬 TMDB", "🎥 OMDB", "🔍 Tavily"])

with tab1:
    st.markdown("### OpenAI API")
    st.markdown("AI-powered text generation for scripts, analysis, and recommendations.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### Configuration")
        
        openai_key = os.getenv("OPENAI_API_KEY")
        
        if openai_key:
            masked_key = openai_key[:10] + "..." + openai_key[-4:]
            st.success(f"✅ API Key configured: {masked_key}")
        else:
            st.error("❌ API Key not configured")
        
        st.markdown("""
        **Features:**
        - Script generation
        - Script analysis
        - Casting recommendations
        - Financial forecasting
        - Market insights
        """)
        
        st.markdown("**Documentation:** [OpenAI API Docs](https://platform.openai.com/docs)")
    
    with col2:
        st.markdown("#### Test Connection")
        
        if st.button("🧪 Test OpenAI", use_container_width=True):
            if not openai_key:
                st.error("API key not configured")
            else:
                try:
                    with st.spinner("Testing..."):
                        from langchain_openai import ChatOpenAI
                        
                        llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0.7, max_tokens=50)
                        response = llm.invoke("Say 'OpenAI API is working!' in a creative way.")
                        result = response.content
                        
                        st.success("✅ Connection successful!")
                        st.info(f"Response: {result}")
                
                except Exception as e:
                    st.error(f"❌ Connection failed: {str(e)}")
        
        st.markdown("#### Usage Stats")
        st.info("Usage statistics available in OpenAI dashboard")

with tab2:
    st.markdown("### TMDB API")
    st.markdown("The Movie Database - comprehensive movie and actor information.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### Configuration")
        
        tmdb_key = os.getenv("TMDB_API_KEY")
        tmdb_token = os.getenv("TMDB_API_READ_TOKEN")
        
        if tmdb_key:
            masked_key = tmdb_key[:6] + "..." + tmdb_key[-4:]
            st.success(f"✅ API Key configured: {masked_key}")
        else:
            st.error("❌ API Key not configured")
        
        if tmdb_token:
            st.success("✅ Read Access Token configured")
        else:
            st.warning("⚠️ Read Access Token not configured")
        
        st.markdown("""
        **Features:**
        - Actor search
        - Movie information
        - Popular actors
        - Genre data
        - Images and posters
        """)
        
        st.markdown("**Documentation:** [TMDB API Docs](https://developers.themoviedb.org/3)")
    
    with col2:
        st.markdown("#### Test Connection")
        
        if st.button("🧪 Test TMDB", use_container_width=True):
            if not tmdb_key:
                st.error("API key not configured")
            else:
                try:
                    with st.spinner("Testing..."):
                        tmdb = TMDb()
                        tmdb.api_key = tmdb_key
                        tmdb.language = 'en'
                        
                        person = Person()
                        results = person.popular()
                        
                        if results:
                            st.success("✅ Connection successful!")
                            st.info(f"Found {len(results)} popular actors")
                            
                            # Show first result
                            if len(results) > 0:
                                st.markdown(f"**Sample:** {results[0].name} (Popularity: {results[0].popularity:.1f})")
                        else:
                            st.warning("Connection successful but no data returned")
                
                except Exception as e:
                    st.error(f"❌ Connection failed: {str(e)}")
    
    # TMDB Browser
    st.markdown("---")
    st.markdown("#### 🔍 TMDB Data Browser")
    
    browse_type = st.selectbox(
        "Browse Data Type",
        ["Popular Movies", "Popular Actors", "Trending", "Top Rated Movies"]
    )
    
    if st.button("📊 Browse TMDB Data", key="browse_tmdb"):
        if not tmdb_key:
            st.error("API key not configured")
        else:
            try:
                with st.spinner("Fetching data..."):
                    tmdb = TMDb()
                    tmdb.api_key = tmdb_key
                    tmdb.language = 'en'
                    
                    if browse_type == "Popular Movies":
                        movie = Movie()
                        results = movie.popular()
                        
                        st.markdown("### 🎬 Popular Movies")
                        
                        for m in results[:10]:
                            with st.expander(f"{m.title} ({m.vote_average:.1f}⭐)"):
                                col1, col2 = st.columns([1, 3])
                                
                                with col1:
                                    if m.poster_path:
                                        st.image(f"https://image.tmdb.org/t/p/w200{m.poster_path}")
                                
                                with col2:
                                    st.markdown(f"**Title:** {m.title}")
                                    st.markdown(f"**Rating:** {m.vote_average}/10")
                                    st.markdown(f"**Release Date:** {m.release_date if hasattr(m, 'release_date') else 'N/A'}")
                                    st.markdown(f"**Overview:** {m.overview[:200]}...")
                    
                    elif browse_type == "Popular Actors":
                        person = Person()
                        results = person.popular()
                        
                        st.markdown("### 🎭 Popular Actors")
                        
                        cols = st.columns(4)
                        
                        for idx, actor in enumerate(results[:12]):
                            with cols[idx % 4]:
                                if actor.profile_path:
                                    st.image(f"https://image.tmdb.org/t/p/w200{actor.profile_path}")
                                st.markdown(f"**{actor.name}**")
                                st.markdown(f"⭐ {actor.popularity:.1f}")
                    
                    elif browse_type == "Top Rated Movies":
                        movie = Movie()
                        results = movie.top_rated()
                        
                        st.markdown("### 🏆 Top Rated Movies")
                        
                        for m in results[:10]:
                            st.markdown(f"**{m.title}** - {m.vote_average:.1f}⭐")
            
            except Exception as e:
                st.error(f"Error browsing data: {str(e)}")

with tab3:
    st.markdown("### OMDB API")
    st.markdown("Open Movie Database - detailed movie information and ratings.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### Configuration")
        
        omdb_key = os.getenv("OMDB_API_KEY")
        
        if omdb_key:
            st.success(f"✅ API Key configured: {omdb_key}")
        else:
            st.error("❌ API Key not configured")
        
        st.markdown("""
        **Features:**
        - Detailed movie information
        - IMDb ratings
        - Rotten Tomatoes scores
        - Box office data
        - Awards information
        """)
        
        st.markdown("**Documentation:** [OMDB API Docs](http://www.omdbapi.com/)")
    
    with col2:
        st.markdown("#### Test Connection")
        
        if st.button("🧪 Test OMDB", use_container_width=True):
            if not omdb_key:
                st.error("API key not configured")
            else:
                try:
                    with st.spinner("Testing..."):
                        # Test with a popular movie
                        url = f"http://www.omdbapi.com/?apikey={omdb_key}&t=Inception"
                        response = requests.get(url)
                        
                        if response.status_code == 200:
                            data = response.json()
                            
                            if data.get('Response') == 'True':
                                st.success("✅ Connection successful!")
                                st.info(f"Sample: {data.get('Title')} ({data.get('Year')})")
                                st.markdown(f"**IMDb Rating:** {data.get('imdbRating')}")
                            else:
                                st.error(f"API Error: {data.get('Error')}")
                        else:
                            st.error(f"HTTP Error: {response.status_code}")
                
                except Exception as e:
                    st.error(f"❌ Connection failed: {str(e)}")
    
    # OMDB Search
    st.markdown("---")
    st.markdown("#### 🔍 OMDB Movie Search")
    
    search_title = st.text_input("Search for a movie", placeholder="Enter movie title...")
    
    if st.button("🔍 Search OMDB", key="search_omdb"):
        if not omdb_key:
            st.error("API key not configured")
        elif not search_title:
            st.warning("Please enter a movie title")
        else:
            try:
                with st.spinner("Searching..."):
                    url = f"http://www.omdbapi.com/?apikey={omdb_key}&t={search_title}"
                    response = requests.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data.get('Response') == 'True':
                            col1, col2 = st.columns([1, 2])
                            
                            with col1:
                                if data.get('Poster') != 'N/A':
                                    st.image(data.get('Poster'))
                            
                            with col2:
                                st.markdown(f"### {data.get('Title')} ({data.get('Year')})")
                                st.markdown(f"**Director:** {data.get('Director')}")
                                st.markdown(f"**Cast:** {data.get('Actors')}")
                                st.markdown(f"**Genre:** {data.get('Genre')}")
                                st.markdown(f"**IMDb Rating:** {data.get('imdbRating')}/10")
                                st.markdown(f"**Box Office:** {data.get('BoxOffice', 'N/A')}")
                                st.markdown(f"**Plot:** {data.get('Plot')}")
                        else:
                            st.error(f"Movie not found: {data.get('Error')}")
            
            except Exception as e:
                st.error(f"Error searching: {str(e)}")

with tab4:
    st.markdown("### Tavily API")
    st.markdown("AI-powered search for real-time market data and trends.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### Configuration")
        
        tavily_key = os.getenv("TAVILY_API_KEY")
        
        if tavily_key:
            masked_key = tavily_key[:10] + "..." + tavily_key[-4:]
            st.success(f"✅ API Key configured: {masked_key}")
        else:
            st.error("❌ API Key not configured")
        
        st.markdown("""
        **Features:**
        - Real-time market research
        - Industry trends
        - Competitive analysis
        - Financial data
        - News and insights
        """)
        
        st.markdown("**Documentation:** [Tavily API Docs](https://tavily.com/)")
    
    with col2:
        st.markdown("#### Test Connection")
        
        if st.button("🧪 Test Tavily", use_container_width=True):
            if not tavily_key:
                st.error("API key not configured")
            else:
                try:
                    with st.spinner("Testing..."):
                        client = TavilyClient(api_key=tavily_key)
                        
                        # Test search
                        response = client.search("movie industry trends 2024", max_results=1)
                        
                        if response:
                            st.success("✅ Connection successful!")
                            
                            if 'results' in response and len(response['results']) > 0:
                                result = response['results'][0]
                                st.info(f"Sample result: {result.get('title', 'N/A')}")
                        else:
                            st.warning("Connection successful but no data returned")
                
                except Exception as e:
                    st.error(f"❌ Connection failed: {str(e)}")
    
    # Tavily Search
    st.markdown("---")
    st.markdown("#### 🔍 Market Research Search")
    
    search_query = st.text_input(
        "Search market trends and insights",
        placeholder="e.g., 'product placement ROI in action movies'",
        key="tavily_search"
    )
    
    max_results = st.slider("Maximum Results", min_value=1, max_value=10, value=5)
    
    if st.button("🔍 Search Tavily", key="search_tavily"):
        if not tavily_key:
            st.error("API key not configured")
        elif not search_query:
            st.warning("Please enter a search query")
        else:
            try:
                with st.spinner("Searching..."):
                    client = TavilyClient(api_key=tavily_key)
                    
                    response = client.search(search_query, max_results=max_results)
                    
                    if response and 'results' in response:
                        st.success(f"✅ Found {len(response['results'])} results")
                        
                        for idx, result in enumerate(response['results'], 1):
                            with st.expander(f"{idx}. {result.get('title', 'No title')}", expanded=(idx == 1)):
                                st.markdown(f"**URL:** [{result.get('url', 'N/A')}]({result.get('url', '#')})")
                                st.markdown(f"**Content:** {result.get('content', 'No content available')}")
                                
                                if result.get('score'):
                                    st.markdown(f"**Relevance Score:** {result.get('score'):.2f}")
                    else:
                        st.warning("No results found")
            
            except Exception as e:
                st.error(f"Error searching: {str(e)}")

# Configuration section
st.markdown("---")
st.markdown("## ⚙️ API Configuration")

with st.expander("📝 Update API Keys"):
    st.markdown("""
    To update API keys, edit the `.env` file in the project root directory:
    
    ```
    OPENAI_API_KEY=your_key_here
    TMDB_API_KEY=your_key_here
    TMDB_API_READ_TOKEN=your_token_here
    OMDB_API_KEY=your_key_here
    TAVILY_API_KEY=your_key_here
    ```
    
    After updating, restart the Streamlit application for changes to take effect.
    """)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666;">
    <p>💡 Tip: Test each API connection regularly to ensure optimal performance.</p>
</div>
""", unsafe_allow_html=True)
