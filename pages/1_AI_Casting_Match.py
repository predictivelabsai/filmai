import streamlit as st
import os
import sys
from dotenv import load_dotenv
from tmdbv3api import TMDb, Person, Movie
import json
from utils.ai_casting_util import generate_recommendations, score_actor_for_script
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.pdf_script_extractor import extract_pdf_text

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="AI Casting Match - Film AI",
    page_icon="🎭",
    layout="wide"
)

st.title("🎭 AI Casting Match")
st.markdown("AI-first casting recommendations using your script context, plus TMDB search.")

# Initialize TMDB
tmdb = TMDb()
tmdb.api_key = os.getenv('TMDB_API_KEY')
tmdb.language = 'en'

person = Person()
movie = Movie()

# Initialize session state
if 'selected_actors' not in st.session_state:
    st.session_state.selected_actors = []
if 'script_genre' not in st.session_state:
    st.session_state.script_genre = None

# Sidebar
with st.sidebar:
    st.markdown("### 🤖 AI Model")
    ai_model = st.selectbox(
        "Choose AI Model",
        [
            "Google Gemini 2.0 Flash (Default)",
            "Google Gemini 2.5 Flash",
            "OpenAI GPT-4.1 Mini",
            "OpenAI GPT-4.1 Nano",
            "XAI Grok 3"
        ],
        index=0,
        help="Model used for AI recommendations and scoring"
    )
    model_mapping = {
        "Google Gemini 2.0 Flash (Default)": {"provider": "google", "model": "gemini-2.0-flash-exp"},
        "Google Gemini 2.5 Flash": {"provider": "openai", "model": "gemini-2.5-flash"},
        "OpenAI GPT-4.1 Mini": {"provider": "openai", "model": "gpt-4.1-mini"},
        "OpenAI GPT-4.1 Nano": {"provider": "openai", "model": "gpt-4.1-nano"},
        "XAI Grok 3": {"provider": "xai", "model": "grok-3"}
    }
    selected_model = model_mapping[ai_model]
    st.caption(f"Provider: {selected_model['provider']} | Model: {selected_model['model']}")
    st.markdown("---")
    st.markdown("### 🧰 Tools Enabled")
    use_tmdb = st.checkbox("TMDb (filmography & metadata)", value=True)
    use_omdb = st.checkbox("OMDb (ratings & years)", value=True)
    use_tavily = st.checkbox("Tavily (web evidence)", value=True)

# Main content (AI Recommendations first)
tab_ai, tab_search, tab_selected = st.tabs(["🎯 AI Recommendations", "🔍 Actor Search", "📋 Selected Cast"])

with tab_ai:
    st.markdown("### Script Context")
    # Prefer modified script from comparison workflow
    script_context = ""
    if st.session_state.get("modified_script"):
        script_context = st.session_state.modified_script
        st.success("Using modified script from Script Comparison.")
    else:
        # Fallback to selecting from saved scripts or paste
        # Upload PDF
        uploaded_pdf = st.file_uploader("Upload script (PDF)", type=["pdf"])
        if uploaded_pdf is not None:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pdf_filename = f"scripts/{timestamp}_casting_{uploaded_pdf.name}"
            with open(pdf_filename, "wb") as f:
                f.write(uploaded_pdf.getbuffer())
            with st.spinner("Extracting text from PDF..."):
                result = extract_pdf_text(pdf_filename)
            if result.get("success"):
                script_context = result.get("text", "")
                st.success(f"Loaded PDF: {uploaded_pdf.name}")
            else:
                st.error(f"PDF extraction failed: {result.get('error')}")
        # Or select existing script files
        if not script_context and os.path.exists("scripts"):
            script_files = sorted([f for f in os.listdir("scripts") if f.endswith('.txt') or f.endswith('.pdf')], reverse=True)
            if script_files:
                selected_script = st.selectbox("Select existing script", ["None"] + script_files)
                if selected_script != "None":
                    path = f"scripts/{selected_script}"
                    if selected_script.lower().endswith(".pdf"):
                        with st.spinner("Extracting text from PDF..."):
                            res = extract_pdf_text(path)
                        if res.get("success"):
                            script_context = res.get("text", "")
                            st.info(f"Loaded: {selected_script}")
                        else:
                            st.error(f"Failed to extract: {res.get('error')}")
                    else:
                        with open(path, "r") as f:
                            script_context = f.read()
                        st.info(f"Loaded: {selected_script}")

    if st.button("🚀 Generate AI Recommendations", type="primary", use_container_width=True, disabled=not bool(script_context.strip())):
        try:
            recs = generate_recommendations(
                script_text=script_context,
                selected_model=selected_model,
                temperature=0.4,
                max_tokens=1200,
                enabled_tools={"tmdb": use_tmdb, "omdb": use_omdb, "tavily": use_tavily}
            )
            st.markdown("### 🎯 AI Recommendations")
            st.markdown(recs)
        except Exception as e:
            st.error(f"Error generating recommendations: {str(e)}")

with tab_search:
    st.markdown("### Search TMDB Database")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_query = st.text_input(
            "Search for actors",
            placeholder="Enter actor name...",
            help="Search TMDB database for actors"
        )
    
    with col2:
        search_button = st.button("🔍 Search", type="primary", use_container_width=True)
    
    if search_button and search_query:
        try:
            with st.spinner("Searching TMDB database..."):
                results = person.search(search_query)
                
                if results:
                    st.success(f"Found {len(results)} results")
                    
                    # Display results
                    for idx, actor in enumerate(results[:10]):
                        with st.expander(f"🎭 {actor.name} (Popularity: {actor.popularity:.1f})"):
                            col1, col2 = st.columns([1, 3])
                            
                            with col1:
                                if actor.profile_path:
                                    image_url = f"https://image.tmdb.org/t/p/w200{actor.profile_path}"
                                    st.image(image_url, use_container_width=True)
                                else:
                                    st.info("No image available")
                            
                            with col2:
                                st.markdown(f"**Name:** {actor.name}")
                                st.markdown(f"**Popularity:** {actor.popularity:.2f}")
                                st.markdown(f"**Known For:** {actor.known_for_department if hasattr(actor, 'known_for_department') else 'Acting'}")
                                
                                if hasattr(actor, 'known_for') and actor.known_for:
                                    known_titles = [item.title if hasattr(item, 'title') else item.name for item in actor.known_for[:3]]
                                    st.markdown(f"**Notable Works:** {', '.join(known_titles)}")
                                
                                if st.button(f"➕ Add to Cast", key=f"add_{actor.id}"):
                                    actor_data = {
                                        'id': actor.id,
                                        'name': actor.name,
                                        'popularity': actor.popularity,
                                        'profile_path': actor.profile_path,
                                        'genre': None
                                    }
                                    
                                    if actor_data not in st.session_state.selected_actors:
                                        st.session_state.selected_actors.append(actor_data)
                                        st.success(f"Added {actor.name} to cast!")
                                        st.rerun()
                                
                                # Score actor vs script
                                if st.session_state.get("modified_script") or script_context:
                                    context_text = st.session_state.get("modified_script") or script_context
                                    if st.button(f"📈 Score vs Script", key=f"score_{actor.id}"):
                                        try:
                                            score_res = score_actor_for_script(
                                                actor_name=actor.name,
                                                script_text=context_text,
                                                selected_model=selected_model,
                                                temperature=0.2,
                                                max_tokens=600
                                            )
                                            st.markdown(f"**Score:** {score_res.get('score', '?')}/100")
                                            st.markdown("**Why:**")
                                            st.markdown(score_res.get("analysis", ""))
                                        except Exception as e:
                                            st.error(f"Scoring error: {str(e)}")
                else:
                    st.warning("No results found. Try a different search term.")
        
        except Exception as e:
            st.error(f"Error searching TMDB: {str(e)}")
    
    # Popular actors by genre
    st.markdown("---")
    st.markdown("### 🌟 Popular Actors")
    
    if st.button("Show Popular Actors", key="show_popular"):
        try:
            with st.spinner("Fetching popular actors..."):
                popular_actors = person.popular()
                
                cols = st.columns(4)
                
                for idx, actor in enumerate(popular_actors[:8]):
                    with cols[idx % 4]:
                        if actor.profile_path:
                            image_url = f"https://image.tmdb.org/t/p/w200{actor.profile_path}"
                            st.image(image_url, use_container_width=True)
                        
                        st.markdown(f"**{actor.name}**")
                        st.markdown(f"⭐ {actor.popularity:.1f}")
                        
                        if st.button("➕", key=f"add_popular_{actor.id}"):
                            actor_data = {
                                'id': actor.id,
                                'name': actor.name,
                                'popularity': actor.popularity,
                                'profile_path': actor.profile_path,
                                'genre': None
                            }
                            
                            if actor_data not in st.session_state.selected_actors:
                                st.session_state.selected_actors.append(actor_data)
                                st.success(f"Added {actor.name}!")
                                st.rerun()
        
        except Exception as e:
            st.error(f"Error fetching popular actors: {str(e)}")

with tab_selected:
    st.markdown("### 📋 Selected Cast")
    
    if st.session_state.selected_actors:
        st.success(f"✅ {len(st.session_state.selected_actors)} actors selected")
        
        # Display selected actors
        for idx, actor in enumerate(st.session_state.selected_actors):
            with st.expander(f"🎭 {actor['name']}", expanded=True):
                col1, col2, col3 = st.columns([1, 3, 1])
                
                with col1:
                    if actor.get('profile_path'):
                        image_url = f"https://image.tmdb.org/t/p/w200{actor['profile_path']}"
                        st.image(image_url, use_container_width=True)
                
                with col2:
                    st.markdown(f"**Name:** {actor['name']}")
                    st.markdown(f"**Popularity:** {actor['popularity']:.2f}")
                    st.markdown(f"**Genre:** {actor.get('genre', 'N/A')}")
                    st.markdown(f"**TMDB ID:** {actor['id']}")
                
                with col3:
                    if st.button("🗑️ Remove", key=f"remove_{actor['id']}"):
                        st.session_state.selected_actors.remove(actor)
                        st.rerun()
        
        # Export cast list
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Save Cast List", use_container_width=True):
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"scripts/cast_list_{timestamp}.json"
                
                with open(filename, 'w') as f:
                    json.dump(st.session_state.selected_actors, f, indent=2)
                
                st.success(f"Cast list saved as: {filename}")
        
        with col2:
            # Create text version for download
            cast_text = "CAST LIST\n" + "="*80 + "\n\n"
            for actor in st.session_state.selected_actors:
                cast_text += f"Name: {actor['name']}\n"
                cast_text += f"Popularity: {actor['popularity']:.2f}\n"
                cast_text += f"Genre: {actor.get('genre', 'N/A')}\n"
                cast_text += f"TMDB ID: {actor['id']}\n"
                cast_text += "-"*80 + "\n"
            
            st.download_button(
                label="⬇️ Download Cast List",
                data=cast_text,
                file_name="cast_list.txt",
                mime="text/plain",
                use_container_width=True
            )
        
        # Clear all button
        if st.button("🗑️ Clear All", type="secondary"):
            st.session_state.selected_actors = []
            st.rerun()
    
    else:
        st.info("No actors selected yet. Search and add actors from the other tabs.")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666;">
    <p>💡 Tip: Use AI recommendations combined with TMDB search for the best casting decisions.</p>
</div>
""", unsafe_allow_html=True)
