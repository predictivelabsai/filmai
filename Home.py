import streamlit as st
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Film AI - Product Placement AI",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        color: #FF4B4B;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .feature-box {
        padding: 1.5rem;
        border-radius: 10px;
        background-color: #f0f2f6;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Main content
st.markdown('<h1 class="main-header">🎬 Film AI</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">AI-Powered Script Generation, Analysis & Insights Platform</p>', unsafe_allow_html=True)

# Introduction
st.markdown("""
Welcome to **Film AI** - your comprehensive solution for
intelligent script generation, product placement analysis, casting recommendations, and financial forecasting.
""")

# Company info
with st.expander("ℹ️ About Film AI"):
    st.markdown("""
    **Film AI** is a comprehensive platform for movie production analysis, specializing in innovative content creation
    and strategic product placement integration.
    """)

# Features overview
st.markdown("## 🚀 Platform Features")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="feature-box">
        <h3>📝 AI Script Generation</h3>
        <p>Generate professional script outlines across multiple genres including thriller, comedy, 
        children's movies, rom-com, and crime using advanced AI technology.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="feature-box">
        <h3>📤 Script Analysis</h3>
        <p>Upload screenplay PDFs or select existing ones for AI analysis and product placement 
        opportunity identification. Choose from multiple LLM providers.</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="feature-box">
        <h3>🎭 AI Casting Match</h3>
        <p>Leverage TMDB data and AI to identify the most suitable actors by country and genre 
        for your script projects.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="feature-box">
        <h3>📊 Feature Importance (Box Office)</h3>
        <p>Fetch real data from TMDb/OMDb, train a RandomForest, and visualize feature importance 
        and an aggregate treemap (Region → Genre → Title) for box office drivers.</p>
    </div>
    """, unsafe_allow_html=True)

# Quick start guide
st.markdown("## 🎯 Quick Start Guide")
st.markdown("""
1. **Generate a Script**: Navigate to the "AI Script Generation" page to create a new script outline
2. **Analyze Scripts**: Use the "Script Analysis" page to upload or analyze scripts (PDF)
3. **Compare Versions**: Use "Script Comparison" to generate a modified script and review structured JSON changes
4. **Actor Matching**: Explore the "AI Casting Match" page to discover suitable actors
5. **Feature Importance**: Open "Feature Importance" to fetch TMDb/OMDb data and analyze box office drivers
6. **Prompt Manager**: Edit and manage markdown prompt templates used by the platform
7. **Manage APIs**: Configure and test API connections in the "API Management" page
""")

# Sidebar
with st.sidebar:
    st.markdown("### 🎬 Navigation")
    st.markdown("""
    Use the pages menu to navigate through different features:
    - **AI Script Generation**
    - **Script Analysis**
    - **Script Comparison**
    - **AI Casting Match**
    - **Feature Importance**
    - **Prompt Manager**
    - **API Management**
    """)
    
    st.markdown("---")
    st.markdown("### ⚙️ System Status")
    
    # Check API keys
    api_status = {
        "OpenAI": "✅" if os.getenv("OPENAI_API_KEY") else "❌",
        "TMDB": "✅" if os.getenv("TMDB_API_KEY") else "❌",
        "OMDB": "✅" if os.getenv("OMDB_API_KEY") else "❌",
        "Tavily": "✅" if os.getenv("TAVILY_API_KEY") else "❌"
    }
    
    for api, status in api_status.items():
        st.markdown(f"{status} {api}")
    
    st.markdown("---")
    st.markdown("### 📞 Support")
    st.markdown("Film AI")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 2rem 0;">
    <p>© 2025 Film AI. All rights reserved.</p>
    <p>Powered by OpenAI, LangChain, and Streamlit</p>
</div>
""", unsafe_allow_html=True)
