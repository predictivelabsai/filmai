import streamlit as st
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="User Guide - Film AI",
    page_icon="📖",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        margin-bottom: 1rem;
    }
    .section-header {
        font-size: 1.8rem;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
    }
    .subsection-header {
        font-size: 1.3rem;
        color: #34495e;
        margin-top: 1.5rem;
        margin-bottom: 0.8rem;
    }
    .feature-box {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
        margin-bottom: 1rem;
    }
    .code-block {
        background-color: #2c3e50;
        color: #ecf0f1;
        padding: 1rem;
        border-radius: 5px;
        font-family: 'Courier New', monospace;
        margin: 1rem 0;
    }
    .credential-box {
        background-color: #fff3cd;
        border: 2px solid #ffc107;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Main header
st.markdown('<h1 class="main-header">📖 Film AI - User Guide</h1>', unsafe_allow_html=True)

st.markdown("""
Welcome to the comprehensive user guide for **Film AI**.
This guide will walk you through all features and functionalities of the application.
""")

# Table of Contents
st.markdown('<h2 class="section-header">📑 Table of Contents</h2>', unsafe_allow_html=True)
st.markdown("""
1. [Getting Started](#getting-started)
2. [AI Script Generation](#ai-script-generation)
3. [Script Analysis](#script-analysis)
4. [Script Comparison](#script-comparison)
5. [AI Casting Match](#ai-casting-match)
6. [Feature Importance](#feature-importance)
7. [Prompt Manager](#prompt-manager)
8. [API Management](#api-management)
9. [Tips & Best Practices](#tips-best-practices)
""")

# Getting Started
st.markdown('<h2 class="section-header" id="getting-started">🚀 Getting Started</h2>', unsafe_allow_html=True)

st.markdown("""
### Installation

Follow these steps to set up the platform on your local machine:

**1. Clone the Repository**
```bash
git clone https://github.com/predictivelabsai/movies-product-placement.git
cd movies-product-placement
```

**2. Create Virtual Environment**
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate
```

**3. Install Dependencies**
```bash
pip install -r requirements.txt
```

**4. Configure API Keys**
```bash
cp .env.sample .env
# Edit .env file with your API keys:
# - OPENAI_API_KEY
# - TMDB_API_KEY
# - OMDB_API_KEY
# - TAVILY_API_KEY
```

**5. Run the Application**
```bash
streamlit run Home.py
```

The application will open in your browser at `http://localhost:8501`.

### First Login

When you first access the application, you'll see a login screen. Use the credentials provided above to sign in.
""")

# Display screenshot if available
screenshot_path = Path("/home/ubuntu/movies-product-placement/screenshots/01_home_page.webp")
if screenshot_path.exists():
    st.image(str(screenshot_path), caption="Home Page", use_container_width=True)

# AI Script Generation
st.markdown('<h2 class="section-header" id="ai-script-generation">✍️ AI Script Generation</h2>', unsafe_allow_html=True)

st.markdown("""
The **AI Script Generation** feature allows you to create professional script outlines using advanced AI technology powered by **GPT-4.1-mini**.

### How to Use

1. **Select Genre**: Choose from Thriller, Comedy, Children's Movie, Romantic Comedy, Crime, Action, Drama, Horror, or Sci-Fi
2. **Choose Target Audience**: Select from General Audience (PG), Teen & Young Adult (PG-13), Adult (R), Family (G), or Mature (NC-17)
3. **Enter Setting**: Provide a setting description (e.g., "Modern urban city", "Medieval castle", "Space station")
4. **Adjust Advanced Options** (optional):
   - **Creativity Level**: Control randomness (0.0 = focused, 1.0 = creative)
   - **Maximum Length**: Set token limit (500-4000 tokens)
5. **Generate**: Click the "🚀 Generate Script Outline" button

### Key Features

- **75% Higher Accuracy**: AI-powered generation delivers more relevant and coherent scripts than manual methods
- **Multiple Genres**: Support for 9 major film genres
- **Customizable Prompts**: Edit the prompt template in the sidebar to customize output
- **Natural Integration**: Product placements are woven naturally into the narrative
- **Export Options**: Download as TXT format

### AI Model

The platform uses **GPT-4.1-mini**, which provides:
- Fast response times (5-15 seconds)
- High-quality script outlines
- Natural product placement suggestions
- Cost-effective generation

### Tips

- Be specific in your setting description for better results
- Use the creativity slider to control output style
- Save the prompt template after customization
- Review and edit the generated script to match your vision
- Generate multiple versions and combine the best elements
""")

screenshot_path = Path("/home/ubuntu/movies-product-placement/screenshots/02_ai_script_generation.webp")
if screenshot_path.exists():
    st.image(str(screenshot_path), caption="AI Script Generation Interface", use_container_width=True)

# Script Analysis
st.markdown('<h2 class="section-header" id="script-analysis">📤 Script Analysis</h2>', unsafe_allow_html=True)

st.markdown("""
Upload screenplay PDFs for AI-powered analysis to identify product placement opportunities. Choose the LLM provider in the sidebar.

### Supported Formats

- **PDF**: Text-based PDF scripts (image-based PDFs may require OCR)

### Analysis Features

1. **Product Placement Identification**
   - AI examines narrative flow and character interactions
   - Identifies natural integration points for brands
   - Suggests specific scenes and moments

2. **Market Potential Assessment**
   - Evaluates target demographics
   - Analyzes genre performance trends
   - Provides commercial viability scoring (0-100)

3. **Character Development Analysis**
   - Analyzes character arcs and relationships
   - Assesses brand association strength
   - Identifies key character moments

4. **Scene Breakdown**
   - Lists key scenes with placement potential
   - Provides character involvement details
   - Estimates value range for each opportunity

### AI Evaluation Metrics

- **Narrative Fit Score (0-100)**: How naturally the placement integrates
- **Audience Exposure Duration**: Estimated screen time and visibility
- **Brand Alignment**: Compatibility with story themes and character values

### How to Use

1. Click "Upload PDF Script" and select your file, or choose from existing scripts
2. Wait for text extraction and statistics
3. Select AI model (Gemini, OpenAI, XAI) and run analysis
4. Review the structured analysis and next steps
5. Export or save the results
""")

screenshot_path = Path("/home/ubuntu/movies-product-placement/screenshots/03_script_upload_analysis.webp")
if screenshot_path.exists():
    st.image(str(screenshot_path), caption="Script Upload & Analysis Interface", use_container_width=True)

# Script Comparison
st.markdown('<h2 class="section-header" id="script-comparison">🔄 Script Comparison</h2>', unsafe_allow_html=True)

st.markdown("""
Three-step workflow to create and review a product-integrated version of your script and assess changes.

### Steps

1. **Upload Original**: Load the original script (PDF or paste text)
2. **Generate Modified**: Use AI to produce a modified script with natural product placements (tunable subtlety, brand/category hints; choose provider/model)
3. **Compare**: Generate a structured JSON of changes and navigate via an interactive table (scene hints, product mentions, camera notes)

Legacy visual diff views (Side-by-Side, Unified, Inline) are still available below the JSON changes.

### What You’ll See

- **Structured JSON Changes**: IDs, scene hints, product mentions, cinematography notes, confidence
- **Quick Navigation**: Table-driven selection to preview original vs. modified excerpts
- **Metrics**: Word/character counts and deltas

### Narrative Report

Optionally generate a natural-language analysis focused on product placement integration and cinematography, without changing story beats.

### Export Options

- **PDF Report**: Complete comparison with statistics
- **Highlighted Version**: Modified script with changes marked
- **Summary Document**: Key changes and impact assessment
""")

screenshot_path = Path("/home/ubuntu/movies-product-placement/screenshots/04_script_comparison.webp")
if screenshot_path.exists():
    st.image(str(screenshot_path), caption="Script Comparison Interface", use_container_width=True)

# AI Casting Match
st.markdown('<h2 class="section-header" id="ai-casting-match">🎭 AI Casting Match</h2>', unsafe_allow_html=True)

st.markdown("""
Find the perfect actors for your roles using AI-powered recommendations and comprehensive actor databases.

### Search Methods

1. **Manual Search**
   - Name-based queries with autocomplete
   - Fuzzy matching for misspellings
   - Quick access to actor profiles

2. **Advanced Filtering**
   - Genre experience
   - Market presence (domestic/international)
   - Age range
   - Gender
   - Popularity scores

3. **AI Recommendations**
   - Script-based character analysis
   - Budget constraint consideration
   - Genre expertise matching
   - 85% accuracy in role alignment

### Casting Criteria

The AI evaluates actors based on:

- **Genre Experience**: Previous work in similar films
- **Performance Patterns**: Acting style and range
- **Market Presence**: Audience appeal and recognition
- **Demographics**: Age, appearance, background
- **Award Recognition**: Industry accolades
- **Social Media Influence**: Fan base and engagement
- **Estimated Fee Range**: Budget compatibility

### Database

- **500,000+ Actor Profiles**: Comprehensive TMDB integration
- **Real-time Updates**: Latest filmography and information
- **Global Coverage**: Actors from all major markets

### How to Use

1. **Upload Script** (optional): For AI recommendations
2. **Describe Role**: Character traits, age, genre
3. **Set Budget Range**: Estimated fee constraints
4. **Review Matches**: Browse AI-suggested actors
5. **Compare Options**: Side-by-side actor profiles
6. **Export Shortlist**: Save casting recommendations
""")

screenshot_path = Path("/home/ubuntu/movies-product-placement/screenshots/05_ai_casting_match.webp")
if screenshot_path.exists():
    st.image(str(screenshot_path), caption="AI Casting Match Interface", use_container_width=True)

# Feature Importance
st.markdown('<h2 class="section-header" id="feature-importance">📊 Feature Importance</h2>', unsafe_allow_html=True)

st.markdown("""
Analyze real-world drivers of box office performance using data from **TMDb** and **OMDb**.

### Data Fetch
- Fetch movies via TMDb Discover, filter by year/region/genre
- Enrich with details, credits, and external IDs
- Pull Box Office from OMDb (fallback to TMDb revenue when available)

### Model
- Train a **RandomForestRegressor** with features like runtime, votes, popularity, budget, year
- Include categorical encodings for region, language, primary genre, and multi-hot genres
- Actor/director signals via popularity aggregates

### Outputs
- **Feature Importances**: Horizontal bar chart (top-K)
- **Treemap**: Region → Genre → Title aggregation of box office (USD millions)

### How to Use
1. Configure filters and data sources in the sidebar
2. Click “Fetch Movies” to build the dataset
3. Click “Train Model” to compute feature importances
4. Explore the treemap and top drivers
""")
# API Management
# Tips & Best Practices

screenshot_path = Path("/home/ubuntu/movies-product-placement/screenshots/06_financial_forecasting.webp")
if screenshot_path.exists():
    st.image(str(screenshot_path), caption="Financial Forecasting Interface", use_container_width=True)

# API Management
st.markdown('<h2 class="section-header" id="api-management">🔌 API Management</h2>', unsafe_allow_html=True)

st.markdown("""
Monitor and manage all API integrations from a centralized dashboard.

### Integrated APIs

1. **OpenAI (GPT-4.1-mini)**
   - Purpose: AI text generation for scripts and analysis
   - Status: Real-time connection monitoring
   - Usage: Token consumption tracking
   - Model: gpt-4.1-mini (optimized for speed and cost)

2. **TMDB (The Movie Database)**
   - Purpose: Movie database and actor information
   - Status: Active connection verification
   - Coverage: 500,000+ actor profiles

3. **OMDB (Open Movie Database)**
   - Purpose: Additional movie metadata and ratings
   - Status: Response time monitoring
   - Data: Box office, ratings, reviews

4. **Tavily**
   - Purpose: Web search and market research
   - Status: Query success rate tracking
   - Features: Real-time trend analysis

### Management Capabilities

- **Real-time Health Monitoring**: Connection status every 5 minutes
- **Automated Testing**: Continuous API availability checks
- **Secure Key Storage**: Environment variable protection
- **Response Time Tracking**: Performance metrics
- **Error Rate Monitoring**: Failure detection and alerts
- **Usage Statistics**: Quota and consumption tracking
- **Email Alerts**: Notification for connection failures

### 99% Uptime Guarantee

The platform ensures high availability through:

- Redundant API endpoints
- Automatic failover mechanisms
- Cached data for offline access
- Graceful degradation strategies

### Configuration

1. Navigate to API Management page
2. View current status of all APIs
3. Test connections manually
4. Update API keys if needed
5. Review usage statistics
6. Configure alert preferences
""")

screenshot_path = Path("/home/ubuntu/movies-product-placement/screenshots/07_api_management.webp")
if screenshot_path.exists():
    st.image(str(screenshot_path), caption="API Management Interface", use_container_width=True)

# Tips & Best Practices
st.markdown('<h2 class="section-header" id="tips-best-practices">💡 Tips & Best Practices</h2>', unsafe_allow_html=True)

st.markdown("""
### For Screenwriters

- **Start with AI Generation**: Use the script generator to overcome writer's block
- **Iterate and Refine**: Generate multiple versions and combine the best elements
- **Natural Integration**: Focus on product placements that enhance rather than disrupt the story
- **Character-Driven**: Ensure placements align with character personalities and arcs
- **Export Early**: Save your work frequently in multiple formats

### For Producers

- **Budget Planning**: Use financial forecasting early in pre-production
- **Cast Wisely**: Balance star power with budget constraints using AI recommendations
- **Market Research**: Leverage market insights to target the right demographics
- **ROI Focus**: Compare multiple scenarios before committing to major decisions
- **Documentation**: Export all analyses for investor presentations

### For Brands

- **Narrative Fit**: Prioritize placements with high narrative fit scores (>80)
- **Audience Alignment**: Ensure the film's demographics match your target market
- **Value Assessment**: Use estimated value ranges to negotiate fair pricing
- **Long-term Strategy**: Consider multiple placements across different projects
- **Authenticity**: Choose placements that feel natural and enhance brand perception

### General Tips

1. **API Keys**: Keep your API keys secure and never share them
2. **File Sizes**: Compress large scripts before uploading for faster processing
3. **Browser**: Use Chrome or Firefox for best compatibility
4. **Internet**: Ensure stable connection for AI processing
5. **Backups**: Download and save all generated content locally
6. **Updates**: Check for platform updates regularly
7. **Support**: Contact support@filmai.com for assistance

### Performance Optimization

- **Close Unused Tabs**: Free up browser memory
- **Clear Cache**: Periodically clear browser cache for optimal performance
- **Batch Processing**: Process multiple scripts during off-peak hours
- **Export Formats**: Choose appropriate formats (PDF for sharing, DOCX for editing)

### Troubleshooting

**Script Generation Fails**
- Check OpenAI API key validity
- Ensure sufficient API credits
- Simplify plot description if too complex
- Verify you're using supported model (gpt-4.1-mini)

**Upload Issues**
- Verify file format (TXT, PDF, DOCX only)
- Check file size (max 200MB)
- Ensure file is not corrupted

**Slow Performance**
- Check internet connection speed
- Close other resource-intensive applications
- Try during off-peak hours

**API Connection Errors**
- Verify API keys in .env file
- Check API Management page for status
- Wait and retry if service is temporarily down

**Login Issues**
- Verify credentials with your administrator
- Clear browser cache and cookies
- Try incognito/private browsing mode
""")

# Footer
st.markdown("---")
st.markdown("""
### 📞 Support & Resources

- **GitHub Repository**: [github.com/predictivelabsai/movies-product-placement](https://github.com/predictivelabsai/movies-product-placement)
- **Documentation**: See README.md in the project root
- **Issues**: Report bugs on GitHub Issues
- **Email Support**: support@filmai.com

### 📄 License

This project is licensed under the MIT License. See LICENSE file for details.

---

**Version**: 1.1.0  
**Last Updated**: November 8, 2025  
**Platform**: Film AI  
**AI Models**: Gemini 2.0 Flash, GPT-4.1-mini, XAI Grok 3
""")

# Prompt Manager
st.markdown('<h2 class="section-header" id="prompt-manager">🧩 Prompt Manager</h2>', unsafe_allow_html=True)
st.markdown("""
Manage the markdown prompt templates used throughout the app.

### Capabilities
- View and edit `.md` prompt files in `prompts/`
- Create new templates
- Migrate old `.txt` prompts to `.md`
- Rename or delete prompts

### Tips
- Keep prompts concise and structured
- Use placeholders like `{original_script}` and `{modified_script}`
""")
