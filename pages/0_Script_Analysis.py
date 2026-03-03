import streamlit as st
import os
from datetime import datetime
from dotenv import load_dotenv
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.pdf_script_extractor import extract_pdf_text
from utils.langchain_util import analyze_script

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Script Analysis - Film AI",
    page_icon="📤",
    layout="wide"
)

st.title("📤 Script Analysis")
st.markdown("Upload PDF scripts or analyze existing ones for product placement opportunities using advanced AI.")

# Ensure directories exist
os.makedirs("scripts", exist_ok=True)

# Initialize session state
if 'analyzed_script' not in st.session_state:
    st.session_state.analyzed_script = None
if 'analysis_result' not in st.session_state:
    st.session_state.analysis_result = None
if 'current_script_name' not in st.session_state:
    st.session_state.current_script_name = None

# Sidebar
with st.sidebar:
    st.markdown("### 📊 Analysis Options")
    
    analysis_type = st.multiselect(
        "Select Analysis Types",
        ["Product Placement Opportunities", "Character Analysis", "Scene Breakdown", "Market Potential", "Budget Estimation"],
        default=["Product Placement Opportunities", "Market Potential"]
    )
    
    st.markdown("---")
    st.markdown("### 🤖 AI Model Selection")
    
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
        help="Select the AI model for script analysis"
    )
    
    # Map display names to model IDs
    model_mapping = {
        "Google Gemini 2.0 Flash (Default)": {"provider": "google", "model": "gemini-2.0-flash-exp"},
        "Google Gemini 2.5 Flash": {"provider": "openai", "model": "gemini-2.5-flash"},
        "OpenAI GPT-4.1 Mini": {"provider": "openai", "model": "gpt-4.1-mini"},
        "OpenAI GPT-4.1 Nano": {"provider": "openai", "model": "gpt-4.1-nano"},
        "XAI Grok 3": {"provider": "xai", "model": "grok-3"}
    }
    
    selected_model = model_mapping[ai_model]
    
    # Display model info
    if selected_model["provider"] == "google":
        st.info(f"🔹 Using **{ai_model}** with large context window (up to 1M tokens)")
    elif selected_model["provider"] == "openai":
        st.info(f"🔹 Using **{ai_model}** with advanced reasoning capabilities")
    elif selected_model["provider"] == "xai":
        st.info(f"🔹 Using **{ai_model}** with real-time knowledge")
    
    st.markdown("---")
    st.markdown("### ⚙️ AI Settings")
    
    temperature = st.slider(
        "Analysis Creativity (LLM temperature)",
        min_value=0.0,
        max_value=1.0,
        value=0.5,
        step=0.1,
        help="Lower values = more focused, Higher values = more creative"
    )

# Main content
tab1, tab2 = st.tabs(["📁 Upload PDF Script", "📚 Analyze Existing Script"])

with tab1:
    st.markdown("### Upload Your PDF Script")
    st.markdown("Upload a screenplay in PDF format for AI-powered analysis.")
    
    uploaded_file = st.file_uploader(
        "Choose a PDF script file",
        type=['pdf'],
        help="Upload your script in PDF format only"
    )
    
    if uploaded_file is not None:
        st.success(f"✅ File uploaded: {uploaded_file.name}")
        
        # Save uploaded PDF
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_filename = f"scripts/{timestamp}_uploaded_{uploaded_file.name}"
        
        with open(pdf_filename, 'wb') as f:
            f.write(uploaded_file.getbuffer())
        
        st.info(f"📄 PDF saved as: `{pdf_filename}`")
        
        # Extract text from PDF
        with st.spinner("📖 Extracting text from PDF..."):
            try:
                result = extract_pdf_text(pdf_filename)
                
                if result['success']:
                    script_content = result['text']
                    metadata = result['metadata']
                    
                    st.success(f"✅ Text extracted successfully!")
                    
                    # Display metadata
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Pages", metadata.get('num_pages', 'N/A'))
                    with col2:
                        st.metric("Words", f"{metadata.get('word_count', 0):,}")
                    with col3:
                        st.metric("Characters", f"{metadata.get('char_count', 0):,}")
                    with col4:
                        file_size_mb = metadata.get('file_size', 0) / (1024 * 1024)
                        st.metric("Size", f"{file_size_mb:.1f} MB")
                    
                    # Display preview
                    with st.expander("📖 Preview Extracted Text"):
                        preview_text = script_content[:2000]
                        st.text_area("Script Content (First 2000 characters)", preview_text, height=300, disabled=True)
                    
                    # Load for analysis
                    if st.button("📊 Load for Analysis", key="load_uploaded", type="primary"):
                        st.session_state.analyzed_script = script_content
                        st.session_state.current_script_name = uploaded_file.name
                        st.success("✅ Script loaded for analysis!")
                        st.rerun()
                else:
                    st.error(f"❌ Failed to extract text: {result['error']}")
                    st.info("💡 The PDF might be image-based or encrypted. Try using a text-based PDF.")
            
            except Exception as e:
                st.error(f"❌ Error processing PDF: {str(e)}")
                st.exception(e)

with tab2:
    st.markdown("### Select from Existing Scripts")
    
    if os.path.exists("scripts"):
        # Get all PDF files
        pdf_files = sorted([f for f in os.listdir("scripts") if f.endswith('.pdf')], reverse=True)
        
        if pdf_files:
            selected_script = st.selectbox(
                "Choose a PDF script to analyze",
                ["Select a script..."] + pdf_files,
                help="Select from existing PDF scripts in the library"
            )
            
            if selected_script != "Select a script...":
                script_path = f"scripts/{selected_script}"
                
                # Extract text from selected PDF
                with st.spinner(f"📖 Loading {selected_script}..."):
                    try:
                        result = extract_pdf_text(script_path)
                        
                        if result['success']:
                            script_content = result['text']
                            metadata = result['metadata']
                            
                            st.success(f"✅ Script loaded: {selected_script}")
                            
                            # Display metadata
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Pages", metadata.get('num_pages', 'N/A'))
                            with col2:
                                st.metric("Words", f"{metadata.get('word_count', 0):,}")
                            with col3:
                                st.metric("Characters", f"{metadata.get('char_count', 0):,}")
                            with col4:
                                file_size_mb = metadata.get('file_size', 0) / (1024 * 1024)
                                st.metric("Size", f"{file_size_mb:.1f} MB")
                            
                            # Display preview
                            with st.expander("📖 Preview Script"):
                                preview_text = script_content[:2000]
                                st.text_area("Script Content (First 2000 characters)", preview_text, height=300, disabled=True)
                            
                            if st.button("📊 Load for Analysis", key="load_existing", type="primary"):
                                st.session_state.analyzed_script = script_content
                                st.session_state.current_script_name = selected_script
                                st.success("✅ Script loaded for analysis!")
                                st.rerun()
                        else:
                            st.error(f"❌ Failed to extract text: {result['error']}")
                    
                    except Exception as e:
                        st.error(f"❌ Error loading script: {str(e)}")
                        st.exception(e)
        else:
            st.info("📁 No PDF scripts available. Upload a PDF script in the 'Upload PDF Script' tab.")
    else:
        st.info("📁 Scripts directory not found.")

# Analysis section
st.markdown("---")
st.markdown("## 🔍 AI Analysis")

if st.session_state.analyzed_script:
    st.success(f"📄 Script loaded: **{st.session_state.current_script_name or 'Unknown'}**")
    
    # Show script stats
    word_count = len(st.session_state.analyzed_script.split())
    char_count = len(st.session_state.analyzed_script)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Words", f"{word_count:,}")
    with col2:
        st.metric("Total Characters", f"{char_count:,}")
    
    # Preview pane before analysis
    with st.expander("📖 Preview Script Content", expanded=False):
        st.markdown("**Full Script Preview**")
        st.text_area(
            "Script Content",
            st.session_state.analyzed_script,
            height=400,
            disabled=True,
            label_visibility="collapsed"
        )
        
        # Show first 1000 characters as quick preview
        st.markdown("**Quick Preview (First 1000 characters):**")
        st.info(st.session_state.analyzed_script[:1000])
    
    st.markdown("---")
    
    if st.button(f"🚀 Analyze Script with {ai_model}", type="primary", use_container_width=True):
        # Check for appropriate API key based on provider
        api_key_missing = False
        if selected_model["provider"] == "google" and not os.getenv("GOOGLE_API_KEY"):
            st.error("❌ Google API key not found. Please configure your .env file.")
            api_key_missing = True
        elif selected_model["provider"] == "openai" and not os.getenv("OPENAI_API_KEY"):
            st.error("❌ OpenAI API key not found. Please configure your .env file.")
            api_key_missing = True
        elif selected_model["provider"] == "xai" and not os.getenv("XAI_API_KEY"):
            st.error("❌ XAI API key not found. Please configure your .venv file.")
            api_key_missing = True
        
        if not api_key_missing:
            with st.spinner(f"🔍 Analyzing script with {ai_model}... This may take a moment due to the comprehensive analysis."):
                try:
                    # Load standardized analysis template
                    template_path_md = "prompts/standardized_analysis_template.md"
                    template_path_txt = "prompts/standardized_analysis_template.txt"
                    if os.path.exists(template_path_md):
                        with open(template_path_md, 'r') as f:
                            analysis_template = f.read()
                    elif os.path.exists(template_path_txt):
                        with open(template_path_txt, 'r') as f:
                            analysis_template = f.read()
                    else:
                        # Fallback template if file doesn't exist
                        analysis_template = """## EXPERT SCREENPLAY ANALYSIS: {SCRIPT_TITLE}

As an expert screenplay analyst specializing in brand integration and market valuation, analyze this screenplay excerpt and provide a comprehensive, structured analysis.

---

## 1. PRODUCT PLACEMENT OPPORTUNITIES (5-7 Specific Integrations)

Provide a detailed table with the following columns:

| # | Scene/Moment Description | Product Category | Recommended Brand Synergy | Integration Strategy |
| :--- | :--- | :--- | :--- | :--- |

[Create 5-7 rows with specific, actionable product placement opportunities]

---

## 2. MARKET POTENTIAL

### Commercial Appeal
**Market Tier:** [A-Tier / B-Tier / C-Tier]

### Target Demographics
**Primary Audience:** [Details]
**Secondary Audience:** [Details]

### Revenue Potential
**Box Office Projection:** [Estimate]
**Product Placement Value:** [Estimate]

---

## 3. CHARACTER ANALYSIS

| Character Name | Role/Archetype | Lifestyle/Preferences | Product Affinity | Key Scenes |
| :--- | :--- | :--- | :--- | :--- |

[Analyze main characters]

---

## 4. SCENE BREAKDOWN

[Identify 5-7 key scenes with high product placement potential]

---

## 5. GENRE-SPECIFIC RECOMMENDATIONS

**Genre:** [Primary genre]
**Recommended Product Categories:** [List with justifications]

---

## 6. COMPETITIVE ANALYSIS

**Similar Films:** [Examples with product placement analysis]

---

## 7. ACTIONABLE RECOMMENDATIONS

### Immediate Actions
### Partnership Strategy
### Integration Timeline
### Success Metrics

---

## 8. RISK ASSESSMENT

### Potential Challenges
### Brand Safety Considerations

---

## SUMMARY

**Overall Assessment:** [Brief summary]
**Top 3 Opportunities:** [List]
**Recommended Next Steps:** [List]"""
                    
                    # Get script title from filename and generate analysis via util
                    script_title = st.session_state.current_script_name.replace('.pdf', '').replace('_', ' ') if st.session_state.current_script_name else "Unknown Script"
                    result = analyze_script(
                        script_title=script_title,
                        script_content=st.session_state.analyzed_script,
                        selected_model=selected_model,
                        temperature=temperature,
                        max_tokens=4000,
                        analysis_template=analysis_template
                    )
                    
                    st.session_state.analysis_result = result
                    st.success("✅ Analysis complete!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error analyzing script: {str(e)}")
                    st.exception(e)
    
    # Display analysis results
    if st.session_state.analysis_result:
        st.markdown("### 📊 Analysis Results")
        st.markdown(st.session_state.analysis_result)
        
        # Save and download options
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("💾 Save Analysis", use_container_width=True):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"scripts/analysis_{timestamp}.txt"
                
                with open(filename, 'w') as f:
                    f.write("SCRIPT ANALYSIS REPORT\n")
                    f.write("="*80 + "\n")
                    f.write(f"Script: {st.session_state.current_script_name or 'Unknown'}\n")
                    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Model: Gemini 2.5 Flash\n")
                    f.write(f"Analysis Types: {', '.join(analysis_type)}\n")
                    f.write("="*80 + "\n\n")
                    f.write(st.session_state.analysis_result)
                
                st.success(f"Analysis saved as: `{filename}`")
        
        with col2:
            st.download_button(
                label="⬇️ Download Analysis",
                data=st.session_state.analysis_result,
                file_name=f"script_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain",
                use_container_width=True
            )
        
        with col3:
            if st.button("🔄 Clear Analysis", use_container_width=True):
                st.session_state.analysis_result = None
                st.session_state.analyzed_script = None
                st.session_state.current_script_name = None
                st.rerun()
        
        # Product placement opportunities section
        if "Product Placement Opportunities" in analysis_type:
            st.markdown("---")
            st.markdown("### 🎯 Next Steps for Product Placement")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.info("""
                **Integration Strategies:**
                - Review identified scenes for natural product integration
                - Match products with character demographics
                - Consider brand alignment with movie theme
                """)
            
            with col2:
                st.success("""
                **Recommended Actions:**
                - Contact brands for partnership opportunities
                - Estimate placement costs and ROI
                - Create detailed integration proposals
                """)
else:
    st.info("👆 Upload a PDF script or select an existing one to begin analysis")
    
    # Show available scripts
    if os.path.exists("scripts"):
        pdf_files = [f for f in os.listdir("scripts") if f.endswith('.pdf')]
        if pdf_files:
            st.markdown("### 📚 Available Scripts")
            for script in pdf_files[:5]:  # Show first 5
                st.markdown(f"- `{script}`")
            if len(pdf_files) > 5:
                st.markdown(f"*...and {len(pdf_files) - 5} more*")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <small>Powered by Gemini 2.5 Flash | Advanced AI Analysis for Product Placement</small>
</div>
""", unsafe_allow_html=True)


