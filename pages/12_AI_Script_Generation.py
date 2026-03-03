import streamlit as st
import os
from datetime import datetime
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="AI Script Generation - Film AI",
    page_icon="📝",
    layout="wide"
)

st.title("📝 AI Script Generation")
st.markdown("Generate professional script outlines using AI across multiple genres.")

# Ensure directories exist
os.makedirs("prompts", exist_ok=True)
os.makedirs("scripts", exist_ok=True)

# Load or create prompt template (.md preferred, fallback .txt)
prompt_file_md = "prompts/script_generation.md"
prompt_file_txt = "prompts/script_generation.txt"
if os.path.exists(prompt_file_md):
    with open(prompt_file_md, 'r') as f:
        default_prompt = f.read()
elif os.path.exists(prompt_file_txt):
    with open(prompt_file_txt, 'r') as f:
        default_prompt = f.read()
else:
    default_prompt = """You are a professional screenwriter. Generate a detailed script outline for a {genre} movie.

The script should include:
1. Title
2. Logline (one-sentence summary)
3. Three-act structure:
   - Act 1: Setup (introduce characters, setting, and inciting incident)
   - Act 2: Confrontation (rising action, obstacles, midpoint twist)
   - Act 3: Resolution (climax and conclusion)
4. Main characters (3-5 characters with brief descriptions)
5. Key scenes (5-7 major scenes with descriptions)
6. Potential product placement opportunities (3-5 natural integration points)

Genre: {genre}
Target audience: {target_audience}
Setting: {setting}

Make the script outline engaging, commercially viable, and suitable for product placement integration."""

# Sidebar - Prompt Template Editor
with st.sidebar:
    st.markdown("### 🎨 Prompt Template Editor")
    st.markdown("Customize the AI prompt template for script generation.")
    
    edited_prompt = st.text_area(
        "Edit Prompt Template",
        value=default_prompt,
        height=300,
        help="Use {genre}, {target_audience}, and {setting} as placeholders"
    )
    
    if st.button("💾 Save Template"):
        with open(prompt_file_md, 'w') as f:
            f.write(edited_prompt)
        st.success("✅ Template saved successfully!")
    
    st.markdown("---")
    st.markdown("### 📋 Generated Scripts")
    
    # List existing scripts
    if os.path.exists("scripts"):
        script_files = sorted([f for f in os.listdir("scripts") if f.endswith('.txt')], reverse=True)
        if script_files:
            st.markdown(f"**Total Scripts:** {len(script_files)}")
            for script_file in script_files[:5]:
                st.markdown(f"- {script_file}")
        else:
            st.info("No scripts generated yet")

# Main content
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("### 🎬 Script Parameters")
    
    # Genre selection
    genre = st.selectbox(
        "Select Genre",
        ["Thriller", "Comedy", "Children's Movie", "Romantic Comedy", "Crime", "Action", "Drama", "Horror", "Sci-Fi"],
        help="Choose the genre for your script"
    )
    
    # Target audience
    target_audience = st.selectbox(
        "Target Audience",
        ["General Audience (PG)", "Teen & Young Adult (PG-13)", "Adult (R)", "Family (G)", "Mature (NC-17)"],
        help="Define the target audience"
    )
    
    # Setting
    setting = st.text_input(
        "Setting",
        value="Modern urban city",
        help="Describe the primary setting of the story"
    )
    
    # Additional parameters
    with st.expander("⚙️ Advanced Options"):
        temperature = st.slider(
            "Creativity Level (LLM temperature)",
            min_value=0.0,
            max_value=1.0,
            value=0.7,
            step=0.1,
            help="Higher values make the output more creative and unpredictable"
        )
        
        max_tokens = st.number_input(
            "Maximum Length (tokens)",
            min_value=500,
            max_value=4000,
            value=2000,
            step=100,
            help="Maximum length of the generated script"
        )

with col2:
    st.markdown("### 🎯 Quick Tips")
    st.info("""
    **Genre Selection:**
    - Thriller: Suspense and tension
    - Comedy: Humor and entertainment
    - Children's: Family-friendly content
    - Rom-Com: Romance and humor
    - Crime: Investigation and mystery
    
    **Product Placement:**
    AI will suggest natural integration points for brands and products.
    """)

# Generate button
st.markdown("---")
if st.button("🚀 Generate Script Outline", type="primary", use_container_width=True):
    if not os.getenv("OPENAI_API_KEY"):
        st.error("❌ OpenAI API key not found. Please configure your .env file.")
    else:
        with st.spinner("🎬 Generating script outline... This may take a moment."):
            try:
                # Initialize LLM
                llm = ChatOpenAI(
                    model="gpt-4.1-mini",
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                
                # Create prompt template
                prompt_template = PromptTemplate(
                    input_variables=["genre", "target_audience", "setting"],
                    template=edited_prompt
                )
                
                # Format prompt and generate script
                formatted_prompt = prompt_template.format(
                    genre=genre,
                    target_audience=target_audience,
                    setting=setting
                )
                
                # Use invoke instead of predict (new LangChain API)
                response = llm.invoke(formatted_prompt)
                result = response.content
                
                # Display result
                st.success("✅ Script outline generated successfully!")
                st.markdown("### 📄 Generated Script Outline")
                st.markdown(result)
                
                # Save script
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"scripts/{timestamp}_{genre.replace(' ', '_').replace(chr(39), '')}.txt"
                
                with open(filename, 'w') as f:
                    f.write(f"Genre: {genre}\n")
                    f.write(f"Target Audience: {target_audience}\n")
                    f.write(f"Setting: {setting}\n")
                    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("\n" + "="*80 + "\n\n")
                    f.write(result)
                
                st.info(f"💾 Script saved as: `{filename}`")
                
                # Download button
                st.download_button(
                    label="⬇️ Download Script",
                    data=result,
                    file_name=f"{genre.replace(' ', '_')}_script_{timestamp}.txt",
                    mime="text/plain"
                )
                
            except Exception as e:
                st.error(f"❌ Error generating script: {str(e)}")
                st.exception(e)

# Display recent scripts
st.markdown("---")
st.markdown("### 📚 Recent Scripts")

if os.path.exists("scripts"):
    script_files = sorted([f for f in os.listdir("scripts") if f.endswith('.txt')], reverse=True)
    
    if script_files:
        selected_script = st.selectbox(
            "View a previous script",
            ["Select a script..."] + script_files[:10]
        )
        
        if selected_script != "Select a script...":
            with open(f"scripts/{selected_script}", 'r') as f:
                script_content = f.read()
            
            with st.expander(f"📖 {selected_script}", expanded=True):
                st.text(script_content)
                
                st.download_button(
                    label="⬇️ Download",
                    data=script_content,
                    file_name=selected_script,
                    mime="text/plain",
                    key=f"download_{selected_script}"
                )
    else:
        st.info("No scripts generated yet. Create your first script above!")
else:
    st.info("No scripts directory found. Generate a script to create it.")
