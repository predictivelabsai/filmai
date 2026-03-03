import streamlit as st
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Prompt Manager - Film AI",
    page_icon="🧩",
    layout="wide"
)

st.title("🧩 Prompt Manager")
st.markdown("View, edit, create, and manage prompt templates in the `prompts/` folder.")

# Ensure directory
os.makedirs("prompts", exist_ok=True)

# Sidebar: actions
with st.sidebar:
    st.markdown("### 📂 Prompt Files")
    # Discover prompt files (.md preferred, show .txt for migration)
    md_files = sorted([f for f in os.listdir("prompts") if f.endswith(".md")])
    txt_files = sorted([f for f in os.listdir("prompts") if f.endswith(".txt")])

    selected_file = st.selectbox(
        "Select a prompt (.md)",
        ["(none)"] + md_files
    )

    st.markdown("---")
    st.markdown("### ➕ Create New")
    new_name = st.text_input("New prompt name (without extension)", value="")
    default_seed = st.text_area("Seed content (optional)", value="", height=100)
    if st.button("Create .md"):
        if not new_name.strip():
            st.error("Please enter a file name.")
        else:
            target = os.path.join("prompts", f"{new_name.strip()}.md")
            if os.path.exists(target):
                st.error("A file with that name already exists.")
            else:
                with open(target, "w", encoding="utf-8") as f:
                    f.write(default_seed or f"# {new_name.strip()}\n")
                st.success(f"Created: `{target}`")
                st.rerun()

    st.markdown("---")
    if txt_files:
        st.markdown("### 🔁 Migrate .txt → .md")
        migrate_choice = st.selectbox("Choose .txt to migrate", ["(none)"] + txt_files)
        if st.button("Migrate to .md", disabled=(migrate_choice == "(none)")):
            base = migrate_choice.rsplit(".", 1)[0]
            src = os.path.join("prompts", migrate_choice)
            dst = os.path.join("prompts", f"{base}.md")
            if os.path.exists(dst):
                st.error(f"Target already exists: `{dst}`")
            else:
                with open(src, "r", encoding="utf-8") as rf:
                    content = rf.read()
                with open(dst, "w", encoding="utf-8") as wf:
                    wf.write(content)
                st.success(f"Migrated to: `{dst}`")
                st.info("You can delete the old .txt below if no longer needed.")
                st.rerun()

st.markdown("---")
col_left, col_right = st.columns([2, 1])

with col_left:
    st.markdown("### ✏️ Editor")
    if selected_file != "(none)":
        file_path = os.path.join("prompts", selected_file)
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        edited = st.text_area("Edit content", value=content, height=500)
        c1, c2 = st.columns(2)
        with c1:
            if st.button("💾 Save Changes", type="primary"):
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(edited)
                st.success("Saved.")
        with c2:
            new_filename = st.text_input("Rename (without extension)", value=selected_file[:-3], key="rename_input")
            if st.button("✏️ Rename"):
                if not new_filename.strip():
                    st.error("Please enter a valid new name.")
                else:
                    new_path = os.path.join("prompts", f"{new_filename.strip()}.md")
                    if os.path.exists(new_path):
                        st.error("A file with that name already exists.")
                    else:
                        os.rename(file_path, new_path)
                        st.success(f"Renamed to `{new_path}`")
                        st.rerun()
    else:
        st.info("Select a .md prompt on the left to edit.")

with col_right:
    st.markdown("### 🧹 Maintenance")
    st.info("Delete unused prompts carefully.")
    # Deletion
    del_choice = st.selectbox("Choose file to delete", ["(none)"] + md_files + txt_files, key="delete_choice")
    if st.button("🗑️ Delete", disabled=(del_choice == "(none)")):
        target = os.path.join("prompts", del_choice)
        try:
            os.remove(target)
            st.success(f"Deleted `{target}`")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to delete: {str(e)}")


