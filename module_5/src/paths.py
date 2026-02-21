# src/paths.py
import os

# Base directory pointing to the root src folder
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))

# Directory containing all JSON files
SRC_FILES_DIR = os.path.join(BASE_DIR, "src_files")

# JSON files
STATE_FILE = os.path.join(SRC_FILES_DIR, "pull_state.json")
NEW_APPLICANT_FILE = os.path.join(SRC_FILES_DIR, "new_applicant_data.json")
LLM_OUTPUT_FILE = os.path.join(SRC_FILES_DIR, "llm_extend_applicant_data.json")
