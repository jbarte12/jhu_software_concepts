import pytest
import json
from src.paths import NEW_APPLICANT_FILE
from src.app import pages

# -------------------------------
# 1️⃣ Fake refresh function
# -------------------------------
def fake_refresh():
    """
    Writes 2 fake applicants to NEW_APPLICANT_FILE to simulate scraping.
    Returns a dict like the real refresh function.
    """
    fake_data = [
        {
            "program_name": "CS",
            "university": "MIT",
            "result_id": 1,
            "url_link": "/result/1"
        },
        {
            "program_name": "CS",
            "university": "Stanford",
            "result_id": 2,
            "url_link": "/result/2"
        }
    ]
    # Write fake data to the staging JSON file
    with open(NEW_APPLICANT_FILE, "w", encoding="utf-8") as f:
        json.dump(fake_data, f, indent=2)
    # Simulate that 2 new records were added
    return {"new": 2}


# -------------------------------
# 2️⃣ Fake LLM function
# -------------------------------
def fake_call_llm(program_text):
    """
    Simulates _call_llm by splitting the program/university text
    and returning a standardized format.
    """
    parts = program_text.split(",")
    program = parts[0]
    university = parts[1].strip() if len(parts) > 1 else ""
    return {
        "standardized_program": program,
        "standardized_university": university
    }


# -------------------------------
# 3️⃣ Pytest fixture for Flask client
# -------------------------------
@pytest.fixture
def client(monkeypatch):
    """
    Creates a Flask test client with refresh() and _call_llm patched.
    """
    # Import app creation function
    from src.app import create_app  # make sure you have a create_app()
    app = create_app()
    app.testing = True

    # Patch refresh function in pages
    monkeypatch.setattr(pages, "refresh", fake_refresh)

    # Patch _call_llm in update_data.py with absolute import path
    monkeypatch.setattr("src.update_data._call_llm", fake_call_llm)

    # Yield the test client
    with app.test_client() as client:
        yield client
