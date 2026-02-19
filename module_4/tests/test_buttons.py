# tests/test_buttons.py
import pytest
import threading
from src.run import create_app
from src.app.pages import write_state, read_state
import src.refresh_gradcafe as refresh_module
import src.app.pages as pages


fake_stats = {
    "total_applicants": 0,
    "fall_2026_count": 0,
    "international_pct": 0,
    "avg_gpa": 3.5,
    "avg_gre": 320,
    "avg_gre_v": 160,
    "avg_gre_aw": 4.5,
    "avg_gpa_us_fall_2026": 3.5,
    "fall_2025_accept_pct": 0,
    "avg_gpa_fall_2025_accept": 3.5,
    "jhu_cs_masters": 0,
    "fall_2026_cs_accept": 0,
    "fall_2026_cs_accept_llm": 0,
    "rejected_fall_2026_gpa_pct": 0,
    "accepted_fall_2026_gpa_pct": 0
}

# Fake rows in GradCafe format
fake_rows = [
    {
        "program_name": "Mechanical And Aerospace Engineering",
        "university": "George Washington University",
        "degree_type": "PhD",
        "comments": "Had a great interview with the PI...",
        "date_added": "February 08, 2026",
        "url_link": "https://www.thegradcafe.com/result/997885",
        "applicant_status": "Accepted: 3 Feb",
        "start_term": "Fall 2026",
        "International/US": "International",
        "gre_general": "",
        "gre_verbal": "",
        "gre_analytical_writing": "",
        "gpa": "3.80"
    },
    {
        "program_name": "Computer Science",
        "university": "Stanford University",
        "degree_type": "PhD",
        "comments": "",
        "date_added": "February 08, 2026",
        "url_link": "https://www.thegradcafe.com/result/997884",
        "applicant_status": "Rejected: 6 Feb",
        "start_term": "Fall 2026",
        "International/US": "US",
        "gre_general": "",
        "gre_verbal": "",
        "gre_analytical_writing": "",
        "gpa": ""
    }
]

# -------------------------------
# FIXTURE: Flask test client
# -------------------------------
@pytest.fixture
def client():
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

# -------------------------------
# FIXTURE: Patch threading to run synchronously
# -------------------------------
@pytest.fixture(autouse=True)
def patch_thread(monkeypatch):
    """Patch threading.Thread so background jobs run immediately."""
    class ImmediateThread:
        def __init__(self, target=None, *args, **kwargs):
            self.target = target

        def start(self):
            if self.target:
                self.target()  # run synchronously

    monkeypatch.setattr(threading, "Thread", ImmediateThread)

# ============================================================
# BUTTON: Pull Data (/refresh)
# ============================================================
@pytest.mark.buttons
def test_pull_data_triggers_refresh(monkeypatch, client):
    # Ensure clean state
    write_state(pulling_data=False, updating_analysis=False)

    called = {"refresh": False}

    def fake_refresh():
        called["refresh"] = True
        return {"new": 0}

    monkeypatch.setattr("src.app.pages.refresh", fake_refresh)
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: {"total_apps": 0})

    response = client.post("/refresh")

    assert response.status_code == 302
    assert called["refresh"], "Pull Data button should call refresh()"

# ============================================================
# BUTTON: Pull Data - follow refresh and return 200
# ============================================================
@pytest.mark.buttons
def test_pull_data_returns_200(monkeypatch, client):
    write_state(pulling_data=False, updating_analysis=False)

    monkeypatch.setattr("src.app.pages.refresh", lambda: None)
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: fake_stats)

    response = client.post("/refresh", follow_redirects=True)
    assert response.status_code == 200


# ============================================================
# BUTTON: Pull Data - handle exception in background job
# ============================================================
@pytest.mark.buttons
def test_pull_data_exception(monkeypatch, client):
    # Ensure clean state
    from src.app.pages import read_state

    write_state(pulling_data=False, updating_analysis=False)

    # Patch refresh to raise an exception
    def fake_refresh():
        raise RuntimeError("Simulated failure")

    monkeypatch.setattr("src.app.pages.refresh", fake_refresh)

    # Call the route
    response = client.post("/refresh")

    # Background thread runs synchronously due to patch_thread
    assert response.status_code == 302  # redirect still happens

    # Check that the state now contains the error message
    state = read_state()
    assert state["message"] is not None
    assert "Simulated failure" in state["message"]
    assert state["pulling_data"] is False
    assert state["pull_complete"] is False

# ============================================================
# BUTTON: Pull Data - trigger loader logic
# # ============================================================
@pytest.mark.buttons
def test_pull_data_triggers_loader(monkeypatch, client):
    write_state(pulling_data=False, updating_analysis=False)

    # Track if loader ran
    called = {"loader": False}

    # Patch refresh_gradcafe functions
    monkeypatch.setattr(refresh_module, "get_seen_ids_from_llm_extend_file", lambda: set())
    monkeypatch.setattr(refresh_module, "scrape_new_records", lambda seen_ids: fake_rows)
    monkeypatch.setattr(refresh_module, "enrich_with_details", lambda rows: rows)
    monkeypatch.setattr(refresh_module, "write_new_applicant_file", lambda rows: called.update({"loader": True}))

    # Patch get_application_stats in pages.py so template renders
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: {
        "total_applicants": 2,
        "fall_2026_count": 2,
        "international_pct": 50,
        "avg_gpa": 3.8,
        "avg_gre": 320,
        "avg_gre_v": 160,
        "avg_gre_aw": 4.5,
        "avg_gpa_us_fall_2026": 3.7,
        "fall_2025_accept_pct": 50,
        "avg_gpa_fall_2025_accept": 3.6,
        "jhu_cs_masters": 1,
        "fall_2026_cs_accept": 1,
        "fall_2026_cs_accept_llm": 1,
        "rejected_fall_2026_gpa_pct": 50,
        "accepted_fall_2026_gpa_pct": 50
    })

    # Call the Flask route and follow redirects to reach final page
    response = client.post("/refresh", follow_redirects=True)

    # Check 200 status and that loader ran
    assert response.status_code == 200
    assert called["loader"], "The loader should have been triggered with fake rows"

# ============================================================
# BUTTON: Update Analysis (/update-analysis)
# ============================================================
@pytest.mark.buttons
def test_update_analysis_triggers_update_data(monkeypatch, client):
    # Ensure clean state
    write_state(pulling_data=False, updating_analysis=False)

    called = {"update_data": False, "sync_db": False}

    def fake_update_data():
        called["update_data"] = True
        return 0

    def fake_sync_db():
        called["sync_db"] = True

    monkeypatch.setattr("src.app.pages.update_data", fake_update_data)
    monkeypatch.setattr("src.app.pages.sync_db_from_llm_file", fake_sync_db)
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: {"total_apps": 0})

    response = client.post("/update-analysis")

    assert response.status_code == 302
    assert called["update_data"], "Update Analysis should call update_data()"
    assert called["sync_db"], "Update Analysis should call sync_db_from_llm_file()"

# ============================================================
# BUTTON: Update Analysis - follow refresh and return 200
# ============================================================
@pytest.mark.buttons
def test_update_analysis_returns_200(monkeypatch, client):
    write_state(pulling_data=False, updating_analysis=False)

    monkeypatch.setattr("src.app.pages.update_data", lambda: None)
    monkeypatch.setattr("src.app.pages.sync_db_from_llm_file", lambda: None)
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: fake_stats)

    response = client.post("/update-analysis", follow_redirects=True)
    assert response.status_code == 200

# ============================================================
# BUTTON: Update Analysis - handle exception in background job
# ============================================================
@pytest.mark.buttons
def test_update_analysis_exception(monkeypatch, client):
    from src.app.pages import read_state, write_state

    # Ensure clean state
    write_state(pulling_data=False, updating_analysis=False, pull_complete=True)

    # Patch update_data to raise an exception
    def fake_update_data():
        raise RuntimeError("Simulated analysis failure")

    # Patch sync_db_from_llm_file so it won't run (optional)
    monkeypatch.setattr("src.app.pages.sync_db_from_llm_file", lambda: None)
    monkeypatch.setattr("src.app.pages.update_data", fake_update_data)

    # Call the route
    response = client.post("/update-analysis")

    # Background thread runs synchronously because of patch_thread fixture
    assert response.status_code == 302  # redirect still happens

    # Check that the state contains the error message
    state = read_state()
    assert state["message"] is not None
    assert "Simulated analysis failure" in state["message"]
    assert state["pulling_data"] is False
    assert state["updating_analysis"] is False
    assert state["analysis_complete"] is False


# ============================================================
# Test: Pull Data runs background job fully
# ============================================================
@pytest.mark.buttons
def test_pull_data_runs(client, monkeypatch):
    write_state(pulling_data=False, updating_analysis=False)

    called = {"refresh": False}

    def fake_refresh():
        called["refresh"] = True
        return {"new": 2}

    monkeypatch.setattr("src.app.pages.refresh", fake_refresh)

    response = client.post("/refresh")

    assert response.status_code == 302
    assert called["refresh"]

# ============================================================
# Test: Update Analysis runs background job fully
# ============================================================
@pytest.mark.buttons
def test_update_analysis_runs(client, monkeypatch):
    write_state(pulling_data=False, updating_analysis=False)

    called = {"update_data": False, "sync_db": False}

    def fake_update_data():
        called["update_data"] = True
        return 2

    def fake_sync_db():
        called["sync_db"] = True

    monkeypatch.setattr("src.app.pages.update_data", fake_update_data)
    monkeypatch.setattr("src.app.pages.sync_db_from_llm_file", fake_sync_db)

    response = client.post("/update-analysis")

    assert response.status_code == 302
    assert called["update_data"]
    assert called["sync_db"]

# ============================================================
# Test: Busy gating for /refresh
# ============================================================
@pytest.mark.buttons
def test_refresh_busy(client):
    write_state(pulling_data=True, updating_analysis=False)
    resp = client.post("/refresh")
    assert resp.status_code == 409

    write_state(pulling_data=False, updating_analysis=True)
    resp = client.post("/refresh")
    assert resp.status_code == 409

# ============================================================
# Test: Busy gating for /update-analysis
# ============================================================
@pytest.mark.buttons
def test_update_analysis_busy(client):
    write_state(pulling_data=True, updating_analysis=False)
    resp = client.post("/update-analysis")
    assert resp.status_code == 409

    write_state(pulling_data=False, updating_analysis=True)
    resp = client.post("/update-analysis")
    assert resp.status_code == 409

# ============================================================
# Test: STATE FILE does not exist/is not found
# ============================================================
@pytest.mark.web
def test_read_state_file_not_found(tmp_path, monkeypatch):
    # Create a fake file path that doesn't exist
    fake_state_file = tmp_path / "nonexistent_state.json"

    # Patch STATE_FILE directly in pages.py
    monkeypatch.setattr(pages, "STATE_FILE", str(fake_state_file))

    # Call read_state() — should hit FileNotFoundError and return defaults
    state = read_state()

    # Check defaults
    assert state["pulling_data"] is False
    assert state["updating_analysis"] is False
    assert state["pull_complete"] is False
    assert state["analysis_complete"] is False
    assert state["message"] is None