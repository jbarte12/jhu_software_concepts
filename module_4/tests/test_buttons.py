# tests/test_buttons.py
import pytest
import threading
from src.run import create_app
from src.app.pages import write_state


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
# Test: Pull Data runs background job fully
# ============================================================
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
def test_update_analysis_busy(client):
    write_state(pulling_data=True, updating_analysis=False)
    resp = client.post("/update-analysis")
    assert resp.status_code == 409

    write_state(pulling_data=False, updating_analysis=True)
    resp = client.post("/update-analysis")
    assert resp.status_code == 409
