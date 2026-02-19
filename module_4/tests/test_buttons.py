"""
tests.test_buttons
===================

Tests for button-triggered Flask routes and busy-state gating.

The two main user actions on the application are:

- **Pull Data** (``POST /refresh``) — triggers a background scrape of
  GradCafe for new applicant records and saves them to disk. The user
  sees a status indicator while this runs; the scraped rows are never
  shown directly but feed into the analysis step.
- **Update Analysis** (``POST /update-analysis``) — reads the saved
  records, runs them through an LLM for standardization, writes the
  results back to the database, and refreshes the stats page.

Both routes use a shared state file to prevent concurrent runs. If
either job is already in progress, the routes return HTTP 409.

All scraper, LLM, and database calls are mocked so these tests run
fully offline. Threading is patched to run synchronously so background
jobs complete before assertions are evaluated.

Tests are marked ``buttons`` (button route tests) or ``web``
(state/infrastructure tests).
"""

import threading
import pytest

import src.app.pages as pages
import src.refresh_gradcafe as refresh_module
from src.run import create_app
from src.app.pages import read_state, write_state


# ============================================================
# SHARED CONSTANTS
# ============================================================

#: Minimal fake stats dict returned by ``get_application_stats`` in
#: tests that need the analysis page to render without a real DB.
FAKE_STATS = {
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
    "accepted_fall_2026_gpa_pct": 0,
}

#: Two realistic GradCafe-format applicant records used to simulate
#: scraper output in loader and pull-data tests.
FAKE_ROWS = [
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
        "gpa": "3.80",
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
        "gpa": "",
    },
]


# ============================================================
# SHARED FIXTURES
# ============================================================

@pytest.fixture
def client():
    """Create a Flask test client in testing mode.

    :returns: Flask test client.
    :rtype: flask.testing.FlaskClient
    """
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture(autouse=True)
def patch_thread(monkeypatch):
    """Replace ``threading.Thread`` with a synchronous stand-in.

    Ensures background jobs triggered by button routes complete
    immediately so assertions can evaluate their side-effects
    without race conditions.

    Applied automatically to every test in this module.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    """
    class ImmediateThread:
        def __init__(self, target=None, *args, **kwargs):
            self.target = target

        def start(self):
            if self.target:
                self.target()

    monkeypatch.setattr(threading, "Thread", ImmediateThread)


# ============================================================
# POST /refresh — Pull Data button
# ============================================================

@pytest.mark.buttons
def test_pull_data_triggers_refresh(monkeypatch, client):
    """Verify ``POST /refresh`` calls ``refresh()`` and redirects.

    Asserts that:

    - The response status is 302 (redirect to analysis page).
    - The fake ``refresh()`` function was actually invoked, confirming
      the Pull Data button wires through to the scrape pipeline.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param client: Flask test client.
    """
    write_state(pulling_data=False, updating_analysis=False)

    called = {"refresh": False}

    def fake_refresh():
        called["refresh"] = True
        return {"new": 2}

    monkeypatch.setattr("src.app.pages.refresh", fake_refresh)
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)

    response = client.post("/refresh")

    assert response.status_code == 302
    assert called["refresh"], "Pull Data button should call refresh()"


@pytest.mark.buttons
def test_pull_data_returns_200(monkeypatch, client):
    """Verify ``POST /refresh`` followed by redirect returns HTTP 200.

    Follows the redirect to the analysis page and confirms the page
    loads successfully after a pull completes.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param client: Flask test client.
    """
    write_state(pulling_data=False, updating_analysis=False)
    monkeypatch.setattr("src.app.pages.refresh", lambda: None)
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)

    response = client.post("/refresh", follow_redirects=True)
    assert response.status_code == 200


@pytest.mark.buttons
def test_pull_data_triggers_loader(monkeypatch, client):
    """Verify ``POST /refresh`` runs the full scrape-and-save pipeline.

    Patches the individual scraper functions inside ``refresh_gradcafe``
    and confirms that ``write_new_applicant_file`` is called with the
    rows returned by the fake scraper. This validates that the Pull Data
    button triggers the loader, not just a no-op.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param client: Flask test client.
    """
    write_state(pulling_data=False, updating_analysis=False)

    called = {"loader": False}

    monkeypatch.setattr(refresh_module, "get_seen_ids_from_llm_extend_file", lambda: set())
    monkeypatch.setattr(refresh_module, "scrape_new_records", lambda seen_ids: FAKE_ROWS)
    monkeypatch.setattr(refresh_module, "enrich_with_details", lambda rows: rows)
    monkeypatch.setattr(
        refresh_module, "write_new_applicant_file",
        lambda rows: called.update({"loader": True})
    )
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)

    response = client.post("/refresh", follow_redirects=True)

    assert response.status_code == 200
    assert called["loader"], "Pull Data should invoke write_new_applicant_file with scraped rows"


@pytest.mark.buttons
def test_pull_data_exception(monkeypatch, client):
    """Verify ``POST /refresh`` handles a scraper exception gracefully.

    When ``refresh()`` raises, the route should still redirect (302) and
    the state file should record the error message with
    ``pulling_data=False`` and ``pull_complete=False``.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param client: Flask test client.
    """
    write_state(pulling_data=False, updating_analysis=False)
    monkeypatch.setattr("src.app.pages.refresh", lambda: (_ for _ in ()).throw(RuntimeError("Simulated failure")))

    response = client.post("/refresh")

    assert response.status_code == 302
    state = read_state()
    assert state["message"] is not None
    assert "Simulated failure" in state["message"]
    assert state["pulling_data"] is False
    assert state["pull_complete"] is False


# ============================================================
# POST /update-analysis — Update Analysis button
# ============================================================

@pytest.mark.buttons
def test_update_analysis_triggers_update_data(monkeypatch, client):
    """Verify ``POST /update-analysis`` calls ``update_data`` and ``sync_db_from_llm_file``.

    The Update Analysis route should run LLM enrichment (``update_data``)
    and then sync the results into the database (``sync_db_from_llm_file``).
    Asserts both are called and the response is a 302 redirect.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param client: Flask test client.
    """
    write_state(pulling_data=False, updating_analysis=False)

    called = {"update_data": False, "sync_db": False}

    monkeypatch.setattr("src.app.pages.update_data", lambda: called.update({"update_data": True}) or 0)
    monkeypatch.setattr("src.app.pages.sync_db_from_llm_file", lambda: called.update({"sync_db": True}))
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)

    response = client.post("/update-analysis")

    assert response.status_code == 302
    assert called["update_data"], "Update Analysis should call update_data()"
    assert called["sync_db"], "Update Analysis should call sync_db_from_llm_file()"


@pytest.mark.buttons
def test_update_analysis_returns_200(monkeypatch, client):
    """Verify ``POST /update-analysis`` followed by redirect returns HTTP 200.

    Follows the redirect to the analysis page and confirms it renders
    successfully after an update completes.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param client: Flask test client.
    """
    write_state(pulling_data=False, updating_analysis=False)
    monkeypatch.setattr("src.app.pages.update_data", lambda: None)
    monkeypatch.setattr("src.app.pages.sync_db_from_llm_file", lambda: None)
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)

    response = client.post("/update-analysis", follow_redirects=True)
    assert response.status_code == 200


@pytest.mark.buttons
def test_update_analysis_exception(monkeypatch, client):
    """Verify ``POST /update-analysis`` handles an LLM exception gracefully.

    When ``update_data()`` raises, the route should still redirect (302)
    and the state file should record the error with
    ``updating_analysis=False`` and ``analysis_complete=False``.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param client: Flask test client.
    """
    write_state(pulling_data=False, updating_analysis=False, pull_complete=True)
    monkeypatch.setattr(
        "src.app.pages.update_data",
        lambda: (_ for _ in ()).throw(RuntimeError("Simulated analysis failure"))
    )
    monkeypatch.setattr("src.app.pages.sync_db_from_llm_file", lambda: None)

    response = client.post("/update-analysis")

    assert response.status_code == 302
    state = read_state()
    assert state["message"] is not None
    assert "Simulated analysis failure" in state["message"]
    assert state["updating_analysis"] is False
    assert state["analysis_complete"] is False


# ============================================================
# BUSY-STATE GATING
# ============================================================

@pytest.mark.buttons
def test_refresh_busy(client):
    """Verify ``POST /refresh`` returns 409 when any job is already running.

    Tests both busy conditions:

    - ``pulling_data=True`` — a pull is already in progress.
    - ``updating_analysis=True`` — an analysis update is in progress.

    :param client: Flask test client.
    """
    write_state(pulling_data=True, updating_analysis=False)
    assert client.post("/refresh").status_code == 409

    write_state(pulling_data=False, updating_analysis=True)
    assert client.post("/refresh").status_code == 409


@pytest.mark.buttons
def test_update_analysis_busy(client):
    """Verify ``POST /update-analysis`` returns 409 when any job is already running.

    Tests both busy conditions:

    - ``pulling_data=True`` — a pull is already in progress.
    - ``updating_analysis=True`` — an analysis update is already running.

    :param client: Flask test client.
    """
    write_state(pulling_data=True, updating_analysis=False)
    assert client.post("/update-analysis").status_code == 409

    write_state(pulling_data=False, updating_analysis=True)
    assert client.post("/update-analysis").status_code == 409


# ============================================================
# STATE FILE INFRASTRUCTURE
# ============================================================

@pytest.mark.buttons
def test_read_state_file_not_found(tmp_path, monkeypatch):
    """Verify ``read_state`` returns safe defaults when the state file is missing.

    Patches ``STATE_FILE`` to a path that does not exist and asserts that
    ``read_state`` returns the expected default values rather than raising.

    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(pages, "STATE_FILE", str(tmp_path / "nonexistent_state.json"))

    state = read_state()

    assert state["pulling_data"] is False
    assert state["updating_analysis"] is False
    assert state["pull_complete"] is False
    assert state["analysis_complete"] is False
    assert state["message"] is None