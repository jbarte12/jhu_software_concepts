"""
tests.test_flask_page
======================

Tests for Flask app creation and page rendering.

The goal of this file is to confirm that the Flask application is wired
up correctly and that the main analysis page is usable end-to-end:

- The app factory produces a correctly configured :class:`flask.Flask` instance.
- All expected routes are registered.
- ``GET /analysis`` returns HTTP 200 with the correct page structure.
- The page contains both action buttons ("Pull Data" and "Update Analysis").
- The page renders real stat categories alongside their values.

All tests are marked ``analysis`` and use ``monkeypatch`` to avoid
touching real database or file resources.
"""

import pytest
from types import SimpleNamespace
from bs4 import BeautifulSoup
from flask import Flask

from src.app import create_app
from src.app.pages import read_state


# ============================================================
# SHARED FIXTURES
# ============================================================

@pytest.fixture
def app():
    """Create and return the Flask app in testing mode.

    :returns: Configured Flask application instance.
    :rtype: flask.Flask
    """
    app = create_app()
    app.config.update({
        "TESTING": True,
        "WTF_CSRF_ENABLED": False,
    })
    return app


@pytest.fixture
def client(app):
    """Return a Flask test client for the app fixture.

    :param app: The Flask application instance.
    :type app: flask.Flask
    :returns: A test client that can make HTTP requests without a real server.
    :rtype: flask.testing.FlaskClient
    """
    return app.test_client()


#: Realistic fake stats object used across rendering tests.
#: Fields mirror the attributes returned by ``get_application_stats()``.
FAKE_STATS = SimpleNamespace(
    fall_2026_count=14464,
    international_pct=48.33,
    avg_gpa=3.78,
    avg_gre=264.68,
    avg_gre_v=161.89,
    avg_gre_aw=4.5,
    avg_gpa_us_fall_2026=3.8,
    fall_2025_accept_pct=25.0,
    avg_gpa_fall_2025_accept=3.6,
    jhu_cs_masters=120,
    total_applicants=5000,
    fall_2026_cs_accept=200,
    fall_2026_cs_accept_llm=210,
    rejected_fall_2026_gpa_pct=15.0,
    accepted_fall_2026_gpa_pct=85.0,
)

#: Clean idle state returned by the fake ``read_state`` in rendering tests.
FAKE_STATE = {
    "pulling_data": False,
    "updating_analysis": False,
    "pull_complete": False,
    "analysis_complete": False,
    "message": None,
}


# ============================================================
# APP FACTORY & CONFIGURATION
# ============================================================

@pytest.mark.web
def test_create_app():
    """Verify the app factory returns a correctly configured Flask instance.

    Asserts that:

    - The returned object is a :class:`flask.Flask` instance.
    - ``SECRET_KEY`` is set to ``"dev"``.
    - ``WTF_CSRF_ENABLED`` is ``False``.
    - The ``pages`` blueprint is registered.
    """
    app = create_app()
    assert isinstance(app, Flask)
    assert app.config["SECRET_KEY"] == "dev"
    assert app.config["WTF_CSRF_ENABLED"] is False
    assert "pages" in app.blueprints


@pytest.mark.web
def test_routes_exist(app):
    """Verify all expected application routes are registered.

    Checks that ``/``, ``/analysis``, ``/refresh``, and
    ``/update-analysis`` are all present in the URL map.

    :param app: Flask application instance.
    :type app: flask.Flask
    """
    routes = [rule.rule for rule in app.url_map.iter_rules()]
    assert "/" in routes
    assert "/analysis" in routes
    assert "/refresh" in routes
    assert "/update-analysis" in routes


# ============================================================
# GET /analysis — PAGE LOAD
# ============================================================

@pytest.mark.web
def test_grad_cafe_page_renders(client, monkeypatch):
    """Verify ``GET /`` returns HTTP 200 and renders key stat values.

    Patches ``get_application_stats`` and ``read_state`` so the route
    does not touch the database or state file, then asserts that the
    rendered HTML contains representative stat values from
    :data:`FAKE_STATS`.

    :param client: Flask test client.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)
    monkeypatch.setattr("src.app.pages.read_state", lambda: FAKE_STATE)

    response = client.get("/")

    assert response.status_code == 200
    assert b"14464" in response.data
    assert b"48.33" in response.data
    assert b"3.78" in response.data


@pytest.mark.web
def test_analysis_page_buttons(app, monkeypatch):
    """Verify ``GET /analysis`` renders both action buttons.

    Patches ``get_application_stats`` and ``read_state`` via monkeypatch
    (rather than mutating module globals directly), parses the response HTML
    with BeautifulSoup, and asserts that both the "Pull Data" (``btn-refresh``)
    and "Update Analysis" (``btn-update``) buttons are present.

    :param app: Flask application instance.
    :type app: flask.Flask
    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    """
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)
    monkeypatch.setattr("src.app.pages.read_state", lambda: FAKE_STATE)

    response = app.test_client().get("/analysis")
    assert response.status_code == 200

    soup = BeautifulSoup(response.data, "html.parser")
    assert soup.find("button", class_="btn-refresh") is not None, "Pull Data button not found"
    assert soup.find("button", class_="btn-update") is not None, "Update Analysis button not found"


@pytest.mark.web
def test_page_contains_stat_categories_and_values(app, monkeypatch):
    """Verify ``GET /analysis`` renders at least one stat category alongside its value.

    Patches ``get_application_stats`` and ``read_state`` via monkeypatch
    and checks the full page text for any matching label/value pair from
    the expected stat display. This confirms that the template is wiring
    stat categories to their computed values correctly.

    :param app: Flask application instance.
    :type app: flask.Flask
    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    """
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)
    monkeypatch.setattr("src.app.pages.read_state", lambda: FAKE_STATE)

    response = app.test_client().get("/analysis")
    assert response.status_code == 200

    text = BeautifulSoup(response.data, "html.parser").get_text()

    expected_pairs = [
        ("Fall 2026 Applicants:", str(FAKE_STATS.fall_2026_count)),
        ("International Applicants (%):", str(FAKE_STATS.international_pct)),
        ("Average GPA:", str(FAKE_STATS.avg_gpa)),
        ("Average GRE:", str(FAKE_STATS.avg_gre)),
        ("Average GRE Verbal:", str(FAKE_STATS.avg_gre_v)),
        ("Average GRE Analytical Writing:", str(FAKE_STATS.avg_gre_aw)),
        ("Average GPA (US, Fall 2026):", str(FAKE_STATS.avg_gpa_us_fall_2026)),
        ("Fall 2025 Acceptance Rate (%):", str(FAKE_STATS.fall_2025_accept_pct)),
        ("Average GPA of Accepted Fall 2025 Applicants:", str(FAKE_STATS.avg_gpa_fall_2025_accept)),
        ("JHU CS Master's Applicants:", str(FAKE_STATS.jhu_cs_masters)),
        ("2026 Acceptances, Georgetown, MIT, Stanford, CMU (Raw):", str(FAKE_STATS.fall_2026_cs_accept)),
        ("2026 Acceptances, Georgetown, MIT, Stanford, CMU (LLM):", str(FAKE_STATS.fall_2026_cs_accept_llm)),
        ("Fall 2026 Rejected Applicants Reporting GPA (%):", str(FAKE_STATS.rejected_fall_2026_gpa_pct)),
        ("Fall 2026 Accepted Applicants Reporting GPA (%):", str(FAKE_STATS.accepted_fall_2026_gpa_pct)),
    ]

    found = any(label in text and value in text for label, value in expected_pairs)
    assert found, "No stat category/value pair found on the analysis page"
