"""
tests.test_analysis_format
===========================

Tests for analysis page HTML structure, label formatting, and numeric rounding.

Purpose
-------
This file tests the **visual contract** of the analysis page — that every
stat category renders with a visible label and a correctly formatted value.
It is primarily an HTML formatting test: it renders the real Jinja template
(``gradcafe_stats.html``) through a minimal Flask test client and parses the
resulting HTML with BeautifulSoup.

Why render through Flask instead of opening the file directly
-------------------------------------------------------------
``gradcafe_stats.html`` is a Jinja2 template that uses ``{% extends %}``,
``{% block %}``, and ``{{ }}`` expressions. Opening it with
:func:`pathlib.Path.read_text` or BeautifulSoup directly would expose raw
template syntax rather than rendered HTML — BeautifulSoup would see
``{{ stats.avg_gpa }}`` instead of ``"3.78"``. Flask's test client is the
correct way to render Jinja templates: it processes the full inheritance
chain (``base.html`` → ``gradcafe_stats.html``), evaluates all expressions,
and returns the final HTML that a browser would receive.

This means the formatting tests are **dynamic** — they test the real
template file. If a label is renamed, a ``<em>`` becomes a ``<span>``, or
a new stat is added without a corresponding label, the tests will fail and
tell you exactly what broke.

``get_application_stats`` and ``read_state`` are monkeypatched with fake
data so no real database or state file is needed.

Covers two spec requirements:

1. **Labels and answers** — every stat category has a visible label
   (``<strong>``) paired with its computed value (``<em>``), and the
   values match the stats object exactly.
2. **Percentage rounding** — all percentage fields are formatted to
   exactly two decimal places, regardless of the precision of the
   underlying float.

All tests are marked ``analysis``.
"""

import pytest
from types import SimpleNamespace
from bs4 import BeautifulSoup

from src.app import create_app


# ============================================================
# SHARED CONSTANTS
# ============================================================

#: Realistic stats dict passed to the template via the monkeypatched
#: ``get_application_stats``. Uses a plain dict rather than SimpleNamespace
#: because the real function returns a dict and Jinja accesses keys via
#: dot notation through the stats object returned by query_data.
FAKE_STATS = SimpleNamespace(
    total_applicants=1000,
    fall_2026_count=500,
    international_pct=48.33,
    avg_gpa=3.78,
    avg_gre=264.68,
    avg_gre_v=161.89,
    avg_gre_aw=4.5,
    avg_gpa_us_fall_2026=3.8,
    fall_2025_accept_pct=25.0,
    avg_gpa_fall_2025_accept=3.6,
    jhu_cs_masters=120,
    fall_2026_cs_accept=200,
    fall_2026_cs_accept_llm=210,
    rejected_fall_2026_gpa_pct=15.0,
    accepted_fall_2026_gpa_pct=85.0,
)

#: Idle state returned by the fake ``read_state`` so no status banners
#: or auto-refresh tags appear in the rendered HTML.
FAKE_STATE = {
    "pulling_data": False,
    "updating_analysis": False,
    "pull_complete": False,
    "analysis_complete": False,
    "message": None,
}


# ============================================================
# SHARED FIXTURE
# ============================================================

@pytest.fixture
def soup(monkeypatch):
    """Render the real ``gradcafe_stats.html`` template and return parsed HTML.

    Creates a Flask test client, patches ``get_application_stats`` and
    ``read_state`` with fake data, hits ``GET /analysis``, and returns
    the response parsed by BeautifulSoup. All tests in this file share
    this fixture so they always parse the live template rather than a
    hardcoded copy.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :returns: Parsed HTML of the rendered analysis page.
    :rtype: bs4.BeautifulSoup
    """
    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: FAKE_STATS)
    monkeypatch.setattr("src.app.pages.read_state", lambda: FAKE_STATE)

    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        response = client.get("/analysis")
        return BeautifulSoup(response.data, "html.parser")


@pytest.fixture
def soup_high_precision(monkeypatch):
    """Render the template with high-precision float stats for rounding tests.

    Uses percentage values with many decimal places (e.g. ``45.6789``)
    to verify the template's ``"%.2f"`` Jinja filter truncates correctly.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :returns: Parsed HTML of the rendered analysis page.
    :rtype: bs4.BeautifulSoup
    """
    high_precision_stats = SimpleNamespace(
        total_applicants=1000,
        fall_2026_count=500,
        international_pct=45.6789,
        avg_gpa=3.5,
        avg_gre=300,
        avg_gre_v=155,
        avg_gre_aw=4.0,
        avg_gpa_us_fall_2026=3.6,
        fall_2025_accept_pct=78.90123,
        avg_gpa_fall_2025_accept=3.4,
        jhu_cs_masters=50,
        fall_2026_cs_accept=25,
        fall_2026_cs_accept_llm=30,
        rejected_fall_2026_gpa_pct=12.3456,
        accepted_fall_2026_gpa_pct=56.789,
    )

    monkeypatch.setattr("src.app.pages.get_application_stats", lambda: high_precision_stats)
    monkeypatch.setattr("src.app.pages.read_state", lambda: FAKE_STATE)

    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        response = client.get("/analysis")
        return BeautifulSoup(response.data, "html.parser")


# ============================================================
# SPEC 1: LABELS AND ANSWERS
# ============================================================

@pytest.mark.analysis
def test_labels_and_answers(soup):
    """Verify every stat category label is paired with the correct rendered value.

    Parses the real rendered template and asserts that for each expected
    ``<strong>`` label, the immediately following ``<em>`` contains the
    correctly formatted value. This confirms the template wires each stat
    category to its computed answer and that the Jinja formatting
    expressions (``"%.2f"|format(...)``) produce the expected output.

    :param soup: Parsed HTML of the rendered analysis page.
    :type soup: bs4.BeautifulSoup
    """
    expected = {
        "Fall 2026 Applicants:":                            str(FAKE_STATS.fall_2026_count),
        "International Applicants (%):":                    f"{FAKE_STATS.international_pct:.2f}%",
        "Average GPA:":                                     f"{FAKE_STATS.avg_gpa:.2f}",
        "Average GRE:":                                     f"{FAKE_STATS.avg_gre:.2f}",
        "Average GRE Verbal:":                              f"{FAKE_STATS.avg_gre_v:.2f}",
        "Average GRE AW:":                                  f"{FAKE_STATS.avg_gre_aw:.2f}",
        "Average GPA (US, Fall 2026):":                     f"{FAKE_STATS.avg_gpa_us_fall_2026:.2f}",
        "Fall 2025 Acceptance Rate (%):":                   f"{FAKE_STATS.fall_2025_accept_pct:.2f}%",
        "Total Applicants in Pipeline:":                    str(FAKE_STATS.total_applicants),
        "Average GPA of Accepted Fall 2025 Applicants:":    f"{FAKE_STATS.avg_gpa_fall_2025_accept:.2f}",
        "JHU CS Master\u2019s Applicants:":                  str(FAKE_STATS.jhu_cs_masters),
        "2026 Acceptances, Georgetown, MIT, Stanford, CMU (Raw):": str(FAKE_STATS.fall_2026_cs_accept),
        "2026 Acceptances, Georgetown, MIT, Stanford, CMU (LLM):": str(FAKE_STATS.fall_2026_cs_accept_llm),
        "Fall 2026 Rejected Applicants Reporting GPA (%):": f"{FAKE_STATS.rejected_fall_2026_gpa_pct:.2f}%",
        "Fall 2026 Accepted Applicants Reporting GPA (%):": f"{FAKE_STATS.accepted_fall_2026_gpa_pct:.2f}%",
    }

    for label_text, expected_value in expected.items():
        label = soup.find("strong", string=label_text)
        assert label is not None, f"Label '{label_text}' not found in rendered template"
        value = label.find_next("em").get_text()
        assert value == expected_value, (
            f"'{label_text}' — expected '{expected_value}', got '{value}'"
        )


@pytest.mark.analysis
def test_buttons_exist(soup):
    """Verify the Pull Data and Update Analysis buttons are present in the rendered page.

    Parses the real template and confirms both ``btn-refresh`` and
    ``btn-update`` button elements are rendered. If the template's button
    classes or form structure changes, this test will catch it.

    :param soup: Parsed HTML of the rendered analysis page.
    :type soup: bs4.BeautifulSoup
    """
    assert soup.find("button", class_="btn-refresh") is not None, "Pull Data button not found"
    assert soup.find("button", class_="btn-update") is not None, "Update Analysis button not found"


# ============================================================
# SPEC 2: PERCENTAGE ROUNDING
# ============================================================

@pytest.mark.analysis
def test_percentages_two_decimals(soup_high_precision):
    """Verify the template rounds all percentage fields to exactly two decimal places.

    Uses the ``soup_high_precision`` fixture which passes floats with many
    decimal places (e.g. ``45.6789``) into the real template and asserts
    that Jinja's ``"%.2f"|format(...)`` filter produces the correct
    two-decimal output with a trailing ``%``.

    .. note::
        The expected values below are **intentionally hardcoded** rather than
        derived from the fixture constants.  The whole point of this test is
        to verify rounding behaviour: computing the expected string from the
        source float would use Python's own ``:.2f``, not Jinja's, and would
        therefore not catch a template that formatted to the wrong precision.
        If the fixture floats change, update these expected strings manually
        to match the correctly-rounded two-decimal representation.

    :param soup_high_precision: Parsed HTML rendered with high-precision stats.
    :type soup_high_precision: bs4.BeautifulSoup
    """
    # Expected values derived by hand from the fixture constants:
    #   45.6789  → "45.68%"   (rounds up)
    #   78.90123 → "78.90%"   (trailing zero preserved)
    #   12.3456  → "12.35%"   (rounds up)
    #   56.789   → "56.79%"   (rounds up)
    expected_percentages = {
        "International Applicants (%):":                    "45.68%",
        "Fall 2025 Acceptance Rate (%):":                   "78.90%",
        "Fall 2026 Rejected Applicants Reporting GPA (%):": "12.35%",
        "Fall 2026 Accepted Applicants Reporting GPA (%):": "56.79%",
    }

    for label_text, expected_value in expected_percentages.items():
        label = soup_high_precision.find("strong", string=label_text)
        assert label is not None, f"Label '{label_text}' not found in rendered template"
        value = label.find_next("em").get_text()
        assert value == expected_value, (
            f"'{label_text}' — expected '{expected_value}', got '{value}'"
        )
