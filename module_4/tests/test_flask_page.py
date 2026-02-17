import pytest
from types import SimpleNamespace
from bs4 import BeautifulSoup
from src.app import create_app

# ------------------------------
# Pytest fixture for Flask app
# ------------------------------
@pytest.fixture
def app():
    return create_app()

# ------------------------------
# Test that routes exist
# ------------------------------
@pytest.mark.web
def test_routes_exist(app):
    routes = [rule.rule for rule in app.url_map.iter_rules()]
    assert "/" in routes
    assert "/analysis" in routes
    assert "/refresh" in routes
    assert "/update-analysis" in routes

# ------------------------------
# Fake stats to render page
# ------------------------------
fake_stats = SimpleNamespace(
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
    accepted_fall_2026_gpa_pct=85.0
)

# ------------------------------
# Test that buttons exist
# ------------------------------
@pytest.mark.web
def test_analysis_page_buttons(app):
    # Patch get_application_stats to return fake stats
    app.view_functions["pages.grad_cafe"].__globals__["get_application_stats"] = lambda: fake_stats

    client = app.test_client()
    response = client.get("/analysis")
    assert response.status_code == 200

    # Parse HTML
    soup = BeautifulSoup(response.data, "html.parser")

    # Look for buttons by class
    pull_button = soup.find("button", class_="btn-refresh")
    update_button = soup.find("button", class_="btn-update")

    assert pull_button is not None, "Pull Data button not found"
    assert update_button is not None, "Update Analysis button not found"

# ------------------------------
# Test that page includes at least one label + answer
# ------------------------------
@pytest.mark.web
def test_page_contains_label_and_answer(app):
    # Patch get_application_stats to return fake stats
    app.view_functions["pages.grad_cafe"].__globals__["get_application_stats"] = lambda: fake_stats

    client = app.test_client()
    response = client.get("/analysis")
    assert response.status_code == 200

    soup = BeautifulSoup(response.data, "html.parser")
    text = soup.get_text()

    # List of labels and their expected values
    label_value_pairs = [
        ("Fall 2026 Applicants:", str(fake_stats.fall_2026_count)),
        ("International Applicants (%):", str(fake_stats.international_pct)),
        ("Average GPA:", str(fake_stats.avg_gpa)),
        ("Average GRE:", str(fake_stats.avg_gre)),
        ("Average GRE Verbal:", str(fake_stats.avg_gre_v)),
        ("Average GRE Analytical Writing:", str(fake_stats.avg_gre_aw)),
        ("Average GPA (US, Fall 2026):", str(fake_stats.avg_gpa_us_fall_2026)),
        ("Fall 2025 Acceptance Rate (%):", str(fake_stats.fall_2025_accept_pct)),
        ("Average GPA of Accepted Fall 2025 Applicants:", str(fake_stats.avg_gpa_fall_2025_accept)),
        ("JHU CS Master’s Applicants:", str(fake_stats.jhu_cs_masters)),
        ("2026 Acceptances, Georgetown, MIT, Stanford, CMU (Raw):", str(fake_stats.fall_2026_cs_accept)),
        ("2026 Acceptances, Georgetown, MIT, Stanford, CMU (LLM):", str(fake_stats.fall_2026_cs_accept_llm)),
        ("Fall 2026 Rejected Applicants Reporting GPA (%):", str(fake_stats.rejected_fall_2026_gpa_pct)),
        ("Fall 2026 Accepted Applicants Reporting GPA (%):", str(fake_stats.accepted_fall_2026_gpa_pct)),
    ]

    # At least one label+value should appear
    found = any(label in text and value in text for label, value in label_value_pairs)
    assert found, "No label/answer pair found on the page"
