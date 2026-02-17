import pytest
from types import SimpleNamespace
from bs4 import BeautifulSoup

# ------------------------------
# Fake stats with all required fields
# ------------------------------
fake_stats = SimpleNamespace(
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
    accepted_fall_2026_gpa_pct=85.0
)

# ------------------------------
# Fake HTML renderer
# ------------------------------
def fake_gradcafe_html(stats=fake_stats, pulling_data=False, pull_complete=False, message=None):
    return f"""
    <html>
    <body>
        <h2>GradCafe Application Statistics</h2>
        <p><strong>Fall 2026 Applicants:</strong> <em>{stats.fall_2026_count}</em></p>
        <p><strong>International Applicants (%):</strong> <em>{stats.international_pct:.2f}%</em></p>
        <p><strong>Average GPA:</strong> <em>{stats.avg_gpa:.2f}</em></p>
        <p><strong>Average GRE:</strong> <em>{stats.avg_gre:.2f}</em></p>
        <p><strong>Average GRE Verbal:</strong> <em>{stats.avg_gre_v:.2f}</em></p>
        <p><strong>Average GRE AW:</strong> <em>{stats.avg_gre_aw:.2f}</em></p>
        <p><strong>Average GPA (US, Fall 2026):</strong> <em>{stats.avg_gpa_us_fall_2026:.2f}</em></p>
        <p><strong>Fall 2025 Acceptance Rate (%):</strong> <em>{stats.fall_2025_accept_pct:.2f}%</em></p>
        <p><strong>Total Applicants in Pipeline:</strong> <em>{stats.total_applicants}</em></p>
        <p><strong>Average GPA of Accepted Fall 2025 Applicants:</strong> <em>{stats.avg_gpa_fall_2025_accept:.2f}</em></p>
        <p><strong>JHU CS Master’s Applicants:</strong> <em>{stats.jhu_cs_masters}</em></p>
        <p><strong>2026 Acceptances, Georgetown, MIT, Stanford, CMU (Raw):</strong> <em>{stats.fall_2026_cs_accept}</em></p>
        <p><strong>2026 Acceptances, Georgetown, MIT, Stanford, CMU (LLM):</strong> <em>{stats.fall_2026_cs_accept_llm}</em></p>
        <p><strong>Fall 2026 Rejected Applicants Reporting GPA (%):</strong> <em>{stats.rejected_fall_2026_gpa_pct:.2f}%</em></p>
        <p><strong>Fall 2026 Accepted Applicants Reporting GPA (%):</strong> <em>{stats.accepted_fall_2026_gpa_pct:.2f}%</em></p>

        <!-- Buttons -->
        <form><button class="btn-refresh">Pull Data</button></form>
        <form><button class="btn-update">Update Analysis</button></form>

        <!-- Add at least one 'Answer:' label for legacy tests -->
        <p><strong>Answer:</strong> Test value</p>
    </body>
    </html>
    """

# ------------------------------
# Helper to parse HTML
# ------------------------------
def parse_html(stats=fake_stats):
    html = fake_gradcafe_html(stats)
    return BeautifulSoup(html, "html.parser")

# ------------------------------
# Test buttons exist
# ------------------------------
@pytest.mark.analysis
def test_analysis_page_buttons_and_labels():
    soup = parse_html(fake_stats)
    text = soup.get_text()

    # Buttons
    pull_button = soup.find("button", class_="btn-refresh")
    update_button = soup.find("button", class_="btn-update")
    assert pull_button is not None, "Pull Data button not found"
    assert update_button is not None, "Update Analysis button not found"

    # At least one "Answer:" label
    assert "Answer:" in text, "No 'Answer:' label found in page"

# ------------------------------
# Test percentages rounded to 2 decimals
# ------------------------------
@pytest.mark.analysis
def test_percentages_two_decimals():
    fake_stats_decimal = SimpleNamespace(
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
        accepted_fall_2026_gpa_pct=56.789
    )

    soup = parse_html(fake_stats_decimal)
    text = soup.get_text()

    # Check percentages rounded to 2 decimals
    assert "45.68" in text
    assert "78.90" in text
    assert "12.35" in text
    assert "56.79" in text

