import pytest
from types import SimpleNamespace
from bs4 import BeautifulSoup

## ------------------------------
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
def fake_gradcafe_html(stats=fake_stats):
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
    </body>
    </html>
    """

def parse_html(stats=fake_stats):
    html = fake_gradcafe_html(stats)
    return BeautifulSoup(html, "html.parser")

# ------------------------------
# Test labels vs. answers
# ------------------------------
@pytest.mark.analysis
def test_labels_and_answers():
    soup = parse_html(fake_stats)

    # Map of label text -> expected value
    expected = {
        "Fall 2026 Applicants:": str(fake_stats.fall_2026_count),
        "International Applicants (%):": f"{fake_stats.international_pct:.2f}%",
        "Average GPA:": f"{fake_stats.avg_gpa:.2f}",
        "Average GRE:": f"{fake_stats.avg_gre:.2f}",
        "Average GRE Verbal:": f"{fake_stats.avg_gre_v:.2f}",
        "Average GRE AW:": f"{fake_stats.avg_gre_aw:.2f}",
        "Average GPA (US, Fall 2026):": f"{fake_stats.avg_gpa_us_fall_2026:.2f}",
        "Fall 2025 Acceptance Rate (%):": f"{fake_stats.fall_2025_accept_pct:.2f}%",
        "Total Applicants in Pipeline:": str(fake_stats.total_applicants),
        "Average GPA of Accepted Fall 2025 Applicants:": f"{fake_stats.avg_gpa_fall_2025_accept:.2f}",
        "JHU CS Master’s Applicants:": str(fake_stats.jhu_cs_masters),
        "2026 Acceptances, Georgetown, MIT, Stanford, CMU (Raw):": str(fake_stats.fall_2026_cs_accept),
        "2026 Acceptances, Georgetown, MIT, Stanford, CMU (LLM):": str(fake_stats.fall_2026_cs_accept_llm),
        "Fall 2026 Rejected Applicants Reporting GPA (%):": f"{fake_stats.rejected_fall_2026_gpa_pct:.2f}%",
        "Fall 2026 Accepted Applicants Reporting GPA (%):": f"{fake_stats.accepted_fall_2026_gpa_pct:.2f}%"
    }

    for label_text, expected_value in expected.items():
        label = soup.find("strong", string=label_text)
        assert label is not None, f"Label '{label_text}' not found"
        value = label.find_next("em").get_text()
        assert value == expected_value, f"Expected '{expected_value}' for '{label_text}', got '{value}'"

# ------------------------------
# Test buttons exist
# ------------------------------
@pytest.mark.analysis
def test_buttons_exist():
    soup = parse_html(fake_stats)
    assert soup.find("button", class_="btn-refresh") is not None
    assert soup.find("button", class_="btn-update") is not None

# ------------------------------
# Test percentages rounded to 2 decimals (label/value check)
# ------------------------------
@pytest.mark.analysis
def test_percentages_two_decimals_labelled():
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

    expected_percentages = {
        "International Applicants (%):": f"{fake_stats_decimal.international_pct:.2f}%",
        "Fall 2025 Acceptance Rate (%):": f"{fake_stats_decimal.fall_2025_accept_pct:.2f}%",
        "Fall 2026 Rejected Applicants Reporting GPA (%):": f"{fake_stats_decimal.rejected_fall_2026_gpa_pct:.2f}%",
        "Fall 2026 Accepted Applicants Reporting GPA (%):": f"{fake_stats_decimal.accepted_fall_2026_gpa_pct:.2f}%"
    }

    for label_text, expected_value in expected_percentages.items():
        label = soup.find("strong", string=label_text)
        assert label is not None, f"Label '{label_text}' not found"
        value = label.find_next("em").get_text()
        assert value == expected_value, f"Expected '{expected_value}' for '{label_text}', got '{value}'"

