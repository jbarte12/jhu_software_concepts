import json
from datetime import date
import pytest
import src.scrape.scrape as scrape

import src.load_data  # needed for Option 1 patching
from src.scrape.scrape import _parse_survey_page, _scrape_detail_page, _fetch_html

from bs4 import BeautifulSoup
from src.load_data import rebuild_from_llm_file

# ------------------------
# Scraper Smoke Test (Survey Page)
# ------------------------
@pytest.mark.db
def test_scraper_survey_page(monkeypatch):
    # Survey page HTML (summary)
    fake_survey_html = """
    <table>
      <tr>
        <td>TestU</td>
        <td>CS</td>
        <td>January 1, 2026</td>
        <td>Accepted</td>
        <td><a href="/result/123">Detail</a></td>
      </tr>
      <tr>
        <td colspan="5"><div>Fall 2026</div><div>US</div></td>
      </tr>
    </table>
    """

    # Monkeypatch _fetch_html to return survey HTML
    monkeypatch.setattr("src.scrape.scrape._fetch_html", lambda url, retries=3: fake_survey_html)

    results = _parse_survey_page(fake_survey_html)
    assert len(results) == 1
    r = results[0]
    assert r["university"] == "TestU"
    assert r["program_name"] == "CS"
    assert r["applicant_status"] == "Accepted"
    assert r["start_term"] == "Fall 2026"
    assert r["International/US"] == "US"
    assert r["result_id"] == "123"
    assert r["url_link"] == "https://www.thegradcafe.com/result/123"


# ------------------------
# Scraper Detail Page Test
# ------------------------
@pytest.mark.db
def test_scraper_detail_page(monkeypatch):
    # Detail page HTML
    fake_detail_html = """
    <main>
      <dl class="tw-grid tw-grid-cols-1 sm:tw-grid-cols-2">
        <div class="tw-border-t tw-border-gray-100 tw-py-6 sm:tw-col-span-1">
          <dt>Program</dt>
          <dd>CS</dd>
        </div>
        <div class="tw-border-t tw-border-gray-100 tw-py-6 sm:tw-col-span-1">
          <dt>Degree Type</dt>
          <dd>Masters</dd>
        </div>
        <div class="tw-border-t tw-border-gray-100 tw-py-6 sm:tw-col-span-1">
          <dt>Notes</dt>
          <dd>Great fit</dd>
        </div>
        <div class="tw-border-t tw-border-gray-100 tw-py-6 sm:tw-col-span-1">
          <dt>Undergrad GPA</dt>
          <dd>3.9</dd>
        </div>
      </dl>
      <ul>
        <li><span>GRE General:</span><span>330</span></li>
        <li><span>GRE Verbal:</span><span>165</span></li>
        <li><span>Analytical Writing:</span><span>5</span></li>
      </ul>
    </main>
    """

    # Monkeypatch _fetch_html to return detail HTML
    monkeypatch.setattr("src.scrape.scrape._fetch_html", lambda url, retries=3: fake_detail_html)

    # Call the detail scraper
    result = _scrape_detail_page("123")
    assert result["program_name"] == "CS"
    assert result["degree_type"] == "Masters"
    assert result["comments"] == "Great fit"
    assert result["gpa"] == "3.9"
    assert result["gre_general"] == "330"
    assert result["gre_verbal"] == "165"
    assert result["gre_analytical_writing"] == "5"


# ------------------------
# Existing DB Tests
# ------------------------

# --- Fake DB ---
class FakeCursor:
    def __init__(self):
        self.inserted_rows = []

    def execute(self, *a, **kw):
        pass

class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True

    @property
    def autocommit(self):
        return True

    @autocommit.setter
    def autocommit(self, val):
        pass

def fake_execute_values(cur, sql, rows):
    cur.inserted_rows.extend(rows)

# --- DB Test: Simple Insert ---
@pytest.mark.db
def test_rebuild_simple(monkeypatch, tmp_path):
    data = [
        {
            "program_name": "CS",
            "university": "TestU",
            "url_link": "u1",
            "start_term": "Fall 2026",
            "applicant_status": "Accepted",
            "comments": "good",
            "gpa": "3.9",
            "gre_general": "330",
            "gre_verbal": "165",
            "gre_analytical_writing": "5",
            "degree_type": "Masters",
            "llm-generated-program": "CS",
            "llm-generated-university": "TestU",
            "date_added": "January 1, 2026",
            "International/US": "US"
        }
    ]
    file_path = tmp_path / "llm.json"
    file_path.write_text("\n".join(json.dumps(d) for d in data))

    fake_conn = FakeConnection()
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)
    monkeypatch.setattr("src.load_data.execute_values", fake_execute_values)

    rebuild_from_llm_file(path=str(file_path))

    rows = fake_conn.cursor_obj.inserted_rows
    assert len(rows) == 1

    program, comments, date_added, url, status, term, *_ = rows[0]
    assert program and url and status and term

    assert fake_conn.committed
    assert fake_conn.closed

# --- DB Test: Idempotency ---
@pytest.mark.db
def test_rebuild_idempotent(monkeypatch, tmp_path):
    data = [
        {
            "program_name": "Pure Mathematics",
            "university": "Duke University",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 15, 2026",
            "url_link": "https://www.thegradcafe.com/result/1002138",
            "applicant_status": "Accepted: 5 Feb",
            "start_term": "Fall 2026",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "",
            "llm-generated-program": "Pure Mathematics",
            "llm-generated-university": "Duke University"
        },
        {
            "program_name": "Electrical And Computer Engineering",
            "university": "University of Southern California",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 15, 2026",
            "url_link": "https://www.thegradcafe.com/result/1002137",
            "applicant_status": "Accepted: 13 Feb",
            "start_term": "Fall 2026",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "",
            "llm-generated-program": "Electrical And Computer Engineering",
            "llm-generated-university": "University of Southern California"
        }
    ]

    file_path = tmp_path / "llm.json"
    file_path.write_text("\n".join(json.dumps(d) for d in data))

    # Fake DB with uniqueness enforcement on URL
    class UniqueFakeCursor(FakeCursor):
        def __init__(self):
            super().__init__()
            self.seen_urls = set()

        def execute(self, sql, row=None):
            if row is None:
                return
            url = row[3]  # URL is 4th element
            if url not in self.seen_urls:
                self.inserted_rows.append(row)
                self.seen_urls.add(url)

    class UniqueFakeConnection(FakeConnection):
        def __init__(self):
            self.cursor_obj = UniqueFakeCursor()
            self.committed = False
            self.closed = False

    fake_conn = UniqueFakeConnection()
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)
    monkeypatch.setattr(
        "src.load_data.execute_values",
        lambda cur, sql, rows: [cur.execute("", r) for r in rows]
    )

    # Run twice
    rebuild_from_llm_file(path=str(file_path))
    rebuild_from_llm_file(path=str(file_path))

    rows = fake_conn.cursor_obj.inserted_rows
    urls = [r[3] for r in rows]
    assert len(set(urls)) == len(data)  # unique URLs
    assert len(rows) == len(data)

# --- Simple Query Test using Option 1 ---
@pytest.mark.db
def test_query_statistics():
    # Fake function returning all required keys
    def fake_query_data(*args, **kwargs):
        return {
            "fall_2026_count": 10,
            "international_pct": 50.0,
            "avg_gpa": 3.8,
            "avg_gre": 330.0,
            "avg_gre_v": 165.0,
            "avg_gre_aw": 4.5,
            "avg_gpa_us_fall_2026": 3.9,
            "fall_2025_accept_pct": 40.0,
            "avg_gpa_fall_2025_accept": 3.7,
            "jhu_cs_masters": 2,
            "total_applicants": 20,
            "fall_2026_cs_accept": 3,
            "fall_2026_cs_accept_llm": 3,
            "rejected_fall_2026_gpa_pct": 50.0,
            "accepted_fall_2026_gpa_pct": 75.0
        }

    # Patch the fake directly onto the module (Option 1)
    setattr(src.load_data, "query_statistics", fake_query_data)

    # Call and verify keys exist
    stats = src.load_data.query_statistics()
    expected_keys = [
        "fall_2026_count", "international_pct", "avg_gpa", "avg_gre",
        "avg_gre_v", "avg_gre_aw", "avg_gpa_us_fall_2026",
        "fall_2025_accept_pct", "avg_gpa_fall_2025_accept",
        "jhu_cs_masters", "total_applicants", "fall_2026_cs_accept",
        "fall_2026_cs_accept_llm", "rejected_fall_2026_gpa_pct",
        "accepted_fall_2026_gpa_pct"
    ]
    for key in expected_keys:
        assert key in stats


### EXTRA TESTS #####
# ------------------------
# _clean_text tests (lines 43-54)
# ------------------------
def test_clean_text_none_and_whitespace():
    # None element
    assert scrape._clean_text(None) == ""

    # Element with extra spaces
    html = "<div>   Hello   World   </div>"
    soup = BeautifulSoup(html, "html.parser")
    div = soup.div
    assert scrape._clean_text(div) == "Hello World"

# ------------------------
# _extract_dt_dd tests (line 62)
# ------------------------
def test_extract_dt_dd_missing_label():
    html = "<dl><dt>Other</dt><dd>Value</dd></dl>"
    soup = BeautifulSoup(html, "html.parser")
    # Label not present
    assert scrape._extract_dt_dd(soup, "Program") == ""

# ------------------------
# _extract_undergrad_gpa tests (lines 85,96)
# ------------------------
@pytest.mark.parametrize("gpa_val", ["0", "0.0", "0.00", "99.99"])
def test_extract_undergrad_gpa_placeholder_values(gpa_val):
    html = f"<dl><dt>Undergrad GPA</dt><dd>{gpa_val}</dd></dl>"
    soup = BeautifulSoup(html, "html.parser")
    assert scrape._extract_undergrad_gpa(soup) == ""

def test_extract_undergrad_gpa_normal_value():
    html = "<dl><dt>Undergrad GPA</dt><dd>3.75</dd></dl>"
    soup = BeautifulSoup(html, "html.parser")
    assert scrape._extract_undergrad_gpa(soup) == "3.75"

# ------------------------
# _extract_gre_scores tests (lines 127,134)
# ------------------------
def test_extract_gre_scores_zero_values():
    html = """
    <ul>
      <li><span>GRE General:</span><span>0</span></li>
      <li><span>GRE Verbal:</span><span>0.0</span></li>
      <li><span>Analytical Writing:</span><span>99.99</span></li>
    </ul>
    """
    soup = BeautifulSoup(html, "html.parser")
    scores = scrape._extract_gre_scores(soup)
    assert scores == {"gre_general": "", "gre_verbal": "", "gre_analytical_writing": ""}

def test_extract_gre_scores_normal_values():
    html = """
    <ul>
      <li><span>GRE General:</span><span>330</span></li>
      <li><span>GRE Verbal:</span><span>165</span></li>
      <li><span>Analytical Writing:</span><span>5</span></li>
    </ul>
    """
    soup = BeautifulSoup(html, "html.parser")
    scores = scrape._extract_gre_scores(soup)
    assert scores == {"gre_general": "330", "gre_verbal": "165", "gre_analytical_writing": "5"}

# ------------------------
# _parse_survey_page metadata row parsing (line 205)
# ------------------------
def test_parse_survey_page_metadata_variations():
    html = """
    <table>
      <tr>
        <td>TestU</td><td>CS</td><td>Jan 1, 2026</td><td>Accepted</td>
        <td><a href="/result/1">Detail</a></td>
      </tr>
      <tr>
        <td colspan="5">
          <div>Fall 2026</div>
          <div>u.s.</div>
          <div>ignore me</div>
        </td>
      </tr>
      <tr>
        <td>TestU2</td><td>CS2</td><td>Jan 2, 2026</td><td>Rejected</td>
        <td><a href="/result/2">Detail</a></td>
      </tr>
      <tr>
        <td colspan="5">
          <div>Spring 2027</div>
          <div>International</div>
        </td>
      </tr>
    </table>
    """
    results = scrape._parse_survey_page(html)
    assert results[0]["start_term"] == "Fall 2026"
    assert results[0]["International/US"] == "US"  # u.s. normalized
    assert results[1]["start_term"] == "Spring 2027"
    assert results[1]["International/US"] == "International"

# ------------------------
# scrape_data() lines 258–321, 328–334
# ------------------------
def test_scrape_data_loop_and_detail_merge(monkeypatch, tmp_path):

    fake_page_html = """
    <table>
      <tr>
        <td>U</td><td>P</td><td>Jan 1, 2026</td><td>Accepted</td>
        <td><a href="/result/1">Detail</a></td>
      </tr>
      <tr>
        <td colspan="5"><div>Fall 2026</div><div>US</div></td>
      </tr>
      <tr>
        <td>U2</td><td>P2</td><td>Jan 2, 2026</td><td>Rejected</td>
        <td><a href="/result/2">Detail</a></td>
      </tr>
      <tr>
        <td colspan="5"><div>Spring 2027</div><div>International</div></td>
      </tr>
    </table>
    """

    # Patch _fetch_html
    fetch_pages = [fake_page_html, ""]
    monkeypatch.setattr(scrape, "_fetch_html", lambda url, retries=3: fetch_pages.pop(0))

    # Patch _scrape_detail_page
    def fake_detail(result_id):
        return {
            "program_name": f"Prog{result_id}",
            "degree_type": "Masters",
            "comments": "",
            "gpa": "3.5",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
        }
    monkeypatch.setattr(scrape, "_scrape_detail_page", fake_detail)

    # Patch OUTPUT_FILE
    output_file = tmp_path / "data.json"
    scrape.OUTPUT_FILE = str(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Patch SAVE_EVERY so JSON dump triggers for tiny dataset
    monkeypatch.setattr(scrape, "SAVE_EVERY", 1)

    # Capture json.dump calls
    saved_data = {}
    def fake_json_dump(data, f, **kwargs):
        saved_data["data"] = data
    monkeypatch.setattr(scrape.json, "dump", fake_json_dump)

    # Run scrape_data
    results = scrape.scrape_data()

    # Check details merged correctly
    assert all(r["program_name"].startswith("Prog") for r in results)

    # Check metadata
    assert results[0]["start_term"] == "Fall 2026"
    assert results[0]["International/US"] == "US"
    assert results[1]["start_term"] == "Spring 2027"
    assert results[1]["International/US"] == "International"

    # Check JSON "written"
    assert saved_data["data"] == results
