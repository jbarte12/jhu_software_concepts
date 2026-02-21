"""
tests.test_scraper_clean
=========================

Tests for the initial GradCafe data pipeline: scraping and cleaning.

This file covers the backend work required to pull the **initial dataset**
from the GradCafe website. That dataset serves as the foundation for the
entire application — everything else (LLM enrichment, DB sync, analysis)
builds on top of it via the re-scrape flow.

Modules under test:

- :mod:`src.scrape.scrape` — HTTP fetching, HTML parsing, detail-page
  enrichment, and file I/O for the raw scraped data.
- :mod:`src.scrape.clean` — Normalization of raw records into the final
  application schema.
- :func:`src.refresh_gradcafe.get_seen_ids_from_llm_extend_file` — ID
  deduplication helper used across scrape runs.

All tests are marked ``database`` and use ``monkeypatch`` / ``tmp_path``
to avoid touching real network or disk resources.
"""

import json
import pytest
from pathlib import Path
from bs4 import BeautifulSoup

from src.scrape import scrape
from src.scrape import clean


# ============================================================
# SHARED FIXTURES
# ============================================================

@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    """Disable ``time.sleep`` globally so retry loops complete instantly.

    Applied automatically to every test in this module.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    """
    monkeypatch.setattr("time.sleep", lambda x: None)


# ============================================================
# FAKE HTML CONSTANTS
# ============================================================

#: A single realistic GradCafe survey-page row (Political Science PhD, accepted).
FAKE_SURVEY_HTML = """
<tr>
<td class="tw-px-3 tw-py-5 tw-text-sm tw-text-gray-500">
    <div class="tw-text-gray-900">
        <a href="/result/FAKE_ID">Political Science</a>
        <svg viewBox="0 0 2 2" class="tw-h-0.5 tw-w-0.5 tw-fill-gray-400 tw-mx-1 tw-inline-block">
            <circle cx="1" cy="1" r="1" />
        </svg>
        <span class="tw-text-gray-500">PhD</span>
    </div>
</td>
<td class="tw-px-3 tw-py-5 tw-text-sm tw-text-gray-500 tw-whitespace-nowrap tw-hidden md:tw-table-cell ">
    February 18, 2026
</td>
<td class="tw-px-3 tw-py-5 tw-text-sm tw-text-gray-500 tw-whitespace-nowrap tw-hidden md:tw-table-cell ">
    <div class="tw-inline-flex tw-items-center tw-rounded-md tw-bg-green-50 tw-text-green-700 tw-ring-green-600/20 tw-px-2 tw-py-1 tw-text-sm tw-font-medium tw-ring-1 tw-ring-inset">
        Accepted on 13 Feb
    </div>
</td>
<tr class="tw-border-none">
    <td colspan="100%" class="tw-pb-5 tw-pr-4 sm:tw-pl-0 tw-pl-4">
        <p class="tw-text-gray-500 tw-text-sm tw-my-0">4a/1w/4r/2p

Gpa is grad. IR subfield.</p>
    </td>
</tr>
"""

#: A realistic GradCafe detail-page HTML containing GPA and GRE scores.
FAKE_DETAIL_HTML = """
<main>
    <dl>
        <div>
            <dt>Program</dt>
            <dd>Political Science</dd>
        </div>
        <div>
            <dt>Degree Type</dt>
            <dd>PhD</dd>
        </div>
        <div>
            <dt>Notes</dt>
            <dd></dd>
        </div>
        <div>
            <dt>Undergrad GPA</dt>
            <dd>3.90</dd>
        </div>
        <div>
            <span>GRE General:</span><span>0</span>
        </div>
        <div>
            <span>GRE Verbal:</span><span>0</span>
        </div>
        <div>
            <span>Analytical Writing:</span><span>0.00</span>
        </div>
    </dl>
</main>
"""

#: Survey HTML containing metadata rows (term and citizenship) but no main result row.
FAKE_SURVEY_HTML_METADATA = """
<tr><td colspan="4"><div>Random text not a term or citizenship</div></td></tr>
<tr><td colspan="4"><div>Fall 2025</div></td></tr>
<tr><td colspan="4"><div>international</div></td></tr>
"""

#: Survey HTML with metadata rows and a row that has no result link (triggers ``continue``).
FAKE_SURVEY_EMPTY = """
<tr><td colspan="4"><div></div></td></tr>
<tr><td colspan="4"><div>Fall 2026</div></td></tr>
<tr><td colspan="4"><div>US</div></td></tr>
<tr><td colspan="4"><div>Some result without link</div></td></tr>
"""

#: Detail page with an empty ``<dl>`` — triggers all default/empty return paths.
FAKE_DETAIL_EMPTY = """
<main><dl></dl></main>
"""


# ============================================================
# _fetch_html
# ============================================================

@pytest.mark.db
def test_fetch_html_retry(monkeypatch):
    """Verify ``_fetch_html`` retries on transient errors and succeeds.

    Simulates two failures followed by a successful response and asserts
    that the function returns the decoded body after the retries.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    attempts = []

    class FakeResponse:
        def read(self): return b"OK"
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): return False

    def fake_urlopen(request, timeout):
        if len(attempts) < 2:
            attempts.append(1)
            raise Exception("Temporary fail")
        return FakeResponse()

    monkeypatch.setattr(scrape.urllib.request, "urlopen", fake_urlopen)

    result = scrape._fetch_html("http://fakeurl")

    assert result == "OK"
    assert len(attempts) == 2


@pytest.mark.db
def test_fetch_html_fail(monkeypatch):
    """Verify ``_fetch_html`` raises after exhausting all retries.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(
        scrape.urllib.request, "urlopen",
        lambda request, timeout: (_ for _ in ()).throw(Exception("Fail"))
    )
    with pytest.raises(Exception):
        scrape._fetch_html("http://failurl")


# ============================================================
# _clean_text
# ============================================================

@pytest.mark.db
def test_clean_text_none():
    """Verify ``_clean_text`` returns an empty string when passed ``None``."""
    assert scrape._clean_text(None) == ""


# ============================================================
# _extract_undergrad_gpa
# ============================================================

@pytest.mark.db
def test_extract_undergrad_gpa_zero_values():
    """Verify ``_extract_undergrad_gpa`` normalizes placeholder zero values to ``""``.

    Covers both ``"0"`` and ``"0.00"`` placeholder forms that GradCafe
    uses when a GPA was not submitted.
    """
    for placeholder in ("0", "0.00"):
        html = f"<dl><div><dt>Undergrad GPA</dt><dd>{placeholder}</dd></div></dl>"
        soup = BeautifulSoup(html, "html.parser")
        assert scrape._extract_undergrad_gpa(soup) == ""


@pytest.mark.db
def test_extract_undergrad_gpa_placeholder_branch():
    """Verify the ``return ""`` branch inside ``_extract_undergrad_gpa`` is reached.

    Passes a ``"0"`` GPA value through the full function call path
    (including the internal ``_extract_dt_dd`` call) to confirm the
    normalization branch executes correctly.

    :param monkeypatch: Pytest monkeypatch fixture (unused; soup built inline).
    """
    html = """
    <dl>
        <div>
            <dt>Undergrad GPA</dt>
            <dd>0</dd>
        </div>
    </dl>
    """
    soup = BeautifulSoup(html, "html.parser")
    assert scrape._extract_undergrad_gpa(soup) == ""


# ============================================================
# _extract_gre_scores
# ============================================================

@pytest.mark.db
def test_gre_scores_missing_next_span():
    """Verify ``_extract_gre_scores`` returns empty strings when the value span is absent.

    Passes HTML with a label span but no following value span, which
    triggers the ``if i + 1 >= len(spans): continue`` guard.
    """
    html = "<span>GRE General:</span>"
    soup = BeautifulSoup(html, "html.parser")
    scores = scrape._extract_gre_scores(soup)
    assert scores["gre_general"] == ""
    assert scores["gre_verbal"] == ""
    assert scores["gre_analytical_writing"] == ""


@pytest.mark.db
def test_gre_scores_skip_non_label_span():
    """Verify ``_extract_gre_scores`` ignores spans whose text does not end with ``:``.

    The span ``"Random text"`` does not match the label pattern so no
    scores should be assigned.
    """
    html = "<span>Random text</span><span>0</span>"
    soup = BeautifulSoup(html, "html.parser")
    scores = scrape._extract_gre_scores(soup)
    assert scores["gre_general"] == ""
    assert scores["gre_verbal"] == ""
    assert scores["gre_analytical_writing"] == ""


@pytest.mark.db
def test_extract_gre_scores_continue_branch():
    """Verify the ``continue`` branch fires when a GRE label span has no following span.

    Passes a single ``"GRE General:"`` span with no value span so that
    ``i + 1 >= len(spans)`` is ``True`` and the loop skips the
    assignment block entirely.
    """
    html = "<span>GRE General:</span>"
    soup = BeautifulSoup(html, "html.parser")
    scores = scrape._extract_gre_scores(soup)
    assert scores["gre_general"] == ""
    assert scores["gre_verbal"] == ""
    assert scores["gre_analytical_writing"] == ""


# ============================================================
# _scrape_detail_page
# ============================================================

@pytest.mark.db
def test_scrape_detail_page(monkeypatch):
    """Verify ``_scrape_detail_page`` extracts all fields from a realistic detail page.

    Patches ``_fetch_html`` to return :data:`FAKE_DETAIL_HTML` and asserts
    that GPA is extracted correctly and that zero-value GRE scores are
    normalized to empty strings.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(scrape, "_fetch_html", lambda url: FAKE_DETAIL_HTML)
    result = scrape._scrape_detail_page("FAKE_ID")

    assert result["program_name"] == "Political Science"
    assert result["degree_type"] == "PhD"
    assert result["comments"] == ""
    assert result["gpa"] == "3.90"
    assert result["gre_general"] == ""
    assert result["gre_verbal"] == ""
    assert result["gre_analytical_writing"] == ""


@pytest.mark.db
def test_empty_detail_fields(monkeypatch):
    """Verify ``_scrape_detail_page`` returns empty strings for all fields on a blank page.

    Patches ``_fetch_html`` to return :data:`FAKE_DETAIL_EMPTY` (an empty
    ``<dl>``) and asserts every field defaults to ``""``.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(scrape, "_fetch_html", lambda url: FAKE_DETAIL_EMPTY)
    result = scrape._scrape_detail_page("FAKE_EMPTY")

    for key in ["program_name", "degree_type", "comments", "gpa",
                "gre_general", "gre_verbal", "gre_analytical_writing"]:
        assert result[key] == ""


# ============================================================
# _parse_survey_page
# ============================================================

@pytest.mark.db
def test_parse_survey_page_metadata_assignment_with_main_result():
    """Verify ``_parse_survey_page`` assigns term and citizenship metadata to the record.

    The HTML contains one main result row followed by metadata rows for
    ``"Fall 2025"`` and ``"international"``. Asserts that both are attached
    to the parsed record via the metadata-assignment branches.
    """
    html = """
    <tr>
        <td><a href="/result/FAKE_ID">Some University</a></td>
        <td>Program</td>
        <td>Feb 18, 2026</td>
        <td>Accepted</td>
    </tr>
    <tr><td colspan="4"><div>Fall 2025</div></td></tr>
    <tr><td colspan="4"><div>international</div></td></tr>
    """
    results = scrape._parse_survey_page(html)

    assert len(results) == 1
    assert results[0]["result_id"] == "FAKE_ID"
    assert results[0]["university"] == "Some University"
    assert results[0]["start_term"] == "Fall 2025"
    assert results[0]["International/US"] == "International"


@pytest.mark.db
def test_parse_survey_page_skip_no_link():
    """Verify ``_parse_survey_page`` skips rows that have no result link.

    The first row has no ``<a href="/result/...">`` so it triggers the
    ``if not link: continue`` branch. Only the second row should be returned.
    """
    html = """
    <tr>
        <td>Random University</td>
        <td>Program</td>
        <td>Feb 18, 2026</td>
        <td>Accepted</td>
    </tr>
    <tr>
        <td><a href="/result/FAKE_ID">Some University</a></td>
        <td>Program</td>
        <td>Feb 18, 2026</td>
        <td>Accepted</td>
    </tr>
    """
    results = scrape._parse_survey_page(html)

    assert len(results) == 1
    assert results[0]["result_id"] == "FAKE_ID"
    assert results[0]["university"] == "Some University"


# ============================================================
# scrape_data (full pipeline)
# ============================================================

@pytest.mark.db
def test_scrape_data(monkeypatch, tmp_path):
    """Verify ``scrape_data`` runs the full survey + detail pipeline and writes output.

    Patches ``_fetch_html`` to return survey HTML for survey URLs and
    detail HTML otherwise, limits scraping to 1 record, and asserts that:

    - Exactly one record is returned.
    - The output JSON file is created.
    - Survey fields (``result_id``, ``url_link``, ``date_added``) are populated.
    - Detail fields (``program_name``, ``gpa``, GRE scores) are merged in correctly.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    tmp_file = tmp_path / "output.json"
    monkeypatch.setattr(scrape, "OUTPUT_FILE", str(tmp_file))
    monkeypatch.setattr(scrape, "SAVE_EVERY", 1)
    monkeypatch.setattr(scrape, "MAX_RECORDS", 1)

    def fake_fetch(url):
        if "survey" in url:
            return FAKE_SURVEY_HTML
        return FAKE_DETAIL_HTML

    monkeypatch.setattr(scrape, "_fetch_html", fake_fetch)

    results = scrape.scrape_data()

    assert len(results) == 1
    assert tmp_file.exists()

    record = results[0]
    assert record["result_id"] == "FAKE_ID"
    assert record["url_link"].endswith("/FAKE_ID")
    assert record["date_added"] != ""
    assert record["applicant_status"] != ""
    assert record["program_name"] == "Political Science"
    assert record["degree_type"] == "PhD"
    assert record["comments"] == ""
    assert record["gpa"] == "3.90"
    assert record["gre_general"] == ""
    assert record["gre_verbal"] == ""
    assert record["gre_analytical_writing"] == ""


@pytest.mark.db
def test_survey_page_continue_and_metadata(monkeypatch):
    """Verify ``scrape_data`` correctly assigns metadata even when rows lack result links.

    Uses :data:`FAKE_SURVEY_EMPTY` which contains metadata rows for
    ``"Fall 2026"`` and ``"US"`` but no anchor link. For any records
    that do get parsed, asserts the metadata fields are set correctly.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(scrape, "_fetch_html", lambda url: FAKE_SURVEY_EMPTY)
    monkeypatch.setattr(scrape, "MAX_RECORDS", 1)

    results = scrape.scrape_data()

    for r in results:
        assert "start_term" in r
        assert r["start_term"] == "Fall 2026"
        assert "International/US" in r
        assert r["International/US"] == "US"


@pytest.mark.db
def test_survey_page_break(monkeypatch):
    """Verify ``scrape_data`` exits cleanly when a survey page returns no results.

    Passes blank HTML so ``_parse_survey_page`` returns ``[]``, triggering
    the ``if not page_results: break`` early-exit branch. Asserts the
    function returns an empty list without raising.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(scrape, "_fetch_html", lambda url: "<html></html>")
    monkeypatch.setattr(scrape, "MAX_RECORDS", 1)

    assert scrape.scrape_data() == []


# ============================================================
# save_data (scrape)
# ============================================================

@pytest.mark.db
def test_save_data_with_gradcafe_format(tmp_path, monkeypatch):
    """Verify ``scrape.save_data`` writes JSON correctly and prints confirmation.

    Uses two realistic GradCafe-format records and asserts that:

    - The output file is created.
    - The loaded JSON matches the input exactly.
    - The printed message contains the record count and file path.

    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    data = [
        {
            "program_name": "History",
            "university": "University of Georgia",
            "degree_type": "PhD",
            "comments": "to check portal",
            "date_added": "February 06, 2026",
            "url_link": "https://www.thegradcafe.com/result/997458",
            "applicant_status": "Accepted: 6 Feb",
            "start_term": "Fall 2026",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "3.70",
        },
        {
            "program_name": "Applied Physics",
            "university": "Rice University",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 06, 2026",
            "url_link": "https://www.thegradcafe.com/result/997457",
            "applicant_status": "Waitlisted",
            "start_term": "Fall 2025",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "3.40",
        },
    ]
    tmp_file = tmp_path / "gradcafe_output.json"
    monkeypatch.setattr(scrape, "OUTPUT_FILE", str(tmp_file))

    printed = {}
    monkeypatch.setattr("builtins.print", lambda msg: printed.setdefault("msg", msg))

    scrape.save_data(data)

    assert tmp_file.exists()
    assert json.loads(tmp_file.read_text(encoding="utf-8")) == data
    assert f"Saved {len(data)} records to" in printed["msg"]
    assert str(tmp_file) in printed["msg"]


# ============================================================
# clean.load_data and clean.clean_data
# ============================================================

@pytest.mark.db
def test_load_data_reads_json(tmp_path, monkeypatch):
    """Verify ``clean.load_data`` reads raw JSON and ``clean_data`` normalizes it.

    Writes two sample records to a temp file, patches ``RAW_FILE``, and asserts:

    - ``load_data`` returns a list of length 2 with correct field values.
    - ``_norm`` strips whitespace and handles empty strings.
    - ``clean_data`` preserves program names, GPAs, comments, and statuses.

    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    sample_data = [
        {
            "program_name": "History",
            "university": "University of Georgia",
            "degree_type": "PhD",
            "comments": "to check portal",
            "date_added": "February 06, 2026",
            "url_link": "https://www.thegradcafe.com/result/997458",
            "applicant_status": "Accepted: 6 Feb",
            "start_term": "Fall 2026",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "3.70",
        },
        {
            "program_name": "Applied Physics",
            "university": "Rice University",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 06, 2026",
            "url_link": "https://www.thegradcafe.com/result/997457",
            "applicant_status": "Waitlisted",
            "start_term": "Fall 2025",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "3.40",
        },
    ]
    tmp_file = tmp_path / "applicant_data.json"
    tmp_file.write_text(json.dumps(sample_data, indent=2), encoding="utf-8")
    monkeypatch.setattr(clean, "RAW_FILE", str(tmp_file))

    raw = clean.load_data()
    assert isinstance(raw, list)
    assert len(raw) == 2
    assert raw[0]["program_name"] == "History"
    assert raw[1]["university"] == "Rice University"

    assert clean._norm(raw[0]["program_name"]) == "History"
    assert clean._norm(raw[1]["comments"]) == ""

    cleaned = clean.clean_data(raw)
    assert cleaned[0]["program_name"] == "History"
    assert cleaned[0]["gpa"] == "3.70"
    assert cleaned[1]["program_name"] == "Applied Physics"
    assert cleaned[1]["comments"] == ""
    assert cleaned[1]["applicant_status"] == "Waitlisted"


# ============================================================
# clean._normalize_status
# ============================================================

@pytest.mark.db
def test_normalize_status_branches():
    """Verify ``_normalize_status`` covers all decision-status normalization branches.

    Tests each branch in order:

    1. Empty / ``None`` input returns ``""``.
    2. Waitlist variations normalize to ``"Waitlisted"``.
    3. Interview variations normalize to ``"Interview"``.
    4. Accepted/Rejected with a parseable date → ``"Decision: D Mon"`` format.
    5. Accepted/Rejected without a date → bare decision word.
    6. All other statuses are returned unchanged.
    """
    # 1. Empty / None
    assert clean._normalize_status("") == ""
    assert clean._normalize_status(None) == ""

    # 2. Waitlist
    assert clean._normalize_status("waitlisted") == "Waitlisted"
    assert clean._normalize_status("Wait List") == "Waitlisted"
    assert clean._normalize_status("WAIT for decision") == "Waitlisted"

    # 3. Interview
    assert clean._normalize_status("Interview") == "Interview"
    assert clean._normalize_status("interview scheduled") == "Interview"
    assert clean._normalize_status("INTERVIEW round 1") == "Interview"

    # 4. Accepted / Rejected with date
    assert clean._normalize_status("Accepted: 6 Feb") == "Accepted: 6 Feb"
    assert clean._normalize_status("accepted 12 Mar") == "accepted: 12 Mar"
    assert clean._normalize_status("Rejected 2 Dec") == "Rejected: 2 Dec"

    # 5. Accepted / Rejected without date
    assert clean._normalize_status("Accepted") == "Accepted"
    assert clean._normalize_status("accepted") == "accepted"
    assert clean._normalize_status("Rejected") == "Rejected"

    # 6. Unrecognized status — returned as-is
    assert clean._normalize_status("Pending decision") == "Pending decision"
    assert clean._normalize_status("Deferred") == "Deferred"
    assert clean._normalize_status("Under review") == "Under review"


# ============================================================
# clean.save_data
# ============================================================

@pytest.mark.db
def test_save_data_writes_file_and_prints(tmp_path, monkeypatch):
    """Verify ``clean.save_data`` writes cleaned JSON and prints a confirmation message.

    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    data = [
        {
            "program_name": "History",
            "university": "University of Georgia",
            "degree_type": "PhD",
            "comments": "to check portal",
            "date_added": "February 06, 2026",
            "url_link": "https://www.thegradcafe.com/result/997458",
            "applicant_status": "Accepted: 6 Feb",
            "start_term": "Fall 2026",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "3.70",
        }
    ]
    tmp_file = tmp_path / "cleaned_output.json"
    monkeypatch.setattr(clean, "OUT_FILE", str(tmp_file))

    printed = {}
    monkeypatch.setattr("builtins.print", lambda msg: printed.setdefault("message", msg))

    clean.save_data(data)

    assert tmp_file.exists()
    assert json.loads(tmp_file.read_text(encoding="utf-8")) == data
    assert printed["message"] == f"Cleaned data saved to {tmp_file}"


# ============================================================
# get_seen_ids_from_llm_extend_file
# ============================================================

@pytest.mark.db
def test_get_seen_ids_file_not_found():
    """Verify ``get_seen_ids_from_llm_extend_file`` returns an empty set when the file is missing."""
    from src.refresh_gradcafe import get_seen_ids_from_llm_extend_file

    seen_ids = get_seen_ids_from_llm_extend_file(path="/tmp/definitely_not_exist.json")

    assert seen_ids == set()


@pytest.mark.db
def test_get_seen_ids_json_decode_error(tmp_path):
    """Verify ``get_seen_ids_from_llm_extend_file`` skips lines with invalid JSON.

    Writes one malformed JSON line and asserts the result is an empty set
    (the bad line is skipped rather than raising).

    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    from src.refresh_gradcafe import get_seen_ids_from_llm_extend_file

    bad_file = tmp_path / "bad.json"
    bad_file.write_text("{ this is not valid json }\n")

    assert get_seen_ids_from_llm_extend_file(path=bad_file) == set()


@pytest.mark.db
def test_get_seen_ids_no_url(tmp_path):
    """Verify ``get_seen_ids_from_llm_extend_file`` skips records that have no ``url_link``.

    Writes a valid JSON object that lacks the ``url_link`` key and asserts
    the result is an empty set.

    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    from src.refresh_gradcafe import get_seen_ids_from_llm_extend_file

    no_url_file = tmp_path / "no_url.json"
    no_url_file.write_text(json.dumps({"some_key": 123}) + "\n")

    assert get_seen_ids_from_llm_extend_file(path=no_url_file) == set()