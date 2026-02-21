Name: Jayna Bartel (jbarte12)


Module Info: Module 4 - Testing and Documentation Assignment
             Due on 02/12/2026 at 11:59 EST

Approach:

- Overall:
This assignment contains the test suite for the GradCafe Analysis Pipeline
built in the previous module. The goal is to verify that the application
works correctly at every layer: the Flask web app, the database, the scraper,
the buttons, and the analysis page formatting. Tests are organized into five
marker groups and run automatically via a GitHub Actions workflow on every push.
All tests are written to run fully offline using monkeypatch and tmp_path so no
real database, network connection, or LLM is required to run them.

------Test Files------

- test_scraper_function.py (marker: db):
This file tests the scraping and cleaning functions that form the foundation
of the data pipeline. Tests are ordered to follow the call chain of the source
code: _fetch_html, _clean_text, _extract_undergrad_gpa, _extract_gre_scores,
_scrape_detail_page, _parse_survey_page, scrape_data, save_data, the clean
module, and get_seen_ids. All scraper functions are patched so no real HTTP
requests are made. The goal is to confirm the scraper correctly parses HTML,
handles missing or malformed data, and that the clean module normalizes records
before they are saved.

- test_db_insert.py (marker: db):
This file tests that the database layer loads and queries data correctly. It
covers three things. First, it tests insert on pull: before a pull the target
table is empty, and after a load operation new rows exist with all required
non-null fields (program, date_added, url, status, term). Second, it tests
idempotency: duplicate rows sharing the same URL do not create duplicate entries
in the database, matching the ON CONFLICT DO NOTHING behavior in load_data.py.
Third, it tests query correctness: get_application_stats returns a dictionary
containing all fifteen expected stat keys. All database calls are intercepted
using FakeConnection and FakeCursor classes so no real PostgreSQL instance is
needed.

- test_flask_page.py (marker: analysis):
This file tests that the Flask app is created correctly and that the main
analysis page renders as expected. It verifies that the app factory returns a
properly configured Flask instance, that all expected routes are registered
(/analysis, /refresh, /update-analysis), that GET /analysis returns HTTP 200,
that both action buttons are present on the page, and that at least one stat
category and its value appear in the rendered HTML. Stats and state are
monkeypatched so no database is needed.

- test_buttons.py (marker: buttons, web):
This file tests the two action buttons and the busy-state gating logic. For
the Pull Data button (POST /refresh) it verifies that the route calls refresh(),
that the full scrape-and-save pipeline runs with fake rows, that it returns 200
after redirect, and that exceptions are caught and written to the state file.
For the Update Analysis button (POST /update-analysis) it verifies that
update_data and sync_db_from_llm_file are both called, that it returns 200 after
redirect, and that exceptions are handled gracefully. The busy-state tests
confirm that both routes return HTTP 409 when either pulling_data or
updating_analysis is True in the state file, preventing concurrent runs.
Threading is patched to run synchronously so background jobs complete before
assertions are evaluated.

- test_analysis_format.py (marker: analysis):
This file tests the visual contract of the analysis page. Rather than using a
hardcoded HTML copy, it renders the real gradcafe_stats.html Jinja template
through a Flask test client with fake stats injected. This means if a label is
renamed or formatting changes in the template, the tests will catch it. Two
things are tested. First, labels and answers: every stat category label
(<strong>) is paired with the correctly formatted value (<em>) for all fifteen
stats. Second, percentage rounding: all percentage fields are formatted to
exactly two decimal places, verified by passing high-precision floats
(e.g. 45.6789) and confirming the Jinja %.2f filter renders 45.68%.

- test_integration_end_to_end.py (marker: integration):
This file tests the full lifecycle of the application from Flask startup through
the complete pull-update-render pipeline. It covers Flask app startup and route
registration, POST /refresh success and 409 conflict handling, deduplication
of already-seen record IDs during a pull, all branches of scrape_new_records
via parametrize (early exit on SEEN_LIMIT and empty-page break), enrich_with_details
via ThreadPoolExecutor, write_new_applicant_file file I/O, the full refresh()
pipeline, update_data LLM processing branches (success, file not found, empty
file), sync_db_from_llm_file NDJSON-to-DB insertion, POST /update-analysis
conflict handling, and a complete end-to-end happy path test that runs pull
then update then render and asserts the page contains updated stats. A second
end-to-end test runs two pulls with overlapping data and verifies that exactly
three unique records are present after deduplication.

------GitHub Actions------

- .github/workflows/tests.yml:
This file defines a CI workflow that runs automatically on every push and pull
request to any branch. It spins up a PostgreSQL 15 service container matching
the credentials used in load_data.py (sm_app, postgres, abc123), installs
dependencies from requirements.txt, and then runs each marker group as a
separate step: integration, db, analysis, buttons, and web. Running them
separately means a failure in one group does not hide results from others.

------Sphinx Documentation------

- source/conf.py:
Configures Sphinx to find source modules and test modules via sys.path. Enables
sphinx.ext.autodoc to pull docstrings automatically and sphinx.ext.napoleon to
render :param:/:type:/:returns: style docstrings as formatted argument tables.

- source/index.rst:
Top-level table of contents linking to overview, architecture, api, and testing
pages.

- source/overview.rst:
Covers project purpose, prerequisites, environment setup, database setup, LLM
model placement, how to run the application, and how to run the tests.

- source/architecture.rst:
Describes the three-layer architecture (web, ETL pipeline, database) with an
ASCII diagram, explains the role of each source file, and documents the state
file format used to coordinate background jobs.

- source/api.rst:
Auto-generated API reference for scrape.py, clean.py, load_data.py,
query_data.py, refresh_gradcafe.py, update_data.py, and pages.py using
automodule directives. Docstrings are pulled directly from source files.

- source/testing.rst:
Testing guide covering how to run tests, the five marker groups and what each
covers, all shared fixtures and their purpose, and auto-generated docstrings
for all six test files.