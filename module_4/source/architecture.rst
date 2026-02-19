Architecture
============

The application is divided into three layers: web, ETL pipeline, and database.

.. code-block:: text

   ┌─────────────────────────────────────────┐
   │              Flask Web Layer            │
   │  GET /analysis   POST /refresh          │
   │  POST /update-analysis                  │
   │  src/app/pages.py  src/run.py           │
   └────────────────┬────────────────────────┘
                    │
   ┌────────────────▼────────────────────────┐
   │            ETL Pipeline                 │
   │                                         │
   │  scrape.py  ──►  clean.py               │
   │       │                                 │
   │       ▼                                 │
   │  refresh_gradcafe.py                    │
   │       │                                 │
   │       ▼                                 │
   │  update_data.py  (LLM enrichment)       │
   └────────────────┬────────────────────────┘
                    │
   ┌────────────────▼────────────────────────┐
   │           Database Layer                │
   │                                         │
   │  load_data.py   (write)                 │
   │  query_data.py  (read)                  │
   │  PostgreSQL: grad_applications table    │
   └─────────────────────────────────────────┘

Web layer — ``src/app/``
-------------------------

``pages.py``
    Defines the Flask Blueprint with three routes:

    - ``GET /`` and ``GET /analysis`` — render the stats dashboard.
    - ``POST /refresh`` — start a background pull from GradCafe.
    - ``POST /update-analysis`` — start a background LLM enrichment and DB sync.

    Both POST routes use a shared state file (``src/src_files/pull_state.json``)
    to prevent concurrent runs, returning HTTP 409 if a job is already running.

``run.py``
    App factory entry point. Calls ``create_app()``, registers the blueprint,
    and starts the Flask development server.

ETL pipeline — ``src/``
------------------------

``scrape/scrape.py``
    Fetches and parses GradCafe survey pages. Entry points:

    - ``_fetch_html(url)`` — downloads raw HTML.
    - ``_parse_survey_page(html)`` — extracts applicant rows from a listing page.
    - ``_scrape_detail_page(result_id)`` — fetches GPA/GRE from a detail page.

``scrape/clean.py``
    Normalizes raw scraped records. Handles GPA extraction, GRE score
    parsing, and text cleaning before records are written to disk.

``refresh_gradcafe.py``
    Orchestrates the pull pipeline:

    1. Calls ``get_seen_ids_from_llm_extend_file()`` to load already-processed URLs.
    2. Calls ``scrape_new_records(seen_ids)`` to fetch new applicants only.
    3. Calls ``enrich_with_details(records)`` via ``ThreadPoolExecutor``.
    4. Calls ``write_new_applicant_file(records)`` to save to disk.

``update_data.py``
    Reads ``new_applicant_data.json``, calls the local TinyLlama LLM once
    per record to standardize program and university names, and appends
    results to ``llm_extend_applicant_data.json``.

Database layer — ``src/``
--------------------------

``load_data.py``
    Writes data to PostgreSQL:

    - ``create_connection()`` — opens a psycopg2 connection.
    - ``rebuild_from_llm_file()`` — full table rebuild from the LLM output file.
    - ``sync_db_from_llm_file()`` — incremental insert with ``ON CONFLICT DO NOTHING``.

``query_data.py``
    Reads from PostgreSQL:

    - ``get_application_stats()`` — runs all analytics queries and returns a
      dict of statistics consumed by the Flask template.

State file
----------

Both POST routes read and write ``src/src_files/pull_state.json`` to
coordinate background job status. The file tracks five keys:

.. code-block:: json

   {
     "pulling_data": false,
     "updating_analysis": false,
     "pull_complete": false,
     "analysis_complete": false,
     "message": null
   }

The template polls this state via a ``<meta http-equiv="refresh">`` tag
while a job is running, giving the user live status feedback.
