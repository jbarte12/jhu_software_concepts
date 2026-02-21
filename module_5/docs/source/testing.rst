Testing Guide
=============

The test suite is organized into five marker groups, each targeting a
different layer of the application. All tests run offline using
``monkeypatch`` and ``tmp_path`` — no real database or network connection
is required except where noted.

Running tests
-------------

Run everything:

.. code-block:: bash

   pytest -m "web or buttons or analysis or db or integration"

Run one marker group with verbose output:

.. code-block:: bash

   pytest -m integration -v
   pytest -m db -v
   pytest -m analysis -v
   pytest -m buttons -v
   pytest -m web -v

Run a single file:

.. code-block:: bash

   pytest tests/test_db_insert.py -v

Run with coverage:

.. code-block:: bash

   pytest --cov=src --cov-report=html

Markers
-------

.. list-table::
   :header-rows: 1
   :widths: 15 25 60

   * - Marker
     - File
     - What it covers
   * - ``integration``
     - ``test_integration_end_to_end.py``
     - Full pipeline: Flask startup, ``/refresh``, ``/update-analysis``,
       scraper branches, enrichment, file I/O, DB sync, end-to-end flow
   * - ``db``
     - ``test_db_insert.py``
     - Database writes: insert on pull, uniqueness constraints,
       ``get_application_stats`` key coverage
   * - ``analysis``
     - ``test_analysis_format.py``, ``test_flask_page.py``
     - HTML rendering: label/value pairs, percentage rounding,
       Flask app factory, route registration
   * - ``buttons``
     - ``test_buttons.py``
     - Button routes: ``POST /refresh``, ``POST /update-analysis``,
       busy-state 409 gating, exception handling
   * - ``web``
     - ``test_flask_page.py``, ``test_buttons.py``
     - State file defaults, app config, route existence

Fixtures
--------

``instant_thread`` / ``patch_thread``
    Replaces ``threading.Thread`` with a synchronous stand-in so background
    jobs triggered by button routes complete immediately before assertions run.
    Used in ``test_buttons.py`` and ``test_integration_end_to_end.py``.

``client``
    Flask test client. Used in ``test_flask_page.py``, ``test_buttons.py``,
    and ``test_integration_end_to_end.py``.

``app``
    Minimal Flask app with a simple template written to ``tmp_path``.
    Used in integration tests where the real template is not needed.

``soup`` / ``soup_high_precision``
    Renders the real ``gradcafe_stats.html`` Jinja template through a Flask
    test client and returns a parsed ``BeautifulSoup`` object. Used in
    ``test_analysis_format.py`` so formatting tests run against the live
    template rather than a hardcoded copy.

``fake_scraper_injected_data``
    A scraper payload containing adversarial inputs (SQL injection, XSS,
    prompt injection, Log4Shell) used to verify the pipeline handles
    untrusted data safely.

Test file reference
--------------------

.. automodule:: tests.test_scraper_function
   :members:

.. automodule:: tests.test_db_insert
   :members:

.. automodule:: tests.test_flask_page
   :members:

.. automodule:: tests.test_buttons
   :members:

.. automodule:: tests.test_analysis_format
   :members:

.. automodule:: tests.test_integration_end_to_end
   :members:
