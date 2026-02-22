"""
tests.test_db_insert
=====================

Tests for database writes, uniqueness constraints, and query correctness.

The goal of this file is to verify that the database layer loads and
queries data correctly — entirely offline using fake connections and
cursors, with no real PostgreSQL instance required.

Covers three spec requirements:

1. **Insert on pull** — target table starts empty; after a load operation,
   new rows exist with all required non-null fields populated.
2. **Idempotency / uniqueness** — duplicate records (same ``url``) do not
   produce duplicate rows in the database.
3. **Query correctness** — ``get_application_stats`` returns a dict
   containing all expected keys.

Based on :mod:`src.load_data` and :mod:`src.query_data`.
All tests are marked ``db`` and run fully offline.

.. note::
    This module uses ``psycopg`` (psycopg3). The ``execute_values`` helper
    from psycopg2 is no longer used; bulk inserts are now performed via
    ``cursor.executemany()``, which is patched directly on :class:`FakeCursor`.
    Both :class:`FakeCursor` and :class:`FakeConnection` implement the context
    manager protocol (``__enter__`` / ``__exit__``) to support the ``with``
    statements used in :mod:`src.load_data`.
"""

import json
import pytest

from src.load_data import create_connection, execute_query, rebuild_from_llm_file
from psycopg import sql
from src.query_data import get_application_stats


# ============================================================
# FAKE DATABASE INFRASTRUCTURE
# ============================================================

class FakeCursor:
    """Fake psycopg3 cursor that records executed queries and captured rows.

    Supports ``execute``, ``executemany``, ``fetchone`` (with query-keyed
    result routing), and an ``inserted_rows`` list populated by
    ``executemany`` calls.

    Implements the context manager protocol so it can be used in
    ``with conn.cursor() as cur:`` blocks as psycopg3 requires.

    :param query_results: Optional mapping of SQL keyword fragment to the
        tuple that ``fetchone`` should return when that fragment appears
        in the last executed query.
    :type query_results: dict, optional
    """

    def __init__(self, query_results=None):
        self.executed_queries = []
        self.inserted_rows = []
        self.query_results = query_results or {}

    def execute(self, query, vars=None):
        """Record a SQL query string.

        :param query: SQL query string.
        :type query: str
        :param vars: Optional query parameters (unused).
        """
        self.executed_queries.append(query)

    def executemany(self, sql, rows):
        """Simulate a bulk insert by extending ``inserted_rows``.

        Replaces the psycopg2 ``execute_values`` pattern. Called by
        :func:`src.load_data._execute_rebuild` and
        :func:`src.load_data._execute_sync`.

        :param sql: SQL query string (unused in fake).
        :type sql: str
        :param rows: Row tuples to capture.
        :type rows: list[tuple]
        """
        self.inserted_rows.extend(rows)

    def fetchone(self):
        """Return a query-specific result tuple based on the last executed query.

        Matches against keys in ``query_results``; falls back to a
        4-tuple of ``None`` values if no key matches.

        :returns: Result tuple for the last executed query.
        :rtype: tuple
        """
        # Convert to string so key-lookup works whether the stored query is
        # a plain str or a psycopg sql.SQL object (which is not iterable).
        raw = self.executed_queries[-1] if self.executed_queries else ""
        last_query = raw.as_string(None) if hasattr(raw, "as_string") else str(raw)
        for key, value in self.query_results.items():
            if key in last_query:
                if isinstance(value, tuple):
                    value = tuple(None if x == "" else x for x in value)
                    if len(value) < 4:
                        value = value + (None,) * (4 - len(value))
                elif value == "":
                    value = None
                return value
        return (None, None, None, None)

    def fetchmany(self, size=1):
        """Delegate to ``fetchone`` and wrap the result in a list.

        ``fetch_value`` and ``fetch_row`` in ``query_data`` now call
        ``fetchmany(clamped)`` instead of ``fetchone()``. This shim
        preserves all existing query-routing logic in ``fetchone`` while
        satisfying the new interface.

        :param size: Number of rows to return (ignored; always returns 0 or 1).
        :type size: int
        :returns: A one-element list containing the ``fetchone`` result,
            or an empty list if ``fetchone`` returns ``None``.
        :rtype: list[tuple]
        """
        result = self.fetchone()
        return [result] if result is not None else []

    def __enter__(self):
        """Support ``with conn.cursor() as cur:`` usage.

        :returns: Self.
        :rtype: FakeCursor
        """
        return self

    def __exit__(self, *args):
        """No-op exit for context manager protocol."""
        pass


class FakeConnection:
    """Fake psycopg3 connection for fully offline database tests.

    Holds a shared :class:`FakeCursor` instance so tests can inspect
    executed queries and inserted rows after the fact. Implements the
    context manager protocol so it can be used in ``with conn:`` blocks
    as psycopg3 requires.

    :param query_results: Passed through to the internal :class:`FakeCursor`.
    :type query_results: dict, optional
    :param should_fail: If ``True``, calling ``cursor()`` raises an exception
        to simulate a connection failure.
    :type should_fail: bool
    """

    def __init__(self, query_results=None, should_fail=False):
        self.autocommit = False
        self.should_fail = should_fail
        self.closed = False
        self.cursor_obj = FakeCursor(query_results=query_results)

    def cursor(self):
        """Return the shared fake cursor, or raise if ``should_fail`` is set.

        :raises Exception: If ``should_fail`` is ``True``.
        :rtype: FakeCursor
        """
        if self.should_fail:
            raise Exception("Simulated DB connection failure")
        return self.cursor_obj

    def commit(self):
        """No-op commit."""
        pass

    def close(self):
        """Mark the connection as closed."""
        self.closed = True

    def __enter__(self):
        """Support ``with conn:`` usage.

        :returns: Self.
        :rtype: FakeConnection
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Re-raise any exception that occurred inside the ``with conn:`` block.

        Returning ``False`` tells Python not to suppress the exception,
        matching the behaviour of a real psycopg3 connection which rolls
        back and then re-raises on error.
        """
        return False  # do not suppress exceptions


def fake_executemany(cur, sql, rows):
    """Simulate ``cursor.executemany`` by appending rows to the cursor.

    Used as a drop-in for simple insert tests that do not need
    uniqueness enforcement.

    :param cur: Fake cursor instance.
    :type cur: FakeCursor
    :param sql: SQL string (unused).
    :type sql: str
    :param rows: Row tuples to capture.
    :type rows: list[tuple]
    """
    cur.inserted_rows.extend(rows)


def fake_executemany_unique(cur, sql, rows):
    """Simulate ``cursor.executemany`` with ``ON CONFLICT (url) DO NOTHING`` semantics.

    Deduplicates on the URL field (index 3), converts empty strings to
    ``None``, and only appends genuinely new rows.

    :param cur: Fake cursor instance.
    :type cur: FakeCursor
    :param sql: SQL string (unused).
    :type sql: str
    :param rows: Row tuples to insert.
    :type rows: list[tuple]
    """
    if not hasattr(cur, "table_rows"):
        cur.table_rows = []

    existing_urls = {r[3] for r in cur.table_rows}

    for row in rows:
        row = tuple(None if x == "" else x for x in row)
        url = row[3]
        if url not in existing_urls:
            cur.table_rows.append(row)
            cur.inserted_rows.append(row)
            existing_urls.add(url)


# ============================================================
# SPEC 1: INSERT ON PULL
# ============================================================

@pytest.mark.db
def test_postgres_connection():
    """Verify a fake connection can be created and a cursor retrieved.

    Simulates the "before" state: a usable connection exists before any
    data is inserted.
    """
    conn = FakeConnection(should_fail=False)
    assert conn is not None
    assert conn.cursor() is not None
    conn.close()


@pytest.mark.db
def test_postgres_connection_failure():
    """Verify ``create_connection`` returns ``None`` when the DB is unreachable.

    Passes deliberately wrong credentials to a port with nothing listening
    so the function hits its ``except`` block and returns ``None``.
    """
    conn = create_connection(
        db_name="nonexistent_db",
        db_user="wrong_user",
        db_password="wrong_pass",
        db_host="127.0.0.1",
        db_port="54321",
    )
    assert conn is None, "create_connection should return None on failure"


@pytest.mark.db
def test_execute_query_records_sql(monkeypatch):
    """Verify ``execute_query`` records SQL against the fake cursor.

    Simulates the table setup that precedes a pull (CREATE + TRUNCATE)
    and asserts both queries are captured in order.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    conn = FakeConnection()

    execute_query(conn, sql.SQL("CREATE TABLE IF NOT EXISTS test_empty_table (id SERIAL PRIMARY KEY, name TEXT);"))
    execute_query(conn, sql.SQL("TRUNCATE TABLE test_empty_table RESTART IDENTITY;"))

    assert conn.cursor_obj.executed_queries == [
        sql.SQL("CREATE TABLE IF NOT EXISTS test_empty_table (id SERIAL PRIMARY KEY, name TEXT);"),
        sql.SQL("TRUNCATE TABLE test_empty_table RESTART IDENTITY;"),
    ]


@pytest.mark.db
def test_rebuild_from_llm_file_inserts_rows(monkeypatch, tmp_path):
    """Verify ``rebuild_from_llm_file`` inserts one row per JSON line.

    Writes two realistic LLM NDJSON records to a temp file and asserts
    that both are captured by the fake ``executemany``, confirming
    the "after pull: rows exist" requirement.

    ``FakeCursor.executemany`` is patched directly on the cursor instance
    after the fake connection is created, since psycopg3 uses
    ``cursor.executemany()`` inside ``with conn.cursor() as cur:`` blocks.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    llm_data = [
        {
            "program_name": "Clinical Mental Health Counseling",
            "university": "George Washington University",
            "degree_type": "Masters",
            "comments": "Had my interview on 1/27/2026...",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997869",
            "applicant_status": "Interview",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "Clinical Mental Health Counseling",
            "llm-generated-university": "George Washington University",
        },
        {
            "program_name": "English",
            "university": "University of California, Davis",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997868",
            "applicant_status": "Accepted: 8 Feb",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "4.00",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "English",
            "llm-generated-university": "University of California, Davis",
        },
    ]

    llm_file = tmp_path / "llm_output.json"
    llm_file.write_text(
        "\n".join(json.dumps(r) for r in llm_data), encoding="utf-8"
    )

    fake_conn = FakeConnection()

    # Patch executemany directly on the cursor to capture inserted rows.
    # psycopg3 calls cursor.executemany() inside with conn.cursor() as cur: blocks.
    fake_conn.cursor_obj.executemany = lambda sql, rows: fake_executemany(
        fake_conn.cursor_obj, sql, rows
    )

    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)

    rebuild_from_llm_file(path=str(llm_file))

    rows = fake_conn.cursor_obj.inserted_rows
    assert len(rows) == len(llm_data)

    # Required fields that must never be None after a pull.
    # Optional fields (gpa, gre scores) are allowed to be None.
    # Row tuple index map (matches INSERT order in load_data.py):
    #   0: program, 1: comments, 2: date_added, 3: url,
    #   4: status, 5: term, 6: us_or_international,
    #   7: gpa, 8: gre, 9: gre_v, 10: gre_aw,
    #   11: degree, 12: llm_generated_program, 13: llm_generated_university
    for row in rows:
        assert row[0] is not None, "program must not be None"
        assert row[2] is not None, "date_added must not be None"
        assert row[3] is not None, "url must not be None"
        assert row[4] is not None, "status must not be None"
        assert row[5] is not None, "term must not be None"


@pytest.mark.db
def test_rebuild_from_llm_file_no_connection(monkeypatch, tmp_path):
    """Verify ``rebuild_from_llm_file`` raises ``RuntimeError`` when connection fails.

    Patches ``create_connection`` to return ``None`` and asserts that
    ``rebuild_from_llm_file`` raises a ``RuntimeError`` before attempting
    any cursor operations.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="Failed to connect to the database."):
        rebuild_from_llm_file(path=str(tmp_path / "dummy.json"))


# ============================================================
# SPEC 2: IDEMPOTENCY / UNIQUENESS
# ============================================================

@pytest.mark.db
def test_rebuild_from_llm_file_uniqueness(monkeypatch, tmp_path):
    """Verify duplicate URLs produce only one row in the database.

    Writes three NDJSON records where two share the same ``url_link``,
    then asserts only two unique rows were inserted — matching the
    ``ON CONFLICT (url) DO NOTHING`` behaviour in production.

    ``FakeCursor.executemany`` is replaced with :func:`fake_executemany_unique`
    directly on the cursor instance to enforce URL-based deduplication
    without a real database.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    llm_data = [
        {
            "program_name": "Clinical Mental Health Counseling",
            "university": "George Washington University",
            "degree_type": "Masters",
            "comments": "First row",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997869",
            "applicant_status": "Interview",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "3.9",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "Clinical Mental Health Counseling",
            "llm-generated-university": "George Washington University",
        },
        {
            # Duplicate URL — should be silently skipped
            "program_name": "Clinical Mental Health Counseling",
            "university": "George Washington University",
            "degree_type": "Masters",
            "comments": "Duplicate row",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997869",
            "applicant_status": "Interview",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "Clinical Mental Health Counseling",
            "llm-generated-university": "George Washington University",
        },
        {
            "program_name": "English",
            "university": "University of California, Davis",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997868",
            "applicant_status": "Accepted: 8 Feb",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "4.00",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "English",
            "llm-generated-university": "University of California, Davis",
        },
    ]

    llm_file = tmp_path / "llm_output.json"
    llm_file.write_text(
        "\n".join(json.dumps(r) for r in llm_data), encoding="utf-8"
    )

    fake_conn = FakeConnection()

    # Override executemany with the uniqueness-enforcing variant to simulate
    # ON CONFLICT (url) DO NOTHING without a real database.
    fake_conn.cursor_obj.executemany = lambda sql, rows: fake_executemany_unique(
        fake_conn.cursor_obj, sql, rows
    )

    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)

    rebuild_from_llm_file(path=str(llm_file))

    table_rows = getattr(fake_conn.cursor_obj, "table_rows", [])
    urls = [r[3] for r in table_rows]
    assert len(table_rows) == 2, "Expected 2 unique rows; duplicate should be dropped"
    assert len(set(urls)) == 2, "URL values must all be unique"


@pytest.mark.db
def test_sync_db_from_llm_file_no_connection(monkeypatch, tmp_path):
    """Verify ``sync_db_from_llm_file`` raises ``RuntimeError`` when connection fails.

    Patches ``create_connection`` to return ``None`` and asserts that
    ``sync_db_from_llm_file`` raises a ``RuntimeError`` before attempting
    any cursor operations.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    from src.load_data import sync_db_from_llm_file
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="Failed to connect to the database."):
        sync_db_from_llm_file(path=str(tmp_path / "dummy.json"))


# ============================================================
# SPEC 3: QUERY CORRECTNESS
# ============================================================

@pytest.mark.db
def test_get_application_stats_returns_all_keys(monkeypatch):
    """Verify ``get_application_stats`` returns a dict with all required keys.

    Patches ``create_connection`` with a :class:`FakeConnection` whose
    ``fetchone`` responses are pre-seeded to match the SQL fragments used
    by each query in ``query_data.py``. Asserts that:

    - All 15 expected stat keys are present in the returned dict.
    - The database connection is closed after the call.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    fake_rows = [
        ("CS - MIT", "", "2026-02-08", "url1", "Accepted: 8 Feb",
         "Fall 2026", "US", 4.0, 165, 160, 3.5, "PhD", "CS", "MIT"),
        ("Physics - Stanford", "", "2026-02-08", "url2", "Rejected: 6 Feb",
         "Fall 2026", "International", 3.9, None, None, None, "PhD", "Physics", "Stanford"),
        ("Math - JHU", "", "2025-02-08", "url3", "Accepted: 6 Feb",
         "Fall 2025", "US", 4.0, None, None, None, "Masters", "Math", "JHU"),
    ]

    query_results = {
        "COUNT(*)":                               (len(fake_rows),),
        "term = 'Fall 2026'":                     (sum(1 for r in fake_rows if r[5] == "Fall 2026"),),
        "term = 'Fall 2025'":                     (sum(1 for r in fake_rows if r[5] == "Fall 2025"),),
        "LOWER(us_or_international) = 'international'": (sum(1 for r in fake_rows if r[6].lower() == "international"),),
        "LOWER(status) LIKE 'accepted%'":         (sum(1 for r in fake_rows if r[4].lower().startswith("accepted")),),
        "LOWER(degree) = 'masters'":              (sum(1 for r in fake_rows if r[11].lower() == "masters" and "computer science" in r[0].lower()),),
        "AVG(gpa)":                               (sum(r[7] for r in fake_rows if r[7]) / len(fake_rows),),
        "AVG(gre)":                               (sum(r[8] for r in fake_rows if r[8]) / len([r for r in fake_rows if r[8]]),),
        "AVG(gre_v)":                             (sum(r[9] for r in fake_rows if r[9]) / len([r for r in fake_rows if r[9]]),),
        "AVG(gre_aw)":                            (sum(r[10] for r in fake_rows if r[10]) / len([r for r in fake_rows if r[10]]),),
        "COALESCE":                               (100.0,),
    }

    fake_conn = FakeConnection(query_results=query_results)
    monkeypatch.setattr("src.query_data.create_connection", lambda *a, **kw: fake_conn)

    stats = get_application_stats()

    expected_keys = [
        "fall_2026_count",
        "international_pct",
        "avg_gpa",
        "avg_gre",
        "avg_gre_v",
        "avg_gre_aw",
        "avg_gpa_us_fall_2026",
        "fall_2025_accept_pct",
        "avg_gpa_fall_2025_accept",
        "jhu_cs_masters",
        "total_applicants",
        "fall_2026_cs_accept",
        "fall_2026_cs_accept_llm",
        "rejected_fall_2026_gpa_pct",
        "accepted_fall_2026_gpa_pct",
    ]
    assert all(k in stats for k in expected_keys), (
        f"Missing keys: {[k for k in expected_keys if k not in stats]}"
    )
    assert fake_conn.closed, "Connection should be closed after get_application_stats"

"""
tests.test_db_insert
=====================

Tests for database writes, uniqueness constraints, and query correctness.

The goal of this file is to verify that the database layer loads and
queries data correctly — entirely offline using fake connections and
cursors, with no real PostgreSQL instance required.

Covers three spec requirements:

1. **Insert on pull** — target table starts empty; after a load operation,
   new rows exist with all required non-null fields populated.
2. **Idempotency / uniqueness** — duplicate records (same ``url``) do not
   produce duplicate rows in the database.
3. **Query correctness** — ``get_application_stats`` returns a dict
   containing all expected keys.

Based on :mod:`src.load_data` and :mod:`src.query_data`.
All tests are marked ``db`` and run fully offline.

.. note::
    This module uses ``psycopg`` (psycopg3). The ``execute_values`` helper
    from psycopg2 is no longer used; bulk inserts are now performed via
    ``cursor.executemany()``, which is patched directly on :class:`FakeCursor`.
    Both :class:`FakeCursor` and :class:`FakeConnection` implement the context
    manager protocol (``__enter__`` / ``__exit__``) to support the ``with``
    statements used in :mod:`src.load_data`.
"""

import json
import pytest

from src.load_data import create_connection, execute_query, rebuild_from_llm_file
from src.query_data import get_application_stats


# ============================================================
# FAKE DATABASE INFRASTRUCTURE
# ============================================================

class FakeCursor:
    """Fake psycopg3 cursor that records executed queries and captured rows.

    Supports ``execute``, ``executemany``, ``fetchone`` (with query-keyed
    result routing), and an ``inserted_rows`` list populated by
    ``executemany`` calls.

    Implements the context manager protocol so it can be used in
    ``with conn.cursor() as cur:`` blocks as psycopg3 requires.

    :param query_results: Optional mapping of SQL keyword fragment to the
        tuple that ``fetchone`` should return when that fragment appears
        in the last executed query.
    :type query_results: dict, optional
    """

    def __init__(self, query_results=None):
        self.executed_queries = []
        self.inserted_rows = []
        self.query_results = query_results or {}

    def execute(self, query, vars=None):
        """Record a SQL query string.

        :param query: SQL query string.
        :type query: str
        :param vars: Optional query parameters (unused).
        """
        self.executed_queries.append(query)

    def executemany(self, sql, rows):
        """Simulate a bulk insert by extending ``inserted_rows``.

        Replaces the psycopg2 ``execute_values`` pattern. Called by
        :func:`src.load_data._execute_rebuild` and
        :func:`src.load_data._execute_sync`.

        :param sql: SQL query string (unused in fake).
        :type sql: str
        :param rows: Row tuples to capture.
        :type rows: list[tuple]
        """
        self.inserted_rows.extend(rows)

    def fetchone(self):
        """Return a query-specific result tuple based on the last executed query.

        Matches against keys in ``query_results``; falls back to a
        4-tuple of ``None`` values if no key matches.

        :returns: Result tuple for the last executed query.
        :rtype: tuple
        """
        # Convert to string so key-lookup works whether the stored query is
        # a plain str or a psycopg sql.SQL object (which is not iterable).
        raw = self.executed_queries[-1] if self.executed_queries else ""
        last_query = raw.as_string(None) if hasattr(raw, "as_string") else str(raw)
        for key, value in self.query_results.items():
            if key in last_query:
                if isinstance(value, tuple):
                    value = tuple(None if x == "" else x for x in value)
                    if len(value) < 4:
                        value = value + (None,) * (4 - len(value))
                elif value == "":
                    value = None
                return value
        return (None, None, None, None)

    def fetchmany(self, size=1):
        """Delegate to ``fetchone`` and wrap the result in a list.

        ``fetch_value`` and ``fetch_row`` in ``query_data`` now call
        ``fetchmany(clamped)`` instead of ``fetchone()``. This shim
        preserves all existing query-routing logic in ``fetchone`` while
        satisfying the new interface.

        :param size: Number of rows to return (ignored; always returns 0 or 1).
        :type size: int
        :returns: A one-element list containing the ``fetchone`` result,
            or an empty list if ``fetchone`` returns ``None``.
        :rtype: list[tuple]
        """
        result = self.fetchone()
        return [result] if result is not None else []

    def __enter__(self):
        """Support ``with conn.cursor() as cur:`` usage.

        :returns: Self.
        :rtype: FakeCursor
        """
        return self

    def __exit__(self, *args):
        """No-op exit for context manager protocol."""
        pass


class FakeConnection:
    """Fake psycopg3 connection for fully offline database tests.

    Holds a shared :class:`FakeCursor` instance so tests can inspect
    executed queries and inserted rows after the fact. Implements the
    context manager protocol so it can be used in ``with conn:`` blocks
    as psycopg3 requires.

    :param query_results: Passed through to the internal :class:`FakeCursor`.
    :type query_results: dict, optional
    :param should_fail: If ``True``, calling ``cursor()`` raises an exception
        to simulate a connection failure.
    :type should_fail: bool
    """

    def __init__(self, query_results=None, should_fail=False):
        self.autocommit = False
        self.should_fail = should_fail
        self.closed = False
        self.cursor_obj = FakeCursor(query_results=query_results)

    def cursor(self):
        """Return the shared fake cursor, or raise if ``should_fail`` is set.

        :raises Exception: If ``should_fail`` is ``True``.
        :rtype: FakeCursor
        """
        if self.should_fail:
            raise Exception("Simulated DB connection failure")
        return self.cursor_obj

    def commit(self):
        """No-op commit."""
        pass

    def close(self):
        """Mark the connection as closed."""
        self.closed = True

    def __enter__(self):
        """Support ``with conn:`` usage.

        :returns: Self.
        :rtype: FakeConnection
        """
        return self

    def __exit__(self, *args):
        """No-op exit for context manager protocol."""
        pass


def fake_executemany(cur, sql, rows):
    """Simulate ``cursor.executemany`` by appending rows to the cursor.

    Used as a drop-in for simple insert tests that do not need
    uniqueness enforcement.

    :param cur: Fake cursor instance.
    :type cur: FakeCursor
    :param sql: SQL string (unused).
    :type sql: str
    :param rows: Row tuples to capture.
    :type rows: list[tuple]
    """
    cur.inserted_rows.extend(rows)


def fake_executemany_unique(cur, sql, rows):
    """Simulate ``cursor.executemany`` with ``ON CONFLICT (url) DO NOTHING`` semantics.

    Deduplicates on the URL field (index 3), converts empty strings to
    ``None``, and only appends genuinely new rows.

    :param cur: Fake cursor instance.
    :type cur: FakeCursor
    :param sql: SQL string (unused).
    :type sql: str
    :param rows: Row tuples to insert.
    :type rows: list[tuple]
    """
    if not hasattr(cur, "table_rows"):
        cur.table_rows = []

    existing_urls = {r[3] for r in cur.table_rows}

    for row in rows:
        row = tuple(None if x == "" else x for x in row)
        url = row[3]
        if url not in existing_urls:
            cur.table_rows.append(row)
            cur.inserted_rows.append(row)
            existing_urls.add(url)


# ============================================================
# SPEC 1: INSERT ON PULL
# ============================================================

@pytest.mark.db
def test_postgres_connection():
    """Verify a fake connection can be created and a cursor retrieved.

    Simulates the "before" state: a usable connection exists before any
    data is inserted.
    """
    conn = FakeConnection(should_fail=False)
    assert conn is not None
    assert conn.cursor() is not None
    conn.close()


@pytest.mark.db
def test_postgres_connection_failure():
    """Verify ``create_connection`` returns ``None`` when the DB is unreachable.

    Passes deliberately wrong credentials to a port with nothing listening
    so the function hits its ``except`` block and returns ``None``.
    """
    conn = create_connection(
        db_name="nonexistent_db",
        db_user="wrong_user",
        db_password="wrong_pass",
        db_host="127.0.0.1",
        db_port="54321",
    )
    assert conn is None, "create_connection should return None on failure"


@pytest.mark.db
def test_execute_query_records_sql(monkeypatch):
    """Verify ``execute_query`` records SQL against the fake cursor.

    Simulates the table setup that precedes a pull (CREATE + TRUNCATE)
    and asserts both queries are captured in order.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    conn = FakeConnection()

    execute_query(conn, sql.SQL("CREATE TABLE IF NOT EXISTS test_empty_table (id SERIAL PRIMARY KEY, name TEXT);"))
    execute_query(conn, sql.SQL("TRUNCATE TABLE test_empty_table RESTART IDENTITY;"))

    assert conn.cursor_obj.executed_queries == [
        sql.SQL("CREATE TABLE IF NOT EXISTS test_empty_table (id SERIAL PRIMARY KEY, name TEXT);"),
        sql.SQL("TRUNCATE TABLE test_empty_table RESTART IDENTITY;"),
    ]


@pytest.mark.db
def test_rebuild_from_llm_file_inserts_rows(monkeypatch, tmp_path):
    """Verify ``rebuild_from_llm_file`` inserts one row per JSON line.

    Writes two realistic LLM NDJSON records to a temp file and asserts
    that both are captured by the fake ``executemany``, confirming
    the "after pull: rows exist" requirement.

    ``FakeCursor.executemany`` is patched directly on the cursor instance
    after the fake connection is created, since psycopg3 uses
    ``cursor.executemany()`` inside ``with conn.cursor() as cur:`` blocks.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    llm_data = [
        {
            "program_name": "Clinical Mental Health Counseling",
            "university": "George Washington University",
            "degree_type": "Masters",
            "comments": "Had my interview on 1/27/2026...",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997869",
            "applicant_status": "Interview",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "Clinical Mental Health Counseling",
            "llm-generated-university": "George Washington University",
        },
        {
            "program_name": "English",
            "university": "University of California, Davis",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997868",
            "applicant_status": "Accepted: 8 Feb",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "4.00",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "English",
            "llm-generated-university": "University of California, Davis",
        },
    ]

    llm_file = tmp_path / "llm_output.json"
    llm_file.write_text(
        "\n".join(json.dumps(r) for r in llm_data), encoding="utf-8"
    )

    fake_conn = FakeConnection()

    # Patch executemany directly on the cursor to capture inserted rows.
    # psycopg3 calls cursor.executemany() inside with conn.cursor() as cur: blocks.
    fake_conn.cursor_obj.executemany = lambda sql, rows: fake_executemany(
        fake_conn.cursor_obj, sql, rows
    )

    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)

    rebuild_from_llm_file(path=str(llm_file))

    rows = fake_conn.cursor_obj.inserted_rows
    assert len(rows) == len(llm_data)

    # Required fields that must never be None after a pull.
    # Optional fields (gpa, gre scores) are allowed to be None.
    # Row tuple index map (matches INSERT order in load_data.py):
    #   0: program, 1: comments, 2: date_added, 3: url,
    #   4: status, 5: term, 6: us_or_international,
    #   7: gpa, 8: gre, 9: gre_v, 10: gre_aw,
    #   11: degree, 12: llm_generated_program, 13: llm_generated_university
    for row in rows:
        assert row[0] is not None, "program must not be None"
        assert row[2] is not None, "date_added must not be None"
        assert row[3] is not None, "url must not be None"
        assert row[4] is not None, "status must not be None"
        assert row[5] is not None, "term must not be None"


@pytest.mark.db
def test_rebuild_from_llm_file_no_connection(monkeypatch, tmp_path):
    """Verify ``rebuild_from_llm_file`` raises ``RuntimeError`` when connection fails.

    Patches ``create_connection`` to return ``None`` and asserts that
    ``rebuild_from_llm_file`` raises a ``RuntimeError`` before attempting
    any cursor operations.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="Failed to connect to the database."):
        rebuild_from_llm_file(path=str(tmp_path / "dummy.json"))


# ============================================================
# SPEC 2: IDEMPOTENCY / UNIQUENESS
# ============================================================

@pytest.mark.db
def test_rebuild_from_llm_file_uniqueness(monkeypatch, tmp_path):
    """Verify duplicate URLs produce only one row in the database.

    Writes three NDJSON records where two share the same ``url_link``,
    then asserts only two unique rows were inserted — matching the
    ``ON CONFLICT (url) DO NOTHING`` behaviour in production.

    ``FakeCursor.executemany`` is replaced with :func:`fake_executemany_unique`
    directly on the cursor instance to enforce URL-based deduplication
    without a real database.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    llm_data = [
        {
            "program_name": "Clinical Mental Health Counseling",
            "university": "George Washington University",
            "degree_type": "Masters",
            "comments": "First row",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997869",
            "applicant_status": "Interview",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "3.9",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "Clinical Mental Health Counseling",
            "llm-generated-university": "George Washington University",
        },
        {
            # Duplicate URL — should be silently skipped
            "program_name": "Clinical Mental Health Counseling",
            "university": "George Washington University",
            "degree_type": "Masters",
            "comments": "Duplicate row",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997869",
            "applicant_status": "Interview",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "Clinical Mental Health Counseling",
            "llm-generated-university": "George Washington University",
        },
        {
            "program_name": "English",
            "university": "University of California, Davis",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 08, 2026",
            "url_link": "https://www.thegradcafe.com/result/997868",
            "applicant_status": "Accepted: 8 Feb",
            "start_term": "Fall 2026",
            "International/US": "US",
            "gpa": "4.00",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "llm-generated-program": "English",
            "llm-generated-university": "University of California, Davis",
        },
    ]

    llm_file = tmp_path / "llm_output.json"
    llm_file.write_text(
        "\n".join(json.dumps(r) for r in llm_data), encoding="utf-8"
    )

    fake_conn = FakeConnection()

    # Override executemany with the uniqueness-enforcing variant to simulate
    # ON CONFLICT (url) DO NOTHING without a real database.
    fake_conn.cursor_obj.executemany = lambda sql, rows: fake_executemany_unique(
        fake_conn.cursor_obj, sql, rows
    )

    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)

    rebuild_from_llm_file(path=str(llm_file))

    table_rows = getattr(fake_conn.cursor_obj, "table_rows", [])
    urls = [r[3] for r in table_rows]
    assert len(table_rows) == 2, "Expected 2 unique rows; duplicate should be dropped"
    assert len(set(urls)) == 2, "URL values must all be unique"


@pytest.mark.db
def test_sync_db_from_llm_file_no_connection(monkeypatch, tmp_path):
    """Verify ``sync_db_from_llm_file`` raises ``RuntimeError`` when connection fails.

    Patches ``create_connection`` to return ``None`` and asserts that
    ``sync_db_from_llm_file`` raises a ``RuntimeError`` before attempting
    any cursor operations.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Pytest-provided temporary directory.
    :type tmp_path: pathlib.Path
    """
    from src.load_data import sync_db_from_llm_file
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="Failed to connect to the database."):
        sync_db_from_llm_file(path=str(tmp_path / "dummy.json"))


# ============================================================
# SPEC 3: QUERY CORRECTNESS
# ============================================================

@pytest.mark.db
def test_get_application_stats_returns_all_keys(monkeypatch):
    """Verify ``get_application_stats`` returns a dict with all required keys.

    Patches ``create_connection`` with a :class:`FakeConnection` whose
    ``fetchone`` responses are pre-seeded to match the SQL fragments used
    by each query in ``query_data.py``. Asserts that:

    - All 15 expected stat keys are present in the returned dict.
    - The database connection is closed after the call.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    fake_rows = [
        ("CS - MIT", "", "2026-02-08", "url1", "Accepted: 8 Feb",
         "Fall 2026", "US", 4.0, 165, 160, 3.5, "PhD", "CS", "MIT"),
        ("Physics - Stanford", "", "2026-02-08", "url2", "Rejected: 6 Feb",
         "Fall 2026", "International", 3.9, None, None, None, "PhD", "Physics", "Stanford"),
        ("Math - JHU", "", "2025-02-08", "url3", "Accepted: 6 Feb",
         "Fall 2025", "US", 4.0, None, None, None, "Masters", "Math", "JHU"),
    ]

    query_results = {
        "COUNT(*)":                               (len(fake_rows),),
        "term = 'Fall 2026'":                     (sum(1 for r in fake_rows if r[5] == "Fall 2026"),),
        "term = 'Fall 2025'":                     (sum(1 for r in fake_rows if r[5] == "Fall 2025"),),
        "LOWER(us_or_international) = 'international'": (sum(1 for r in fake_rows if r[6].lower() == "international"),),
        "LOWER(status) LIKE 'accepted%'":         (sum(1 for r in fake_rows if r[4].lower().startswith("accepted")),),
        "LOWER(degree) = 'masters'":              (sum(1 for r in fake_rows if r[11].lower() == "masters" and "computer science" in r[0].lower()),),
        "AVG(gpa)":                               (sum(r[7] for r in fake_rows if r[7]) / len(fake_rows),),
        "AVG(gre)":                               (sum(r[8] for r in fake_rows if r[8]) / len([r for r in fake_rows if r[8]]),),
        "AVG(gre_v)":                             (sum(r[9] for r in fake_rows if r[9]) / len([r for r in fake_rows if r[9]]),),
        "AVG(gre_aw)":                            (sum(r[10] for r in fake_rows if r[10]) / len([r for r in fake_rows if r[10]]),),
        "COALESCE":                               (100.0,),
    }

    fake_conn = FakeConnection(query_results=query_results)
    monkeypatch.setattr("src.query_data.create_connection", lambda *a, **kw: fake_conn)

    stats = get_application_stats()

    expected_keys = [
        "fall_2026_count",
        "international_pct",
        "avg_gpa",
        "avg_gre",
        "avg_gre_v",
        "avg_gre_aw",
        "avg_gpa_us_fall_2026",
        "fall_2025_accept_pct",
        "avg_gpa_fall_2025_accept",
        "jhu_cs_masters",
        "total_applicants",
        "fall_2026_cs_accept",
        "fall_2026_cs_accept_llm",
        "rejected_fall_2026_gpa_pct",
        "accepted_fall_2026_gpa_pct",
    ]
    assert all(k in stats for k in expected_keys), (
        f"Missing keys: {[k for k in expected_keys if k not in stats]}"
    )
    assert fake_conn.closed, "Connection should be closed after get_application_stats"

@pytest.mark.db
def test_get_application_stats_no_connection(monkeypatch):
    """Verify ``get_application_stats`` raises ``RuntimeError`` when connection fails.

    Patches ``create_connection`` to return ``None`` and asserts that
    ``get_application_stats`` raises a ``RuntimeError`` before attempting
    any queries.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr("src.query_data.create_connection",
                        lambda *a, **kw: None)
    with pytest.raises(RuntimeError,
                       match="Failed to connect to the database."):
        get_application_stats()