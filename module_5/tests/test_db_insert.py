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
"""

import json
import pytest

from src.load_data import create_connection, execute_query, rebuild_from_llm_file
from src.query_data import get_application_stats


# ============================================================
# FAKE DATABASE INFRASTRUCTURE
# ============================================================

@pytest.mark.db
class FakeCursor:
    """Fake psycopg2 cursor that records executed queries and captured rows.

    Supports ``execute``, ``fetchone`` (with query-keyed result routing),
    and an ``inserted_rows`` list populated by fake ``execute_values``
    helpers.

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

    def fetchone(self):
        """Return a query-specific result tuple based on the last executed query.

        Matches against keys in ``query_results``; falls back to a
        4-tuple of ``None`` values if no key matches.

        :returns: Result tuple for the last executed query.
        :rtype: tuple
        """
        last_query = self.executed_queries[-1] if self.executed_queries else ""
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


class FakeConnection:
    """Fake psycopg2 connection for fully offline database tests.

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

@pytest.mark.db
def fake_execute_values(cur, sql, rows):
    """Simulate ``psycopg2.extras.execute_values`` by appending rows to the cursor.

    :param cur: Fake cursor instance.
    :type cur: FakeCursor
    :param sql: SQL string (unused).
    :type sql: str
    :param rows: Row tuples to capture.
    :type rows: list[tuple]
    """
    cur.inserted_rows.extend(rows)

@pytest.mark.db
def fake_execute_values_unique(cur, sql, rows):
    """Simulate ``execute_values`` with ``ON CONFLICT (url) DO NOTHING`` semantics.

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
    table = "test_empty_table"

    execute_query(conn, f"CREATE TABLE IF NOT EXISTS {table} (id SERIAL PRIMARY KEY, name TEXT);")
    execute_query(conn, f"TRUNCATE TABLE {table} RESTART IDENTITY;")

    assert conn.cursor_obj.executed_queries == [
        f"CREATE TABLE IF NOT EXISTS {table} (id SERIAL PRIMARY KEY, name TEXT);",
        f"TRUNCATE TABLE {table} RESTART IDENTITY;",
    ]


@pytest.mark.db
def test_rebuild_from_llm_file_inserts_rows(monkeypatch, tmp_path):
    """Verify ``rebuild_from_llm_file`` inserts one row per JSON line.

    Writes two realistic LLM NDJSON records to a temp file and asserts
    that both are captured by the fake ``execute_values``, confirming
    the "after pull: rows exist" requirement.

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
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)
    monkeypatch.setattr("src.load_data.execute_values", fake_execute_values)

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


# ============================================================
# SPEC 2: IDEMPOTENCY / UNIQUENESS
# ============================================================

@pytest.mark.db
def test_rebuild_from_llm_file_uniqueness(monkeypatch, tmp_path):
    """Verify duplicate URLs produce only one row in the database.

    Writes three NDJSON records where two share the same ``url_link``,
    then asserts only two unique rows were inserted — matching the
    ``ON CONFLICT (url) DO NOTHING`` behaviour in production.

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
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)
    monkeypatch.setattr("src.load_data.execute_values", fake_execute_values_unique)

    rebuild_from_llm_file(path=str(llm_file))

    table_rows = getattr(fake_conn.cursor_obj, "table_rows", [])
    urls = [r[3] for r in table_rows]
    assert len(table_rows) == 2, "Expected 2 unique rows; duplicate should be dropped"
    assert len(set(urls)) == 2, "URL values must all be unique"


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