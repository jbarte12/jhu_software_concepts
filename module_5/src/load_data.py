"""
Database connection and data loading utilities for the GradCafe application.

Provides functions to create PostgreSQL connections, execute queries, and
load or sync applicant data from LLM-generated NDJSON files into the
``grad_applications`` table.
"""

# Used to convert string dates into Python date objects
from datetime import datetime

# Used to load JSON lines from scraped / LLM files
import json

# Used for optional return type annotation on create_connection
from typing import Optional

# PostgreSQL database adapter for Python (psycopg3)
import psycopg

# Connection used as a typed parameter so Pylint can resolve member access
# OperationalError used to catch connection failures
from psycopg import Connection, OperationalError

from .paths import LLM_OUTPUT_FILE


def create_connection(
    db_name="sm_app",
    db_user="postgres",
    db_password="abc123",
    db_host="127.0.0.1",
    db_port="5432"
) -> Optional[Connection]:
    """Create and return a psycopg3 connection to the PostgreSQL database.

    Attempts to open a connection using the provided credentials. Returns
    ``None`` and prints an error message if the connection fails (e.g. the
    database is unreachable or credentials are wrong).

    :param db_name: Name of the PostgreSQL database to connect to.
    :type db_name: str
    :param db_user: PostgreSQL username.
    :type db_user: str
    :param db_password: PostgreSQL password.
    :type db_password: str
    :param db_host: Host address of the PostgreSQL server.
    :type db_host: str
    :param db_port: Port the PostgreSQL server is listening on.
    :type db_port: str
    :returns: An open psycopg3 connection, or ``None`` on failure.
    :rtype: psycopg.Connection or None
    """
    try:
        # Attempt to open a connection to PostgreSQL
        # Note: psycopg3 uses 'dbname' not 'database'
        return psycopg.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
    except OperationalError as e:
        # Print error if the connection fails
        print(f"DB connection error: {e}")
        return None


def execute_query(connection, query):
    """Execute a single SQL statement without returning results.

    Enables autocommit on the connection so changes take effect
    immediately without requiring an explicit ``commit()`` call.
    Intended for DDL statements (``CREATE``, ``TRUNCATE``, etc.)
    and other one-off queries.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :param query: SQL statement to execute.
    :type query: str
    """
    # Enable autocommit so changes persist immediately
    connection.autocommit = True

    # Create a database cursor
    cursor = connection.cursor()

    # Execute the provided SQL query
    cursor.execute(query)


def _build_rows(path: str) -> list:
    """Parse an NDJSON file and return a list of row tuples for DB insertion.

    Reads the file at ``path`` line by line, parses each line as JSON, and
    converts each record into a tuple matching the ``grad_applications``
    column order. Numeric fields are cast to ``float`` where present;
    missing or empty values become ``None``.

    :param path: Path to the NDJSON file to parse.
    :type path: str or pathlib.Path
    :returns: List of 14-element tuples ready for ``executemany``.
    :rtype: list[tuple]
    :raises json.JSONDecodeError: If any line in the file is not valid JSON.
    :raises ValueError: If a numeric field contains a non-numeric string.
    """
    rows = []

    # Open the LLM-generated JSON file (one JSON object per line)
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            # Parse each JSON line into a dictionary
            r = json.loads(line)

            # Append a tuple representing one DB row
            rows.append(
                (
                    # Combine program name and university when both exist
                    f"{r.get('program_name')} - {r.get('university')}"
                    if r.get("program_name") and r.get("university")
                    else r.get("program_name") or r.get("university"),

                    # Free-text applicant comments
                    r.get("comments"),

                    # Convert date string to DATE object
                    datetime.strptime(r["date_added"], "%B %d, %Y").date()
                    if r.get("date_added") else None,

                    # Application URL (used as unique key)
                    r.get("url_link"),

                    # Applicant decision status
                    r.get("applicant_status"),

                    # Application term (e.g., Fall 2026)
                    r.get("start_term"),

                    # US or International flag
                    r.get("International/US"),

                    # GPA converted to float
                    float(r["gpa"]) if r.get("gpa") else None,

                    # GRE total score
                    float(r["gre_general"]) if r.get("gre_general") else None,

                    # GRE verbal score
                    float(r["gre_verbal"]) if r.get("gre_verbal") else None,

                    # GRE analytical writing score
                    float(r["gre_analytical_writing"])
                    if r.get("gre_analytical_writing") else None,

                    # Degree type (e.g., Masters, PhD)
                    r.get("degree_type"),

                    # Program name normalized by LLM
                    r.get("llm-generated-program"),

                    # University name normalized by LLM
                    r.get("llm-generated-university"),
                )
            )

    return rows


def _execute_rebuild(conn: Connection, rows: list) -> None:
    """Execute the full table rebuild inside an already-open connection.

    Creates the ``grad_applications`` table if it does not exist, truncates
    all existing rows, then bulk-inserts ``rows``. Called by
    :func:`rebuild_from_llm_file` after the connection is established.

    Pylint can resolve ``.cursor()`` here because ``conn`` is typed as
    :class:`psycopg.Connection` directly on the parameter.

    :param conn: An open psycopg3 database connection.
    :type conn: psycopg.Connection
    :param rows: Row tuples to insert, as returned by :func:`_build_rows`.
    :type rows: list[tuple]
    """
    with conn.cursor() as cur:

        # Create the grad_applications table if it does not already exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS grad_applications (
              p_id SERIAL PRIMARY KEY,
              program TEXT,
              comments TEXT,
              date_added DATE,
              url TEXT UNIQUE,
              status TEXT,
              term TEXT,
              us_or_international TEXT,
              gpa FLOAT,
              gre FLOAT,
              gre_v FLOAT,
              gre_aw FLOAT,
              degree TEXT,
              llm_generated_program TEXT,
              llm_generated_university TEXT
            );
        """)

        # Delete all existing rows and reset the primary key counter
        cur.execute("TRUNCATE grad_applications RESTART IDENTITY;")

        # Bulk insert all rows into the database.
        # psycopg3 uses executemany with explicit %s placeholders per column.
        cur.executemany(
            """
            INSERT INTO grad_applications (
                program, comments, date_added, url, status, term,
                us_or_international, gpa, gre, gre_v, gre_aw,
                degree, llm_generated_program, llm_generated_university
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
            """,
            rows
        )


def _execute_sync(conn: Connection, rows: list) -> None:
    """Execute an incremental insert inside an already-open connection.

    Inserts only rows whose URL is not already present in the database,
    via ``ON CONFLICT (url) DO NOTHING``. Called by
    :func:`sync_db_from_llm_file` after the connection is established.

    Pylint can resolve ``.cursor()`` here because ``conn`` is typed as
    :class:`psycopg.Connection` directly on the parameter.

    :param conn: An open psycopg3 database connection.
    :type conn: psycopg.Connection
    :param rows: Row tuples to insert, as returned by :func:`_build_rows`.
    :type rows: list[tuple]
    """
    with conn.cursor() as cur:

        # Insert only new rows (based on unique URL).
        # psycopg3 uses executemany with explicit %s placeholders per column.
        cur.executemany(
            """
            INSERT INTO grad_applications (
                program, comments, date_added, url, status, term,
                us_or_international, gpa, gre, gre_v, gre_aw,
                degree, llm_generated_program, llm_generated_university
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
            """,
            rows
        )


def rebuild_from_llm_file(path=LLM_OUTPUT_FILE):
    """Fully rebuild the ``grad_applications`` table from an LLM NDJSON file.

    Drops all existing rows (via ``TRUNCATE``), recreates the table if it
    does not exist, then bulk-inserts every record from the NDJSON file at
    ``path``. Each line in the file must be a valid JSON object containing
    the fields expected by the ``grad_applications`` schema.

    Duplicate URLs are silently ignored via ``ON CONFLICT (url) DO NOTHING``.
    Uses a ``with`` context manager on the connection so commit and cleanup
    are handled automatically on success or failure.

    :param path: Path to the NDJSON file produced by the LLM enrichment step.
        Defaults to :data:`src.paths.LLM_OUTPUT_FILE`.
    :type path: str or pathlib.Path
    :raises RuntimeError: If the database connection could not be established.
    :raises json.JSONDecodeError: If any line in the file is not valid JSON.
    :raises ValueError: If a numeric field (GPA, GRE) contains a
        non-numeric string that cannot be cast to ``float``.
    """
    # Open a database connection
    conn = create_connection()

    # Guard against a failed connection before attempting cursor access
    if conn is None:
        raise RuntimeError("Failed to connect to the database.")

    # Parse the NDJSON file into row tuples
    rows = _build_rows(path)

    # Use context manager: commits on success, rolls back on exception,
    # and closes the connection automatically
    with conn:
        _execute_rebuild(conn, rows)


def sync_db_from_llm_file(path=LLM_OUTPUT_FILE):
    """Incrementally sync new records from an LLM NDJSON file into the database.

    Unlike :func:`rebuild_from_llm_file`, this function does **not** truncate
    the table first. It reads every record from the NDJSON file and attempts
    to insert each one. Records whose URL already exists in the database are
    silently skipped via ``ON CONFLICT (url) DO NOTHING``, making this safe
    to call repeatedly on a growing file.

    Uses a ``with`` context manager on the connection so commit and cleanup
    are handled automatically on success or failure.

    :param path: Path to the NDJSON file produced by the LLM enrichment step.
        Defaults to :data:`src.paths.LLM_OUTPUT_FILE`.
    :type path: str or pathlib.Path
    :raises RuntimeError: If the database connection could not be established.
    :raises json.JSONDecodeError: If any line in the file is not valid JSON.
    :raises ValueError: If a numeric field (GPA, GRE) contains a
        non-numeric string that cannot be cast to ``float``.
    """
    # Open a database connection
    conn = create_connection()

    # Guard against a failed connection before attempting cursor access
    if conn is None:
        raise RuntimeError("Failed to connect to the database.")

    # Parse the NDJSON file into row tuples
    rows = _build_rows(path)

    # Use context manager: commits on success, rolls back on exception,
    # and closes the connection automatically
    with conn:
        _execute_sync(conn, rows)
