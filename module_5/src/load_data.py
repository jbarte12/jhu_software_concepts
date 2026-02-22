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

# Used to read database credentials from environment variables
import os

# Used for optional return type annotation on create_connection
from typing import Optional

# PostgreSQL database adapter for Python (psycopg3)
import psycopg

# sql module for safe SQL composition — prevents raw string injection
from psycopg import sql

# Connection used as a typed parameter so Pylint can resolve member access
# OperationalError used to catch connection failures
from psycopg import Connection, OperationalError

from .paths import LLM_OUTPUT_FILE


def create_connection(
    db_name=None,
    db_user=None,
    db_password=None,
    db_host=None,
    db_port=None,
) -> Optional[Connection]:
    """Create and return a psycopg3 connection to the PostgreSQL database.

    Credentials are resolved from the explicit arguments first; if an
    argument is ``None``, the corresponding environment variable is used
    as a fallback (``DB_NAME``, ``DB_USER``, ``DB_PASSWORD``, ``DB_HOST``,
    ``DB_PORT``). Hard-coded defaults are provided only for non-sensitive
    values (host, port, db name, user) so the function remains usable
    without any configuration in local development. Passwords must be
    supplied via the argument or the ``DB_PASSWORD`` environment variable;
    no hard-coded password default is provided.

    :param db_name: Name of the PostgreSQL database to connect to.
    :type db_name: str or None
    :param db_user: PostgreSQL username.
    :type db_user: str or None
    :param db_password: PostgreSQL password.
    :type db_password: str or None
    :param db_host: Host address of the PostgreSQL server.
    :type db_host: str or None
    :param db_port: Port the PostgreSQL server is listening on.
    :type db_port: str or None
    :returns: An open psycopg3 connection, or ``None`` on failure.
    :rtype: psycopg.Connection or None
    """
    # Resolve each credential: explicit argument → env var → safe default.
    # Passwords are never hard-coded; an empty string is used as the last
    # resort so psycopg can still attempt a connection (e.g. trust auth).
    resolved_name = db_name or os.environ.get("DB_NAME", "sm_app")
    resolved_user = db_user or os.environ.get("DB_USER", "postgres")
    resolved_password = db_password or os.environ.get("DB_PASSWORD", "")
    resolved_host = db_host or os.environ.get("DB_HOST", "127.0.0.1")
    resolved_port = db_port or os.environ.get("DB_PORT", "5432")

    try:
        # Attempt to open a connection to PostgreSQL.
        # Note: psycopg3 uses 'dbname' not 'database'.
        return psycopg.connect(
            dbname=resolved_name,
            user=resolved_user,
            password=resolved_password,
            host=resolved_host,
            port=resolved_port,
        )
    except OperationalError as e:
        # Print error if the connection fails
        print(f"DB connection error: {e}")
        return None


def execute_query(connection, query: sql.Composable):
    """Execute a single SQL statement without returning results.

    Enables autocommit on the connection so changes take effect
    immediately without requiring an explicit ``commit()`` call.
    Intended for DDL statements (``CREATE``, ``TRUNCATE``, etc.)
    and other one-off queries.

    The ``query`` parameter must be a :class:`psycopg.sql.Composable`
    object (e.g. ``sql.SQL("...")``), not a raw string. This enforces
    safe SQL composition at every call site and prevents accidental
    injection via raw string concatenation.

    Example usage::

        execute_query(conn, sql.SQL("TRUNCATE grad_applications RESTART IDENTITY;"))

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :param query: SQL statement to execute, as a ``sql.Composable`` object.
    :type query: psycopg.sql.Composable
    """
    # Enable autocommit so changes persist immediately
    connection.autocommit = True

    # Create a database cursor and execute the provided SQL query
    cursor = connection.cursor()
    cursor.execute(query)


def _build_rows(path: str) -> list:
    """Parse an NDJSON file and return a list of row tuples for DB insertion.

    Reads the file at ``path`` line by line, parses each line as JSON, and
    converts each record into a tuple matching the ``grad_applications``
    column order. Numeric fields are cast to ``float`` where present;
    missing or empty values become ``None``. Records with a malformed
    ``date_added`` field are logged and skipped rather than crashing the
    entire load, so one bad line is never fatal.

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

            # Parse date string to a DATE object.  Catch malformed dates and
            # skip the record with a warning rather than crashing the load.
            date_added = None
            if r.get("date_added"):
                try:
                    date_added = datetime.strptime(r["date_added"], "%B %d, %Y").date()
                except ValueError:
                    print(
                        f"Warning: skipping malformed date_added "
                        f"'{r['date_added']}' for url {r.get('url_link')}"
                    )
                    continue

            # Append a tuple representing one DB row
            rows.append(
                (
                    # Combine program name and university when both exist
                    f"{r.get('program_name')} - {r.get('university')}"
                    if r.get("program_name") and r.get("university")
                    else r.get("program_name") or r.get("university"),

                    # Free-text applicant comments
                    r.get("comments"),

                    # Parsed date (may be None if date_added was absent)
                    date_added,

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


def _execute_upsert(conn: Connection, rows: list, rebuild: bool) -> None:
    """Execute a table rebuild or incremental sync inside an already-open connection.

    When ``rebuild`` is ``True``, creates the ``grad_applications`` table if
    it does not exist and truncates all existing rows before bulk-inserting
    ``rows``. When ``rebuild`` is ``False``, performs an incremental sync:
    inserts only rows whose URL is not already present in the database,
    via ``ON CONFLICT (url) DO NOTHING``, making it safe to call
    repeatedly on a growing file.

    This single helper replaces the former ``_execute_rebuild`` /
    ``_execute_sync`` pair, which were identical apart from the
    ``TRUNCATE`` call.

    Pylint can resolve ``.cursor()`` here because ``conn`` is typed as
    :class:`psycopg.Connection` directly on the parameter.

    :param conn: An open psycopg3 database connection.
    :type conn: psycopg.Connection
    :param rows: Row tuples to insert, as returned by :func:`_build_rows`.
    :type rows: list[tuple]
    :param rebuild: If ``True``, truncate the table before inserting
        (full rebuild). If ``False``, perform an incremental upsert.
    :type rebuild: bool
    """
    with conn.cursor() as cur:

        # Create the grad_applications table if it does not already exist.
        # SQL object constructed separately from the execute call.
        create_stmt = sql.SQL("""
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
        cur.execute(create_stmt)

        if rebuild:
            # Delete all existing rows and reset the primary key counter.
            # SQL object constructed separately from the execute call.
            truncate_stmt = sql.SQL("TRUNCATE grad_applications RESTART IDENTITY;")
            cur.execute(truncate_stmt)

        # Bulk insert all rows into the database.
        # SQL object constructed separately from the executemany call.
        # psycopg3 uses %s placeholders for data values (never string interpolation).
        # ON CONFLICT (url) DO NOTHING silently skips duplicates for both
        # the rebuild path (which truncates first) and the sync path.
        insert_stmt = sql.SQL("""
            INSERT INTO grad_applications (
                program, comments, date_added, url, status, term,
                us_or_international, gpa, gre, gre_v, gre_aw,
                degree, llm_generated_program, llm_generated_university
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
        """)
        cur.executemany(insert_stmt, rows)


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
    conn = create_connection()

    if conn is None:
        raise RuntimeError("Failed to connect to the database.")

    rows = _build_rows(path)

    # Use context manager: commits on success, rolls back on exception,
    # and closes the connection automatically.
    with conn:
        _execute_upsert(conn, rows, rebuild=True)


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
    conn = create_connection()

    if conn is None:
        raise RuntimeError("Failed to connect to the database.")

    rows = _build_rows(path)

    # Use context manager: commits on success, rolls back on exception,
    # and closes the connection automatically.
    with conn:
        _execute_upsert(conn, rows, rebuild=False)
