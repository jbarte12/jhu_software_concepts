"""
Query utilities for the GradCafe application.

Provides helper functions to run SQL queries against the PostgreSQL
database and a main analytics function used by the Flask application
to retrieve applicant statistics.
"""

# Connection imported for type annotation so Pylint can resolve member access
from psycopg import Connection

# Import the database connection helper function from load_data.py
# load_data.py uses psycopg (psycopg3) instead of psycopg2
from .load_data import create_connection



def fetch_value(connection, query):
    """Run a SQL query and return a single scalar value.

    Creates a cursor, executes the query, and returns the first column
    of the first result row. Returns ``None`` if the query produces no rows.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :param query: SQL query expected to return a single value.
    :type query: str
    :returns: The scalar result, or ``None`` if no rows were returned.
    :rtype: any
    """
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result is not None else None


def fetch_row(connection, query):
    """Run a SQL query and return the first result row as a tuple.

    Creates a cursor, executes the query, and returns the entire first
    row. Returns ``None`` if the query produces no rows.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :param query: SQL query expected to return one or more columns.
    :type query: str
    :returns: The first result row, or ``None`` if no rows were returned.
    :rtype: tuple or None
    """
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchone()


def _fetch_international_pct(connection: Connection) -> float:
    """Calculate the percentage of international applicants.

    Queries total and international applicant counts and returns the
    rounded percentage. Returns ``0`` if there are no applicants.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :returns: Percentage of international applicants, rounded to 2 decimal places.
    :rtype: float
    """
    total_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications;"
    )
    international_count = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE LOWER(us_or_international) = 'international';
        """
    )
    return (
        round((international_count / total_count) * 100, 2)
        if total_count and total_count > 0
        else 0
    )


def _fetch_fall_2025_accept_pct(connection: Connection) -> float:
    """Calculate the Fall 2025 acceptance rate.

    Queries total and accepted Fall 2025 application counts and returns
    the rounded acceptance percentage. Returns ``0`` if there are no
    Fall 2025 applications.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :returns: Fall 2025 acceptance rate, rounded to 2 decimal places.
    :rtype: float
    """
    fall_2025_total = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2025';"
    )
    fall_2025_accept = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2025'
          AND LOWER(status) LIKE 'accepted%';
        """
    )
    return (
        round((fall_2025_accept / fall_2025_total) * 100, 2)
        if fall_2025_total and fall_2025_total > 0
        else 0
    )


def _fetch_fall_2026_gpa_pcts(connection: Connection) -> tuple:
    """Calculate GPA reporting rates for rejected and accepted Fall 2026 applicants.

    Queries the percentage of rejected and accepted Fall 2026 applicants
    who reported a GPA, returning both values as a tuple.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :returns: Tuple of (rejected_gpa_pct, accepted_gpa_pct).
    :rtype: tuple[float, float]
    """
    # Percentage of rejected Fall 2026 applicants who reported a GPA
    rejected = fetch_value(
        connection,
        """
        SELECT
            COALESCE(
                ROUND(
                    100.0 *
                    COUNT(*) FILTER (WHERE gpa IS NOT NULL AND gpa > 0)
                    / NULLIF(COUNT(*), 0),
                    2
                ),
                0
            )
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'rejected:%';
        """
    )
    # Percentage of accepted Fall 2026 applicants who reported a GPA
    accepted = fetch_value(
        connection,
        """
        SELECT
            COALESCE(
                ROUND(
                    100.0 *
                    COUNT(*) FILTER (WHERE gpa IS NOT NULL AND gpa > 0)
                    / NULLIF(COUNT(*), 0),
                    2
                ),
                0
            )
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'accepted:%';
        """
    )
    return rejected, accepted


def _fetch_fall_2026_cs_accepts(connection: Connection) -> tuple:
    """Count Fall 2026 PhD CS acceptances using raw and LLM-generated fields.

    Queries accepted Fall 2026 PhD Computer Science applications at target
    schools using both the raw program field and the LLM-normalised fields,
    returning both counts as a tuple.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :returns: Tuple of (fall_2026_cs_accept, fall_2026_cs_accept_llm).
    :rtype: tuple[int, int]
    """
    # Count Fall 2026 PhD CS acceptances at target schools (RAW fields)
    raw_accept = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'accepted%'
          AND LOWER(degree) = 'phd'
          AND (
                LOWER(program) LIKE '%computer science%'
             OR LOWER(program) LIKE '%comp sci%'
             OR LOWER(program) = '%cs%'
             OR LOWER(program) LIKE '%computer-science%'
             OR LOWER(program) LIKE '%computerscience%'
          )
          AND (
                LOWER(program) LIKE '%georgetown%'
             OR LOWER(program) LIKE '%george town%'
             OR LOWER(program) LIKE '%geoerge town%'
             OR LOWER(program) LIKE '%george-town%'
             OR LOWER(program) LIKE '%georgetown university%'
             OR LOWER(program) LIKE '%george town university%'
             OR LOWER(program) LIKE '%geoerge town university%'
             OR LOWER(program) LIKE '%georgetown univeristy%'
             OR LOWER(program) LIKE '%georgetown univrsity%'
             OR LOWER(program) LIKE '%georgetown unversity%'
             OR LOWER(program) LIKE '%georgetown univercity%'
             OR LOWER(program) LIKE '%georgetown univ%'
             OR LOWER(program) LIKE '%george town univeristy%'
             OR LOWER(program) LIKE '%geoerge town univeristy%'
             OR LOWER(program) LIKE '%george town univrsity%'
             OR LOWER(program) LIKE '%geoerge town univrsity%'
             OR LOWER(program) LIKE '%mit%'
             OR LOWER(program) LIKE '%m.i.t%'
             OR LOWER(program) LIKE '%massachusetts institute of technology%'
             OR LOWER(program) LIKE '%massachusetts inst of technology%'
             OR LOWER(program) LIKE '%institute of technology (mit)%'
             OR LOWER(program) LIKE '%mass tech%'
             OR LOWER(program) LIKE '%stanford%'
             OR LOWER(program) LIKE '%standford%'
             OR LOWER(program) LIKE '%stanfod%'
             OR LOWER(program) LIKE '%stanforrd%'
             OR LOWER(program) LIKE '%stanford university%'
             OR LOWER(program) LIKE '%standford university%'
             OR LOWER(program) LIKE '%stanford univeristy%'
             OR LOWER(program) LIKE '%stanford univrsity%'
             OR LOWER(program) LIKE '%stanford univ%'
             OR LOWER(program) LIKE '%carnegie mellon%'
             OR LOWER(program) LIKE '%carnegie melon%'
             OR LOWER(program) LIKE '%carnegiemelon%'
             OR LOWER(program) LIKE '%carnegie-mellon%'
             OR LOWER(program) LIKE '%carnegi mellon%'
             OR LOWER(program) LIKE '%carnigie mellon%'
             OR LOWER(program) LIKE '%carnegie mellon university%'
             OR LOWER(program) LIKE '%carnegie melon university%'
             OR LOWER(program) LIKE '%carnegie mellon univeristy%'
             OR LOWER(program) LIKE '%carnegie mellon univrsity%'
             OR LOWER(program) LIKE '%carnegie mellon univ%'
             OR LOWER(program) LIKE '%cmu%'
          );
        """
    )

    # Count Fall 2026 PhD CS acceptances using LLM-generated fields
    llm_accept = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'accepted%'
          AND LOWER(degree) = 'phd'
          AND (
                LOWER(llm_generated_program) LIKE '%computer science%'
             OR LOWER(llm_generated_program) LIKE '%comp sci%'
             OR LOWER(llm_generated_program) = '%cs%'
             OR LOWER(llm_generated_program) LIKE '%computer-science%'
             OR LOWER(llm_generated_program) LIKE '%computerscience%'
          )
          AND (
                LOWER(llm_generated_university) LIKE '%georgetown%'
             OR LOWER(llm_generated_university) LIKE '%george town%'
             OR LOWER(llm_generated_university) LIKE '%geoerge town%'
             OR LOWER(llm_generated_university) LIKE '%george-town%'
             OR LOWER(llm_generated_university) LIKE '%georgetown university%'
             OR LOWER(llm_generated_university) LIKE '%george town university%'
             OR LOWER(llm_generated_university) LIKE '%geoerge town university%'
             OR LOWER(llm_generated_university) LIKE '%georgetown univeristy%'
             OR LOWER(llm_generated_university) LIKE '%georgetown univrsity%'
             OR LOWER(llm_generated_university) LIKE '%georgetown unversity%'
             OR LOWER(llm_generated_university) LIKE '%georgetown univercity%'
             OR LOWER(llm_generated_university) LIKE '%georgetown univ%'
             OR LOWER(llm_generated_university) LIKE '%george town univeristy%'
             OR LOWER(llm_generated_university) LIKE '%geoerge town univeristy%'
             OR LOWER(llm_generated_university) LIKE '%george town univrsity%'
             OR LOWER(llm_generated_university) LIKE '%geoerge town univrsity%'
             OR LOWER(llm_generated_university) LIKE '%mit%'
             OR LOWER(llm_generated_university) LIKE '%m.i.t%'
             OR LOWER(llm_generated_university) LIKE '%massachusetts institute of technology%'
             OR LOWER(llm_generated_university) LIKE '%massachusetts inst of technology%'
             OR LOWER(llm_generated_university) LIKE '%institute of technology (mit)%'
             OR LOWER(llm_generated_university) LIKE '%mass tech%'
             OR LOWER(llm_generated_university) LIKE '%stanford%'
             OR LOWER(llm_generated_university) LIKE '%standford%'
             OR LOWER(llm_generated_university) LIKE '%stanfod%'
             OR LOWER(llm_generated_university) LIKE '%stanforrd%'
             OR LOWER(llm_generated_university) LIKE '%stanford university%'
             OR LOWER(llm_generated_university) LIKE '%standford university%'
             OR LOWER(llm_generated_university) LIKE '%stanford univeristy%'
             OR LOWER(llm_generated_university) LIKE '%stanford univrsity%'
             OR LOWER(llm_generated_university) LIKE '%stanford univ%'
             OR LOWER(llm_generated_university) LIKE '%carnegie mellon%'
             OR LOWER(llm_generated_university) LIKE '%carnegie melon%'
             OR LOWER(llm_generated_university) LIKE '%carnegiemelon%'
             OR LOWER(llm_generated_university) LIKE '%carnegie-mellon%'
             OR LOWER(llm_generated_university) LIKE '%carnegi mellon%'
             OR LOWER(llm_generated_university) LIKE '%carnigie mellon%'
             OR LOWER(llm_generated_university) LIKE '%carnegie mellon university%'
             OR LOWER(llm_generated_university) LIKE '%carnegie melon university%'
             OR LOWER(llm_generated_university) LIKE '%carnegie mellon univeristy%'
             OR LOWER(llm_generated_university) LIKE '%carnegie mellon univrsity%'
             OR LOWER(llm_generated_university) LIKE '%carnegie mellon univ%'
             OR LOWER(llm_generated_university) LIKE '%cmu%'
          );
        """
    )

    return raw_accept, llm_accept


def _fetch_averages(connection: Connection) -> dict:
    """Fetch average GPA and GRE scores across all applicants.

    Queries the overall averages for GPA, GRE total, GRE verbal, and
    GRE analytical writing, plus the average GPA for US Fall 2026
    applicants and accepted Fall 2025 applicants. Returns all values
    as a dictionary.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :returns: Dict with keys ``avg_gpa``, ``avg_gre``, ``avg_gre_v``,
        ``avg_gre_aw``, ``avg_gpa_us_fall_2026``, ``avg_gpa_fall_2025_accept``.
    :rtype: dict
    """
    # Query average GPA, GRE, GRE Verbal, and GRE Analytical Writing scores
    avg_gpa, avg_gre, avg_gre_v, avg_gre_aw = fetch_row(
        connection,
        """
        SELECT AVG(gpa), AVG(gre), AVG(gre_v), AVG(gre_aw)
        FROM grad_applications;
        """
    )

    # Query average GPA for US applicants applying for Fall 2026
    avg_gpa_us_fall_2026 = fetch_value(
        connection,
        """
        SELECT AVG(gpa)
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(us_or_international) = 'us'
          AND gpa IS NOT NULL;
        """
    )

    # Query average GPA of accepted Fall 2025 applicants
    avg_gpa_fall_2025_accept = fetch_value(
        connection,
        """
        SELECT AVG(gpa)
        FROM grad_applications
        WHERE term = 'Fall 2025'
          AND LOWER(status) LIKE 'accepted%'
          AND gpa IS NOT NULL;
        """
    )

    return {
        "avg_gpa": avg_gpa,
        "avg_gre": avg_gre,
        "avg_gre_v": avg_gre_v,
        "avg_gre_aw": avg_gre_aw,
        "avg_gpa_us_fall_2026": avg_gpa_us_fall_2026,
        "avg_gpa_fall_2025_accept": avg_gpa_fall_2025_accept,
    }


def _fetch_stats(connection: Connection) -> dict:
    """Run all analytics queries against an open database connection.

    Executes every SQL query needed to populate the GradCafe stats page
    and returns the results as a dictionary. Closes the connection before
    returning.

    Accepts a typed ``Connection`` parameter so Pylint can resolve
    ``.close()`` and other member access without false positives.
    Delegates percentage, average, and grouped calculations to private
    helpers to stay within the local variable limit.

    :param connection: An open psycopg3 database connection.
    :type connection: psycopg.Connection
    :returns: Dictionary of computed statistics for the Flask template.
    :rtype: dict
    """
    # Query total number of applicants in the database
    total_applicants = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications;"
    )

    # Query total number of Fall 2026 applications
    fall_2026_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2026';"
    )

    # Query number of JHU Computer Science master's applications
    jhu_cs_masters = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE LOWER(degree) = 'masters'
          AND (
                LOWER(program) LIKE '%computer science%'
             OR LOWER(program) LIKE '%comp sci%'
             OR LOWER(program) = '%cs%'
             OR LOWER(program) LIKE '%computer-science%'
             OR LOWER(program) LIKE '%computerscience%'
             OR LOWER(program) LIKE '%csci%'
          )
          AND (
                LOWER(program) LIKE '%johns hopkins%'
             OR LOWER(program) LIKE '%john hopkins%'
             OR LOWER(program) LIKE '%jhu%'
             OR LOWER(program) LIKE '%johns-hopkins%'
             OR LOWER(program) LIKE '%john hopkins university%'
             OR LOWER(program) LIKE '%johns hopkins university%'
             OR LOWER(program) LIKE '%johns hopkins univ%'
             OR LOWER(program) LIKE '%johns hopkins univeristy%'
             OR LOWER(program) LIKE '%johns hopkins univrsity%'
             OR LOWER(program) LIKE '%johns hopkins univertiy%'
             OR LOWER(program) LIKE '%johns hopkins universty%'
             OR LOWER(program) LIKE '%johns hopkins u%'
             OR LOWER(program) LIKE '%johs hopkins%'
             OR LOWER(program) LIKE '%jonhs hopkins%'
             OR LOWER(program) LIKE '%johns hopkinss%'
             OR LOWER(program) LIKE '%john hopkinss%'
          );
        """
    )

    # Delegate grouped calculations to private helpers
    averages = _fetch_averages(connection)
    international_pct = _fetch_international_pct(connection)
    fall_2025_accept_pct = _fetch_fall_2025_accept_pct(connection)
    rejected_gpa_pct, accepted_gpa_pct = _fetch_fall_2026_gpa_pcts(connection)
    cs_accept_raw, cs_accept_llm = _fetch_fall_2026_cs_accepts(connection)

    # Close the database connection
    connection.close()

    # Return all computed statistics as a dictionary for Flask
    return {
        "fall_2026_count": fall_2026_count,
        "international_pct": international_pct,
        "avg_gpa": averages["avg_gpa"],
        "avg_gre": averages["avg_gre"],
        "avg_gre_v": averages["avg_gre_v"],
        "avg_gre_aw": averages["avg_gre_aw"],
        "avg_gpa_us_fall_2026": averages["avg_gpa_us_fall_2026"],
        "fall_2025_accept_pct": fall_2025_accept_pct,
        "avg_gpa_fall_2025_accept": averages["avg_gpa_fall_2025_accept"],
        "jhu_cs_masters": jhu_cs_masters,
        "total_applicants": total_applicants,
        "fall_2026_cs_accept": cs_accept_raw,
        "fall_2026_cs_accept_llm": cs_accept_llm,
        "rejected_fall_2026_gpa_pct": rejected_gpa_pct,
        "accepted_fall_2026_gpa_pct": accepted_gpa_pct,
    }


def get_application_stats() -> dict:
    """Create a database connection and return all GradCafe application statistics.

    Public entry point used by the Flask application. Creates a connection
    via :func:`src.load_data.create_connection`, raises ``RuntimeError`` if
    the connection fails, then delegates all query work to
    :func:`_fetch_stats`.

    :returns: Dictionary of computed statistics for the Flask template.
        Contains the following keys: ``fall_2026_count``,
        ``international_pct``, ``avg_gpa``, ``avg_gre``, ``avg_gre_v``,
        ``avg_gre_aw``, ``avg_gpa_us_fall_2026``, ``fall_2025_accept_pct``,
        ``avg_gpa_fall_2025_accept``, ``jhu_cs_masters``,
        ``total_applicants``, ``fall_2026_cs_accept``,
        ``fall_2026_cs_accept_llm``, ``rejected_fall_2026_gpa_pct``,
        ``accepted_fall_2026_gpa_pct``.
    :rtype: dict
    :raises RuntimeError: If the database connection could not be established.
    """
    # Create a connection to the PostgreSQL database
    connection = create_connection(
        "sm_app",        # Database name
        "postgres",      # Database user
        "abc123",        # Database password
        "127.0.0.1",     # Database host
        "5432"           # Database port
    )

    # Guard against a failed connection before attempting any queries
    if connection is None:
        raise RuntimeError("Failed to connect to the database.")

    # Delegate all query work to the typed helper so Pylint can resolve
    # connection member access (close, cursor, etc.)
    return _fetch_stats(connection)
