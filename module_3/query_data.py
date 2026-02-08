# Import the database connection helper from load_data.py
from load_data import create_connection


# Define a helper function to run a query and return a single value
def fetch_value(connection, query):
    # Create a cursor object for database interaction
    cursor = connection.cursor()

    # Execute the SQL query
    cursor.execute(query)

    # Fetch the first column of the first row
    return cursor.fetchone()[0]


# Define a helper function to run a query and return an entire row
def fetch_row(connection, query):
    # Create a cursor object for database interaction
    cursor = connection.cursor()

    # Execute the SQL query
    cursor.execute(query)

    # Fetch and return the full row
    return cursor.fetchone()


# Define the main analytics function used by Flask
def get_application_stats():
    # Create a database connection using shared credentials
    connection = create_connection(
        "sm_app",
        "postgres",
        "abc123",
        "127.0.0.1",
        "5432"
    )

    # Count applications for Fall 2026
    fall_2026_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2026';"
    )

    # Count total applications
    total_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications;"
    )

    # Count international applicants
    international_count = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE LOWER(us_or_international) = 'international';
        """
    )

    # Compute percentage of international applicants
    international_pct = round((international_count / total_count) * 100, 2)

    # Compute average GPA, GRE, GRE Verbal, GRE AW
    avg_gpa, avg_gre, avg_gre_v, avg_gre_aw = fetch_row(
        connection,
        """
        SELECT
            AVG(gpa),
            AVG(gre),
            AVG(gre_v),
            AVG(gre_aw)
        FROM grad_applications;
        """
    )

    # Round GPA average if it exists
    avg_gpa = round(avg_gpa, 2) if avg_gpa is not None else None

    # Round GRE average if it exists
    avg_gre = round(avg_gre, 2) if avg_gre is not None else None

    # Round GRE verbal average if it exists
    avg_gre_v = round(avg_gre_v, 2) if avg_gre_v is not None else None

    # Round GRE analytical writing average if it exists
    avg_gre_aw = round(avg_gre_aw, 2) if avg_gre_aw is not None else None

    # Compute average GPA of US students applying Fall 2026
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

    # Round US GPA average if it exists
    avg_gpa_us_fall_2026 = (
        round(avg_gpa_us_fall_2026, 2)
        if avg_gpa_us_fall_2026 is not None
        else None
    )

    # Count total Fall 2025 applications
    fall_2025_total = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2025';"
    )

    # Count Fall 2025 acceptances
    fall_2025_accept = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2025'
          AND LOWER(status) LIKE 'accepted%';
        """
    )

    # Compute Fall 2025 acceptance rate
    fall_2025_accept_pct = round(
        (fall_2025_accept / fall_2025_total) * 100,
        2
    )

    # Compute average GPA of Fall 2026 accepted applicants
    avg_gpa_fall_2026_accept = fetch_value(
        connection,
        """
        SELECT AVG(gpa)
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'accepted%'
          AND gpa IS NOT NULL;
        """
    )

    # Round accepted GPA average if it exists
    avg_gpa_fall_2026_accept = (
        round(avg_gpa_fall_2026_accept, 2)
        if avg_gpa_fall_2026_accept is not None
        else None
    )

    # Count JHU Masters Computer Science applications
    jhu_cs_masters = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE LOWER(program) LIKE '%computer science%'
          AND LOWER(program) LIKE '%johns hopkins%'
          AND LOWER(degree) = 'masters';
        """
    )

    # Count Fall 2026 PhD CS acceptances at target schools
    fall_2026_cs_accept = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'accepted%'
          AND LOWER(degree) = 'phd'
          AND LOWER(program) LIKE '%computer science%'
          AND (
            LOWER(program) LIKE '%georgetown%'
            OR LOWER(program) LIKE '%mit%'
            OR LOWER(program) LIKE '%stanford%'
            OR LOWER(program) LIKE '%carnegie mellon%'
          );
        """
    )

    # Count same acceptances using LLM-generated fields
    fall_2026_cs_accept_llm = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'accepted%'
          AND LOWER(degree) = 'phd'
          AND LOWER(llm_generated_program) LIKE '%computer science%'
          AND (
            LOWER(llm_generated_university) LIKE '%georgetown%'
            OR LOWER(llm_generated_university) LIKE '%mit%'
            OR LOWER(llm_generated_university) LIKE '%stanford%'
            OR LOWER(llm_generated_university) LIKE '%carnegie mellon%'
          );
        """
    )

    # Print all computed stats
    print(f"fall_2026_count: {fall_2026_count}")
    print(f"international_pct: {international_pct}")
    print(f"avg_gpa: {avg_gpa}")
    print(f"avg_gre: {avg_gre}")
    print(f"avg_gre_v: {avg_gre_v}")
    print(f"avg_gre_aw: {avg_gre_aw}")
    print(f"avg_gpa_us_fall_2026: {avg_gpa_us_fall_2026}")
    print(f"fall_2025_accept_pct: {fall_2025_accept_pct}")
    print(f"avg_gpa_fall_2026_accept: {avg_gpa_fall_2026_accept}")
    print(f"jhu_cs_masters: {jhu_cs_masters}")
    print(f"fall_2026_cs_accept: {fall_2026_cs_accept}")
    print(f"fall_2026_cs_accept_llm: {fall_2026_cs_accept_llm}")

    # Return all computed statistics as a dictionary for Flask
    return {
        "fall_2026_count": fall_2026_count,
        "international_pct": international_pct,
        "avg_gpa": avg_gpa,
        "avg_gre": avg_gre,
        "avg_gre_v": avg_gre_v,
        "avg_gre_aw": avg_gre_aw,
        "avg_gpa_us_fall_2026": avg_gpa_us_fall_2026,
        "fall_2025_accept_pct": fall_2025_accept_pct,
        "avg_gpa_fall_2026_accept": avg_gpa_fall_2026_accept,
        "jhu_cs_masters": jhu_cs_masters,
        "fall_2026_cs_accept": fall_2026_cs_accept,
        "fall_2026_cs_accept_llm": fall_2026_cs_accept_llm
    }

