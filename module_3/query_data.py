# Import the database connection function from load_data.py
from load_data import create_connection


# Define a helper to run a query and return a single value
def fetch_value(connection, query):
    # Create a cursor for the query
    cursor = connection.cursor()
    # Execute the query
    cursor.execute(query)
    # Return the first value from the first row
    return cursor.fetchone()[0]


# Define a helper to run a query and return a tuple
def fetch_row(connection, query):
    # Create a cursor for the query
    cursor = connection.cursor()
    # Execute the query
    cursor.execute(query)
    # Return the full row
    return cursor.fetchone()


# Define a function to run all analytics queries
def run_queries(connection):
    # Question 1: How many entries for Fall 2026?
    fall_2026_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2026';"
    )

    # Question 2: Percent international (not US / Other)
    total_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications;"
    )

    international_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE LOWER(us_or_international) = 'international';"
    )

    international_pct = round((international_count / total_count) * 100, 2)

    # Question 3: Average GPA, GRE, GRE V, GRE AW
    avg_gpa, avg_gre, avg_gre_v, avg_gre_aw = fetch_row(
        connection,
        """
        SELECT AVG(gpa), AVG(gre), AVG(gre_v), AVG(gre_aw)
        FROM grad_applications
        WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL;
        """
    )

    # Round averages to 2 decimals if they exist
    avg_gpa = round(avg_gpa, 2) if avg_gpa is not None else None
    avg_gre = round(avg_gre, 2) if avg_gre is not None else None
    avg_gre_v = round(avg_gre_v, 2) if avg_gre_v is not None else None
    avg_gre_aw = round(avg_gre_aw, 2) if avg_gre_aw is not None else None

    # Question 4: Average GPA of American students in Fall 2026
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

    avg_gpa_us_fall_2026 = round(avg_gpa_us_fall_2026, 2) if avg_gpa_us_fall_2026 is not None else None

    # Question 5: Percent Fall 2025 entries that are acceptances
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

    fall_2025_accept_pct = round((fall_2025_accept / fall_2025_total) * 100, 2)

    # Question 6: Avg GPA of Fall 2026 acceptances
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

    avg_gpa_fall_2026_accept = round(avg_gpa_fall_2026_accept, 2) if avg_gpa_fall_2026_accept is not None else None

    # Question 7: JHU Masters Computer Science count
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

    # Question 8: Fall 2026 acceptances at target schools (PhD CS)
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

    # Question 9: Same as Q8 but using LLM-generated fields
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

    # Print results
    print("\n----- RESULTS -----")
    print("1) Fall 2026 count:", fall_2026_count)
    print("2) International %:", international_pct)
    print(
        "3) Avg GPA:", avg_gpa,
        "| GRE:", avg_gre,
        "| GRE V:", avg_gre_v,
        "| GRE AW:", avg_gre_aw
    )
    print("4) Avg GPA (US Fall 2026):", avg_gpa_us_fall_2026)
    print("5) Fall 2025 Acceptance %:", fall_2025_accept_pct)
    print("6) Avg GPA (Fall 2026 Acceptances):", avg_gpa_fall_2026_accept)
    print("7) JHU Masters CS count:", jhu_cs_masters)
    print("8) Fall 2026 PhD CS acceptances:", fall_2026_cs_accept)
    print("9) Fall 2026 PhD CS acceptances (LLM):", fall_2026_cs_accept_llm)


# Create connection using the same settings as load_data.py
connection = create_connection("sm_app", "postgres", "abc123", "127.0.0.1", "5432")

# Run all analytics
run_queries(connection)
