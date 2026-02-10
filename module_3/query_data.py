# Import the database connection helper function from load_data.py
from load_data import create_connection


# Helper function that runs a SQL query and returns a single scalar value
def fetch_value(connection, query):
    # Create a new database cursor
    cursor = connection.cursor()
    # Execute the provided SQL query
    cursor.execute(query)
    # Fetch the first row of the result
    result = cursor.fetchone()
    # Return the first column if a result exists, otherwise None
    return result[0] if result is not None else None


# Helper function that runs a SQL query and returns an entire row
def fetch_row(connection, query):
    # Create a new database cursor
    cursor = connection.cursor()
    # Execute the provided SQL query
    cursor.execute(query)
    # Fetch and return the first row of the result
    return cursor.fetchone()


# Main analytics function used by the Flask application
def get_application_stats():

    # Create a connection to the PostgreSQL database
    connection = create_connection(
        "sm_app",        # Database name
        "postgres",      # Database user
        "abc123",        # Database password
        "127.0.0.1",     # Database host
        "5432"           # Database port
    )

    # Query total number of applicants in the database
    total_applicants = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications;
        """
    )

    # Query total number of Fall 2026 applications
    fall_2026_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2026';"
    )

    # Query total number of applications across all terms
    total_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications;"
    )

    # Query total number of international applicants
    international_count = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE LOWER(us_or_international) = 'international';
        """
    )

    # Calculate percentage of international applicants, rounded to 2 decimal places
    international_pct = (
        round((international_count / total_count) * 100, 2)
        if total_count and total_count > 0
        else 0
    )

    # Query average GPA, GRE, GRE Verbal, and GRE Analytical Writing scores
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

    # Preserve NULL values if no GPA data exists
    avg_gpa = avg_gpa if avg_gpa is not None else None
    # Preserve NULL values if no GRE data exists
    avg_gre = avg_gre if avg_gre is not None else None
    # Preserve NULL values if no GRE verbal data exists
    avg_gre_v = avg_gre_v if avg_gre_v is not None else None
    # Preserve NULL values if no GRE analytical writing data exists
    avg_gre_aw = avg_gre_aw if avg_gre_aw is not None else None

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

    # Preserve NULL if no qualifying GPA values exist
    avg_gpa_us_fall_2026 = (
        avg_gpa_us_fall_2026
        if avg_gpa_us_fall_2026 is not None
        else None
    )

    # Query total number of Fall 2025 applications
    fall_2025_total = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2025';"
    )

    # Query number of accepted Fall 2025 applications
    fall_2025_accept = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2025'
          AND LOWER(status) LIKE 'accepted%';
        """
    )

    # Calculate Fall 2025 acceptance rate, rounded to 2 decimal places
    fall_2025_accept_pct = (
        round((fall_2025_accept / fall_2025_total) * 100, 2)
        if fall_2025_total and fall_2025_total > 0
        else 0
    )

    # Query average GPA of accepted Fall 2026 applicants
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

    # Preserve NULL if no accepted GPA values exist
    avg_gpa_fall_2026_accept = (
        avg_gpa_fall_2026_accept
        if avg_gpa_fall_2026_accept is not None
        else None
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

    # Query percentage of rejected Fall 2026 applicants who reported a GPA
    rejected_fall_2026_gpa_pct = fetch_value(
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

    # Query percentage of accepted Fall 2026 applicants who reported a GPA
    accepted_fall_2026_gpa_pct = fetch_value(
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

    # Print summary statistics to the console for debugging
    print(f"Total Applicants in Scraped Database: {total_applicants}")
    print(f"Fall 2026 Applicants: {fall_2026_count}")
    print(f"Percent of International Applicants: {international_pct}")
    print(f"Average GPA: {avg_gpa}")
    print(f"Average GRE: {avg_gre}")
    print(f"Average GRE Verbal: {avg_gre_v}")
    print(f"Average GRE Analytical Writing: {avg_gre_aw}")
    print(f"Average GPA American Applicants, Fall 2026: {avg_gpa_us_fall_2026}")
    print(f"Acceptance Rate, Fall 2025: {fall_2025_accept_pct}")
    print(f"Average Acceptance GPA, Fall 2026: {avg_gpa_fall_2026_accept}")
    print(f"JHU M.S. Computer Science Applicants: {jhu_cs_masters}")
    print(f"Percent of GPAs included with Rejection, Fall 2026: {rejected_fall_2026_gpa_pct}")
    print(f"Percent of GPAs included with Acceptance, Fall 2026: {accepted_fall_2026_gpa_pct}")

    # Close the database connection
    connection.close()

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
        "total_applicants": total_applicants,
        "rejected_fall_2026_gpa_pct": rejected_fall_2026_gpa_pct,
        "accepted_fall_2026_gpa_pct": accepted_fall_2026_gpa_pct
    }


# Run the analytics function directly when the file is executed
if __name__ == "__main__":
    get_application_stats()
