# Import the database connection helper function from load_data.py
from .load_data import create_connection


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

    # Preserve NULL if no accepted GPA values exist
    avg_gpa_fall_2025_accept = (
        avg_gpa_fall_2025_accept
        if avg_gpa_fall_2025_accept is not None
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

    # Count Fall 2026 PhD CS acceptances at target schools (RAW fields)
    fall_2026_cs_accept = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'accepted%'
          AND LOWER(degree) = 'phd'

          -- Program matching (CS variants, RAW program)
          AND (
                LOWER(program) LIKE '%computer science%'
             OR LOWER(program) LIKE '%comp sci%'
             OR LOWER(program) = '%cs%'
             OR LOWER(program) LIKE '%computer-science%'
             OR LOWER(program) LIKE '%computerscience%'
          )

          -- University matching (RAW program instead of university)
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

    # Count same acceptances using LLM-generated fields
    # Fall 2026 PhD CS acceptances (LLM-generated fields)
    fall_2026_cs_accept_llm = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2026'
          AND LOWER(status) LIKE 'accepted%'
          AND LOWER(degree) = 'phd'

          -- Program matching (CS variants)
          AND (
                LOWER(llm_generated_program) LIKE '%computer science%'
             OR LOWER(llm_generated_program) LIKE '%comp sci%'
             OR LOWER(llm_generated_program) = '%cs%'
             OR LOWER(llm_generated_program) LIKE '%computer-science%'
             OR LOWER(llm_generated_program) LIKE '%computerscience%'
          )

          -- University matching 
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
    print(f"Average Acceptance GPA, Fall 2026: {avg_gpa_fall_2025_accept}")
    print(f"JHU M.S. Computer Science Applicants: {jhu_cs_masters}")
    print(f"2026 Acceptances, Georgetown, MIT, Stanford, CMU (Raw):: {fall_2026_cs_accept}")
    print(f"2026 Acceptances, Georgetown, MIT, Stanford, CMU (LLM)::"
          f" {fall_2026_cs_accept_llm}")
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
        "avg_gpa_fall_2025_accept": avg_gpa_fall_2025_accept,
        "jhu_cs_masters": jhu_cs_masters,
        "total_applicants": total_applicants,
        "fall_2026_cs_accept": fall_2026_cs_accept,
        "fall_2026_cs_accept_llm": fall_2026_cs_accept_llm,
        "rejected_fall_2026_gpa_pct": rejected_fall_2026_gpa_pct,
        "accepted_fall_2026_gpa_pct": accepted_fall_2026_gpa_pct
    }


# Run the analytics function directly when the file is executed
if __name__ == "__main__":
    get_application_stats()
