# Import the database connection helper from load_data.py
from load_data import create_connection


# Define a helper function to run a query and return a single value
def fetch_value(connection, query):
    # Create a cursor object for database interaction
    cursor = connection.cursor()

    # Execute the SQL query
    cursor.execute(query)

    # Fetch the first column of the first row
    result = cursor.fetchone()
    return result[0] if result is not None else None


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

    total_applicants = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications;
        """
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

    # Compute percentage of international applicants (guard divide-by-zero)
    international_pct = (
        round((international_count / total_count) * 100, 2)
        if total_count and total_count > 0
        else 0
    )

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

    # Compute Fall 2025 acceptance rate (guard divide-by-zero)
    fall_2025_accept_pct = (
        round((fall_2025_accept / fall_2025_total) * 100, 2)
        if fall_2025_total and fall_2025_total > 0
        else 0
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

    jhu_cs_masters = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE LOWER(degree) = 'masters'

          -- Program matching (CS variants)
          AND (
                LOWER(program) LIKE '%computer science%'
             OR LOWER(program) LIKE '%comp sci%'
             OR LOWER(program) LIKE '%cs%'
             OR LOWER(program) LIKE '%computer-science%'
             OR LOWER(program) LIKE '%computerscience%'
             OR LOWER(program) LIKE '%csci%'
          )

          -- University matching (Johns Hopkins variants + typos) using program column
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
             OR LOWER(program) LIKE '%cs%'
             OR LOWER(program) LIKE '%computer-science%'
             OR LOWER(program) LIKE '%computerscience%'
          )

          -- University matching (RAW program instead of university)
          AND (

                /* =========================
                   Georgetown University
                   ========================= */
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

                /* =========================
                   MIT
                   ========================= */
             OR LOWER(program) LIKE '%mit%'
             OR LOWER(program) LIKE '%m.i.t%'
             OR LOWER(program) LIKE '%massachusetts institute of technology%'
             OR LOWER(program) LIKE '%mass institute of technology%'
             OR LOWER(program) LIKE '%massachusets institute of technology%'
             OR LOWER(program) LIKE '%massachussetts institute of technology%'
             OR LOWER(program) LIKE '%massachusetts inst of technology%'
             OR LOWER(program) LIKE '%institute of technology (mit)%'
             OR LOWER(program) LIKE '%mass tech%'

                /* =========================
                   Stanford University
                   ========================= */
             OR LOWER(program) LIKE '%stanford%'
             OR LOWER(program) LIKE '%standford%'
             OR LOWER(program) LIKE '%stanfod%'
             OR LOWER(program) LIKE '%stanforrd%'

             OR LOWER(program) LIKE '%stanford university%'
             OR LOWER(program) LIKE '%standford university%'
             OR LOWER(program) LIKE '%stanford univeristy%'
             OR LOWER(program) LIKE '%stanford univrsity%'
             OR LOWER(program) LIKE '%stanford univ%'

                /* =========================
                   Carnegie Mellon University
                   ========================= */
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
             OR LOWER(llm_generated_program) LIKE '%cs%'
             OR LOWER(llm_generated_program) LIKE '%computer-science%'
             OR LOWER(llm_generated_program) LIKE '%computerscience%'
          )

          -- University matching (messy but VERY defensive)
          AND (

                /* =========================
                   Georgetown University
                   ========================= */
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

                /* =========================
                   MIT
                   ========================= */
             OR LOWER(llm_generated_university) LIKE '%mit%'
             OR LOWER(llm_generated_university) LIKE '%m.i.t%'
             OR LOWER(llm_generated_university) LIKE '%massachusetts institute of technology%'
             OR LOWER(llm_generated_university) LIKE '%mass institute of technology%'
             OR LOWER(llm_generated_university) LIKE '%massachusets institute of technology%'
             OR LOWER(llm_generated_university) LIKE '%massachussetts institute of technology%'
             OR LOWER(llm_generated_university) LIKE '%massachusetts inst of technology%'
             OR LOWER(llm_generated_university) LIKE '%institute of technology (mit)%'
             OR LOWER(llm_generated_university) LIKE '%mass tech%'

                /* =========================
                   Stanford University
                   ========================= */
             OR LOWER(llm_generated_university) LIKE '%stanford%'
             OR LOWER(llm_generated_university) LIKE '%standford%'
             OR LOWER(llm_generated_university) LIKE '%stanfod%'
             OR LOWER(llm_generated_university) LIKE '%stanforrd%'

             OR LOWER(llm_generated_university) LIKE '%stanford university%'
             OR LOWER(llm_generated_university) LIKE '%standford university%'
             OR LOWER(llm_generated_university) LIKE '%stanford univeristy%'
             OR LOWER(llm_generated_university) LIKE '%stanford univrsity%'
             OR LOWER(llm_generated_university) LIKE '%stanford univ%'

                /* =========================
                   Carnegie Mellon University
                   ========================= */
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
    print(f"total_applicants: {total_applicants}")


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
        "fall_2026_cs_accept": fall_2026_cs_accept,
        "fall_2026_cs_accept_llm": fall_2026_cs_accept_llm,
        "total_applicants": total_applicants
    }


if __name__ == "__main__":
    get_application_stats()
