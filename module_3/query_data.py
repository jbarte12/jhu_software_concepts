# Import the database connection helper from load_data.py
from load_data import create_connection


# ---------------------------------------------
# Helper: run a query and return a single value
# ---------------------------------------------
def fetch_value(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result is not None else None


# ---------------------------------------------
# Helper: run a query and return a full row
# ---------------------------------------------
def fetch_row(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchone()


# ---------------------------------------------
# Main analytics function used by Flask
# ---------------------------------------------
def get_application_stats():

    # Create database connection
    connection = create_connection(
        "sm_app",
        "postgres",
        "abc123",
        "127.0.0.1",
        "5432"
    )

    # Total applicants
    total_applicants = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications;
        """
    )

    # Total Fall 2026 applications
    fall_2026_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2026';"
    )

    # Total applications (all terms)
    total_count = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications;"
    )

    # Total international applicants
    international_count = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE LOWER(us_or_international) = 'international';
        """
    )

    # Percentage of international applicants
    international_pct = (
        round((international_count / total_count) * 100, 2)
        if total_count and total_count > 0
        else 0
    )

    # Average GPA, GRE, GRE Verbal, GRE AW
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

    # Round averages where present
    avg_gpa = round(avg_gpa, 2) if avg_gpa is not None else None
    avg_gre = round(avg_gre, 2) if avg_gre is not None else None
    avg_gre_v = round(avg_gre_v, 2) if avg_gre_v is not None else None
    avg_gre_aw = round(avg_gre_aw, 2) if avg_gre_aw is not None else None

    # Average GPA of US students applying Fall 2026
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

    avg_gpa_us_fall_2026 = (
        round(avg_gpa_us_fall_2026, 2)
        if avg_gpa_us_fall_2026 is not None
        else None
    )

    # Total Fall 2025 applications
    fall_2025_total = fetch_value(
        connection,
        "SELECT COUNT(*) FROM grad_applications WHERE term = 'Fall 2025';"
    )

    # Fall 2025 acceptances
    fall_2025_accept = fetch_value(
        connection,
        """
        SELECT COUNT(*)
        FROM grad_applications
        WHERE term = 'Fall 2025'
          AND LOWER(status) LIKE 'accepted%';
        """
    )

    # Fall 2025 acceptance rate
    fall_2025_accept_pct = (
        round((fall_2025_accept / fall_2025_total) * 100, 2)
        if fall_2025_total and fall_2025_total > 0
        else 0
    )

    # Average GPA of Fall 2026 accepted applicants
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

    avg_gpa_fall_2026_accept = (
        round(avg_gpa_fall_2026_accept, 2)
        if avg_gpa_fall_2026_accept is not None
        else None
    )

    # JHU CS Masters applications
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

    # Fall 2026 PhD CS acceptances (RAW fields)
    fall_2026_cs_accept = fetch_value(
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
             OR LOWER(program) LIKE '%mit%'
             OR LOWER(program) LIKE '%m.i.t%'
             OR LOWER(program) LIKE '%massachusetts institute of technology%'
             OR LOWER(program) LIKE '%mass institute of technology%'
             OR LOWER(program) LIKE '%massachusets institute of technology%'
             OR LOWER(program) LIKE '%massachussetts institute of technology%'
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

    # Fall 2026 PhD CS acceptances (LLM-generated fields)
    fall_2026_cs_accept_llm = fetch_value(
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
             OR LOWER(llm_generated_university) LIKE '%mit%'
             OR LOWER(llm_generated_university) LIKE '%m.i.t%'
             OR LOWER(llm_generated_university) LIKE '%massachusetts institute of technology%'
             OR LOWER(llm_generated_university) LIKE '%mass institute of technology%'
             OR LOWER(llm_generated_university) LIKE '%massachusets institute of technology%'
             OR LOWER(llm_generated_university) LIKE '%massachussetts institute of technology%'
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

    # ---------------------------------------------
    # Percentage of Fall 2026 REJECTED applicants
    # who reported a GPA
    # ---------------------------------------------
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

    # ---------------------------------------------
    # Percentage of Fall 2026 ACCEPTED applicants
    # who reported a GPA
    # ---------------------------------------------
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


    # Debug output
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
    print(f"2026 Acceptances, Georgetown, MIT, Stanford, CMU (Raw Data): {fall_2026_cs_accept}")
    print(f"2026 Acceptances, Georgetown, MIT, Stanford, CMU (LLM Data): {fall_2026_cs_accept_llm}")
    print(f"Percent of GPAs included with Rejection, Fall 2026: {rejected_fall_2026_gpa_pct}")
    print(f"Percent of GPAs included with Acceptance, Fall 2026: {accepted_fall_2026_gpa_pct}")

    # Close database connection
    connection.close()

    # Return stats dictionary
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
        "total_applicants": total_applicants,
        "rejected_fall_2026_gpa_pct": rejected_fall_2026_gpa_pct,
        "accepted_fall_2026_gpa_pct": accepted_fall_2026_gpa_pct
    }


if __name__ == "__main__":
    get_application_stats()
