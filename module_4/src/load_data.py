# load_data.py

# PostgreSQL database adapter for Python
import psycopg2

# Exception class for database connection errors
from psycopg2 import OperationalError

# Helper for efficiently inserting many rows at once
from psycopg2.extras import execute_values

# Used to convert string dates into Python date objects
from datetime import datetime

from .paths import LLM_OUTPUT_FILE

# Used to load JSON lines from scraped / LLM files
import json

# Create and return a database connection
def create_connection(
    db_name="sm_app",          # Database name
    db_user="postgres",        # Database user
    db_password="abc123",      # Database password
    db_host="127.0.0.1",       # Database host (local)
    db_port="5432"             # PostgreSQL port
):
    try:
        # Attempt to open a connection to PostgreSQL
        return psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
    except OperationalError as e:
        # Print error if the connection fails
        print(f"DB connection error: {e}")
        return None

# Execute a SQL query without returning results
def execute_query(connection, query):
    # Enable autocommit so changes persist immediately
    connection.autocommit = True

    # Create a database cursor
    cursor = connection.cursor()

    # Execute the provided SQL query
    cursor.execute(query)

# FULL rebuild of the database from LLM JSON file
def rebuild_from_llm_file(path=LLM_OUTPUT_FILE):

    # Open a database connection
    conn = create_connection()

    # Create a cursor for executing SQL commands
    cur = conn.cursor()

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
          degree TEXT,å
          llm_generated_program TEXT,
          llm_generated_university TEXT
        );
    """)

    # Delete all existing rows and reset the primary key counter
    cur.execute("TRUNCATE grad_applications RESTART IDENTITY;")

    # List to store rows before bulk insert
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

    # Bulk insert all rows into the database
    execute_values(
        cur,
        """
        INSERT INTO grad_applications (
            program, comments, date_added, url, status, term,
            us_or_international, gpa, gre, gre_v, gre_aw,
            degree, llm_generated_program, llm_generated_university
        ) VALUES %s
        ON CONFLICT (url) DO NOTHING;
        """,
        rows
    )

    # Commit changes to the database
    conn.commit()

    # Close the database connection
    conn.close()

# Incremental DB sync from LLM JSON file
def sync_db_from_llm_file(path=LLM_OUTPUT_FILE):

    # Open a database connection
    conn = create_connection()

    # Create a cursor
    cur = conn.cursor()

    # List to store new rows
    rows = []

    # Open the LLM JSON file
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            # Parse JSON object
            r = json.loads(line)

            # Build row tuple
            rows.append(
                (
                    f"{r.get('program_name')} - {r.get('university')}"
                    if r.get("program_name") and r.get("university")
                    else r.get("program_name") or r.get("university"),

                    r.get("comments"),

                    datetime.strptime(r["date_added"], "%B %d, %Y").date()
                    if r.get("date_added") else None,

                    r.get("url_link"),
                    r.get("applicant_status"),
                    r.get("start_term"),
                    r.get("International/US"),

                    float(r["gpa"]) if r.get("gpa") else None,
                    float(r["gre_general"]) if r.get("gre_general") else None,
                    float(r.get("gre_verbal")) if r.get("gre_verbal") else None,
                    float(r.get("gre_analytical_writing"))
                    if r.get("gre_analytical_writing") else None,

                    r.get("degree_type"),
                    r.get("llm-generated-program"),
                    r.get("llm-generated-university"),
                )
            )

    # Insert only new rows (based on unique URL)
    execute_values(
        cur,
        """
        INSERT INTO grad_applications (
            program, comments, date_added, url, status, term,
            us_or_international, gpa, gre, gre_v, gre_aw,
            degree, llm_generated_program, llm_generated_university
        ) VALUES %s
        ON CONFLICT (url) DO NOTHING;
        """,
        rows
    )

    # Commit changes
    conn.commit()

    # Close connection
    conn.close()
