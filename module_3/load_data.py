# load_data.py

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from datetime import datetime
import json


def create_connection(
    db_name="sm_app",
    db_user="postgres",
    db_password="abc123",
    db_host="127.0.0.1",
    db_port="5432"
):
    try:
        return psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
    except OperationalError as e:
        print(f"DB connection error: {e}")
        return None


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(query)


def rebuild_from_llm_file(path="llm_extend_applicant_data.json"):
    """
    FULL REBUILD – unchanged
    """
    conn = create_connection()
    cur = conn.cursor()

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

    cur.execute("TRUNCATE grad_applications RESTART IDENTITY;")

    rows = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)

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
                    float(r["gre_verbal"]) if r.get("gre_verbal") else None,
                    float(r["gre_analytical_writing"])
                    if r.get("gre_analytical_writing") else None,
                    r.get("degree_type"),
                    r.get("llm-generated-program"),
                    r.get("llm-generated-university"),
                )
            )

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

    conn.commit()
    conn.close()


# 🔑 NEW, SMALL, SAFE FUNCTION
def sync_db_from_llm_file(path="llm_extend_applicant_data.json"):
    """
    Incrementally inserts records from llm_extend_applicant_data.json
    that are not already in the DB.
    """
    conn = create_connection()
    cur = conn.cursor()

    rows = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)

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

    conn.commit()
    conn.close()
