# Import psycopg2 for PostgreSQL connection
import psycopg2

# Import OperationalError for error handling
from psycopg2 import OperationalError

# Import execute_values for fast bulk inserts
from psycopg2.extras import execute_values

# Import datetime for date conversion
from datetime import datetime

# Import json to parse JSON lines
import json


"""
This script connects to the 'sm_app' database, creates the grad_applications
table if it doesn't exist, truncates it, reads the NDJSON file line-by-line,
and loads 30,000 records into the database.
"""


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
    try:
        cursor.execute(query)
        print("Query executed succesfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def safe_value(value):
    if value == "" or value is None:
        return None
    return value


def safe_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def convert_date(value):
    if safe_value(value) is None:
        return None
    return datetime.strptime(value, "%B %d, %Y").date()


def combine_program_and_university(program_name, university):
    program_name_clean = safe_value(program_name)
    university_clean = safe_value(university)

    if program_name_clean is None and university_clean is None:
        return None
    if program_name_clean is None:
        return university_clean
    if university_clean is None:
        return program_name_clean

    return f"{program_name_clean} - {university_clean}"


# Create a connection to the database
connection = create_connection("sm_app", "postgres", "abc123", "127.0.0.1", "5432")

# SQL query to create the grad_applications table
create_applications_table = """
CREATE TABLE IF NOT EXISTS grad_applications (
  p_id SERIAL PRIMARY KEY,
  program TEXT,
  comments TEXT,
  date_added DATE,
  url TEXT,
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
"""

execute_query(connection, create_applications_table)
execute_query(connection, "TRUNCATE grad_applications RESTART IDENTITY;")

records = []

with open("llm_extend_applicant_data.json", "r") as file:
    for line in file:
        json_data = json.loads(line)

        date_obj = convert_date(json_data.get("date_added"))

        combined_program = combine_program_and_university(
            json_data.get("program_name"),
            json_data.get("university")
        )

        record = (
            combined_program,
            safe_value(json_data.get("comments")),
            date_obj,
            safe_value(json_data.get("url_link")),
            safe_value(json_data.get("applicant_status")),
            safe_value(json_data.get("start_term")),
            safe_value(json_data.get("International/US")),
            safe_float(json_data.get("gpa")),
            safe_float(json_data.get("gre_general")),
            safe_float(json_data.get("gre_verbal")),
            safe_float(json_data.get("gre_analytical_writing")),
            safe_value(json_data.get("degree_type")),
            safe_value(json_data.get("llm-generated-program")),
            safe_value(json_data.get("llm-generated-university")),
        )

        records.append(record)

insert_query = """
INSERT INTO grad_applications (
    program,
    comments,
    date_added,
    url,
    status,
    term,
    us_or_international,
    gpa,
    gre,
    gre_v,
    gre_aw,
    degree,
    llm_generated_program,
    llm_generated_university
) VALUES %s
ON CONFLICT (url) DO NOTHING
"""

cursor = connection.cursor()
execute_values(cursor, insert_query, records)
connection.commit()

print("Bulk insert completed successfully")
