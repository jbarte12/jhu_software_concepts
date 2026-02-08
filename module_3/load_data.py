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


def create_connection(db_name, db_user, db_password, db_host, db_port):
    # Initialize connection variable
    connection = None
    # Try to connect to the database
    try:
        # Establish connection using psycopg2
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        # Print success message
        print("Connection to PostgreSQL DB successful")
    # Handle connection errors
    except OperationalError as e:
        # Print error message
        print(f"The error '{e}' occurred")
    # Return connection object
    return connection


def execute_query(connection, query):
    # Enable autocommit mode
    connection.autocommit = True
    # Create a cursor
    cursor = connection.cursor()
    # Try to execute the query
    try:
        # Execute the SQL query
        cursor.execute(query)
        # Print success message
        print("Query executed succesfully")
    # Handle execution errors
    except OperationalError as e:
        # Print error message
        print(f"The error '{e}' occurred")


def safe_value(value):
    # Check if value is empty or None
    if value == "" or value is None:
        # Return None for empty values
        return None
    # Return the original value
    return value


def safe_float(value):
    # Check if value is empty or None
    if value is None or value == "":
        # Return None for empty values
        return None
    # Try to convert to float
    try:
        # Return float value
        return float(value)
    # Handle non-numeric values
    except ValueError:
        # Return None if conversion fails
        return None


def convert_date(value):
    # Return None for empty values
    if safe_value(value) is None:
        # Return None if date is missing
        return None
    # Convert string to date object using format
    return datetime.strptime(value, "%B %d, %Y").date()


def combine_program_and_university(program_name, university):
    # Clean program_name
    program_name_clean = safe_value(program_name)
    # Clean university
    university_clean = safe_value(university)

    # If both are missing, return None
    if program_name_clean is None and university_clean is None:
        # Return None when both fields missing
        return None
    # If program is missing, return university only
    if program_name_clean is None:
        # Return university if program missing
        return university_clean
    # If university is missing, return program only
    if university_clean is None:
        # Return program if university missing
        return program_name_clean

    # Combine program and university
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

# Execute the table creation query
execute_query(connection, create_applications_table)

# Truncate table to remove old records
execute_query(connection, "TRUNCATE grad_applications RESTART IDENTITY;")

# Initialize list to hold all records
records = []

# Open the NDJSON file line-by-line
with open("llm_extend_applicant_data.json", "r") as file:
    # Loop through each line in the file
    for line in file:
        # Parse the JSON object from the line
        json_data = json.loads(line)

        # Convert the date_added field to a date object
        date_obj = convert_date(json_data.get("date_added"))

        # Combine program_name and university into one field
        combined_program = combine_program_and_university(
            json_data.get("program_name"),
            json_data.get("university")
        )

        # Create a tuple for each record
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

        # Append the record tuple to the list
        records.append(record)

# SQL query for bulk insert
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
"""

# Create a cursor for database operations
cursor = connection.cursor()

# Bulk insert records using execute_values
execute_values(cursor, insert_query, records)

# Commit the transaction to save data
connection.commit()

# Print completion message
print("Bulk insert completed successfully")
