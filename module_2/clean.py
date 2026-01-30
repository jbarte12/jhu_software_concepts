"""
clean.py

This module is responsible ONLY for:
- Loading scraped applicant data from a JSON file
- Cleaning and restructuring that data into a consistent format
- Saving the cleaned data back to the same JSON file

IMPORTANT:
- No web scraping should happen in this file
- This file only works with local JSON data
"""

# Import the built-in json module so we can read and write JSON files
import json


# Name of the JSON file that stores applicant data
# This file must already exist before this script runs
DATA_FILE = "applicant_data.json"


def load_data():
    """
    Load applicant data from applicant_data.json.

    This function:
    - Opens the JSON file from disk
    - Parses the JSON into Python objects
    - Returns the data as a list of dictionaries

    Raises:
        FileNotFoundError:
            If the JSON file does not exist yet
    """

    # Begin a try/except block to handle missing files cleanly
    try:

        # Open the JSON file in read mode using UTF-8 encoding
        with open(DATA_FILE, "r", encoding="utf-8") as f:

            # Convert the JSON text into Python data structures
            # (lists and dictionaries) and return it
            return json.load(f)

    # Catch the specific error that occurs if the file is missing
    except FileNotFoundError:

        # Re-raise the error with a clearer, user-friendly message
        raise FileNotFoundError(
            "applicant_data.json not found. Run main.py first."
        )


def clean_data(raw_data):
    """
    Convert raw scraped applicant records into a clean,
    structured, and consistent format.

    Parameters:
        raw_data (list):
            A list of dictionaries containing raw scraped applicant data

    Returns:
        list:
            A new list of cleaned dictionaries with a standardized schema
    """

    # Create an empty list to store cleaned applicant records
    cleaned = []

    # Iterate through each raw applicant record
    for record in raw_data:

        # Append a newly cleaned dictionary to the cleaned list
        cleaned.append({

            # Combine program name and school into a single readable field
            "program": f'{record.get("program", "")}, {record.get("school", "")}',

            # Rename the raw "notes" field to "comments"
            "comments": record.get("notes", ""),

            # Rename "added_on" to the more descriptive "date_added"
            "date_added": record.get("added_on", ""),

            # Construct a full GradCafe result URL using the result ID
            "url": f'https://www.thegradcafe.com/result/{record.get("result_id")}',

            # Rename "decision" to "status"
            "status": record.get("decision", ""),

            # Rename "start_term" to "term"
            "term": record.get("start_term", ""),

            # Preserve citizenship field using a clear label
            "US/International": record.get("citizenship", ""),

            # Rename "degree_type" to "degree"
            "degree": record.get("degree_type", "")
        })

    # Return the fully cleaned list of applicant records
    return cleaned


def save_data(data):
    """
    Save cleaned applicant data back to applicant_data.json.

    This function:
    - Overwrites the existing file
    - Writes formatted JSON for readability
    """

    # Open the JSON file in write mode using UTF-8 encoding
    with open(DATA_FILE, "w", encoding="utf-8") as f:

        # Serialize the cleaned Python data into JSON format
        json.dump(
            data,               # The cleaned applicant data
            f,                  # The file object to write to
            indent=2,           # Indent JSON for human readability
            ensure_ascii=False  # Preserve non-ASCII characters
        )

    # Print confirmation message indicating success
    print("Cleaned data saved to applicant_data.json")