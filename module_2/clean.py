# Import JSON utilities
import json

# Import regular expressions for date parsing
import re

# Input file containing raw scraped data
RAW_FILE = "applicant_data_test.json"

# Output file containing cleaned applicant data
OUT_FILE = "applicant_data_test.json"


# Load raw applicant data from disk
def load_data():

    # Open raw data file
    with open(RAW_FILE, "r", encoding="utf-8") as f:

        # Parse JSON into Python objects
        return json.load(f)


# Normalize text values and guarantee string output
def _norm(value):

    # Return empty string if value is missing or empty
    if not value:
        return ""

    # Normalize whitespace
    return " ".join(value.split())


# Normalize applicant status values and fix decision dates
def _normalize_status(status):

    # Return empty string if status is missing
    if not status:
        return ""

    # Normalize case for matching
    lower = status.lower()

    # Handle waitlist
    if "wait" in lower:
        return "Waitlisted"

    # Handle interview
    if "interview" in lower:
        return "Interview"

    # Handle accepted / rejected with date
    if lower.startswith("accepted") or lower.startswith("rejected"):

        # Extract decision word
        decision = status.split()[0].rstrip(":")

        # Match day + month anywhere in string
        match = re.search(
            r"\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b",
            status
        )

        # If no date found, return decision only
        if not match:
            return decision

        # Extract day and month
        day = match.group(1)
        month = match.group(2)

        # Month-based year inference
        if month in {"Jan", "Feb"}:
            year = "2026"
        else:
            year = "2025"

        return f"{decision}: {day} {month} {year}"

    # Preserve anything else as-is
    return status


# Convert raw records into final schema
def clean_data(raw_records):

    # Storage for cleaned records
    cleaned = []

    # Iterate through raw records
    for r in raw_records:

        # Append normalized record
        cleaned.append({
            "program_name": _norm(r.get("program_name")),
            "university": _norm(r.get("university")),
            "degree_type": _norm(r.get("degree_type")),
            "comments": _norm(r.get("comments")),
            "date_added": _norm(r.get("date_added")),
            "url_link": _norm(r.get("url_link")),
            "applicant_status": _normalize_status(
                _norm(r.get("applicant_status"))
            ),
            "start_term": _norm(r.get("start_term")),
            "International/US": _norm(r.get("International/US")),
            "gre_general": _norm(r.get("gre_general")),
            "gre_verbal": _norm(r.get("gre_verbal")),
            "gre_analytical_writing": _norm(r.get("gre_analytical_writing")),
            "gpa": _norm(r.get("gpa")),
        })

    # Return cleaned dataset
    return cleaned


# Save cleaned applicant data to disk
def save_data(data):

    # Open output file
    with open(OUT_FILE, "w", encoding="utf-8") as f:

        # Write formatted JSON
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Confirmation message
    print(f"Cleaned data saved to {OUT_FILE}")
