"""
Data cleaning utilities for GradCafe applicant records.

Reads raw scraped JSON, normalizes text fields and applicant status
values into a consistent schema, and writes the cleaned output to disk.
"""

# Import JSON utilities
import json

# Import regular expressions for date parsing
import re

# Input file containing raw scraped data
RAW_FILE = "applicant_data.json"

# Output file containing cleaned applicant data
OUT_FILE = "applicant_data.json"


def load_data():
    """Load raw applicant data from disk.

    Reads the JSON file at :data:`RAW_FILE` and returns its contents
    as a Python list.

    :returns: List of raw applicant record dicts.
    :rtype: list[dict]
    """
    with open(RAW_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _norm(value):
    """Normalize a text value and guarantee string output.

    Returns an empty string if the value is ``None`` or falsy, otherwise
    strips and collapses internal whitespace.

    :param value: Raw string value to normalize, or ``None``.
    :type value: str or None
    :returns: Whitespace-normalized string, or ``""`` if value is falsy.
    :rtype: str
    """
    if not value:
        return ""
    return " ".join(value.split())


def _normalize_status(status):
    """Normalize an applicant status string into a consistent format.

    Applies the following rules in order:

    1. Returns ``""`` if ``status`` is falsy.
    2. Returns ``"Waitlisted"`` if the status contains ``"wait"``.
    3. Returns ``"Interview"`` if the status contains ``"interview"``.
    4. For statuses starting with ``"accepted"`` or ``"rejected"``,
       extracts the decision word and any day/month date present in the
       string, returning ``"Decision: D Mon"`` if a date is found or
       just ``"Decision"`` if not.
    5. Returns the status unchanged for anything else.

    :param status: Raw applicant status string, or ``None``.
    :type status: str or None
    :returns: Normalized status string.
    :rtype: str
    """
    if not status:
        return ""

    lower = status.lower()

    if "wait" in lower:
        return "Waitlisted"

    if "interview" in lower:
        return "Interview"

    if lower.startswith("accepted") or lower.startswith("rejected"):
        decision = status.split()[0].rstrip(":")
        match = re.search(
            r"\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b",
            status
        )
        if not match:
            return decision
        return f"{decision}: {match.group(1)} {match.group(2)}"

    return status


def clean_data(raw_records):
    """Convert raw scraped records into the final normalized schema.

    Iterates over ``raw_records``, applying :func:`_norm` to all text
    fields and :func:`_normalize_status` to the applicant status field.

    :param raw_records: List of raw applicant record dicts as returned
        by the scraper.
    :type raw_records: list[dict]
    :returns: List of cleaned applicant record dicts conforming to the
        application schema.
    :rtype: list[dict]
    """
    cleaned = []
    for r in raw_records:
        cleaned.append({
            "program_name": _norm(r.get("program_name")),
            "university": _norm(r.get("university")),
            "degree_type": _norm(r.get("degree_type")),
            "comments": _norm(r.get("comments")),
            "date_added": _norm(r.get("date_added")),
            "url_link": _norm(r.get("url_link")),
            "applicant_status": _normalize_status(_norm(r.get("applicant_status"))),
            "start_term": _norm(r.get("start_term")),
            "International/US": _norm(r.get("International/US")),
            "gre_general": _norm(r.get("gre_general")),
            "gre_verbal": _norm(r.get("gre_verbal")),
            "gre_analytical_writing": _norm(r.get("gre_analytical_writing")),
            "gpa": _norm(r.get("gpa")),
        })
    return cleaned


def save_data(data):
    """Save cleaned applicant data to disk as formatted JSON.

    Writes ``data`` to :data:`OUT_FILE` and prints a confirmation message.

    :param data: List of cleaned applicant record dicts to save.
    :type data: list[dict]
    """
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Cleaned data saved to {OUT_FILE}")
