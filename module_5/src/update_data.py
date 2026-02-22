"""
LLM enrichment pipeline for GradCafe applicant data.

Reads newly scraped applicant records from the staging file, calls the
locally hosted LLM to standardize program and university names, appends
the enriched records to the cumulative NDJSON output file, and clears
the staging file on completion.
"""

# Import the json module for reading and writing JSON data
import json

# Import the internal function used to call the LLM for standardization
from .scrape.llm_hosting.app import _call_llm

from .paths import NEW_APPLICANT_FILE, LLM_OUTPUT_FILE


def update_data(
    new_data_path=NEW_APPLICANT_FILE,
    llm_output_path=LLM_OUTPUT_FILE,
):
    """Process newly scraped applicant records through the LLM enrichment pipeline.

    Reads records from ``new_data_path``, calls the LLM once per record to
    standardize the program and university names, appends each enriched record
    as a JSON line to ``llm_output_path``, then clears the staging file.

    Returns early with ``0`` if the staging file is missing or empty.

    :param new_data_path: Path to the staging JSON file containing newly
        scraped, unprocessed applicant records.
    :type new_data_path: str
    :param llm_output_path: Path to the cumulative NDJSON file where
        LLM-enriched records are appended.
    :type llm_output_path: str
    :returns: Number of records successfully processed by the LLM.
    :rtype: int
    """
    print("update_data() CALLED")

    try:
        # Attempt to open and load the new applicant data file
        with open(new_data_path, "r", encoding="utf-8") as f:
            rows = json.load(f)
    except FileNotFoundError:
        # If the staging file does not exist, log and exit early
        print("No new_applicant_data.json found")
        return 0

    # If the file exists but contains no records, log and exit early
    if not rows:
        print("No new records to analyze")
        return 0

    # Counter to track how many records are successfully processed
    processed = 0

    # Open the LLM output file in append mode so existing data is preserved
    with open(llm_output_path, "a", encoding="utf-8") as out:
        for row in rows:
            # Combine program name and university into a single text prompt
            program_text = row.get("program_name", "") + ", " + row.get("university", "")

            # Call the LLM to standardize program and university names
            result = _call_llm(program_text)

            # Store the LLM-standardized fields back into the record
            row["llm-generated-program"] = result.get("standardized_program")
            row["llm-generated-university"] = result.get("standardized_university")

            # Write the enriched record as a single JSON line (NDJSON format)
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
            processed += 1

    # Overwrite the staging file with an empty list to prevent re-processing
    with open(new_data_path, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)

    print(f"LLM analysis complete; processed {processed} records")
    print("new_applicant_data.json cleared")

    return processed
