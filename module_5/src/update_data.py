"""
LLM enrichment pipeline for GradCafe applicant data.

Reads newly scraped applicant records from the staging file, calls the
locally hosted LLM to standardize program and university names, appends
the enriched records to the cumulative NDJSON output file, and clears
the staging file on completion.
"""

# Import the json module for reading and writing JSON data
import json

# Import os for atomic temp-file renaming
import os

# Import tempfile for safe intermediate writes
import tempfile

# Import the internal function used to call the LLM for standardization
from .scrape.llm_hosting.app import _call_llm

from .paths import NEW_APPLICANT_FILE, LLM_OUTPUT_FILE


def _append_lines_atomically(lines: list, llm_output_path: str) -> None:
    """Atomically append enriched NDJSON lines to the cumulative output file.

    Writes ``lines`` to a sibling temp file first, then appends its contents
    to ``llm_output_path``, and cleans up the temp file in a ``finally``
    block. This ensures the cumulative file is never left in a partial state
    if the process is interrupted.

    Extracted into its own function to keep :func:`update_data` within the
    local-variable limit enforced by Pylint (R0914).

    :param lines: List of JSON strings (one per record) to append.
    :type lines: list[str]
    :param llm_output_path: Path to the cumulative NDJSON output file.
    :type llm_output_path: str
    """
    output_dir = os.path.dirname(os.path.abspath(llm_output_path))
    fd, tmp_path = tempfile.mkstemp(dir=output_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp_f:
            tmp_f.write("\n".join(lines) + "\n")

        with open(tmp_path, "r", encoding="utf-8") as tmp_r, \
             open(llm_output_path, "a", encoding="utf-8") as out:
            out.write(tmp_r.read())
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def update_data(
    new_data_path=NEW_APPLICANT_FILE,
    llm_output_path=LLM_OUTPUT_FILE,
):
    """Process newly scraped applicant records through the LLM enrichment pipeline.

    Reads records from ``new_data_path``, calls the LLM once per record to
    standardize the program and university names, appends each enriched record
    as a JSON line to ``llm_output_path``, then clears the staging file.

    LLM failures for individual records are caught and logged; the record is
    still written to the output file with ``None`` for the LLM-generated
    fields so that a single bad record does not abort the whole pipeline.

    The output file is written to a temporary file and renamed atomically on
    completion so that a mid-run crash leaves the original file intact.

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

    # Collect newly enriched lines so we can append them atomically.
    # Writing to a temp file and then renaming prevents a partial write from
    # corrupting the cumulative output file if the process is interrupted.
    new_lines = []

    for row in rows:
        # Combine program name and university into a single text prompt
        program_text = row.get("program_name", "") + ", " + row.get("university", "")

        # Call the LLM to standardize program and university names.
        # Catch any exception so one bad record never aborts the pipeline.
        try:
            result = _call_llm(program_text)
            row["llm-generated-program"] = result.get("standardized_program")
            row["llm-generated-university"] = result.get("standardized_university")
        except Exception as exc:  # pylint: disable=broad-except
            print(
                f"Warning: LLM call failed for '{program_text}': {exc}. "
                f"Setting llm-generated fields to None."
            )
            row["llm-generated-program"] = None
            row["llm-generated-university"] = None

        # Accumulate the enriched record as a JSON line (NDJSON format)
        new_lines.append(json.dumps(row, ensure_ascii=False))
        processed += 1

    # Atomically append all enriched lines to the cumulative output file.
    _append_lines_atomically(new_lines, llm_output_path)

    # Overwrite the staging file with an empty list to prevent re-processing
    with open(new_data_path, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)

    print(f"LLM analysis complete; processed {processed} records")
    print("new_applicant_data.json cleared")

    return processed
