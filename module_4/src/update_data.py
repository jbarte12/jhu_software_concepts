# Import the json module for reading and writing JSON data
import json

# Import the internal function used to call the LLM for standardization
from .scrape.llm_hosting.app import _call_llm

from .paths import NEW_APPLICANT_FILE, LLM_OUTPUT_FILE

# Define a function to process newly scraped applicant data with the LLM
def update_data(

    # Path to newly scraped, unprocessed data
    new_data_path= NEW_APPLICANT_FILE,

    # Path to the cumulative LLM-processed output file
    llm_output_path=LLM_OUTPUT_FILE,
):

    # Log that the update_data function has been called
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

        # Loop over each newly scraped applicant record
        for row in rows:

            # Combine program name and university into a single text prompt
            program_text = f"{row.get('program_name','')}, {row.get('university','')}"

            # Call the LLM to standardize program and university names
            result = _call_llm(program_text)

            # Store the LLM-standardized program name back into the record
            row["llm-generated-program"] = result.get("standardized_program")

            # Store the LLM-standardized university name back into the record
            row["llm-generated-university"] = result.get("standardized_university")

            # Write the enriched record as a single JSON line (NDJSON format)
            out.write(json.dumps(row, ensure_ascii=False) + "\n")

            # Increment the processed record counter
            processed += 1

    # After successful processing, overwrite the staging file with an empty list
    # This prevents re-processing the same records again
    with open(new_data_path, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)

    # Log completion status and number of processed records
    print(f"LLM analysis complete; processed {processed} records")

    # Log that the staging file has been cleared
    print("new_applicant_data.json cleared")

    # Return the number of records processed
    return processed
