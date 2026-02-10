# refresh_gradcafe.py

# Import regular expressions module for pattern matching
import re
# Import JSON module for reading and writing JSON data
import json
# Import ThreadPoolExecutor for parallel execution of tasks
from concurrent.futures import ThreadPoolExecutor

# Import scraping and cleaning utilities from the scrape module
from scrape import scrape, clean


# Load IDs that have already been seen from the LLM-extended applicant data file
def get_seen_ids_from_llm_extend_file(path="llm_extend_applicant_data.json"):
    # Initialize an empty set to store seen result IDs
    seen_ids = set()

    try:
        # Open the JSON file containing previously processed records
        with open(path, "r", encoding="utf-8") as f:
            # Iterate through the file line by line
            for line in f:
                try:
                    # Parse each line as a JSON object
                    record = json.loads(line)
                except json.JSONDecodeError:
                    # Skip lines that are not valid JSON
                    continue

                # Extract the URL field from the record
                url = record.get("url_link")
                # Skip records without a URL
                if not url:
                    continue

                # Extract the numeric result ID from the URL
                match = re.search(r"/result/(\d+)", url)
                if match:
                    # Add the extracted ID to the seen IDs set
                    seen_ids.add(int(match.group(1)))
    except FileNotFoundError:
        # Handle the case where the file does not exist
        print("llm_extend_applicant_data.json not found; starting fresh")

    # Log how many previously seen IDs were loaded
    print(f"Loaded {len(seen_ids)} seen IDs")
    # Return the set of seen IDs
    return seen_ids


# Scrape survey pages and collect records that have not been seen before
def scrape_new_records(seen_ids):
    # Initialize a list to store newly discovered records
    new_records = []
    # Start scraping from the first survey page
    page = 1
    # Track how many seen records appear consecutively
    consecutive_seen = 0
    # Stop scraping after encountering this many seen records in a row
    SEEN_LIMIT = 3

    # Continue scraping until a stopping condition is reached
    while True:
        # Log which survey page is being scraped
        print(f"Scraping survey page {page}")

        # Fetch the raw HTML for the current survey page
        html = scrape._fetch_html(scrape.SURVEY_URL.format(page))
        # Parse the HTML into structured survey results
        page_results = scrape._parse_survey_page(html)

        # Stop if no results are returned for the page
        if not page_results:
            break

        # Iterate through each record on the page
        for record in page_results:
            # Convert the record ID to an integer
            result_id = int(record["result_id"])

            # Check if this record has already been seen
            if result_id in seen_ids:
                # Increment the consecutive seen counter
                consecutive_seen += 1
            else:
                # Reset the counter when a new record is found
                consecutive_seen = 0
                # Add the new record to the results list
                new_records.append(record)

            # Stop scraping once the seen limit is reached
            if consecutive_seen >= SEEN_LIMIT:
                return new_records

        # Move on to the next survey page
        page += 1

    # Return all newly discovered records
    return new_records


# Enrich basic survey records with detailed application data
def enrich_with_details(records):
    # Log how many records will be enriched
    print(f"Enriching {len(records)} records")

    # Create a thread pool to scrape detail pages concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Fetch detailed data for each record in parallel
        details = executor.map(
            scrape._scrape_detail_page,
            [r["result_id"] for r in records],
        )

    # Merge each detailed record into its corresponding base record
    for record, detail in zip(records, details):
        record.update(detail)

    # Return the enriched records
    return records


# Write newly scraped and cleaned applicant data to a JSON file
def write_new_applicant_file(records):
    # Clean and norma
