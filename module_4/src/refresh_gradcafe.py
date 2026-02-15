# refresh_gradcafe.py

# Import the regular expressions module for searching text patterns
import re

# Import JSON module for reading and writing JSON data
import json

# Import ThreadPoolExecutor to run tasks in parallel threads
from concurrent.futures import ThreadPoolExecutor

# Import scrape and clean utilities from the scrape module
from scrape import clean, scrape


def get_seen_ids_from_llm_extend_file(
        path="src/llm_extend_applicant_data.json"):
    # Initialize an empty set to store IDs that have already been processed
    seen_ids = set()

    try:
        # Open the LLM-processed JSON lines file in read mode
        with open(path, "r", encoding="utf-8") as f:
            # Read the file line by line
            for line in f:
                try:
                    # Try to parse the line as a JSON object
                    record = json.loads(line)
                except json.JSONDecodeError:
                    # If parsing fails, skip that line and continue
                    continue

                # Extract the URL link from the record
                url = record.get("url_link")
                # If URL is missing, skip this record
                if not url:
                    continue

                # Find the numeric ID in the URL using regex
                match = re.search(r"/result/(\d+)", url)
                if match:
                    # Convert the found ID to integer and add to seen_ids set
                    seen_ids.add(int(match.group(1)))
    except FileNotFoundError:
        # If the file doesn't exist, start with an empty seen_ids set
        print("llm_extend_applicant_data.json not found; starting fresh")

    # Print how many IDs were loaded for debugging
    print(f"Loaded {len(seen_ids)} seen IDs")
    # Return the set of already-seen IDs
    return seen_ids


def scrape_new_records(seen_ids):
    # Initialize a list to collect new records
    new_records = []

    # Start scraping from the first survey page
    page = 1

    # Counter for consecutive already-seen records
    consecutive_seen = 0

    # Stop scraping if this many seen records are found in a row
    SEEN_LIMIT = 3

    # Loop until no more pages or stop condition is met
    while True:
        # Log current page being scraped
        print(f"Scraping survey page {page}")

        # Download the HTML for the current survey page
        html = scrape._fetch_html(scrape.SURVEY_URL.format(page))

        # Parse the HTML into a list of records
        page_results = scrape._parse_survey_page(html)

        # If no results are found on the page, stop scraping
        if not page_results:
            break

        # Loop through each record on the page
        for record in page_results:
            # Convert the result ID from string to integer
            result_id = int(record["result_id"])

            # If ID already seen, increase the consecutive counter
            if result_id in seen_ids:
                consecutive_seen += 1
            else:
                # If not seen, reset counter and add record to new_records
                consecutive_seen = 0
                new_records.append(record)

            # If we reach the consecutive seen limit, stop scraping
            if consecutive_seen >= SEEN_LIMIT:
                return new_records

        # Move to the next survey page
        page += 1

    # Return all newly found records
    return new_records


def enrich_with_details(records):
    # Log how many records will be enriched
    print(f"Enriching {len(records)} records")

    # Use a thread pool to scrape details concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:

        # Map each record ID to the detail scraping function
        details = executor.map(
            scrape._scrape_detail_page,
            [r["result_id"] for r in records],
        )

    # Combine each detail result into its original record
    for record, detail in zip(records, details):
        record.update(detail)

    # Return enriched records
    return records


def write_new_applicant_file(records):
    # Clean and normalize the data using clean module
    cleaned = clean.clean_data(records)

    # Write the cleaned data to a JSON file (overwrites previous content)
    with open("src/new_applicant_data.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    # Log how many records were written
    print(f"Wrote {len(cleaned)} records to new_applicant_data.json")


def refresh():
    # Log that refresh process has started
    print("Starting GradCafe refresh")

    # Load IDs that have already been processed
    seen_ids = get_seen_ids_from_llm_extend_file()

    # Scrape new records excluding already seen ones
    new_records = scrape_new_records(seen_ids)

    # If no new records found, stop and return zero
    if not new_records:
        print("No new records found")
        return {"new": 0}

    # Enrich the new records with detail page data
    enriched = enrich_with_details(new_records)

    # Write enriched data to staging file
    write_new_applicant_file(enriched)

    # Log completion and return number of new records
    print(f"Refresh complete; added {len(enriched)} records")
    return {"new": len(enriched)}
