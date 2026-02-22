"""
GradCafe refresh pipeline.

Orchestrates the end-to-end process of loading previously seen result IDs,
scraping new applicant records, enriching them with detail-page data, and
writing the results to the staging JSON file for downstream LLM processing
and database sync.
"""

# Import the regular expressions module for searching text patterns
import re

# Import JSON module for reading and writing JSON data
import json

# Import ThreadPoolExecutor to run tasks in parallel threads
from concurrent.futures import ThreadPoolExecutor

# Import public scrape and clean utilities from the scrape module
from .scrape import scrape, clean
from .paths import NEW_APPLICANT_FILE, LLM_OUTPUT_FILE


def get_seen_ids_from_llm_extend_file(path=LLM_OUTPUT_FILE):
    """Load previously processed result IDs from the LLM output file.

    Reads the NDJSON file line by line, extracts the numeric ID from each
    ``url_link`` field, and returns the full set of already-seen IDs.
    Returns an empty set if the file does not exist.

    :param path: Path to the LLM output NDJSON file.
    :type path: str
    :returns: Set of integer result IDs that have already been processed.
    :rtype: set[int]
    """
    # Initialize an empty set to store IDs that have already been processed
    seen_ids = set()

    try:
        # Open the LLM-processed JSON lines file in read mode
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                url = record.get("url_link")
                if not url:
                    continue

                match = re.search(r"/result/(\d+)", url)
                if match:
                    seen_ids.add(int(match.group(1)))
    except FileNotFoundError:
        print("llm_extend_applicant_data.json not found; starting fresh")

    print(f"Loaded {len(seen_ids)} seen IDs")
    return seen_ids


def scrape_new_records(seen_ids):
    """Scrape survey pages and return records not yet in the database.

    Pages through the GradCafe survey, skipping result IDs already in
    ``seen_ids``. Stops early once ``seen_limit`` consecutive already-seen
    records are encountered in a row.

    :param seen_ids: Set of result IDs to skip.
    :type seen_ids: set[int]
    :returns: List of new applicant record dicts.
    :rtype: list[dict]
    """
    new_records = []
    page = 1
    consecutive_seen = 0

    # Stop scraping if this many seen records are found in a row
    seen_limit = 1

    while True:
        print(f"Scraping survey page {page}")

        html = scrape.fetch_html(scrape.SURVEY_URL.format(page))
        page_results = scrape.parse_survey_page(html)

        if not page_results:
            break

        for record in page_results:
            result_id = int(record["result_id"])

            if result_id in seen_ids:
                consecutive_seen += 1
            else:
                consecutive_seen = 0
                new_records.append(record)

            if consecutive_seen >= seen_limit:
                return new_records

        page += 1

    return new_records


def enrich_with_details(records):
    """Fetch and merge detail-page data into each record using a thread pool.

    :param records: List of survey-level applicant records to enrich.
    :type records: list[dict]
    :returns: The same list with detail fields merged in-place.
    :rtype: list[dict]
    """
    print(f"Enriching {len(records)} records")

    with ThreadPoolExecutor(max_workers=10) as executor:
        details = executor.map(
            scrape.scrape_detail_page,
            [r["result_id"] for r in records],
        )

    for record, detail in zip(records, details):
        record.update(detail)

    return records


def write_new_applicant_file(records):
    """Clean and write enriched records to the new-applicant staging file.

    :param records: List of enriched applicant record dicts.
    :type records: list[dict]
    """
    cleaned = clean.clean_data(records)

    with open(NEW_APPLICANT_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(cleaned)} records to new_applicant_data.json")


def refresh():
    """Run the full GradCafe refresh pipeline.

    Loads seen IDs, scrapes new records, enriches them with detail data,
    and writes the results to the staging file. Returns a summary dict
    with the count of new records added.

    :returns: Dict with key ``new`` containing the number of new records.
    :rtype: dict[str, int]
    """
    print("Starting GradCafe refresh")

    seen_ids = get_seen_ids_from_llm_extend_file()
    new_records = scrape_new_records(seen_ids)

    if not new_records:
        print("No new records found")
        return {"new": 0}

    enriched = enrich_with_details(new_records)
    write_new_applicant_file(enriched)

    print(f"Refresh complete; added {len(enriched)} records")
    return {"new": len(enriched)}
