# refresh_gradcafe.py

import re
import json
from concurrent.futures import ThreadPoolExecutor

from scrape import scrape, clean


def get_seen_ids_from_llm_extend_file(path="llm_extend_applicant_data.json"):
    seen_ids = set()

    try:
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
    new_records = []
    page = 1
    consecutive_seen = 0
    SEEN_LIMIT = 3

    while True:
        print(f"Scraping survey page {page}")

        html = scrape._fetch_html(scrape.SURVEY_URL.format(page))
        page_results = scrape._parse_survey_page(html)

        if not page_results:
            break

        for record in page_results:
            result_id = int(record["result_id"])

            if result_id in seen_ids:
                consecutive_seen += 1
            else:
                consecutive_seen = 0
                new_records.append(record)

            if consecutive_seen >= SEEN_LIMIT:
                return new_records

        page += 1

    return new_records


def enrich_with_details(records):
    print(f"Enriching {len(records)} records")

    with ThreadPoolExecutor(max_workers=10) as executor:
        details = executor.map(
            scrape._scrape_detail_page,
            [r["result_id"] for r in records],
        )

    for record, detail in zip(records, details):
        record.update(detail)

    return records


def write_new_applicant_file(records):
    cleaned = clean.clean_data(records)

    with open("new_applicant_data.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(cleaned)} records to new_applicant_data.json")


def refresh():
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
