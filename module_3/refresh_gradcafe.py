# --------------------------------------------------
# GradCafe incremental refresh pipeline
# --------------------------------------------------

# -------------------------------
# Standard library imports
# -------------------------------

import re
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# -------------------------------
# Third-party imports
# -------------------------------

from psycopg2.extras import execute_values

# -------------------------------
# Local application imports
# -------------------------------

from load_data import create_connection
from scrape import scrape, clean
from update_data import update_data


# ==================================================
# FILE-BASED CURSOR (NEW)
# ==================================================

def get_seen_ids_from_llm_extend_file(path="llm_extend_applicant_data.json"):
    """
    Return a set of all result_ids already present in
    llm_extend_applicant_data.json.
    """
    seen_ids = set()

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

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

    print(f"Loaded {len(seen_ids)} seen ids")
    return seen_ids


# ==================================================
# Scraping logic (UPDATED)
# ==================================================

def scrape_new_records(seen_ids):
    """
    Scrape GradCafe survey pages until we hit already-seen IDs.

    Stops only after N consecutive already-seen records
    to avoid false positives due to page ordering.
    """

    new_records = []
    page = 1
    consecutive_seen = 0
    SEEN_LIMIT = 3  # stop after 3 consecutive seen IDs

    while True:
        print(f"Scraping survey page {page}")

        html = scrape._fetch_html(scrape.SURVEY_URL.format(page))
        page_results = scrape._parse_survey_page(html)

        if not page_results:
            print("No results returned; stopping scrape")
            break

        page_new_count = 0
        page_seen_ids = []  # NEW: store seen ids on this page

        for record in page_results:
            result_id = int(record["result_id"])

            if result_id in seen_ids:
                consecutive_seen += 1
                page_seen_ids.append(result_id)
            else:
                consecutive_seen = 0
                page_new_count += 1
                new_records.append(record)

        # NEW: debug output for duplicates on this page
        if page_seen_ids:
            print(
                f"Page {page} contained already-seen IDs: {page_seen_ids[:10]} "
                f"{'(and more)' if len(page_seen_ids) > 10 else ''}"
            )

        if consecutive_seen >= SEEN_LIMIT:
            print(
                f"Stopping because we hit {SEEN_LIMIT} consecutive seen IDs. "
                f"Last seen id: {page_seen_ids[-1] if page_seen_ids else 'unknown'}"
            )
            break

        page += 1

    return new_records


# ==================================================
# Detail page enrichment
# ==================================================

def enrich_with_details(records):
    print(f"Enriching {len(records)} records with detail pages")

    with ThreadPoolExecutor(max_workers=10) as executor:
        details = executor.map(
            scrape._scrape_detail_page,
            [r["result_id"] for r in records]
        )

    for record, detail in zip(records, details):
        record.update(detail)

    return records


# ==================================================
# File output (for LLM input)
# ==================================================

def write_new_applicant_file(records):
    print("Writing new_applicant_data.json")

    cleaned = clean.clean_data(records)

    with open("new_applicant_data.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(cleaned)} records to new_applicant_data.json")


# ==================================================
# Database insertion
# ==================================================

def insert_new_records(records):
    print(f"Inserting {len(records)} records into database")

    conn = create_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
        idx_grad_applications_url
        ON grad_applications(url);
        """
    )

    conn.commit()

    cleaned = clean.clean_data(records)

    rows = []

    for r in cleaned:
        rows.append(
            (
                (
                    f"{r['program_name']} - {r['university']}"
                    if r["program_name"] and r["university"]
                    else r["program_name"] or r["university"]
                ),
                r["comments"],
                (
                    datetime.strptime(r["date_added"], "%B %d, %Y").date()
                    if r["date_added"]
                    else None
                ),
                r["url_link"],
                r["applicant_status"],
                r["start_term"],
                r["International/US"],
                float(r["gpa"]) if r["gpa"] else None,
                float(r["gre_general"]) if r["gre_general"] else None,
                float(r["gre_verbal"]) if r["gre_verbal"] else None,
                (
                    float(r["gre_analytical_writing"])
                    if r["gre_analytical_writing"]
                    else None
                ),
                r["degree_type"],
                None,
                None,
            )
        )

    insert_sql = """
        INSERT INTO grad_applications (
            program,
            comments,
            date_added,
            url,
            status,
            term,
            us_or_international,
            gpa,
            gre,
            gre_v,
            gre_aw,
            degree,
            llm_generated_program,
            llm_generated_university
        )
        VALUES %s
        ON CONFLICT (url) DO NOTHING;
    """

    execute_values(cur, insert_sql, rows)

    conn.commit()
    conn.close()

    print("Database insert completed")


# ==================================================
# Main refresh entry point
# ==================================================

def refresh():
    print("Starting GradCafe refresh")

    # 🔑 Use file-based cursor instead of DB
    seen_ids = get_seen_ids_from_llm_extend_file()

    new_records = scrape_new_records(seen_ids)

    if not new_records:
        print("No new records found")
        return {"new": 0}

    enriched_records = enrich_with_details(new_records)

    write_new_applicant_file(enriched_records)

    insert_new_records(enriched_records)

    print("Running LLM on new_applicant_data.json")
    update_data("new_applicant_data.json")

    print(f"Refresh complete; added {len(enriched_records)} new records")

    return {"new": len(enriched_records)}
