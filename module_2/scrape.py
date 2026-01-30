"""
scraper.py

This file is responsible ONLY for:
- Scraping raw application data from GradCafe
- Saving the raw data to applicant_data.json

No cleaning or restructuring logic should live here.
"""

import urllib.request
import json
import re
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

# ================= CONFIG =================

BASE_URL = "https://www.thegradcafe.com"
SURVEY_URL = "https://www.thegradcafe.com/survey/"

NUM_RESULTS = 1000        # Total number of results to scrape
NUM_WORKERS = 8         # Number of threads (I/O-bound → threads are good)
TIMEOUT = 30            # Seconds before a request times out

OUT_FILE = "applicant_data.json"  # Raw data output file

# =========================================


def _fetch(url):
    """
    Fetch HTML from a URL using urllib.

    This function handles:
    - Adding a User-Agent header
    - Applying a timeout
    - Returning decoded HTML
    """
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"}
    )

    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return response.read().decode("utf-8")


def _scrape_detail(result_id):
    """
    Fetch an individual GradCafe result page
    and extract the comments section.
    """
    url = f"{BASE_URL}/result/{result_id}"
    html = _fetch(url)
    soup = BeautifulSoup(html, "html.parser")

    notes_div = soup.find("div", class_="post_body")
    return notes_div.get_text(" ", strip=True) if notes_div else ""


def _parse_survey_page(html):
    """
    Parse a single survey page.

    Extracts:
    - School
    - Program
    - Date added
    - Decision
    - Result ID
    - Tags (term, citizenship, degree)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")

    results = []
    current = None

    for row in rows:
        cells = row.find_all("td")

        # Main row containing core data
        if len(cells) >= 4:
            link = row.find("a", href=re.compile(r"/result/"))
            if not link:
                continue

            result_id = link["href"].split("/")[-1]

            current = {
                "result_id": result_id,
                "school": cells[0].get_text(strip=True),
                "program": cells[1].get_text(" ", strip=True),
                "added_on": cells[2].get_text(strip=True),
                "decision": cells[3].get_text(strip=True),
                "start_term": "",
                "citizenship": "",
                "degree_type": "",
                "notes": ""
            }

            results.append(current)

        # Tag row (term, citizenship, degree)
        if current:
            tags = row.find_all("div", class_=re.compile("tw-inline-flex"))
            for tag in tags:
                text = tag.get_text(strip=True)

                if re.search(r"(Fall|Spring|Summer|Winter)\s+\d{4}", text):
                    current["start_term"] = text
                elif text.lower() in ["international", "american", "us", "u.s."]:
                    current["citizenship"] = (
                        "International" if "inter" in text.lower() else "US"
                    )
                elif text.lower() in ["phd", "ms", "ma", "mba", "msc"]:
                    current["degree_type"] = text

    return results


def scrape_data():
    """
    Main scraping function.

    Returns:
        List of raw applicant dictionaries
    """
    all_results = []
    seen_ids = set()
    page = 1

    # Loop through survey pages until we have enough results
    while len(all_results) < NUM_RESULTS:
        html = _fetch(f"{SURVEY_URL}?page={page}")
        page_results = _parse_survey_page(html)

        if not page_results:
            break

        for result in page_results:
            if result["result_id"] not in seen_ids:
                seen_ids.add(result["result_id"])
                all_results.append(result)

            if len(all_results) >= NUM_RESULTS:
                break

        page += 1

    # Fetch detail pages (comments) in parallel
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        notes = executor.map(
            _scrape_detail,
            [r["result_id"] for r in all_results]
        )

    # Attach notes to each record
    for record, note in zip(all_results, notes):
        record["notes"] = note

    return all_results


def save_data(data):
    """
    Save raw scraped data to applicant_data.json.
    """
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(data)} records to {OUT_FILE}")