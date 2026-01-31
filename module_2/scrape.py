# Import JSON for saving scraped data
import json

# Import regular expressions for pattern matching
import re

# Import urllib for HTTP requests
import urllib.request

# Import time module for execution timing
import time

# Import thread pool for parallel detail-page scraping
from concurrent.futures import ThreadPoolExecutor

# Import BeautifulSoup for HTML parsing
from bs4 import BeautifulSoup

# Base GradCafe URL
BASE_URL = "https://www.thegradcafe.com"

# Survey page URL template
SURVEY_URL = "https://www.thegradcafe.com/survey/index.php?page={}"

# Maximum number of records to scrape
MAX_RECORDS = 100

# Number of parallel workers for detail pages
NUM_WORKERS = 12

# HTTP timeout in seconds
TIMEOUT = 30

# Output file for raw scraped data
OUTPUT_FILE = "applicant_data.json"

# Fetch HTML content from a URL
def _fetch_html(url):

    # Build HTTP request with user-agent header
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
    )

    # Open URL and read response
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return response.read().decode("utf-8")

# Clean and normalize visible text from an HTML element
def _clean_text(element):

    # Return empty string if element is missing
    if element is None:
        return ""

    # Extract visible text and normalize whitespace
    return " ".join(element.get_text(" ", strip=True).split())

# Extract value from a dt/dd pair by label
def _extract_dt_dd(soup, label):

    # Iterate through all dt elements
    for dt in soup.find_all("dt"):

        # Match label text exactly
        if _clean_text(dt) == label:

            # Locate parent container
            parent = dt.parent

            # Return corresponding dd text
            if parent:
                return _clean_text(parent.find("dd"))

    # Return empty string if not found
    return ""

# Extract undergraduate GPA from detail page
def _extract_undergrad_gpa(soup):

    # Extract GPA value from dt/dd pair
    gpa = _extract_dt_dd(soup, "Undergrad GPA")

    # Normalize placeholder zero values
    if gpa in {"0", "0.0", "0.00"}:
        return ""

    # Return cleaned GPA
    return gpa

# Extract GRE scores from detail page
def _extract_gre_scores(soup):

    # Initialize GRE fields
    scores = {
        "gre_general": "",
        "gre_verbal": "",
        "gre_analytical_writing": "",
    }

    # Find all span elements
    spans = soup.find_all("span")

    # Iterate through span elements with index
    for i, span in enumerate(spans):

        # Extract and normalize label text
        label = _clean_text(span).lower()

        # Skip non-label spans
        if not label.endswith(":"):
            continue

        # Ensure a value span exists
        if i + 1 >= len(spans):
            continue

        # Extract value text
        value = _clean_text(spans[i + 1])

        # Normalize placeholder zero values
        if value in {"0", "0.0", "0.00"}:
            value = ""

        # Assign GRE General score
        if label.startswith("gre general"):
            scores["gre_general"] = value

        # Assign GRE Verbal score
        elif label.startswith("gre verbal"):
            scores["gre_verbal"] = value

        # Assign GRE Analytical Writing score
        elif label.startswith("analytical writing"):
            scores["gre_analytical_writing"] = value

    # Return extracted GRE scores
    return scores

# Scrape an individual GradCafe result page
def _scrape_detail_page(result_id):

    # Build detail page URL
    url = f"{BASE_URL}/result/{result_id}"

    # Fetch and parse HTML
    soup = BeautifulSoup(_fetch_html(url), "html.parser")

    # Extract GRE scores
    gre = _extract_gre_scores(soup)

    # Return extracted detail fields
    return {
        "program_name": _extract_dt_dd(soup, "Program"),
        "degree_type": _extract_dt_dd(soup, "Degree Type"),
        "comments": _extract_dt_dd(soup, "Notes"),
        "gpa": _extract_undergrad_gpa(soup),
        "gre_general": gre["gre_general"],
        "gre_verbal": gre["gre_verbal"],
        "gre_analytical_writing": gre["gre_analytical_writing"],
    }

# Parse a GradCafe survey page
def _parse_survey_page(html):

    # Parse HTML into BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Locate all table rows
    rows = soup.find_all("tr")

    # Initialize result list
    results = []

    # Track current record for metadata rows
    current = None

    # Iterate through table rows
    for row in rows:

        # Extract table cells
        cells = row.find_all("td")

        # Detect main result row
        if len(cells) >= 4:

            # Locate result link
            link = row.find("a", href=re.compile(r"/result/"))

            # Skip rows without result links
            if not link:
                continue

            # Extract result ID
            result_id = link["href"].split("/")[-1]

            # Initialize new record
            current = {
                "result_id": result_id,
                "university": _clean_text(cells[0]),
                "program_name": _clean_text(cells[1]),
                "degree_type": "",
                "date_added": _clean_text(cells[2]),
                "applicant_status": _clean_text(cells[3]),
                "start_term": "",
                "International/US": "",
                "comments": "",
                "url_link": f"{BASE_URL}/result/{result_id}",
                "gre_general": "",
                "gre_verbal": "",
                "gre_analytical_writing": "",
                "gpa": "",
            }

            # Append new record
            results.append(current)

        # Process metadata rows
        if current:

            # Iterate through all div elements
            for div in row.find_all("div"):

                # Extract visible text
                text = _clean_text(div)

                # Match academic term
                if re.search(r"(Fall|Spring|Summer|Winter)\s+\d{4}", text):
                    current["start_term"] = text

                # Match citizenship status
                elif text.lower() in {"international", "us", "u.s.", "american"}:
                    current["International/US"] = (
                        "International" if "inter" in text.lower() else "US"
                    )

    # Return parsed records
    return results

# Scrape survey pages and detail pages
def scrape_data():

    # Record scrape start time
    start_time = time.time()

    # Initialize record storage
    all_results = []

    # Track seen result IDs
    seen_ids = set()

    # Initialize page counter
    page = 1

    # Loop until max records reached
    while len(all_results) < MAX_RECORDS:

        # Fetch survey page HTML
        html = _fetch_html(SURVEY_URL.format(page))

        # Parse survey page
        page_results = _parse_survey_page(html)

        # Stop if no results found
        if not page_results:
            break

        # Add new records
        for result in page_results:
            if result["result_id"] not in seen_ids:
                seen_ids.add(result["result_id"])
                all_results.append(result)

            # Stop collecting records once max count is achieved
            if len(all_results) >= MAX_RECORDS:
                break

        # Advance to next page
        page += 1

    # Scrape detail pages in parallel
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        details = executor.map(
            _scrape_detail_page,
            [r["result_id"] for r in all_results],
        )

    # Merge detail data into records
    for record, detail in zip(all_results, details):
        record.update(detail)

    # Record scrape end time
    end_time = time.time()

    # Print elapsed time
    print(f"Scraping completed in {end_time - start_time:.2f} seconds")

    # Return final dataset
    return all_results

# Save scraped data to disk
def save_data(data):

    # Open output file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:

        # Write formatted JSON
        json.dump(data, file, indent=2, ensure_ascii=False)

    # Print confirmation message
    print(f"Saved {len(data)} records to {OUTPUT_FILE}")
