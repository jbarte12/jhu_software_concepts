# Import JSON for saving scraped data
import json

# Import regular expressions for text pattern matching
import re

# Import urllib for HTTP requests
import urllib.request

# Import urllib error handling
import urllib.error

# Import time module for timing and throttling
import time

# Import thread pool executor for parallel requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import BeautifulSoup for HTML parsing
from bs4 import BeautifulSoup

# Base GradCafe URL
BASE_URL = "https://www.thegradcafe.com"

# Survey page URL template
SURVEY_URL = "https://www.thegradcafe.com/survey/index.php?page={}"

# Maximum number of records to scrape
MAX_RECORDS = 50000

# Number of parallel workers for detail-page scraping
NUM_WORKERS = 10

# HTTP timeout in seconds
TIMEOUT = 30

# Output JSON file
OUTPUT_FILE = "applicant_data.json"

# Progress logging interval
PROGRESS_INTERVAL = 500

# Fetch HTML from a URL safely
def _fetch_html(url):

    # Build HTTP request with browser-like headers
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
    )

    try:
        # Open the URL with timeout protection
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:

            # Decode and return HTML content
            return response.read().decode("utf-8")

    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        # Return empty string on failure to prevent crashes
        return ""

# Normalize visible text from HTML elements
def _clean_text(element):

    # Return empty string if element does not exist
    if element is None:
        return ""

    # Extract text, normalize whitespace, and return
    return " ".join(element.get_text(" ", strip=True).split())

# Extract value from a dt/dd label pair
def _extract_dt_dd(soup, label):

    # Iterate through all definition term elements
    for dt in soup.find_all("dt"):

        # Match exact label text
        if _clean_text(dt) == label:

            # Locate the parent container
            parent = dt.parent

            # Return associated definition description text
            if parent:
                return _clean_text(parent.find("dd"))

    # Return empty string if label not found
    return ""

# Extract undergraduate GPA
def _extract_undergrad_gpa(soup):

    # Extract GPA value
    gpa = _extract_dt_dd(soup, "Undergrad GPA")

    # Normalize placeholder zero values
    if gpa in {"0", "0.0", "0.00"}:
        return ""

    # Return cleaned GPA
    return gpa

# Extract GRE scores
def _extract_gre_scores(soup):

    # Initialize GRE score dictionary
    scores = {
        "gre_general": "",
        "gre_verbal": "",
        "gre_analytical_writing": "",
    }

    # Iterate through all span elements
    spans = soup.find_all("span")

    # Loop through spans with index access
    for i in range(len(spans)):

        # Extract label text
        label = _clean_text(spans[i]).lower()

        # Skip non-label spans
        if not label.endswith(":"):
            continue

        # Prevent out-of-range access
        if i + 1 >= len(spans):
            continue

        # Extract associated value
        value = _clean_text(spans[i + 1])

        # Normalize zero values
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

# Scrape a single GradCafe result page
def _scrape_detail_page(result_id):

    # Build detail page URL
    url = f"{BASE_URL}/result/{result_id}"

    # Fetch page HTML
    html = _fetch_html(url)

    # Return empty fields if request failed
    if not html:
        return {}

    # Parse HTML into BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Extract GRE scores
    gre = _extract_gre_scores(soup)

    # Return structured detail data
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

    # Find all table rows
    rows = soup.find_all("tr")

    # Initialize parsed result list
    results = []

    # Track current record
    current = None

    # Iterate through table rows
    for row in rows:
        # Extract table cells
        cells = row.find_all("td")

        # Identify main result rows
        if len(cells) >= 4:
            # Locate result link
            link = row.find("a", href=re.compile(r"/result/"))

            # Skip invalid rows
            if not link:
                continue

            # Extract result ID
            result_id = link["href"].split("/")[-1]

            # Initialize record
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

            # Append record
            results.append(current)

        # Parse metadata rows
        if current:
            for div in row.find_all("div"):
                text = _clean_text(div)

                # Match start term
                if re.search(r"(Fall|Spring|Summer|Winter)\s+\d{4}", text):
                    current["start_term"] = text

                # Match citizenship
                elif text.lower() in {"international", "us", "u.s.", "american"}:
                    current["International/US"] = (
                        "International" if "inter" in text.lower() else "US"
                    )

    # Return parsed page results
    return results

# Main scraping function
def scrape_data():
    # Record start time
    start_time = time.time()

    # Initialize storage
    all_results = []

    # Track unique result IDs
    seen_ids = set()

    # Initialize page counter
    page = 1

    # Scrape survey pages until limit reached
    while len(all_results) < MAX_RECORDS:
        # Fetch survey page
        html = _fetch_html(SURVEY_URL.format(page))

        # Stop if fetch failed
        if not html:
            break

        # Parse page results
        page_results = _parse_survey_page(html)

        # Stop if no results found
        if not page_results:
            break

        # Add new records
        for record in page_results:
            if record["result_id"] not in seen_ids:
                seen_ids.add(record["result_id"])
                all_results.append(record)

                # Print progress every N records
                if len(all_results) % PROGRESS_INTERVAL == 0:
                    print(f"Collected {len(all_results)} records...")

            if len(all_results) >= MAX_RECORDS:
                break

        # Advance page counter
        page += 1

    # Scrape detail pages in parallel
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(_scrape_detail_page, r["result_id"]): r
            for r in all_results
        }

        # Merge detail results as they complete
        for i, future in enumerate(as_completed(futures), start=1):
            record = futures[future]
            detail = future.result()

            if detail:
                record.update(detail)

            # Progress logging
            if i % PROGRESS_INTERVAL == 0:
                print(f"Detail pages scraped: {i}")

    # Print total elapsed time
    print(f"Scraping completed in {time.time() - start_time:.2f} seconds")

    # Return full dataset
    return all_results

# Save data to JSON file
def save_data(data):
    # Open output file for writing
    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        # Write formatted JSON
        json.dump(data, file, indent=2, ensure_ascii=False)

    # Print confirmation message
    print(f"Saved {len(data)} records to {OUTPUT_FILE}")
