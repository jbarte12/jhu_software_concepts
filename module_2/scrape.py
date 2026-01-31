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
MAX_RECORDS = 50000

# Number of parallel workers for detail pages
NUM_WORKERS = 8  # reduced to avoid throttling/timeouts

# HTTP timeout in seconds
TIMEOUT = 30

# Output file for raw scraped data
OUTPUT_FILE = "applicant_data.json"


# Fetch HTML content from a URL safely
def _fetch_html(url):
    """Fetch HTML from a URL, returning None on failure."""

    try:
        # Build HTTP request with user-agent header
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
        )

        # Open URL and read response
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return response.read().decode("utf-8", errors="ignore")

    # Catch timeouts explicitly
    except TimeoutError:
        return None

    # Catch all other network-related issues
    except Exception:
        return None


# Clean and normalize visible text from an HTML element
def _clean_text(element):
    """Normalize text content from a BeautifulSoup element."""

    # Return empty string if element is missing
    if element is None:
        return ""

    # Extract visible text and normalize whitespace
    return " ".join(element.get_text(" ", strip=True).split())


# Extract value from a dt/dd pair by label
def _extract_dt_dd(soup, label):
    """Extract value for a given dt label from a definition list."""

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
    """Extract and normalize undergraduate GPA."""

    # Extract GPA value
    gpa = _extract_dt_dd(soup, "Undergrad GPA")

    # Normalize placeholder zero values
    if gpa in {"0", "0.0", "0.00"}:
        return ""

    return gpa


# Extract GRE scores from detail page
def _extract_gre_scores(soup):
    """Extract GRE scores from detail page."""

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

        # Normalize label text
        label = _clean_text(span).lower()

        # Skip non-label spans
        if not label.endswith(":"):
            continue

        # Ensure value span exists
        if i + 1 >= len(spans):
            continue

        # Extract value text
        value = _clean_text(spans[i + 1])

        # Normalize placeholder values
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

    return scores


# Scrape an individual GradCafe result page
def _scrape_detail_page(result_id):
    """Scrape detailed fields for a single result."""

    # Build detail page URL
    url = f"{BASE_URL}/result/{result_id}"

    # Fetch HTML safely
    html = _fetch_html(url)

    # Skip if page failed to load
    if not html:
        return {}

    # Parse HTML
    soup = BeautifulSoup(html, "html.parser")

    # Extract GRE scores
    gre = _extract_gre_scores(soup)

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
    """Parse a GradCafe survey page into structured records."""

    # Return empty list if page failed
    if not html:
        return []

    # Parse HTML
    soup = BeautifulSoup(html, "html.parser")

    # Locate all table rows
    rows = soup.find_all("tr")

    results = []
    current = None

    for row in rows:
        cells = row.find_all("td")

        # Detect main result row
        if len(cells) >= 4:
            link = row.find("a", href=re.compile(r"/result/"))
            if not link:
                continue

            result_id = link["href"].split("/")[-1]

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

            results.append(current)

        # Parse metadata rows
        if current:
            for div in row.find_all("div"):
                text = _clean_text(div)

                if re.search(r"(Fall|Spring|Summer|Winter)\s+\d{4}", text):
                    current["start_term"] = text
                elif text.lower() in {"international", "us", "u.s.", "american"}:
                    current["International/US"] = (
                        "International" if "inter" in text.lower() else "US"
                    )

    return results


# Scrape survey pages and detail pages
def scrape_data():
    """Scrape GradCafe survey and detail pages."""

    start_time = time.time()

    all_results = []
    seen_ids = set()
    page = 1

    # Scrape survey pages
    while len(all_results) < MAX_RECORDS:
        html = _fetch_html(SURVEY_URL.format(page))
        page_results = _parse_survey_page(html)

        if not page_results:
            break

        for result in page_results:
            if result["result_id"] not in seen_ids:
                seen_ids.add(result["result_id"])
                all_results.append(result)

            if len(all_results) >= MAX_RECORDS:
                break

        page += 1

    # Scrape detail pages in parallel (safe)
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        details = executor.map(
            _scrape_detail_page,
            [r["result_id"] for r in all_results],
        )

    # Merge detail data safely
    for record, detail in zip(all_results, details, strict=False):
        record.update(detail)

    elapsed = time.time() - start_time
    print(f"Scraping completed in {elapsed:.2f} seconds")

    return all_results


# Save scraped data to disk
def save_data(data):
    """Save scraped records to disk as JSON."""

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)

    print(f"Saved {len(data)} records to {OUTPUT_FILE}")
