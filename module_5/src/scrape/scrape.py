"""
GradCafe scraping utilities.

Provides functions to fetch HTML pages, parse survey results, scrape
individual result detail pages, and save the collected applicant data
to disk.
"""

# Import JSON for saving scraped data
import json

# Import regular expressions for pattern matching
import re

# Import urllib for HTTP requests
import urllib.request

# Import urllib errors for specific exception handling
from urllib.error import URLError, HTTPError

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
MAX_RECORDS = 30000

# Number of parallel workers for detail pages
NUM_WORKERS = 10

# HTTP timeout in seconds
TIMEOUT = 30

# Output file for raw scraped data
OUTPUT_FILE = "applicant_data.json"

# Save every N records
SAVE_EVERY = 1000


def fetch_html(url, retries=3):
    """Fetch HTML content from a URL with retry logic.

    Attempts to download the page up to ``retries`` times, sleeping
    between attempts with exponential backoff. Raises on the final failure.

    :param url: URL to fetch.
    :type url: str
    :param retries: Number of attempts before raising.
    :type retries: int
    :returns: Decoded HTML string.
    :rtype: str
    :raises HTTPError: If the server returns an HTTP error on the final attempt.
    :raises URLError: If the connection fails on the final attempt.
    """
    last_error = None
    for attempt in range(retries):
        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                return response.read().decode("utf-8")
        except (HTTPError, URLError) as e:
            last_error = e
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    raise last_error


def clean_text(element):
    """Clean and normalize visible text from an HTML element.

    :param element: A BeautifulSoup element, or ``None``.
    :returns: Normalized text string, or ``""`` if element is ``None``.
    :rtype: str
    """
    if element is None:
        return ""
    return " ".join(element.get_text(" ", strip=True).split())


def extract_dt_dd(soup, label):
    """Extract the value of a dt/dd pair by matching the label text.

    :param soup: Parsed BeautifulSoup object to search.
    :type soup: bs4.BeautifulSoup
    :param label: Exact label text to match.
    :type label: str
    :returns: Text of the matching ``<dd>``, or ``""`` if not found.
    :rtype: str
    """
    for dt in soup.find_all("dt"):
        if clean_text(dt) == label:
            parent = dt.parent
            if parent:
                return clean_text(parent.find("dd"))
    return ""


def extract_undergrad_gpa(soup):
    """Extract and normalize the undergraduate GPA from a detail page.

    :param soup: Parsed BeautifulSoup object for the detail page.
    :type soup: bs4.BeautifulSoup
    :returns: GPA string, or ``""`` if missing or a placeholder value.
    :rtype: str
    """
    gpa = extract_dt_dd(soup, "Undergrad GPA")
    if gpa in {"0", "0.0", "0.00", "99.99"}:
        return ""
    return gpa


def extract_gre_scores(soup):
    """Extract GRE scores from a detail page.

    :param soup: Parsed BeautifulSoup object for the detail page.
    :type soup: bs4.BeautifulSoup
    :returns: Dict with keys ``gre_general``, ``gre_verbal``,
        ``gre_analytical_writing``.
    :rtype: dict[str, str]
    """
    scores = {
        "gre_general": "",
        "gre_verbal": "",
        "gre_analytical_writing": "",
    }
    spans = soup.find_all("span")
    for i, span in enumerate(spans):
        label = clean_text(span).lower()
        if not label.endswith(":"):
            continue
        if i + 1 >= len(spans):
            continue
        value = clean_text(spans[i + 1])
        if value in {"0", "0.0", "0.00", "99.99"}:
            value = ""
        if label.startswith("gre general"):
            scores["gre_general"] = value
        elif label.startswith("gre verbal"):
            scores["gre_verbal"] = value
        elif label.startswith("analytical writing"):
            scores["gre_analytical_writing"] = value
    return scores


def scrape_detail_page(result_id):
    """Scrape an individual GradCafe result page.

    :param result_id: Numeric GradCafe result ID.
    :type result_id: int or str
    :returns: Dict with keys ``program_name``, ``degree_type``,
        ``comments``, ``gpa``, ``gre_general``, ``gre_verbal``,
        ``gre_analytical_writing``.
    :rtype: dict[str, str]
    """
    url = f"{BASE_URL}/result/{result_id}"
    soup = BeautifulSoup(fetch_html(url), "html.parser")
    gre = extract_gre_scores(soup)
    return {
        "program_name": extract_dt_dd(soup, "Program"),
        "degree_type": extract_dt_dd(soup, "Degree Type"),
        "comments": extract_dt_dd(soup, "Notes"),
        "gpa": extract_undergrad_gpa(soup),
        "gre_general": gre["gre_general"],
        "gre_verbal": gre["gre_verbal"],
        "gre_analytical_writing": gre["gre_analytical_writing"],
    }


def parse_survey_page(html):
    """Parse a GradCafe survey page into a list of applicant records.

    :param html: Raw HTML string of a survey page.
    :type html: str
    :returns: List of record dicts, one per applicant found on the page.
    :rtype: list[dict]
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    results = []
    current = None

    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 4:
            link = row.find("a", href=re.compile(r"/result/"))
            if not link:
                continue
            result_id = link["href"].split("/")[-1]
            current = {
                "result_id": result_id,
                "university": clean_text(cells[0]),
                "program_name": clean_text(cells[1]),
                "degree_type": "",
                "date_added": clean_text(cells[2]),
                "applicant_status": clean_text(cells[3]),
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

        if current:
            for div in row.find_all("div"):
                text = clean_text(div)
                if re.search(r"(Fall|Spring|Summer|Winter)\s+\d{4}", text):
                    current["start_term"] = text
                elif text.lower() in {"international", "us", "u.s.", "american"}:
                    current["International/US"] = (
                        "International" if "inter" in text.lower() else "US"
                    )

    return results


def scrape_data():
    """Scrape GradCafe survey pages and individual result detail pages.

    :returns: List of fully enriched applicant record dicts.
    :rtype: list[dict]
    """
    start_time = time.time()
    all_results = []
    seen_ids = set()
    page = 1

    while len(all_results) < MAX_RECORDS:
        html = fetch_html(SURVEY_URL.format(page))
        page_results = parse_survey_page(html)
        if not page_results:
            break
        for result in page_results:
            if result["result_id"] not in seen_ids:
                seen_ids.add(result["result_id"])
                all_results.append(result)
            if len(all_results) >= MAX_RECORDS:
                break
        page += 1

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        details = executor.map(
            scrape_detail_page,
            [r["result_id"] for r in all_results],
        )

    completed = 0
    for record, detail in zip(all_results, details):
        record.update(detail)
        completed += 1
        if completed % SAVE_EVERY == 0:
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            print(f"Saved {completed} records to {OUTPUT_FILE}")

    print(f"Scraping completed in {time.time() - start_time:.2f} seconds")
    return all_results


def save_data(data):
    """Save scraped applicant data to disk as formatted JSON.

    :param data: List of applicant record dicts to save.
    :type data: list[dict]
    """
    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
    print(f"Saved {len(data)} records to {OUTPUT_FILE}")
