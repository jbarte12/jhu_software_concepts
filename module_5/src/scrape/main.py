"""
GradCafe data pipeline entry point.

Orchestrates the full initial data collection flow: scrapes raw applicant
records from GradCafe, saves them to disk, normalizes them into the final
schema, and writes the cleaned output to disk.
"""

# Import cleaning module for normalizing scraped data
import clean

# Import scraping module for collecting raw GradCafe data
import scrape


def main():
    """Run the full scrape-and-clean pipeline.

    Calls :func:`scrape.scrape_data` to collect raw applicant records,
    persists the raw data via :func:`scrape.save_data`, normalizes the
    records into the application schema via :func:`clean.clean_data`, and
    writes the cleaned output via :func:`clean.save_data`.
    """
    # Scrape raw applicant records from GradCafe
    raw_records = scrape.scrape_data()

    # Save raw scraped data to disk
    scrape.save_data(raw_records)

    # Normalize raw records into final cleaned schema
    cleaned = clean.clean_data(raw_records)

    # Save cleaned applicant data to disk
    clean.save_data(cleaned)


if __name__ == "__main__":
    main()
