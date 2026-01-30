"""
main.py

Single entry point for the GradCafe data pipeline.

This script:
1. Scrapes GradCafe data
2. Saves raw data to applicant_data.json
3. Loads the raw data
4. Cleans and restructures the data
5. Saves the cleaned data back to applicant_data.json
6. Prints total runtime
"""

import time

from scrape import scrape_data, save_data as save_raw_data
from clean import load_data, clean_data, save_data as save_clean_data


def main():
    """
    Orchestrates the full scraping + cleaning pipeline.
    """

    # ------------------------------------------
    # Start timer
    # ------------------------------------------
    start_time = time.time()

    print("Starting GradCafe data pipeline...")

    # ------------------------------------------
    # Step 1: Scrape data
    # ------------------------------------------
    print("Scraping data from GradCafe...")
    raw_data = scrape_data()
    save_raw_data(raw_data)

    # ------------------------------------------
    # Step 2: Load raw data
    # ------------------------------------------
    print("Loading raw applicant data...")
    raw_data = load_data()

    # ------------------------------------------
    # Step 3: Clean data
    # ------------------------------------------
    print("Cleaning applicant data...")
    cleaned_data = clean_data(raw_data)

    # ------------------------------------------
    # Step 4: Save cleaned data
    # ------------------------------------------
    print("Saving cleaned data...")
    save_clean_data(cleaned_data)

    # ------------------------------------------
    # End timer
    # ------------------------------------------
    end_time = time.time()
    elapsed = end_time - start_time

    print(f"Pipeline completed successfully in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()