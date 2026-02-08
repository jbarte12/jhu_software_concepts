# Import scraping module for collecting raw GradCafe data
import scrape

# Import cleaning module for normalizing scraped data
import clean

# Define main execution function
def main():

    # Scrape raw applicant records from GradCafe
    raw_records = scrape.scrape_data()

    # Save raw scraped data to disk
    scrape.save_data(raw_records)

    # Normalize raw records into final cleaned schema
    cleaned = clean.clean_data(raw_records)

    # Save cleaned applicant data to disk
    clean.save_data(cleaned)

# Execute main function only when script is run directly
if __name__ == "__main__":
    main()
