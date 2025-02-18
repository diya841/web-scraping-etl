# main.py

from src.etl_pipeline import WebScraper

def main():
    # Initialize scraper
    scraper = WebScraper()
    
    # List of URLs to scrape (replace with your target websites)
    urls = [
        "https://python.org",
        "https://github.com",
        # Add more URLs
    ]
    
    # Process each URL
    for url in urls:
        success = scraper.run_pipeline(url)
        if success:
            print(f"Successfully processed: {url}")
        else:
            print(f"Failed to process: {url}")

if __name__ == "__main__":
    main()