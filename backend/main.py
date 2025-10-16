import asyncio
import csv
import json
import os

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS
from utils.data_utils import save_venues_to_csv
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)

load_dotenv()

CSV_FILE = "apple_jobs.csv"
JSON_FILE = "apple_jobs.json"


def load_existing_jobs(filename: str) -> set:
    """Load previously saved job title/date pairs to avoid duplicates."""
    if not os.path.exists(filename):
        return set()

    with open(filename, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return {(row["title"], row.get("date", "")) for row in reader}


def append_new_jobs(jobs: list, filename: str, existing_jobs: set):
    """Append only new job postings to the CSV."""
    if not jobs:
        print("No new jobs to append.")
        return []

    new_jobs = [job for job in jobs if (job["title"], job.get("date", "")) not in existing_jobs]

    if not new_jobs:
        print("No new unique jobs found.")
        return []

    file_exists = os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["title", "category", "date", "location"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_jobs)

    print(f"✅ Added {len(new_jobs)} new jobs to '{filename}'.")
    return new_jobs


def update_json_file(csv_filename: str, json_filename: str):
    """Convert the full CSV to JSON for easy data use."""
    if not os.path.exists(csv_filename):
        print("CSV file not found. Skipping JSON export.")
        return

    with open(csv_filename, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)

    with open(json_filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"📁 Updated JSON file: {json_filename} ({len(data)} total jobs)")


async def crawl_jobs():
    """Main function to crawl Apple job data and only save new postings."""
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy()
    session_id = "apple_jobs_session"

    # Load previous jobs
    seen_jobs = load_existing_jobs(CSV_FILE)
    seen_titles = {title for title, _ in seen_jobs}

    page_number = 1
    all_jobs = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        while True:
            jobs, no_results_found = await fetch_and_process_page(
                crawler,
                page_number,
                BASE_URL,
                CSS_SELECTOR,
                llm_strategy,
                session_id,
                REQUIRED_KEYS,
                seen_titles,
            )

            if no_results_found:
                print("No more jobs found. Ending crawl.")
                break

            if not jobs:
                print(f"No jobs extracted from page {page_number}.")
                break

            all_jobs.extend(jobs)
            page_number += 1
            await asyncio.sleep(61)

    if all_jobs:
        new_jobs = append_new_jobs(all_jobs, CSV_FILE, seen_jobs)
        if new_jobs:
            update_json_file(CSV_FILE, JSON_FILE)
    else:
        print("No jobs were found during the crawl.")

    llm_strategy.show_usage()


async def main():
    await crawl_jobs()


if __name__ == "__main__":
    asyncio.run(main())
