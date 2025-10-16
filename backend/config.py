# config.py

BASE_URL = "https://jobs.apple.com/en-gb/search?location=united-kingdom-GBR&"
CSS_SELECTOR = "[class^='search-results-section']"
REQUIRED_KEYS = [
    "title",
    "category",
    "date",
    "location",
    "url",
   
]