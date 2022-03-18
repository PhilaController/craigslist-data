import random
import time

import pandas as pd
import scrapelib
from aws_scraper.scrape import WebScraper
from bs4 import BeautifulSoup
from loguru import logger

from .schema import ApartmentListingSchema

# from rich.progress import track
# from tryagain import retries


BASE_URL = "http://philadelphia.craigslist.org"
SEARCH_API = "/search/apa"
CRAIGSLIST_URL = BASE_URL + SEARCH_API


def scrape_search_results(requests_per_minute: int = 60) -> pd.DataFrame:
    """Scrape search results for aparements on Craigslist."""

    # Create the initial URL
    url = CRAIGSLIST_URL

    # Initialize the scraper
    scraper = scrapelib.Scraper(requests_per_minute=requests_per_minute)

    # Log
    logger.info("Scraping URLs from each search result page...")

    # Loop
    search_results = []
    while url is not None:

        # Request
        soup = BeautifulSoup(scraper.get(url).content, "html.parser")

        # All the apartments
        apts = soup.select(".result-row")

        # Loop
        for apt in apts:

            # Scraper data
            data = {}

            # Get the URL
            data["url"] = apt.select_one(".result-title")["href"]

            # Post id
            data["post_id"] = data["url"].split("/")[-1].split(".")[0]

            # Result id
            data["result_date"] = apt.select_one(".result-date")["datetime"]

            # Save it
            search_results.append(data)

        # Next url
        next_href = soup.select_one("a.button.next")["href"]
        if next_href != "":
            url = BASE_URL + next_href
        else:
            url = None

    logger.info("...done")

    # Make it a dataframe
    return pd.DataFrame(search_results)


class CraigslistScraper(WebScraper):
    """Craigslist scraper."""

    def __init__(self, posted_today=False, sleep=1):

        # How long to sleep between calls
        self.sleep = sleep

        # Create the URL
        url = CRAIGSLIST_URL
        if posted_today:
            url += "?postedToday=1"  # Only query those posted today

        # Call super
        super().__init__(url=url, headless=True)

    def __call__(self, row):

        # Navigate to the URL
        self.driver.get(row["url"])

        # Create the soup
        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        out = {}
        for field in ApartmentListingSchema.schema()["required"]:

            func_name = f"scrape_{field}"
            func = getattr(ApartmentListingSchema, func_name, None)
            if func:
                out[field] = func(soup)

        time.sleep(self.sleep)
        return out
