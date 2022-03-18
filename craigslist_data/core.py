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

    def __init__(self):
        super().__init__(url=CRAIGSLIST_URL, headless=True)

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

        time.sleep(random.randint(1, 5))
        return out


# def scrape_apartment_urls(
#     requests_per_minute=60, max_results=None, min_sleep=30, max_sleep=120
# ):
#     """Scrape apartments from Craigslist."""

#     # Create the initial URL
#     url = BASE_URL + SEARCH_API

#     # Initialize the scraper
#     scraper = scrapelib.Scraper(requests_per_minute=requests_per_minute)

#     # Log
#     logger.info("Scraping URLs from each search result page...")

#     # Loop
#     search_results = []
#     while url is not None and _check_max_results(search_results, max_results):

#         # Request
#         soup = BeautifulSoup(scraper.get(url).content, "html.parser")

#         # All the apartments
#         apts = soup.select(".result-row")

#         # Loop
#         for apt in apts:

#             # Scraper data
#             data = {}

#             # Get the URL
#             data["url"] = apt.select_one(".result-title")["href"]

#             # Post id
#             data["post_id"] = data["url"].split("/")[-1].split(".")[0]

#             # Result id
#             data["result_date"] = apt.select_one(".result-date")["datetime"]

#             # Save it
#             search_results.append(data)

#         # Next url
#         next_href = soup.select_one("a.button.next")["href"]
#         if next_href != "":
#             url = BASE_URL + next_href
#         else:
#             url = None

#     logger.info("...done")

#     # Make it a dataframe
#     search_results = pd.DataFrame(search_results)

#     # Trim to max results
#     if max_results is not None and len(search_results) > max_results:
#         search_results = search_results.iloc[:max_results]

#     def cleanup() -> None:
#         """Clean up and try again."""
#         logger.info("Retrying...")

#     # @retries(
#     #     max_attempts=15,
#     #     cleanup_hook=cleanup,
#     #     wait=lambda n: min(min_sleep + 2**n + random.random(), max_sleep),
#     # )
#     def call(row):
#         # Request
#         soup = BeautifulSoup(scraper.get(row["url"]).content, "html.parser")

#         data = dict(row)
#         for field in ApartmentListingSchema.schema()["required"]:

#             func_name = f"scrape_{field}"
#             func = getattr(ApartmentListingSchema, func_name, None)
#             if func:
#                 data[field] = func(soup)

#         out.append(ApartmentListingSchema(**data))

#     logger.info("Scraping listings...")
#     # Scrape listings too
#     out = []
#     for _, row in track(
#         search_results.iterrows(),
#         total=len(search_results),
#         description="Processing...",
#     ):
#         call(row)
#     logger.info("...done")

#     return out
