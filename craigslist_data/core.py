import time

import pandas as pd
from aws_scraper.scrape import WebScraper
from bs4 import BeautifulSoup
from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .schema import ApartmentListingSchema

BASE_URL = "http://philadelphia.craigslist.org"
SEARCH_API = "/search/apa"
CRAIGSLIST_URL = BASE_URL + SEARCH_API


def scrape_search_results(
    posted_today: bool = False, sleep: int = 1, headless: bool = True
) -> pd.DataFrame:
    """Scrape search results for aparements on Craigslist."""

    # Create the initial URL
    url = CRAIGSLIST_URL
    if posted_today:
        url += "?postedToday=1"

    # Initialize the scraper
    scraper = CraigslistScraper(
        posted_today=posted_today, sleep=sleep, headless=headless
    )

    # Log
    logger.info("Scraping URLs from each search result page...")

    # Loop
    search_results = []
    page_num = 0
    while url is not None:

        # Add page number to url
        _url = url + f"#search=1~gallery~{page_num}~0"

        # Navigate to the URL and pause
        scraper.driver.get(_url)

        # Wait
        WebDriverWait(scraper.driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".cl-search-result")),
        )

        # Create the soup
        soup = BeautifulSoup(scraper.driver.page_source, "html.parser")

        # All the apartments
        apts = soup.select(".cl-search-result")

        # Loop
        for apt in apts:

            # Scraper data
            data = {}

            # Get the URL
            data["url"] = apt.select_one(".titlestring")["href"]

            # Post id
            data["post_id"] = data["url"].split("/")[-1].split(".")[0]

            # Result id
            dt = apt.select_one("div.meta > span:nth-child(1)")["title"]
            i = dt.index("(")
            dt = dt[0:i]
            data["result_date"] = dt

            # Save it
            search_results.append(data)

        # Check for disabled next button
        disabled_next_button = soup.select_one("button.cl-next-page.bd-disabled")

        # No next page
        if disabled_next_button is None:
            page_num += 1
            time.sleep(sleep)
        else:
            url = None

    logger.info("...done")

    # Make it a dataframe
    return pd.DataFrame(search_results)


class CraigslistScraper(WebScraper):
    """Craigslist scraper."""

    def __init__(self, posted_today=False, sleep=1, headless=True):

        # How long to sleep between calls
        self.sleep = sleep

        # Create the URL
        url = CRAIGSLIST_URL
        if posted_today:
            url += "?postedToday=1"  # Only query those posted today

        # Call super
        super().__init__(url=url, headless=headless)

    def __call__(self, row):

        # Navigate to the URL
        self.driver.get(row["url"])

        # Wait
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "h1.postingtitle")),
        )

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
