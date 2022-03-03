import pandas as pd
import scrapelib
from bs4 import BeautifulSoup
from rich.progress import track

from .schema import ApartmentListingSchema

BASE_URL = "http://philadelphia.craigslist.org"
SEARCH_API = "/search/apa"


def _check_max_results(results, max_results):
    """Function to check max results."""
    return True if max_results is None else len(results) <= max_results


def scrape_apartments(requests_per_minute=60, max_results=None):
    """Scrape apartments from Craigslist."""

    # Create the initial URL
    url = BASE_URL + SEARCH_API

    # Initialize the scraper
    scraper = scrapelib.Scraper(requests_per_minute=requests_per_minute)

    # Loop
    search_results = []
    while url is not None and _check_max_results(search_results, max_results):

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

    # Make it a dataframe
    search_results = pd.DataFrame(search_results)

    # Trim to max results
    if max_results is not None and len(search_results) > max_results:
        search_results = search_results.iloc[:max_results]

    # Scrape listings too
    out = []
    for _, row in track(
        search_results.iterrows(),
        total=len(search_results),
        description="Processing...",
    ):

        # Request
        soup = BeautifulSoup(scraper.get(row["url"]).content, "html.parser")

        data = dict(row)
        for field in ApartmentListingSchema.schema()["required"]:

            func_name = f"scrape_{field}"
            func = getattr(ApartmentListingSchema, func_name, None)
            if func:
                data[field] = func(soup)

        out.append(ApartmentListingSchema(**data))

    return out
