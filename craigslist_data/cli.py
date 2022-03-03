import json

import click
from pydantic.json import pydantic_encoder

from .core import scrape_apartments


@click.command()
@click.argument("output_file")
@click.option("--limit", type=int, default=None, help="Limit the number of results.")
@click.option("--rate", type=int, default=60, help="The number of requests per minute.")
def main(output_file, limit=None, rate=60):
    """Scrape Craigslist data, saving as a JSON file."""

    if not output_file.endswith(".json"):
        raise ValueError("Please provide a JSON output file.")

    # Scrape the data
    data = scrape_apartments(requests_per_minute=rate, max_results=limit)

    # Save
    with open(output_file, "w") as ff:
        json.dump(data, ff, default=pydantic_encoder)
