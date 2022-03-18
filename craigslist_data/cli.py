from datetime import datetime

import click
from aws_scraper import __main__ as aws_scraper_cli
from aws_scraper import aws, io
from dotenv import find_dotenv, load_dotenv
from loguru import logger

from . import APP_NAME
from .core import CraigslistScraper, scrape_search_results

BUCKET_NAME = APP_NAME


@click.group()
def cli():
    """CLI for scraping Craigslist apartments."""
    pass


@cli.command()
@click.argument("output_file", type=str)
@aws_scraper_cli.add_submit_options
@click.option("--num-workers", type=int, default=20, help="The number of workers")
def submit(output_file, num_workers=20, **kwargs):
    """Submit craigslist scraping jobs to AWS."""

    # Check input
    if not output_file.endswith(".json"):
        raise ValueError("Please provide a JSON output file.")

    # Load config
    load_dotenv(find_dotenv())

    # Scrape the URLs for individual apartments
    # from the search portal
    search_results = scrape_search_results()

    # Upload to an s3 folder
    tag = datetime.today().strftime("%Y-%m-%d-%H-%M")
    data_path = f"s3://{BUCKET_NAME}/data/search-results-{tag}.csv"
    io.save_data_to_s3(search_results, aws.S3Path(data_path))

    # Submit the jobs
    data = aws_scraper_cli.submit(
        f"{APP_NAME} run {data_path}",
        num_workers=num_workers,
        bucket_name=BUCKET_NAME,
        cluster_name="aws-scraper",
        task_family=APP_NAME,
        **kwargs,
    )

    # Save to JSON file
    logger.info(f"Saving combined file locally to '{output_file}'")
    with open(output_file, "w") as f:
        data.to_json(f, orient="records")


@cli.command()
@click.argument("data_path", type=str)
@aws_scraper_cli.add_run_options
def run(data_path, **kwargs):
    """Run craiglist scraper jobs."""

    # Load config
    load_dotenv(find_dotenv())

    # Initialize the scraper
    scraper = CraigslistScraper()

    # Run the scraper
    aws_scraper_cli.run(
        data_path=data_path,
        scraper=scraper,
        **kwargs,
    )
