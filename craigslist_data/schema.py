from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class ApartmentListingSchema(BaseModel):
    """Data scraped from an apartment listing page."""

    post_id: str
    url: str
    title: str
    description: str
    num_images: int
    lat: float
    lng: float
    price: int
    attrs: List[str]
    posted_date: datetime
    result_date: datetime
    bedrooms: Optional[int] = None
    size: Optional[int] = None
    updated_date: Optional[datetime] = None
    location_description: Optional[str] = None

    @classmethod
    def scrape_lat(cls, soup):
        """Latitude."""
        m = soup.select_one("#map")
        return float(m.attrs["data-latitude"])

    @classmethod
    def scrape_lng(cls, soup):
        """Longitude."""
        m = soup.select_one("#map")
        return float(m.attrs["data-longitude"])

    @classmethod
    def scrape_num_images(cls, soup):
        """Number of images in the listing."""
        return len(soup.select(".swipe-wrap .slide"))

    @classmethod
    def scrape_price(cls, soup):
        """Apartment price."""
        price = soup.select_one(".postingtitletext .price").text.strip()
        return int(price.strip("$").replace(",", ""))

    @classmethod
    def scrape_bedrooms(cls, soup):
        """Number of bedrooms, if available."""

        # Housing attribute
        housing = getattr(soup.select_one(".postingtitletext .housing"), "text", None)
        if housing is None:
            return None

        # Split into fields
        fields = [s.strip() for s in housing.strip("/").split("-") if s.strip()]

        # Bedrooms is always first
        bedrooms = fields[0]
        return int(bedrooms)

    @classmethod
    def scrape_size(cls, soup):
        """Size in square feet, if available."""

        # Housing attribute
        housing = getattr(soup.select_one(".postingtitletext .housing"), "text", None)
        if housing is None:
            return None

        # Split into fields
        fields = [s.strip() for s in housing.strip("/").split("-") if s.strip()]

        # Size is second
        _, *size = fields[0]

        if len(size) == 0:
            return None

        size = size[0].replace("ft2", "")
        return int(size)

    @classmethod
    def scrape_title(cls, soup):
        """The title."""
        return soup.select_one("#titletextonly").text.strip()

    @classmethod
    def scrape_location_description(cls, soup):
        """The description of the location."""
        desc = soup.select_one(".postingtitletext small")
        if desc is None:
            return None

        return desc.text.strip().strip("()")

    @classmethod
    def scrape_description(cls, soup):
        """The description text."""
        body = soup.select_one("#postingbody")
        return "\n".join([s.strip() for s in body.text.splitlines() if s][1:])

    @classmethod
    def scrape_attrs(cls, soup):
        """Additional attributes."""
        return [s.text for s in soup.select(".attrgroup span")]

    @classmethod
    def scrape_posted_date(cls, soup):
        """The posted date."""
        dates = soup.select_one(".postinginfos").select(".date.timeago")
        assert len(dates) > 0
        return dates[0]["datetime"]

    @classmethod
    def scrape_updated_date(cls, soup):
        """The updated date."""
        dates = soup.select_one(".postinginfos").select(".date.timeago")
        assert len(dates) > 0
        if len(dates) > 1:
            return dates[1]["datetime"]
        else:
            return None
