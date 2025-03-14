import logging
import re

import pymongo
import requests
from parsel import Selector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_PROPERTIES = 30
BASE_URL = "https://www.bayut.com"

# MongoDB setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["bayut_mong"]
collection = db["properties_mongo"]


def extract_digits(text):
    """Extract digits from a string."""
    if text:
        digits = "".join(filter(str.isdigit, text))
        return int(digits) if digits else None
    return None


def fetch_property_details(url):
    """Fetch and parse property details from the given URL."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching property details from {url}: {e}")
        return None

    selector = Selector(response.text)
    try:
        bedrooms = extract_digits(selector.xpath('//span[@aria-label="Beds"]//span/text()').get())
        bathrooms = extract_digits(selector.xpath('//span[@aria-label="Baths"]//span/text()').get())
        size = selector.xpath('//span[@aria-label="Area"]//span//span/text()').get()

        breadcrumbs = " > ".join(
            selector.xpath('//div[@aria-label="Breadcrumb"]//span[@aria-label="Link name"]/text()').getall()[:-1]
        ) or None

        amenities = [
            re.sub(r"[^A-Za-z]+", "", amenity).strip()
            for amenity in selector.xpath(
                "//h2[text()='Features / Amenities']/following-sibling::div//span[@class='_7181e5ac']/text()"
            ).getall()
        ]
        amenities = list(filter(None, amenities)) or None

        script_content = selector.xpath('//script[contains(text(), "permitNumber")]/text()').get() or ""
        permit_number = re.search(r'"permitNumber"\s*:\s*"(\d+)"', script_content)
        permit_number = permit_number.group(1) if permit_number else None

        description = " ".join(
            selector.xpath('//div[@aria-label="Property description"]//text()').getall()
        ).strip() or None

        property_details = {
            "property_url": url,
            "property_id": selector.xpath('//span[@aria-label="Reference"]/text()').get() or None,
            "purpose": selector.xpath('//span[@aria-label="Purpose"]/text()').get() or None,
            "type": selector.xpath('//span[@aria-label="Type"]/text()').get() or None,
            "added_on": selector.xpath('//span[@aria-label="Reactivated date"]/text()').get() or None,
            "furnishing": selector.xpath('//span[@aria-label="Furnishing"]/text()').get() or None,
            "price": {
                "currency": selector.xpath('//span[@aria-label="Currency"]/text()').get() or None,
                "amount": selector.xpath('//span[@aria-label="Price"]/text()').get() or None,
            },
            "location": selector.xpath('//div[@aria-label="Property header"]/text()').get() or None,
            "bed_bath_size": {"bedrooms": bedrooms, "bathrooms": bathrooms, "size": size},
            "permit_number": permit_number,
            "agent_name": selector.xpath('//*[@aria-label="Agent name"]/text()').get() or None,
            "property_image_urls": selector.xpath(
                '//div[@aria-label="Gallery dialog photo grid"]//img/@src'
            ).getall() or None,
            "breadcrumbs": breadcrumbs,
            "amenities": amenities,
            "description": description,
            "primary_image_url": selector.xpath('//img[@aria-label="Cover Photo"]/@src').get() or None,
        }

        return property_details

    except Exception as e:
        logger.error(f"Error parsing property details from {url}: {e}")
        return None


def fetch_properties(start_url):
    """Fetch properties from the starting URL."""
    next_url = start_url
    property_count = 0

    while next_url and property_count < MAX_PROPERTIES:
        try:
            response = requests.get(next_url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {next_url}: {e}")
            break

        selector = Selector(response.text)

        for property_item in selector.xpath('//a[@aria-label="Listing link"]'):
            if property_count >= MAX_PROPERTIES:
                return None

            property_url = property_item.xpath("./@href").get()
            full_url = property_url if property_url.startswith("http") else BASE_URL + property_url

            if full_url:
                property_details = fetch_property_details(full_url)
                if property_details:
                    yield property_details
                    property_count += 1

        next_page = selector.xpath('//a[@title="Next"]/@href').get()
        next_url = BASE_URL + next_page if next_page and not next_page.startswith("http") else next_page


if __name__ == "__main__":
    start_url = "https://www.bayut.com/to-rent/property/dubai/"

    logger.info(f"Starting property scraping from {start_url}")

    for property_details in fetch_properties(start_url):
        if property_details:
            collection.insert_one(property_details)

    logger.info("Scraping completed. Data saved to MongoDB.")
