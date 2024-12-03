import requests
from parsel import Selector
import re
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_PROPERTIES = 100
BASE_URL = 'https://www.bayut.com'

def fetch_property_details(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching property details from {url}: {e}")
        return None

    selector = Selector(response.text)

    try:
        breadcrumbs = " > ".join(selector.xpath('//div[@aria-label="Breadcrumb"]//span[@aria-label="Link name"]/text()').getall()[:-1]) or None
        amenities = [
            re.sub(r'[^A-Za-z]+', '', amenity).strip() 
            for amenity in selector.xpath("//span[@class='_7181e5ac']/text()").getall()
        ]
        amenities = list(filter(None, amenities)) or None

        script_content = selector.xpath('//script[contains(text(), "permitNumber")]/text()').get() or ''
        permit_number = re.search(r'"permitNumber"\s*:\s*"(\d+)"', script_content)
        permit_number = permit_number.group(1) if permit_number else None

        description = ' '.join(selector.xpath('//div[@aria-label="Property description"]//text()').getall()).strip() or None

        return {
            'property_url': url or None,
            'property_id': selector.xpath('//span[@aria-label="Reference"]/text()').get() or None,
            'purpose': selector.xpath('//span[@aria-label="Purpose"]/text()').get() or None,
            'type': selector.xpath('//span[@aria-label="Type"]/text()').get() or None,
            'added_on': selector.xpath('//span[@aria-label="Reactivated date"]/text()').get() or None,
            'furnishing': selector.xpath('//span[@aria-label="Furnishing"]/text()').get() or None,
            'price': {
                'currency': selector.xpath('//span[@aria-label="Currency"]/text()').get() or None,
                'amount': selector.xpath('//span[@aria-label="Price"]/text()').get() or None,
            },
            'location': selector.xpath('//div[@aria-label="Property header"]/text()').get() or None,
            'bed_bath_size': {
                'bedrooms': int(selector.xpath('//span[@aria-label="Beds"]/text()').get() or 0),
                'bathrooms': int(selector.xpath('//span[@aria-label="Baths"]/text()').get() or 0),
                'size': selector.xpath('//span[@aria-label="Area"]//text()').get() or None
            },
            'permit_number': permit_number,
            'agent_name': selector.xpath('//span[@aria-label="Agent name"]//text() | //a[@aria-label="Agent name"]//text()').get() or None,
            'primary_image_url': selector.xpath('//img[@aria-label="Cover Photo"]/@src').get() or None,
            'breadcrumbs': breadcrumbs,
            'amenities': amenities,
            'description': description,
            'property_image_urls': selector.xpath('//div[@aria-label="Gallery dialog photo grid"]//img/@src').getall() or None
        }

    except Exception as e:
        logger.error(f"Error parsing property details from {url}: {e}")
        return None

def fetch_properties(start_url):
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

        for property_item in selector.xpath('//article[contains(@class, "fbc619bc") and contains(@class, "058bd30f")]'):
            if property_count >= MAX_PROPERTIES:
                break

            property_url = property_item.xpath('.//a/@href').get()
            full_url = property_url if property_url.startswith('http') else BASE_URL + property_url

            if full_url:
                property_details = fetch_property_details(full_url)
                if property_details:
                    yield property_details
                    property_count += 1

        next_page = selector.xpath('//a[@title="Next"]/@href').get()
        next_url = BASE_URL + next_page if next_page and not next_page.startswith('http') else next_page

if __name__ == "__main__":
    start_url = 'https://www.bayut.com/to-rent/property/dubai/'
    output_file = "properties.json"

    logger.info(f"Starting property scraping from {start_url}")

    property_data = []
    for property_details in fetch_properties(start_url):
        property_data.append(property_details)

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(property_data, file, ensure_ascii=False, indent=4)

    logger.info(f"Scraping completed. Results saved in {output_file}")
