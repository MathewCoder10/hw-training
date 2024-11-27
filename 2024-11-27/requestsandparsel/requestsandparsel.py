import requests
from parsel import Selector
import re
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_PROPERTIES = 100
BASE_URL = 'https://www.bayut.com'

def extract_digits(text):
    if text:
        digits = ''.join(filter(str.isdigit, text))
        return int(digits) if digits else None
    return None

def fetch_property_details(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching property details from {url}: {e}")
        return None

    selector = Selector(response.text)
    
    try:
        breadcrumb_items = selector.xpath('//div[@aria-label="Breadcrumb"]//span[@aria-label="Link name"]/text()').getall()
        breadcrumbs = " > ".join(breadcrumb_items[:-1]) if breadcrumb_items else None

        amenities = [
            amenity.strip().replace(": 1", "").strip()
            for amenity in selector.xpath('//div[contains(@class, "_91c991df")]//span[contains(@class, "_7181e5ac")]/text()').getall()
        ]
        amenities = list(filter(None, amenities))

        bedrooms = extract_digits(selector.xpath('//span[@aria-label="Beds"]//span[contains(@class, "_140e6903")]/text()').get())
        bathrooms = extract_digits(selector.xpath('//span[@aria-label="Baths"]//span[contains(@class, "_140e6903")]/text()').get())
        size = selector.xpath('//span[@aria-label="Area"]//span[contains(@class, "_140e6903")]//span/text()').get()

        script_content = selector.xpath('/html/body/script[1]/text()').get()
        permit_number = re.search(r'"permitNumber"\s*:\s*"(\d+)"', script_content).group(1) if script_content else None

        description = re.sub(
            r'[\u00a0\u2726\uf0d8\t\u2019]', '',
            ' '.join(selector.xpath('//div[@aria-label="Property description"]//text()').getall()).strip()
        )

        return {
            'property_url': url,
            'property_id': selector.xpath('//span[@aria-label="Reference"]/text()').get(),
            'purpose': selector.xpath('//span[@aria-label="Purpose"]/text()').get(),
            'type': selector.xpath('//span[@aria-label="Type"]/text()').get(),
            'added_on': selector.xpath('//span[@aria-label="Reactivated date"]/text()').get(),
            'furnishing': selector.xpath('//span[@aria-label="Furnishing"]/text()').get(),
            'price': {
                'currency': selector.xpath('//span[@aria-label="Currency"]/text()').get(),
                'amount': selector.xpath('//span[@aria-label="Price"]/text()').get(),
            },
            'location': selector.xpath('//div[@aria-label="Property header"]/text()').get(),
            'bed_bath_size': {'bedrooms': bedrooms, 'bathrooms': bathrooms, 'size': size},
            'permit_number': permit_number,
            'agent_name': selector.xpath('//span[@aria-label="Agent name"]//text() | //a[@aria-label="Agent name"]//text()').get(),
            'primary_image_url': selector.xpath('//div[contains(@class, "345bbb7c")]//picture//img/@src').get(),
            'breadcrumbs': breadcrumbs,
            'amenities': amenities,
            'description': description,
            'property_image_urls': selector.xpath('//picture//img/@src').getall()
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

    with open(output_file, "w") as file:
        for property_data in fetch_properties(start_url):
            json.dump(property_data, file)
            file.write("\n")
            logger.info(f"Saved property: {property_data['property_url']}")

    logger.info(f"Scraping completed. Results saved in {output_file}")
