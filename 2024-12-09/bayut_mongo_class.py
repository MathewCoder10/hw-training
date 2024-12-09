import requests
from parsel import Selector
import re
import logging
import pymongo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_PROPERTIES = 100
BASE_URL = 'https://www.bayut.com'


class BayutScraper:
    def __init__(self, start_url, db_url="mongodb://localhost:27017/", db_name="bayut_scrap_class", collection_name="properties"):
        self.start_url = start_url
        self.client = pymongo.MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    @staticmethod
    def extract_digits(text):
        if text:
            digits = ''.join(filter(str.isdigit, text))
            return int(digits) if digits else None
        return None

    def fetch_property_details(self, url):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching property details from {url}: {e}")
            return None

        selector = Selector(response.text)

        try:
            bedrooms = self.extract_digits(selector.xpath('//span[@aria-label="Beds"]//span[contains(@class, "_140e6903")]/text()').get())
            bathrooms = self.extract_digits(selector.xpath('//span[@aria-label="Baths"]//span[contains(@class, "_140e6903")]/text()').get())
            size = selector.xpath('//span[@aria-label="Area"]//span[contains(@class, "_140e6903")]//span/text()').get()

            breadcrumbs = " > ".join(selector.xpath('//div[@aria-label="Breadcrumb"]//span[@aria-label="Link name"]/text()').getall()[:-1]) or None

            amenities = [
                re.sub(r'[^A-Za-z]+', '', amenity).strip()
                for amenity in selector.xpath("//h2[text()='Features / Amenities']/following-sibling::div//span[@class='_7181e5ac']/text()").getall()
            ]
            amenities = list(filter(None, amenities)) or None

            script_content = selector.xpath('//script[contains(text(), "permitNumber")]/text()').get() or ''
            permit_number = re.search(r'"permitNumber"\s*:\s*"(\d+)"', script_content)
            permit_number = permit_number.group(1) if permit_number else None

            description = ' '.join(selector.xpath('//div[@aria-label="Property description"]//text()').getall()).strip() or None

            property_details = {
                'property_url': url,
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
                'bed_bath_size': {'bedrooms': bedrooms, 'bathrooms': bathrooms, 'size': size},
                'permit_number': permit_number,
                'agent_name': selector.xpath('//*[@aria-label="Agent name"]/text()').get() or None,
                'property_image_urls': selector.xpath('//div[@aria-label="Gallery dialog photo grid"]//img/@src').getall() or None,
                'breadcrumbs': breadcrumbs,
                'amenities': amenities,
                'description': description,
                'primary_image_url': selector.xpath('//img[@aria-label="Cover Photo"]/@src').get() or None
            }

            return property_details

        except Exception as e:
            logger.error(f"Error parsing property details from {url}: {e}")
            return None

    def fetch_properties(self):
        next_url = self.start_url
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

                property_url = property_item.xpath('./@href').get()
                full_url = property_url if property_url.startswith('http') else BASE_URL + property_url

                if full_url:
                    property_details = self.fetch_property_details(full_url)
                    if property_details:
                        yield property_details
                        property_count += 1

            next_page = selector.xpath('//a[@title="Next"]/@href').get()
            next_url = BASE_URL + next_page if next_page and not next_page.startswith('http') else next_page

    def save_to_mongodb(self):
        logger.info(f"Starting property scraping from {self.start_url}")
        for property_details in self.fetch_properties():
            if property_details:
                self.collection.insert_one(property_details)
        logger.info("Scraping completed. Data saved to MongoDB.")


if __name__ == "__main__":
    scraper = BayutScraper(start_url='https://www.bayut.com/to-rent/property/dubai/')
    scraper.save_to_mongodb()
