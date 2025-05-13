import logging
import requests
import re
from parsel import Selector
from playwright.sync_api import sync_playwright
from mongoengine import connect, disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCrawlerFailedItem, ProductCrawlerItem
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CATEGORY,
    HEADERS,
    BASE_URL,
    SCRIPT_PATTERN
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Crawler:
    """Crawler that retrieves category listings and product pages."""       
    def __init__(self):
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.category_collection = self.database[MONGO_COLLECTION_CATEGORY]

        dynamic_script_url = self.retrieve_dynamic_script_url()
        if dynamic_script_url:
            self.release_id = self.extract_release_id(dynamic_script_url)
            if self.release_id:
                logging.info(f"Using release ID: {self.release_id}")
            else:
                logging.error("Missing release ID; aborting parser initialization.")
                # Clean up DB connection before exiting
                self.close()
                exit("Initialization failed: Sentry release ID not found.")
        else:
            logging.error("Unable to locate dynamic script URL.")
            # Clean up DB connection before exiting
            self.close()
            exit("Initialization failed: dynamic script URL not found.")

    def retrieve_dynamic_script_url(self):
        """Launch headless browser to fetch the page and locate the dynamic script URL."""
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(user_agent=HEADERS.get('User-Agent'))
            page.goto(BASE_URL, wait_until='domcontentloaded')
            html = page.content()
            browser.close()

        sel = Selector(text=html)
        scripts = sel.xpath('//script[@src]/@src').getall()
        matches = [src for src in scripts if SCRIPT_PATTERN.match(src)]
        return matches[0] if matches else None

    def extract_release_id(self, script_url):
        """Download the JS file and parse out the Sentry release ID."""
        response = requests.get(script_url, headers=HEADERS)
        response.raise_for_status()
        match = re.search(r't4\.SENTRY_RELEASE\s*=\s*\{id:"([0-9a-fA-F]+)"\}', response.text)
        return match.group(1) if match else None

    def start(self):
        """Iterate through each category, do the API request, then hand off to parse_items()."""

        for category in self.category_collection.find():
            url = category.get('url')
            parent_id = category.get('parentId')
            child_id = category.get('childId')

            if parent_id is None or child_id is None:
                logging.warning(f"Category missing parentId/childId: {url}; skipping.")
                continue

            page_number = 1
            while True:
                api_endpoint = (f"{BASE_URL}/_next/data/{self.release_id}/category/shop/{parent_id}/{child_id}.json?page={page_number}")
                logging.info(f"Requesting API endpoint: {api_endpoint}")
                response = requests.get(api_endpoint, headers=HEADERS)

                if response.status_code == 200:
                    products = self.parse_items(response, url, parent_id, child_id, page_number)
                    if products:
                        page_number += 1
                    else:
                        break

                else:
                    failed_item = {}
                    failed_item ['category'] = url
                    failed_item ['page'] = page_number
                    failed_item ['issue'] = f"{response.status_code}"
                    try:
                        product_failed_item = ProductCrawlerFailedItem(**failed_item)
                        product_failed_item.save()
                        logging.info(f"Failed Status code")
                    except Exception as e:
                        logging.exception(f"Failed to insert")

    def parse_items(self, response, category_url, parent_id, child_id, page):
        data = response.json()
        components = data.get('pageProps', {}).get('layout', {}).get('visualComponents', [])
        found = False

        for component in components:
            for product in component.get("items", []):
                url = product.get('productPageURL')
                if not url:
                    logging.debug(f"Missing productPageURL in item on {category_url} page {page}")
                    continue

                full_url = f"{BASE_URL}{url}"
                item = {}
                item["product_url"] = full_url
                item["parentId"] = parent_id
                item["childId"] = child_id
                logging.info(f"Product url saving: {full_url}")
                try:
                    ProductCrawlerItem(**item).save()
                    found = True
                except NotUniqueError:
                    logging.debug(f"Already saved, skipping duplicate: {full_url}")
                except Exception:
                    logging.exception(f"Failed to save product url: {full_url}")
        if not found:
            failed_item = {}
            failed_item ['category'] = category_url
            failed_item ['page'] = page
            failed_item ['issue'] = 'No products'
            try:
                ProductCrawlerFailedItem(**failed_item).save()
                logging.info(f"No products found")
            except Exception as e:
                logging.exception(f"Failed to insert")
        return found
    
    def close(self):
        """Clean up database connections."""
        disconnect()
        logging.info("Disconnected from MongoDB.")

if __name__ == '__main__':
    crawler = Crawler()
    crawler.start()
    crawler.close()
