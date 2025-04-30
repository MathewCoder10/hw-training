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
        self.release_id = None
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.category_collection = self.database[MONGO_COLLECTION_CATEGORY]

        try:
            dynamic_script_url = self.retrieve_dynamic_script_url()
            if dynamic_script_url:
                self.release_id = self.extract_release_id(dynamic_script_url)
                logging.info(f"Using release ID: {self.release_id}")
            else:
                logging.error("Unable to locate dynamic script URL.")
        except Exception:
            logging.exception("Exception during initialization of release ID.")

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
        if not self.release_id:
            logging.error("Missing release ID; aborting crawl.")
            return

        for category in self.category_collection.find():
            url = category.get('url')
            parent_id = category.get('parentId')
            child_id = category.get('childId')

            if parent_id is None or child_id is None:
                logging.warning(f"Category missing parentId/childId: {url}; skipping.")
                continue

            page_num = 1
            while True:
                api_endpoint = (f"{BASE_URL.rstrip('/')}/_next/data/{self.release_id}/category/shop/{parent_id}/{child_id}.json?page={page_num}")
                logging.info(f"Requesting API endpoint: {api_endpoint}")

                # try:
                response = requests.get(api_endpoint, headers=HEADERS)
                if response.status_code == 200:
                    json_data = response.json()
                    has_more = self.parse_items(url, parent_id, child_id, page_num, json_data)
                    if not has_more:
                        logging.info(f"Completed crawling for {url} at page {page_num}.")
                        break

                    page_num += 1

                else:

                    failed_item = {}
                    failed_item ['category'] = url
                    failed_item ['page'] = page_num
                    failed_item ['issue'] = f"{response.status_code}"
                    try:
                        product_failed_item = ProductCrawlerFailedItem(**failed_item)
                        product_failed_item.save()
                        logging.info(f"Failed Status code")
                    except Exception as e:
                        logging.exception(f"Failed to insert")
                    break


    def parse_items(self, category_url, parent_id, child_id, page_number, json_data):
        """
        Process the JSON payload: recursively find all 'productPageURL' values,
        save each as a ProductCrawlerItem, and return True if any found.
        """
        components = (json_data.get("pageProps", {}).get("layout", {}).get("visualComponents", []))
        saved_any = False

        for component in components:
            for product in component.get("items", []):
                rel = product.get("productPageURL")
                if not rel:
                    failed_item = {}
                    failed_item ['category'] = category_url
                    failed_item ['page'] = page_number
                    failed_item ['issue'] = 'No products'
                    try:
                        product_failed_item = ProductCrawlerFailedItem(**failed_item)
                        product_failed_item.save()
                        logging.info(f"No products found")
                    except Exception as e:
                        logging.exception(f"Failed to insert")
                    continue

                full_url = (rel if rel.startswith(BASE_URL) else BASE_URL.rstrip('/') + rel )

                item = {}
                item["product_url"] = full_url
                item["parentId"] = parent_id
                item["childId"] = child_id
                logging.info(f"Product url saving: {full_url}")
                try:
                    ProductCrawlerItem(**item).save()
                    saved_any = True
                except NotUniqueError:
                    logging.debug(f"Already saved, skipping duplicate: {full_url}")
                except Exception:
                    logging.exception(f"Failed to save product url: {full_url}")

        return saved_any

    def close(self):
        """Clean up database connections."""
        disconnect()
        logging.info("Disconnected from MongoDB.")


if __name__ == '__main__':
    crawler = Crawler()
    crawler.start()
    crawler.close()
