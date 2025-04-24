import requests
from parsel import Selector
import logging
from urllib.parse import urljoin
from mongoengine import connect, disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCategoryFailedItem, ProductCategoryItem
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CATEGORY,
    MONGO_COLLECTION_CATEGORY_URL_FAILED,
    HEADERS,
    BASE_URL
)

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

class Category:
    """Crawling categories and products"""

    def __init__(self):
        # Connect to MongoDB
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.crawler_collection = self.database[MONGO_COLLECTION_CATEGORY]
        self.failed_urls_collection = self.database[MONGO_COLLECTION_CATEGORY_URL_FAILED]

    def start(self):
        response = requests.get(BASE_URL, headers=HEADERS)
        if response.status_code == 200:
            self.parse_items(response)
        else:
            failed_item = {}
            failed_item['category_url'] = BASE_URL
            try:
                ProductCategoryFailedItem(**failed_item).save()
                logging.info("Logged base URL failure")
            except Exception:
                logging.exception("Failed to insert base URL failure record")

    def parse_items(self, response):
        """Parse main page, extract category links, then process each."""
        selector = Selector(text=response.text)
        URL_XPATH = ('//h3[normalize-space(.)="Onze categorieën"]/following-sibling::menu[@id="categories-menu"]//a/@href')
        category_paths = selector.xpath(URL_XPATH).getall()
        for path in category_paths:
            full_category_url = urljoin(BASE_URL, path)
            response = requests.get(full_category_url, headers=HEADERS)
            if response.status_code == 200:
                self.category_parse_items(full_category_url, response)
            else:
                failed_item = {}
                failed_item['category_url'] = full_category_url
                try:
                    ProductCategoryFailedItem(**failed_item).save()
                    logging.info(f"Failed status code for {full_category_url}")
                except Exception:
                    logging.exception(f"Failed to insert failure record for category URL {full_category_url}")

    def category_parse_items(self, category_url, response):
        """Parse a category page, extract product-list URLs, and insert them."""
        selector = Selector(text=response.text)
        URL1_XPATH = ('//div[@class="o-PageHeading__all-products-link"]/a[@class="a-link--underline"]/@href')
        URL2_XPATH = ('//a[@class="a-Button--primary" and normalize-space(.)="Ontdek nu"]/@href')

        link1 = selector.xpath(URL1_XPATH).get()
        link2 = selector.xpath(URL2_XPATH).getall()
        # Combine all found links
        links = []
        if link1:
            links.append(link1)
        links.extend(link2)

        if not links:
            logging.warning(f"No product link found on {category_url}")
            return

        for subpath in links:
            full_link = urljoin(BASE_URL, subpath)
            item = {}
            item['category_url'] = full_link
            logging.info(f"Saving item: {item}")
            try:
                ProductCategoryItem(**item).save()
            except NotUniqueError:
                logging.warning(f"Duplicate URL, skipping: {full_link}")

    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    crawler = Category()
    crawler.start()
    crawler.close()
