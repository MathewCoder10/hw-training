import requests
from parsel import Selector
import logging
from mongoengine import connect, disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCategoryFailedItem, ProductCategoryItem
from collections import deque
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CATEGORY,
    MONGO_COLLECTION_CATEGORY_URL_FAILED,
    HEADERS,
    BASE_URL,
    BASE
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
            self.category_parse_items(response)
        else:
            failed_item = {}
            failed_item['category_url'] = BASE_URL
            try:
                ProductCategoryFailedItem(**failed_item).save()
                logging.info("Logged base URL failure")
            except Exception:
                logging.exception("Failed to insert base URL failure record")

    def category_parse_items(self, response):
        """Parse main page, extract category urls, then process each."""
        selector = Selector(text=response.text)
        # XPATH
        URL_XPATH = ('//h3[normalize-space(.)="Onze categorieën"]/following-sibling::menu[@id="categories-menu"]//a/@href')
        # EXTRACT
        category_paths = selector.xpath(URL_XPATH).extract()
        for path in category_paths:
            full_category_url = f"{BASE}{path}"
            if full_category_url:
                self.subcategory_parse_items(full_category_url)
            else:
                failed_item = {}
                failed_item['category_url'] = full_category_url
                try:
                    ProductCategoryFailedItem(**failed_item).save()
                    logging.info(f"Failed status code for {full_category_url}")
                except Exception:
                    logging.exception(f"Failed to insert failure record for category URL {full_category_url}")

    def subcategory_parse_items(self, category_url):
        # XPATH 
        SUBCATEGORY_URL1_XPATH = '//ul[@class="o-CmsNavigationList__secondaryNavTree__list a-list-reset"]/li//@href'
        SUBCATEGORY_URL2_XPATH = '//a[@class="a-Button--primary" and normalize-space(.)="Ontdek nu"]/@href'

        queue = deque([category_url])

        while queue:
            current_url = queue.pop()
            subcategory_response = requests.get(current_url, headers=HEADERS)

            if subcategory_response.status_code == 200:
                selector = Selector(text=subcategory_response.text)
                subpaths = (selector.xpath(SUBCATEGORY_URL1_XPATH).extract() + selector.xpath(SUBCATEGORY_URL2_XPATH).extract())

                if subpaths:
                    # Enqueue deeper subcategory URLs
                    for subpath in subpaths:
                        full_link = f"{BASE}{subpath}"
                        logging.info(f"Queueing: {full_link}")
                        queue.append(full_link)
                else:
                    item = {}
                    item['category_url'] =  current_url
                    logging.info(f"Final category found, saving: {current_url}")
                    try:
                        ProductCategoryItem(**item).save()
                    except NotUniqueError:
                        logging.debug(f"Already saved, skipping duplicate: {current_url}")

            else:
                failed_item = {}
                failed_item['category_url'] = current_url
                ProductCategoryFailedItem(**failed_item).save()
                logging.warning(f"Status {subcategory_response.status_code} for {current_url}, logged failure")


    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    crawler = Category()
    crawler.start()
    crawler.close()



