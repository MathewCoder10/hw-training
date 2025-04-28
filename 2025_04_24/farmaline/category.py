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
        category_paths = selector.xpath(URL_XPATH).getall()
        for path in category_paths:
            full_category_url = urljoin(BASE_URL, path)
            response = requests.get(full_category_url, headers=HEADERS)
            if response.status_code == 200:
                self.subcategory_parse_items(full_category_url, response)
            else:
                failed_item = {}
                failed_item['category_url'] = full_category_url
                try:
                    ProductCategoryFailedItem(**failed_item).save()
                    logging.info(f"Failed status code for {full_category_url}")
                except Exception:
                    logging.exception(f"Failed to insert failure record for category URL {full_category_url}")

    def subcategory_parse_items(self, category_url, response):
        """Recursively crawl sub-category pages until no more SUBCATEGORY xpaths are found, then store the final URLs in the category collection."""
        selector = Selector(text=response.text)
        # XPATH
        SUBCATEGORY_URL1_XPATH = '//ul[@class="o-CmsNavigationList__secondaryNavTree__list a-list-reset"]/li//@href'
        SUBCATEGORY_URL2_XPATH = '//a[@class="a-Button--primary" and normalize-space(.)="Ontdek nu"]/@href'

        # EXTRACT SUBCATEGORY URLS
        links = selector.xpath(SUBCATEGORY_URL1_XPATH).getall() + selector.xpath(SUBCATEGORY_URL2_XPATH).getall()

        if links:
            # still more subcategories to explore
            for subpath in links:
                full_link = urljoin(BASE_URL, subpath)
                logging.info(f"Descending into {full_link}")
                subcategory_response = requests.get(full_link, headers=HEADERS)
                if subcategory_response.status_code == 200:
                    # recurse on the next level
                    self.subcategory_parse_items(full_link, subcategory_response)
                else:
                    failed_item = {}
                    failed_item['category_url'] = full_link
                    ProductCategoryFailedItem(**failed_item).save()
                    logging.warning(f"Status {subcategory_response.status_code} for {full_link}, logged failure")
        else:
            # no deeper subcategories → final category page
            item = {}
            item['category_url'] =  category_url
            logging.info(f"Final category found, saving: {category_url}")
            try:
                ProductCategoryItem(**item).save()
            except NotUniqueError:
                logging.debug(f"Already saved, skipping duplicate: {category_url}")
            except Exception:
                logging.exception(f"Failed to save final category URL: {category_url}")


    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    crawler = Category()
    crawler.start()
    crawler.close()
