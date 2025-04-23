import logging
import requests
from mongoengine import connect,disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCrawlerFailedItem,ProductCrawlerItem
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CRAWLER,
    MONGO_COLLECTION_CRAWLER_URL_FAILED,
    HEADERS,
    CATEGORY_SLUGS,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Crawler:
    """Crawling categories and products"""

    def __init__(self):
        # Connect to MongoDB
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.crawler_collection = self.database[MONGO_COLLECTION_CRAWLER]
        self.failed_urls_collection = self.database[MONGO_COLLECTION_CRAWLER_URL_FAILED]

        # Fetch dynamic module version
        try:
            self.module_version = self.fetch_module_version()
        except Exception:
            logging.error("Could not initialize parser due to module version fetch failure.")
            raise

    def fetch_module_version(self):
        """Fetch module version token for detail API."""
        module_version_url = 'https://www.plus.nl/moduleservices/moduleversioninfo?1745203216246'
        response = requests.get(module_version_url, headers=HEADERS)
        data = response.json()
        token = data.get('versionToken')
        if not token:
            raise ValueError("versionToken missing in module version response")
        logging.info(f"Fetched module version for parser: {token}")
        return token

    def start(self):
        """Requesting start categories and iterating pages"""
        for category in CATEGORY_SLUGS:
            logging.info(f"Processing category: {category}")
            page = 1

            while True:
                payload = {
                            'versionInfo': {
                                'moduleVersion': self.module_version, 
                                'apiVersion': 'bYh0SIb+kuEKWPesnQKP1A',
                            },
                            'viewName': 'MainFlow.ProductListPage',
                            'screenData': {
                                'variables': {
                                    'PageNumber': page,
                                    'CategorySlug': category,
                                }
                            }
                        }
                base_url = 'https://www.plus.nl/screenservices/ECP_Composition_CW/ProductLists/PLP_Content/DataActionGetProductListAndCategoryInfo'
                response = requests.post(base_url, headers=HEADERS, json=payload)
                if response.status_code == 200:

                    products = self.parse_items(response, category, page)
                    if products:
                        page= page+1
                    else:
                        break
                else:
                    failed_item = {}
                    failed_item ['category'] = category
                    failed_item ['page'] = page
                    failed_item ['issue'] = response.status_code

                    try:
                        ProductCrawlerFailedItem(**failed_item).save()
                        logging.info(f"Failed statuscode")
                    except Exception as e:
                        logging.exception(f"Failed to insert")


    def parse_items(self, response, category, page):
        """Parse JSON response, insert items or log failures."""
        data = response.json()
        products = data.get('data', {}).get('ProductList', {}).get('List', [])
        if not products:
            failed_item = {}
            failed_item ['category'] = category
            failed_item ['page'] = page
            failed_item ['issue'] = 'No products'
            try:
                product_item = ProductCrawlerFailedItem(**failed_item)
                product_item.save()
                logging.info(f"No products found")
            except Exception as e:
                logging.exception(f"Failed to insert")
            return False

        # EXTRACT
        for product in products:
            plp_str = product.get('PLP_Str', {})
            item = {}

            sku = plp_str.get('SKU','')
            product_name = plp_str.get('Name','')

            # ITEM YEILD
            item['unique_id'] = sku
            item['product_name'] = product_name

            logging.info(item)
            try:
                product_item = ProductCrawlerItem(**item)
                product_item.save()
 
            except NotUniqueError:
                logging.warning(f"Duplicate unique_id found")

        return True


    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")


if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
    crawler.close()
