import logging
import json
import requests
from pymongo import MongoClient, errors
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CRAWLER,
    MONGO_COLLECTION_URL_FAILED,
    PROJECT_NAME,
    HEADERS,
    CATEGORY_SLUGS,
    BASE_API_URL,
    JSON_TEMPLATE,
    MODULE_VERSION_URL,
)

# Configure logger
logger = logging.getLogger(__name__)

class Crawler:
    """Crawling categories and products"""

    def __init__(self):
        # Connect to MongoDB
        self.mongo = MongoClient(MONGO_URI)
        self.db = self.mongo[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION_CRAWLER]
        self.failed_collection = self.db[MONGO_COLLECTION_URL_FAILED]
        self.collection.create_index("unique_id", unique=True)

        self.categories = CATEGORY_SLUGS
        self.headers = HEADERS.copy()
        self.base_url = BASE_API_URL

        # Fetch dynamic module version
        try:
            resp = requests.get(MODULE_VERSION_URL, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()
            token = data.get('versionToken')
            if not token:
                logger.error("No versionToken found in module version response")
                raise ValueError("versionToken missing")
            logger.info(f"Fetched module version: {token}")
            self.module_version = token
        except Exception as e:
            logger.exception(f"Failed to fetch module version: {e}")
            raise

    def start(self):
        """Requesting start categories and iterating pages"""
        for category in self.categories:
            logger.info(f"Processing category: {category}")
            page = 1

            while True:
                self.headers['referer'] = self.base_url
                payload = self.build_payload(category, page)
                try:
                    response = requests.post(self.base_url, headers=self.headers, json=payload)
                except requests.RequestException as e:
                    logger.exception(f"Request failed for category {category}, page {page}: {e}")
                    self.failed_url(category, page, status_code="RequestException")
                    break

                if response.status_code != 200:
                    logger.error(f"Unexpected status code {response.status_code} for category {category}, page {page}")
                    self.failed_url(category, page, response.status_code)
                    break

                products = self.parse_items(response, category)
                if not products:
                    logger.info(f"Pagination completed for category: {category}")
                    break

                page += 1

    def parse_items(self, response, category):
        """Parse JSON response, format each item and insert into MongoDB."""
        try:
            data = response.json()
        except json.JSONDecodeError:
            logger.error("Invalid JSON response")
            return False

        products = data.get('data', {}).get('ProductList', {}).get('List', [])
        if not products:
            return False

        for product in products:
            plp = product.get('PLP_Str', {})
            item = {}
            sku = plp.get('SKU')
            slug = plp.get('Slug') or ''
            item['unique_id'] = sku
            item['competitor_name'] = PROJECT_NAME
            item['product_name'] = plp.get('Name')
            item['brand'] = plp.get('Brand')
            item['pdp_url'] = f"https://www.plus.nl/product/{slug}" if slug else ''
            category_level = plp.get('Categories', {}).get('List', [])
            item['producthierarchy_level1'] = 'Home'
            item['producthierarchy_level2'] = 'Producten'
            item['producthierarchy_level3'] = category_level[0]['Name'] if len(category_level) > 0 else ''
            item['producthierarchy_level4'] = category_level[1]['Name'] if len(category_level) > 1 else ''
            item['producthierarchy_level5'] = category_level[2]['Name'] if len(category_level) > 2 else ''
            breadcrumb = 'Home > Producten'
            for level in [
                item['producthierarchy_level3'],
                item['producthierarchy_level4'],
                item['producthierarchy_level5']
            ]:
                if level:
                    breadcrumb += f" > {level}"
            item['breadcrumb'] = breadcrumb
            # Price parsing
            try:
                original_price = float(plp.get('OriginalPrice', 0))
            except (TypeError, ValueError):
                original_price = 0.0
            item['regular_price'] = round(original_price, 2)

            try:
                new_price = float(plp.get('NewPrice', 0))
            except (TypeError, ValueError):
                new_price = 0.0
            item['selling_price'] = round(new_price, 2)

            item['image_urls'] = plp.get('ImageURL')
            item['category'] = category

            logger.info(item)
            try:
                self.collection.insert_one(item)
            except errors.DuplicateKeyError:
                logger.warning(f"Duplicate item {sku}, skipping")
            except Exception as e:
                logger.exception(f"Error inserting item {sku}: {e}")

        return True

    def failed_url(self, category, page, status_code):
        """Insert failed URL request details into MongoDB"""
        failed = {}
        failed['category'] = category,
        failed['page'] = page,
        failed['status_code'] = status_code

        try:
            self.failed_collection.insert_one(failed)
            logger.info(f"Logged failed URL: {failed}")
        except Exception as e:
            logger.exception(f"Failed to log URL for category {category}, page {page}: {e}")

    def build_payload(self, category, page_number):
        """Create JSON payload for API request without copying JSON_TEMPLATE."""
        payload = {
            'versionInfo': {
                'moduleVersion': self.module_version,
                'apiVersion': JSON_TEMPLATE['versionInfo']['apiVersion'],
            },
            'viewName': JSON_TEMPLATE['viewName'],
            'screenData': {
                'variables': {
                    'PageNumber': page_number,
                    'CategorySlug': category,
                }
            }
        }
        return payload

    def close(self):
        """Close MongoDB connection"""
        self.mongo.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
    crawler.close()

