import logging
import requests
import re
from collections import deque
from mongoengine import connect,disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCategoryFailedItem,ProductCategoryItem
from settings import (
    BASE_URL,
    MONGO_URI,
    MONGO_DB,
    HEADERS
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Category:
    def __init__(self):
        # Connect to MongoDB
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()

    def start(self):
        url = 'https://redoak.target.com/content-publish/pages/v1'
        params = {'url': '/c/-/-/N-4nav'}
        response = requests.get(url,params=params,headers=HEADERS)
        if response.status_code == 200:
            self.parse_items(response)
        else:
            failed_item = {}
            failed_item['url'] = url
            failed_item['issue'] = f"{response.status_code}"
            try:
                ProductCategoryFailedItem(**failed_item).save()
                logging.info("Logged base URL failure")
            except Exception:
                logging.exception("Failed to insert base URL failure record")
    def parse_items(self, response):
        data = response.json()
        queue = deque([data])

        while queue:
            current = queue.popleft()
            if isinstance(current, dict):
                seo = current.get('seo_data', {})
                canonical = seo.get('canonical_url','')
                if canonical:
                    match = re.search(r'/N-([^/]+)$', canonical)
                    id = match.group(1) if match else ''
                    full_url = f"{BASE_URL}{canonical}"
                    item = {}
                    item["category_id"] = id
                    item["url"] = full_url
                    logging.info(f"Category found, saving: {full_url}")
                    try:
                        ProductCategoryItem(**item).save()
                    except NotUniqueError:
                        logging.debug(f"Already saved, skipping duplicate: {full_url}")
                    except Exception:
                        logging.exception(f"Failed to save final category URL: {full_url}")

                # Only enqueue dicts and lists
                for value in current.values():
                    if isinstance(value, (dict, list)):
                        queue.append(value)

            elif isinstance(current, list):
                for value in current:
                    if isinstance(value, (dict, list)):
                        queue.append(value)      
        
    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")

if __name__ == '__main__':
    category = Category()
    category.start()
    category.close()
 