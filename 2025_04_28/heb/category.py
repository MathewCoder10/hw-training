import logging
import requests
from mongoengine import connect,disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCategoryFailedItem,ProductCategoryItem
from settings import (
    BASE_URL,
    MONGO_URI,
    MONGO_DB,
    HASH_VALUE,
    HEADERS
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Category:
    def __init__(self):
        # Connect to MongoDB
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()

    def start(self):
        """Send the POST request and parse the JSON response."""
        base_url = f'{BASE_URL}/graphql'
        json_data = {
            'operationName': 'ShopNavigation',
            'variables': {},
            'extensions': {
                'persistedQuery': {
                    'version': 1,
                    'sha256Hash': HASH_VALUE,
                },
            },
        }
        response = requests.post(base_url, headers=HEADERS, json=json_data)
        if response.status_code == 200:
            self.parse_items(response)
        else:
            failed_item = {}
            failed_item['url'] = base_url
            failed_item['issue'] = f"{response.status_code}"
            try:
                ProductCategoryFailedItem(**failed_item).save()
                logging.info("Logged base URL failure")
            except Exception:
                logging.exception("Failed to insert base URL failure record")

    def parse_items(self, response):
        """Extract URLs from the navigation items and store them in MongoDB."""
        data = response.json()
        navigation = data.get("data", {}).get("shopNavigation",[])

        # Iterate over navigation items and sub-categories
        for nav in navigation:
            for subcategories in nav.get("subCategories",[]):
                href = subcategories.get("href")
                url = f"{BASE_URL}{href}"
                ids = url.rstrip("/").split("/")
                parent_id = ids[-2] 
                child_id  = ids[-1]

                item = {}
                item["url"] = url
                item["parentId"] = parent_id
                item["childId"] = child_id
                logging.info(f"Final category found, saving: {url}")
                try:
                    ProductCategoryItem(**item).save()
                except NotUniqueError:
                    logging.debug(f"Already saved, skipping duplicate: {url}")
                except Exception:
                    logging.exception(f"Failed to save final category URL: {url}")
        
    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")

if __name__ == '__main__':
    category = Category()
    category.start()
    category.close()
