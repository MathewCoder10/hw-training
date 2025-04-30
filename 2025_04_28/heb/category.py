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
    HEADERS
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Category:
    def __init__(self):
        # Connect to MongoDB
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        # Base URL and request details
        self.json_data = {
            'operationName': 'ShopNavigation',
            'variables': {},
            'extensions': {
                'persistedQuery': {
                    'version': 1,
                    'sha256Hash': '0e669423cef683226cb8eb295664619c8e0f95945734e0a458095f51ee89efb3',
                },
            },
        }
        self.data = {}

    def start(self):
        """Send the POST request and parse the JSON response."""
        base_url = f'{BASE_URL}/graphql'
        response = requests.post(base_url, headers=HEADERS, json=self.json_data)
        if response.status_code == 200:
            self.parse_items(response, base_url)
        else:
            failed_item = {}
            failed_item['url'] = base_url
            failed_item['issue'] = f"{response.status_code}"
            try:
                ProductCategoryFailedItem(**failed_item).save()
                logging.info("Logged base URL failure")
            except Exception:
                logging.exception("Failed to insert base URL failure record")

    def parse_items(self, response, base_url):
        """Extract URLs from the navigation items and store them in MongoDB."""
        """Extract and save category URLs from the GraphQL response."""
        data = response.json()
        navigation = data.get("data", {}).get("shopNavigation")

        # Validate navigation structure
        if not isinstance(navigation, list):
            failed_item = {}
            failed_item ['url'] = base_url
            failed_item ['issue'] = 'No products'
            try:
                product_item = ProductCategoryFailedItem(**failed_item)
                product_item.save()
                logging.info(f"No products found")
            except Exception as e:
                logging.exception(f"Failed to insert")
            return

        # Iterate over navigation items and sub-categories
        for nav in navigation:
            for subcategories in nav.get("subCategories") or []:
                href = subcategories.get("href")
                if not href:
                    continue

                url = f"{BASE_URL}{href}"
                parts = url.rstrip("/").split("/")
                parent_id = parts[-2] 
                child_id  = parts[-1]

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
