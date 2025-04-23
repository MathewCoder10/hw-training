import logging
import requests
from mongoengine import connect,disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCrawlerFailedItem,ProductCrawlerItem
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CATEGORY,
    HEADERS
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Category:
    def __init__(self):
        # Connect to MongoDB
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.crawler_collection = self.database[MONGO_COLLECTION_CATEGORY]
        self.base_url = 'https://www.plus.nl/screenservices/ECP_Composition_CW/ProductLists/PLP_Content/DataActionGetProductListAndCategoryInfo'
        self.module_version_url = 'https://www.plus.nl/moduleservices/moduleversioninfo?1745203216246'
        # Base URL and request details
        self.base_url = "https://www.heb.com"
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
        response = requests.post(f"{self.base_url}/graphql", headers=self.headers, json=self.json_data)
        if response.status_code == 200:
            try:
                self.data = response.json()
            except json.JSONDecodeError:
                print("Response is not valid JSON.")
                self.data = {}
        else:
            print(f"Request failed with status code: {response.status_code}")
            self.data = {}

        # Proceed to parse items from the response
        self.parse_items()

    def parse_items(self):
        """Extract URLs from the navigation items and store them in MongoDB."""
        all_url_values = []
        for nav_item in self.data.get("data", {}).get("shopNavigation", []):
            sub_categories = nav_item.get("subCategories", [])
            for subcat in sub_categories:
                href = subcat.get("href")
                if href:
                    complete_url = self.base_url + href
                    all_url_values.append(complete_url)
                    print(complete_url)
                    
                    # Extract parentId and childId from the URL path
                    url_parts = complete_url.rstrip('/').split('/')
                    if len(url_parts) >= 2:
                        parent_id = url_parts[-2]
                        child_id = url_parts[-1]
                    else:
                        parent_id = None
                        child_id = None
                    
                    # Prepare the document and insert into MongoDB
                    category_document = {
                        "url": complete_url,
                        "parentId": parent_id,
                        "childId": child_id
                    }
                    self.category_collection.insert_one(category_document)
        
        print("All URLs have been stored in MongoDB with their parentId and childId fields.")

    def close(self):
        """Close the MongoDB client connection."""
        self.client.close()


if __name__ == '__main__':
    category = Category()
    category.start()
    category.close()
