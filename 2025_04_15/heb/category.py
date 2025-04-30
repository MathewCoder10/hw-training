import requests
import json
from pymongo import MongoClient

class Category:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="heb_db", collection_name="category_28"):
        # Initialize MongoDB client, database, and collection
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.category_collection = self.db[collection_name]
        
        # Base URL and request details
        self.base_url = "https://www.heb.com"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'amplitude-device-id': 'h-b34c7492-a703-45e9-81b8-05da7ff65eb4',
            'apollographql-client-name': 'WebPlatform-Solar (Production)',
            'apollographql-client-version': '22b50a38e0e0961393883f53de7dad4818a32bee',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'origin': self.base_url,
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': self.base_url + '/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
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
