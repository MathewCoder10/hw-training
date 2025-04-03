import requests
from pymongo import MongoClient

class Next_Category:
    def __init__(self):
        # Define the headers for the GET request.
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.next.co.uk/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-next-correlation-id': 'a6054020-0f8e-11f0-a918-718824c661ec',
            'x-next-language': 'en',
            'x-next-persona': 'DPlatform',
            'x-next-realm': 'next',
            'x-next-session-id': '',
            'x-next-siteurl': 'https://www.next.co.uk',
            'x-next-territory': 'GB',
            'x-next-viewport-size': 'desktop, desktop',
        }
        # URL to fetch data from
        self.url = 'https://www.next.co.uk/secondary-items/home/women'
        # Target titles to look for in the JSON data
        self.TARGET_TITLES = [
            "CLOTHING",
            "DRESSES",
            "WORKWEAR & TAILORING",
            "LINGERIE & NIGHTWEAR",
            "ACCESSORIES",
            "FOOTWEAR",
            "BEAUTY",
            "SHOP BY SIZE TYPE",
            "LUXURY BRANDS",
            "SHOP BY BRAND"
        ]
        self.base_url = "https://www.next.co.uk"
        
        # MongoDB connection parameters (adjust connection string as needed)
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.db = self.mongo_client["next"]
        self.collection = self.db["category"]
        
        self.data = None
        self.documents = []

    def start(self):
        """
        Initiate the GET request to fetch data and parse the response as JSON.
        """
        response = requests.get(self.url, headers=self.headers)
        print("Response status code:", response.status_code)
        self.data = response.json()

    def parse_items(self, data):
        """
        Recursively traverse the JSON structure and extract dictionaries that contain
        a 'title' key matching one of the target titles. Then, process subcategories
        and insert documents into MongoDB.
        """
        def recursive_extract(data):
            results = []
            if isinstance(data, dict):
                if "title" in data and isinstance(data["title"], str) and data["title"] in self.TARGET_TITLES:
                    results.append(data)
                for value in data.values():
                    results.extend(recursive_extract(value))
            elif isinstance(data, list):
                for element in data:
                    results.extend(recursive_extract(element))
            return results

        target_items = recursive_extract(data)

        # Process each main category and its subitems.
        for main_category_dict in target_items:
            main_category = main_category_dict.get("title")
            subitems = main_category_dict.get("items", [])
            for sub_item in subitems:
                subcategory = sub_item.get("title")
                target_url = sub_item.get("target", "")
                # Build the MongoDB document using variable 'item'
                item = {
                    "breadcrumb": f"{main_category}>{subcategory}",
                    "url": self.base_url + target_url,
                    "category": subcategory
                }
                self.documents.append(item)

        # Insert documents into MongoDB.
        if self.documents:
            result = self.collection.insert_many(self.documents)
            print(f"Inserted {len(result.inserted_ids)} records into the 'category' collection.")
        else:
            print("No documents found to insert.")

    def close(self):
        """
        Close the MongoDB connection.
        """
        self.mongo_client.close()
        print("MongoDB connection closed.")

# Example usage:
if __name__ == "__main__":
    next_category = Next_Category()
    next_category.start()
    next_category.parse_items(next_category.data)
    next_category.close()
