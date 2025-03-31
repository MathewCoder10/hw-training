import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

class Category:
    def __init__(self):
        self.headers = {  # API headers
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
            'apollographql-client-name': 'be-dll-web-stores',
            'content-type': 'application/json',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.base_url = "https://www.delhaize.be"
        self.api_url = self.base_url + "/api/v1/"
        self.params = {
            'operationName': 'GetCategoryProductSearch',
            'variables': '{"lang":"nl","searchQuery":":relevance","sort":"relevance","category":"v2DAI","pageNumber":0,"pageSize":20,"filterFlag":true,"plainChildCategories":true}',
            'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"52764906c44e42aec923b3896810a61c85d344084ba2080b5c655b3453d4560e"}}',
        }
        
        # MongoDB connection
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['delhaize']
        self.category_collection = self.db['category_new']
        self.category_collection.create_index("url", unique=True)

    def start(self):
        """Fetch category data from the API and parse/store details."""
        print("Fetching category data from API endpoint...")
        response = requests.get(self.api_url, params=self.params, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            self.parse_items(data)
        else:
            print(f"Failed to fetch API data with status code: {response.status_code}")

    def parse_items(self, data):
        """Extract category details (name, categoryCode, and URL) and store in MongoDB."""
        category_search_tree = data.get("data", {}).get("categoryProductSearch", {}).get("categorySearchTree", [])

        for level in category_search_tree:
            for category in level.get("categoryDataList", []):
                category_code = category.get("categoryCode")
                facet_data = category.get("categoryData", {}).get("facetData", {})
                name = facet_data.get("name")
                url = facet_data.get("query", {}).get("url")
                
                if url:
                    full_url = url if url.startswith(self.base_url) else self.base_url + url
                    category_entry = {
                        "categoryCode": category_code,
                        "name": name,
                        "url": full_url,
                    }
                    
                    try:
                        self.category_collection.insert_one(category_entry)
                        print(f"Stored category: {category_entry}")
                    except DuplicateKeyError:
                        print(f"Category already exists: {category_entry}")

    def close(self):
        """Close the MongoDB connection."""
        print("Closing database connection.")
        self.client.close()

if __name__ == '__main__':
    category = Category()
    category.start()
    category.close()
