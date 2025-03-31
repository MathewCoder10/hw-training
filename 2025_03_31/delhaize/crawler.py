import requests
from pymongo import MongoClient

class Crawler:
    def __init__(self, mongo_uri='mongodb://localhost:27017/', db_name='delhaize'):
        # Initialize MongoDB connection and collections
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.subcategory_collection = self.db['subcategory_final']
        self.crawler_collection = self.db['crawler_final']
        
        # Create unique indexes for product_code and url
        self.crawler_collection.create_index("product_code", unique=True)
        self.crawler_collection.create_index("url", unique=True)
        
        # Set up API base URL and pagination
        self.base_url = "https://www.delhaize.be"
        self.page_size = 20
        
        # Define headers for the API request
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
            'apollographql-client-name': 'be-dll-web-stores',
            'apollographql-client-version': 'b43fe67605ad012a0cc8976ce891aacc46b52c95',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            # 'cookie': '...',  # Include cookies if necessary
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.delhaize.be/nl/shop/Zuivel-kaas-en-plantaardige-alternatieven/Melk/Lactosevrij/c/v2DAIMILLAC?q=:relevance&&&',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-apollo-operation-id': 'ed388fd997c14ae298fb7f3292015e233f68ea20fefc3648c16252230d799198',
            'x-apollo-operation-name': 'GetCategoryProductSearch',
            'x-default-gql-refresh-token-disabled': 'true',
        }
        
        # Define cookies if needed
        self.cookies = {}

    def start(self):
        """Start the crawling process by iterating through subcategories."""
        for doc in self.subcategory_collection.find():
            category = doc.get("categoryCode")
            if not category:
                continue

            print(f"\n--- Fetching products for category: {category} ---")
            self.fetch_category_products(category)

    def fetch_category_products(self, category):
        """Fetch products for a specific category with pagination."""
        page_number = 0
        while True:
            # Build the query parameters with dynamic category and page number
            params = {
                'operationName': 'GetCategoryProductSearch',
                'variables': (
                    '{"lang":"nl","searchQuery":":relevance",'
                    f'"category":"{category}","pageNumber":{page_number},"pageSize":{self.page_size},'
                    '"filterFlag":true,"plainChildCategories":true}'
                ),
                'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"52764906c44e42aec923b3896810a61c85d344084ba2080b5c655b3453d4560e"}}',
            }

            print(f"Fetching products for pageNumber: {page_number}")
            response = requests.get(self.base_url + '/api/v1/', params=params, cookies=self.cookies, headers=self.headers)
            if response.status_code != 200:
                print(f"Failed to fetch API data for category {category} at page {page_number} with status code: {response.status_code}")
                break

            data = response.json()
            products = data.get("data", {}) \
                           .get("categoryProductSearch", {}) \
                           .get("products", [])

            # Exit pagination if no more products are found
            if not products:
                print("No more products found. Exiting pagination loop for this category.")
                break

            # Process and store each product from the current page
            self.parse_items(products)
            page_number += 1

    def parse_items(self, products):
        """Parse each product item and store the product code and URL in MongoDB."""
        for product in products:
            product_code = product.get("code")
            url = product.get("url")
            if url:
                # Ensure the URL is complete
                full_url = url if url.startswith(self.base_url) else self.base_url + url
                # Use upsert to handle duplicates gracefully: if the document exists, it will be updated.
                result = self.crawler_collection.update_one(
                    {"product_code": product_code},
                    {"$set": {"product_code": product_code, "url": full_url}},
                    upsert=True
                )
                if result.upserted_id:
                    print(f"Inserted Product Code: {product_code}\nURL: {full_url}\n")
                else:
                    print(f"Updated Product Code: {product_code}\nURL: {full_url}\n")

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()


if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
    crawler.close()
