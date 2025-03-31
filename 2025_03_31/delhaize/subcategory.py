import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

class Subcategory:
    def __init__(self):
        # Headers for the Delhaize API request
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
            'apollographql-client-name': 'be-dll-web-stores',
            'apollographql-client-version': 'b43fe67605ad012a0cc8976ce891aacc46b52c95',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.delhaize.be/nl/shop/Zuivel-kaas-en-plantaardige-alternatieven/Melk/c/v2DAIMIL?q=:relevance&&sort=relevance&',
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
            'x-dtpc': '9$329298803_313h6vOMEQMMRAENFVGBOAAKLTDWWHHUVUIKMK-0e0',
            'x-dtreferer': 'https://www.delhaize.be/nl/shop/Zuivel-kaas-en-plantaardige-alternatieven/Melk/c/v2DAIMIL?q=%3Arelevance&sort=relevance',
        }
        self.base_url = "https://www.delhaize.be"
        self.api_url = self.base_url + "/api/v1/"
        
        # MongoDB connection
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['delhaize']
        
        # Collection for subcategories
        self.subcat_collection = self.db['subcategory_final']
        self.subcat_collection.create_index("url", unique=True)
        
        # Collection from which to fetch category codes for the API request
        self.category_collection = self.db['category_new']

    def start(self):
        """
        Fetch category codes from 'category_new' collection, then for each code,
        update the API request parameters dynamically and fetch subcategory data.
        If no subcategories are found in the API response, fall back to storing the
        category's own URL, categoryCode and name.
        """
        categories = list(self.category_collection.find())
        if not categories:
            print("No category codes found in the 'category_new' collection.")
            return
        
        for cat in categories:
            category_code = cat.get("categoryCode")
            if not category_code:
                continue
            
            # Update the 'category' variable in the parameters dynamically using the fetched category code.
            params = {
                'operationName': 'GetCategoryProductSearch',
                'variables': (
                    '{"lang":"nl","searchQuery":":relevance","sort":"relevance",'
                    f'"category":"{category_code}","pageNumber":0,"pageSize":20,'
                    '"filterFlag":true,"plainChildCategories":true}'
                ),
                'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"52764906c44e42aec923b3896810a61c85d344084ba2080b5c655b3453d4560e"}}',
            }
            
            print(f"Fetching subcategory data for category code: {category_code}")
            response = requests.get(self.api_url, params=params, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                subcat_found = self.parse_items(data)
                if not subcat_found:
                    # Fallback: no subcategories were found in the API response.
                    # Assume that the original category document has 'url' and 'name' fields.
                    fallback_url = cat.get("url")
                    fallback_name = cat.get("name")
                    if fallback_url:
                        full_url = fallback_url if fallback_url.startswith(self.base_url) else self.base_url + fallback_url
                        document = {
                            'url': full_url,
                            'categoryCode': category_code,
                            'name': fallback_name
                        }
                        try:
                            self.subcat_collection.insert_one(document)
                            print(f"Stored fallback subcategory URL: {full_url}\nCategoryCode: {category_code}\nName: {fallback_name}\n")
                        except DuplicateKeyError:
                            print(f"Subcategory URL already exists: {full_url}")
                    else:
                        print(f"No fallback URL available for category code: {category_code}")
            else:
                print(f"Failed to fetch API data for category {category_code} with status code: {response.status_code}")

    def parse_items(self, data):
        """
        Parse the JSON response to extract subcategory details and store them in MongoDB.
        For each subcategory, the category code is fetched directly from the JSON.
        The JSON structure is:
            data -> categoryProductSearch -> categorySearchTree -> categoryDataList -> 
            categoryData -> facetData -> { "query": {"url": ...}, "name": ... }
            
        Returns True if at least one subcategory is stored, otherwise False.
        """
        stored = False
        category_search_tree = data.get("data", {}) \
                                   .get("categoryProductSearch", {}) \
                                   .get("categorySearchTree", [])
        for level in category_search_tree:
            category_list = level.get("categoryDataList", [])
            if category_list:
                for category in category_list:
                    json_category_code = category.get("categoryCode")
                    facet_data = category.get("categoryData", {}).get("facetData", {})
                    name = facet_data.get("name")
                    url = facet_data.get("query", {}).get("url")
                    if url:
                        full_url = url if url.startswith(self.base_url) else self.base_url + url
                        document = {
                            'url': full_url,
                            'categoryCode': json_category_code,
                            'name': name
                        }
                        try:
                            self.subcat_collection.insert_one(document)
                            print(f"Stored subcategory URL: {full_url}\nCategoryCode: {json_category_code}\nName: {name}\n")
                            stored = True
                        except DuplicateKeyError:
                            print(f"Subcategory URL already exists: {full_url}")
        return stored

    def close(self):
        """Close the MongoDB connection."""
        print("Closing database connection.")
        self.client.close()

if __name__ == '__main__':
    subcategory = Subcategory()
    subcategory.start()
    subcategory.close()
