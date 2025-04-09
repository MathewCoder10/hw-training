import re
import requests
from pymongo import MongoClient

class Parser_1:
    def __init__(self, db_uri, db_name, crawler_collection_name, parser_collection_name):
        # Initialize MongoDB connection and collections
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.crawler_coll = self.db[crawler_collection_name]
        self.parser_coll = self.db[parser_collection_name]
        
        # Initialize a requests session and set headers
        self.session = requests.Session()
        # Dynamically fetch the CSRF token
        self.csrf_token = self._fetch_csrf_token()

        # Update headers with the dynamic token and other necessary fields
        self.headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/json; charset=UTF-8',
            'outsystems-locale': 'nl-NL',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.plus.nl/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'traceparent': '00-527258064b1705846db4a2cc8f2c65bd-8e141e78141c3eec-01',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-csrftoken': self.csrf_token,  # Use dynamic token
        }
        
        # API endpoint to fetch product details.
        self.api_url = 'https://www.plus.nl/screenservices/ECP_Product_CW/ProductDetails/PDPContent/DataActionGetProductDetailsAndAgeInfo'

    def _fetch_csrf_token(self):
        """Fetch the OutSystems.js file and extract the CSRF token dynamically."""
        js_url = 'https://www.plus.nl/scripts/OutSystems.js?H4bR29NkZ15NFYcdxJmseg'
        try:
            response = requests.get(js_url, headers={
                'accept': '*/*',
                'referer': 'https://www.plus.nl/',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                              '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            })
            response.raise_for_status()
            # Extract token using regex
            match = re.search(r'e\.AnonymousCSRFToken\s*=\s*"([^"]+)"', response.text)
            if not match:
                raise RuntimeError("Unable to extract CSRF token from OutSystems.js")
            token = match.group(1)
            print(f"Fetched CSRF token: {token}")
            return token
        except Exception as e:
            raise RuntimeError(f"Error fetching CSRF token: {e}")

    def start(self):
        # This can be used to perform any initial logging or setup.
        print("Parser started.")

    def parse_items(self):
        # Fetch documents from the crawler collection
        cursor = self.crawler_coll.find({})
        for doc in cursor:
            try:
                unique_id = doc.get("unique_id")
                if not unique_id:
                    print("Missing unique_id in document; skipping.")
                    continue

                # Build JSON payload; replace the 'SKU' with the unique_id from the crawler document.
                json_data = {
                    'versionInfo': {
                        'moduleVersion': 'jApIf1I3AoV74zCivjDy4Q',
                        'apiVersion': 'j2jjJJxS4heD58kEZAYPUQ',
                    },
                    'viewName': 'MainFlow.ProductDetailsPage',
                    'screenData': {
                        'variables': {
                            'SKU': unique_id,
                            'ChannelId': '',
                            'Locale': 'nl-NL',
                            'StoreId': '0',
                            'StoreNumber': 0,
                            'CheckoutId': '5c3e7322-2383-42d8-8794-33c8b85a5693',
                            'OrderEditId': '',
                            'IsOrderEditMode': False,
                            'TotalLineItemQuantity': 0,
                            'ShoppingListProducts': {
                                'List': [],
                                'EmptyListItem': {
                                    'SKU': '',
                                    'Quantity': '0',
                                },
                            },
                            'HasDailyValueIntakePercent': False,
                            'CartPromotionDeliveryDate': '2025-04-07',
                            'LineItemQuantity': 0,
                            'IsPhone': False,
                            '_isPhoneInDataFetchStatus': 1,
                            'OneWelcomeUserId': '',
                            '_oneWelcomeUserIdInDataFetchStatus': 1,
                            '_sKUInDataFetchStatus': 1,
                            'TotalCartItems': 0,
                            '_totalCartItemsInDataFetchStatus': 1,
                            '_productNameInDataFetchStatus': 1,
                        },
                    },
                }

                # Make the POST request to the API
                response = self.session.post(self.api_url, headers=self.headers, json=json_data)
                resp_json = response.json()

                # --- 3. Parse the JSON and locate ProductOut ---
                product = None
                if isinstance(resp_json, dict):
                    data_section = resp_json.get("data", {})
                    if "ProductOut" in data_section:
                        product = data_section["ProductOut"]
                    elif "ProductOut" in resp_json:
                        product = resp_json["ProductOut"]

                if product is None:
                    print(f"Could not find 'ProductOut' for SKU {unique_id}. Skipping item.")
                    continue

                # --- 4. Extract fields from the API response ---
                overview = product.get("Overview", {})
                instr = product.get("InstructionsAndSuggestions", {}).get("Instructions", {})
                sugg = product.get("InstructionsAndSuggestions", {}).get("Suggestions", {})
                ingredients_val = product.get("Ingredients", "")
                pdp_info = product.get("Logos", {}).get("PDPInProductInformation", {}).get("List", [])
                nutrients = product.get("Nutrient", {}).get("Nutrients", {}).get("List", [])
                allergen = product.get("Allergen", {}).get("Description", "")

                # A. Subtitle → grammage + price
                subtitle = overview.get("Subtitle", "")
                m = re.search(r"Per\s+\w+\s+(\d+)\s+(\w+)", subtitle)
                grammage_quantity = m.group(1) if m else None
                grammage_unit     = m.group(2) if m else None
                p = re.search(r"\((.*?)\)", subtitle)
                price_per_unit    = p.group(1) if p else None
                product_description = overview.get("Meta", {}).get("Description", "")

                # B. Instructions & storage
                prep = instr.get("Preparation", "").strip()
                usage = instr.get("Usage", "").strip()
                instructions = prep + (" " + usage if usage else "")
                storage_instructions = instr.get("Storage", "").strip()

                # C. Servings
                servings_per_pack = sugg.get("Serving", "").strip()

                # D. Ingredients
                ingredients = ingredients_val.strip()

                # E. Nutritional score
                nutritional_score = None
                for item in pdp_info:
                    name = item.get("Name", "")
                    if name.startswith("Nutri-Score"):
                        score = name.replace("Nutri-Score", "").strip().upper()
                        if score in list("ABCDE"):
                            nutritional_score = score
                        break

                # F. Organic type
                organictype = "Non-Organic"
                for item in pdp_info:
                    if "Biologisch" in item.get("LongDescription", ""):
                        organictype = "Organic"
                        break

                # G. Allergens
                allergens = allergen.strip()

                # H. Fat percentage (example: matching a specific nutrient)
                fat_percentage = None
                for item in nutrients:
                    if item.get("ParentCode") == "FAT" and "meervoudig onverzadigd" in item.get("Description", "").lower():
                        fat_percentage = item.get("QuantityContained", {}).get("Value")
                        break

                # Build the parsed item merging crawler fields and new parsed fields
                parser_item = {
                    # Fields from crawler collection:
                    "unique_id": doc.get("unique_id"),
                    "competitor_name": doc.get("competitor_name"),
                    "product_name": doc.get("product_name"),
                    "brand": doc.get("brand"),
                    "pdp_url": doc.get("pdp_url"),
                    "producthierarchy_level1": doc.get("producthierarchy_level1"),
                    "producthierarchy_level2": doc.get("producthierarchy_level2"),
                    "producthierarchy_level3": doc.get("producthierarchy_level3"),
                    "producthierarchy_level4": doc.get("producthierarchy_level4"),
                    "producthierarchy_level5": doc.get("producthierarchy_level5"),
                    "regular_price": doc.get("regular_price"),
                    "selling_price": doc.get("selling_price"),
                    "promotion_price": doc.get("promotion_price"),
                    "promotion_valid_from": doc.get("promotion_valid_from"),
                    "promotion_valid_upto": doc.get("promotion_valid_upto"),
                    "promotion_type": doc.get("promotion_type"),
                    "breadcrumb": doc.get("breadcrumb"),
                    # New parsed fields from API response:
                    "product_description": product_description,
                    "grammage_quantity": grammage_quantity,
                    "grammage_unit": grammage_unit,
                    "price_per_unit": price_per_unit,
                    "instructions": instructions,
                    "storage_instructions": storage_instructions,
                    "servings_per_pack": servings_per_pack,
                    "ingredients": ingredients,
                    "nutritional_score": nutritional_score,
                    "organictype": organictype,
                    "allergens": allergens,
                    "fat_percentage": fat_percentage,
                }

                # Store the combined document into the parser collection
                self.parser_coll.insert_one(parser_item)
                print(f"Parsed and stored item for SKU {unique_id}.")

            except Exception as e:
                print(f"Error processing document with unique_id {doc.get('unique_id')}: {e}")

    def close(self):
        # Close MongoDB connection
        self.client.close()
        print("Parser closed.")

# Example usage:
if __name__ == "__main__":
    # Replace with your actual MongoDB URI and collection names.
    DB_URI = "mongodb://localhost:27017/"
    DB_NAME = "plus_nl"
    CRAWLER_COLLECTION = "crawler"
    PARSER_COLLECTION = "parser"

    parser = Parser_1(DB_URI, DB_NAME, CRAWLER_COLLECTION, PARSER_COLLECTION)
    parser.start()
    parser.parse_items()
    parser.close()
