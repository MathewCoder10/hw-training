import requests
import re
from pymongo import MongoClient

class Parser:
    def __init__(self, mongo_uri="mongodb://localhost:27017", db_name="delhaize"):
        # Initialize MongoDB client and collections
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.source_collection = self.db["crawler_final"]
        self.target_collection = self.db["parser_final"]

        # Setup request headers common for all API calls
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
            'apollographql-client-name': 'be-dll-web-stores',
            'apollographql-client-version': 'b43fe67605ad012a0cc8976ce891aacc46b52c95',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.delhaize.be/nl/shop/Zuivel-kaas-en-plantaardige-alternatieven/Kaas/Kaas-in-blok/Gouda-Jong-Blok/p/F2019121000048330000',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-apollo-operation-id': 'eddec869e4cf3d29e2c5a1503bbe5c0d4e9eb7efa8cf0c4c3d26aeb89a082415',
            'x-apollo-operation-name': 'ProductDetails',
            'x-default-gql-refresh-token-disabled': 'true',
            'x-dtpc': '9$390940150_418h3vAJQOEPQFHCLKUJQDQMMCFESAHATHBMOR-0e0',
        }
        self.api_url = "https://www.delhaize.be/api/v1/"

    def convert_price(self, price_str):
        """
        Convert a price string (which may include a euro sign and comma as a decimal separator)
        into a float rounded to two decimal places.
        """
        if not price_str:
            return 0.0
        # Remove euro sign and any whitespace
        cleaned = price_str.replace("€", "").strip()
        # Replace comma with period
        cleaned = cleaned.replace(",", ".")
        try:
            value = float(cleaned)
            return round(value, 2)
        except ValueError:
            return 0.0

    def start(self):
        """Begin processing items from the source collection."""
        self.parse_items()

    def parse_items(self):
        """Fetch product codes, call the API, parse the JSON response, and store results."""
        # Fetch all documents from the crawler_final collection with a product_code field
        cursor = self.source_collection.find({"product_code": {"$exists": True}})
        for doc in cursor:
            product_code = doc["product_code"]
            
            # Prepare query parameters using the product_code
            params = {
                'operationName': 'ProductDetails',
                'variables': f'{{"productCode":"{product_code}","lang":"nl"}}',
                'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"a031009b37964f1c04684b7cd15fa29f5787206be2594a1d0efa85a2fd37a4cc"}}',
            }
            
            try:
                # Make API request
                response = requests.get(self.api_url, params=params, headers=self.headers)
                response.raise_for_status()
                data = response.json()

                # Safely get the productDetails dict
                pd = data.get("data", {}).get("productDetails") or {}

                result = {}
                result["unique_id"] = pd.get("code", "")
                result["competitor_name"] = "delhaize"
                result["product_name"] = pd.get("name", "")
                result["brand"] = pd.get("manufacturerName", "")
                
                # Grammage: from wsNutriFactData.otherInfo with key "Net inhoud"
                ws_data = pd.get("wsNutriFactData") or {}
                other_info = ws_data.get("otherInfo", [])
                grammage_value = ""
                for item in other_info:
                    if item.get("key", "") == "Net inhoud":
                        grammage_value = item.get("value", "")
                        break
                if grammage_value:
                    match = re.match(r"([\d.,]+)\s*([a-zA-Z]+)", grammage_value)
                    if match:
                        result["grammage_quantity"] = match.group(1)
                        result["grammage_unit"] = match.group(2)
                    else:
                        result["grammage_quantity"] = ""
                        result["grammage_unit"] = ""
                else:
                    result["grammage_quantity"] = ""
                    result["grammage_unit"] = ""
                
                # Product hierarchy levels
                result["producthierarchy_level1"] = "Delhaize"
                result["producthierarchy_level2"] = "eshop"
                categories = pd.get("categories", [])
                if len(categories) >= 3:
                    result["producthierarchy_level3"] = categories[-1].get("name", "")
                    result["producthierarchy_level4"] = categories[-2].get("name", "")
                    result["producthierarchy_level5"] = categories[-3].get("name", "")
                else:
                    result["producthierarchy_level3"] = categories[-1].get("name", "") if len(categories) >= 1 else ""
                    result["producthierarchy_level4"] = categories[-2].get("name", "") if len(categories) >= 2 else ""
                    result["producthierarchy_level5"] = ""
                
                # Price logic with conversion to float
                price_info = pd.get("price", {}) or {}
                discounted_str = price_info.get("discountedPriceFormatted", "")
                show_strike = price_info.get("showStrikethroughPrice", False)
                value_str = str(price_info.get("value", ""))
                
                discounted = self.convert_price(discounted_str) if discounted_str else 0.0
                value_price = self.convert_price(value_str) if value_str else 0.0
                
                if discounted_str and not show_strike:
                    result["promotion_price"] = discounted
                    result["selling_price"] = discounted
                    result["regular_price"] = 0.0
                    result["price_was"] = 0.0
                elif not discounted_str and not show_strike:
                    result["selling_price"] = value_price
                    result["regular_price"] = value_price
                    result["promotion_price"] = 0.0
                    result["price_was"] = 0.0
                elif discounted_str and show_strike:
                    result["promotion_price"] = discounted
                    result["selling_price"] = discounted
                    result["regular_price"] = value_price
                    result["price_was"] = value_price
                else:
                    result["promotion_price"] = 0.0
                    result["selling_price"] = 0.0
                    result["regular_price"] = 0.0
                    result["price_was"] = 0.0
                
                result["currency"] = price_info.get("currencySymbol", "")
                
                # Promotion details (if available)
                promotions = pd.get("potentialPromotions", [])
                if promotions:
                    promo = promotions[0]
                    result["promotion_valid_from"] = promo.get("fromDate", "")
                    result["promotion_valid_upto"] = promo.get("toDate", "")
                    result["promotion_description"] = promo.get("description", "")
                    result["promotion_type"] = promo.get("promotionType", "")
                    desc = promo.get("description", "")
                    match = re.search(r"(\d+)%", desc)
                    result["percentage_discount"] = match.group(1) if match else ""
                else:
                    result["promotion_valid_from"] = ""
                    result["promotion_valid_upto"] = ""
                    result["promotion_description"] = ""
                    result["promotion_type"] = ""
                    result["percentage_discount"] = ""
                
                # Breadcrumb: join hierarchy levels and product_name
                result["beadcrumb"] = " > ".join([
                    result["producthierarchy_level1"],
                    result["producthierarchy_level2"],
                    result.get("producthierarchy_level3", ""),
                    result.get("producthierarchy_level4", ""),
                    result.get("producthierarchy_level5", ""),
                    result["product_name"]
                ])
                
                # pdp_url
                pdp_path = pd.get("url", "")
                result["pdp_url"] = "https://www.delhaize.be" + pdp_path
                
                result["product_description"] = pd.get("description", "")
                
                # instructions from mobileClassificationAttributes
                instructions = ""
                for attr in pd.get("mobileClassificationAttributes", []):
                    if attr.get("code", "") == "PRODUCTUTILISATIONADVICE":
                        instructions = attr.get("value", "")
                        break
                result["instructions"] = instructions
                
                # storage_instructions from wsNutriFactData.otherInfo with key "Bijzondere bewaarvoorschriften"
                storage_instructions = ""
                for item in ws_data.get("otherInfo", []):
                    if item.get("key", "") == "Bijzondere bewaarvoorschriften":
                        storage_instructions = item.get("value", "")
                        break
                result["storage_instructions"] = storage_instructions
                
                # allergens from wsNutriFactData.allegery, excluding id "doesNotContain"
                allergens_list = []
                for allergy in ws_data.get("allegery", []):
                    if allergy.get("id", "") != "doesNotContain":
                        values = allergy.get("values", [])
                        if isinstance(values, list):
                            allergens_list.extend(values)
                result["allergens"] = allergens_list
                
                result["nutritional_score"] = pd.get("nutriScoreLetter", "")
                
                # organictype: check badges
                organictype = "Non-Organic"
                for badge in pd.get("badges", []):
                    if badge.get("code", "") == "badgeproductisbio":
                        organictype = "Organic"
                        break
                result["organictype"] = organictype
                
                # Build image_urls_list from groupedGalleryImages (format "respProduct")
                image_urls_list = []
                for group in pd.get("groupedGalleryImages", []):
                    for img in group.get("images", []):
                        if img.get("format", "") == "respProduct":
                            image_urls_list.append("https://static.delhaize.be" + img.get("url", ""))
                
                unique_id = result["unique_id"]
                result["file_name_1"] = f"{unique_id}_1.PNG" if len(image_urls_list) > 0 else ""
                result["image_url_1"] = image_urls_list[0] if len(image_urls_list) > 0 else ""
    
                result["file_name_2"] = f"{unique_id}_2.PNG" if len(image_urls_list) > 1 else ""
                result["image_url_2"] = image_urls_list[1] if len(image_urls_list) > 1 else ""
    
                result["file_name_3"] = f"{unique_id}_3.PNG" if len(image_urls_list) > 2 else ""
                result["image_url_3"] = image_urls_list[2] if len(image_urls_list) > 2 else ""
    
                result["file_name_4"] = f"{unique_id}_4.PNG" if len(image_urls_list) > 3 else ""
                result["image_url_4"] = image_urls_list[3] if len(image_urls_list) > 3 else ""
    
                result["file_name_5"] = f"{unique_id}_5.PNG" if len(image_urls_list) > 4 else ""
                result["image_url_5"] = image_urls_list[4] if len(image_urls_list) > 4 else ""
    
                result["file_name_6"] = f"{unique_id}_6.PNG" if len(image_urls_list) > 5 else ""
                result["image_url_6"] = image_urls_list[5] if len(image_urls_list) > 5 else ""
                
                result["ingredients"] = ws_data.get("ingredients", "")
                
                servings = ""
                for item in ws_data.get("otherInfo", []):
                    if item.get("key", "") == "Aantal porties per verpakking":
                        servings = item.get("value", "")
                        break
                result["servings_per_pack"] = servings
                
                result["source_product_code"] = product_code
                
                # Insert the parsed result into the target collection
                self.target_collection.insert_one(result)
                print(f"Processed product code: {product_code}")

            except Exception as e:
                print(f"Error processing product code {product_code}: {e}")

    def close(self):
        """Clean up any resources."""
        self.client.close()


if __name__ == "__main__":
    parser = Parser()
    try:
        parser.start()
    finally:
        parser.close()
