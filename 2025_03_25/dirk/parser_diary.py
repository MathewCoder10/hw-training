import requests
import re
import time
import logging
from parsel import Selector
from pymongo import MongoClient, ASCENDING

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)

class Parser:
    def __init__(self):
        self.headers_graphql = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'api_key': '6d3a42a3-6d93-4f98-838d-bcc0ab2307fd',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'origin': 'https://www.dirk.nl',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.dirk.nl/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.headers_page = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.dirk.nl/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.base_url = "https://www.dirk.nl"
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['dirk']
        # Crawler collection holds the URLs to be parsed.
        self.crawler_collection = self.db['crawler_diary']
        # Parser collection stores the parsed items.
        self.parser_test_collection = self.db['parser_diary']
        # Create unique index on unique_id
        self.parser_test_collection.create_index([("unique_id", ASCENDING)], unique=True)
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    def make_request(self, method, url, headers, **kwargs):
        """
        Helper method to perform HTTP requests with a retry mechanism.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.request(method, url, headers=headers, **kwargs)
                if response.status_code == 200:
                    logging.info(f"Successful {method.upper()} request on attempt {attempt} for URL: {url}")
                    return response
                else:
                    logging.warning(f"Attempt {attempt} for URL: {url} returned status code {response.status_code}")
            except Exception as e:
                logging.exception(f"Attempt {attempt} for URL: {url} raised an exception: {e}")
            time.sleep(self.retry_delay)
        logging.error(f"All {self.max_retries} attempts failed for URL: {url}")
        return None

    def to_float(self, value):
        """
        Converts a value to float rounded to two decimals.
        If conversion fails, returns 0.0.
        """
        try:
            return round(float(value), 2)
        except (ValueError, TypeError):
            return 0.0

    def start(self):
        # Iterate over each document (URL) in the Crawler collection.
        for document in self.crawler_collection.find():
            url = document.get("url", "")
            # Extract productId from the URL (assumes productId is the last segment)
            product_id = url.rstrip("/").split("/")[-1]
            logging.info(f"Processing URL: {url}")
            logging.info(f"Extracted product ID: {product_id}")

            # Fetch product data via GraphQL using the dynamic product_id.
            graphql_data = self.parse_graphql(product_id)
            if graphql_data:
                logging.info("GraphQL Product Data fetched successfully.")
            else:
                logging.error("GraphQL query failed.")

            # Request the product page.
            page_response = self.make_request("GET", url, headers=self.headers_page)
            if page_response:
                self.parse_item(url, page_response, graphql_data)
            else:
                logging.error(f"Failed to retrieve product page for {url} after retries.")

    def parse_graphql(self, product_id):
        # Build a dynamic GraphQL query with the product_id.
        graphql_query = f"""
        query {{
            product(productId: {product_id}) {{
                productId
                department
                headerText
                packaging
                description
                additionalDescription
                images {{
                    image
                    rankNumber
                    mainImage
                }}
                logos {{
                    description
                    image
                }}
                declarations {{
                    storageInstructions
                    cookingInstructions
                    instructionsForUse
                    ingredients
                    contactInformation {{
                        contactName
                        contactAdress
                    }}
                    allergiesInformation {{
                        text
                    }}
                }}
                productAssortment(storeId: 66) {{
                    productId
                    normalPrice
                    offerPrice
                    isSingleUsePlastic
                    singleUsePlasticValue
                    startDate
                    endDate
                    productOffer {{
                        textPriceSign
                        endDate
                        startDate
                        disclaimerStartDate
                        disclaimerEndDate
                    }}
                    productInformation {{
                        productId
                        headerText
                        subText
                        packaging
                        image
                        department
                        webgroup
                        brand
                        logos {{
                            description
                            image
                        }}
                    }}
                }}
            }}
        }}
        """
        json_data = {
            'query': graphql_query,
            'variables': {}
        }
        url = 'https://web-dirk-gateway.detailresult.nl/graphql'
        response = self.make_request("POST", url, headers=self.headers_graphql, json=json_data)
        if response:
            try:
                json_response = response.json()
            except Exception as e:
                logging.exception(f"Error decoding JSON for product {product_id}: {e}")
                return None

            # Ensure that the expected keys are present.
            if not json_response or "data" not in json_response or json_response["data"] is None:
                logging.error("No data found in GraphQL response.")
                return None

            data = json_response.get("data", {}).get("product", {})
            if not data:
                logging.error("No product data found in GraphQL response.")
                return None

            result = {}
            result["unique_id"] = str(data.get("productId", ""))
            result["product_name"] = str(data.get("headerText", ""))
            result["packaging"] = data.get("packaging", "")
            description = str(data.get("description", ""))
            additional = str(data.get("additionalDescription", ""))
            result["product_description"] = f"{description} {additional}".strip()
            result["image_urls"] = [str(img.get("image", "")) for img in data.get("images", []) if img.get("image")]
            result["features"] = [str(logo.get("description", "")) for logo in data.get("logos", []) if logo.get("description")]
            declarations = data.get("declarations", {}) or {}
            result["storageInstructions"] = str(declarations.get("storageInstructions", ""))
            result["instructions"] = str(declarations.get("cookingInstructions", ""))
            result["instructionsForUse"] = str(declarations.get("instructionsForUse", ""))
            result["ingredients"] = str(declarations.get("ingredients", ""))
            contact_info = declarations.get("contactInformation", {}) or {}
            result["distributor_address"] = str(contact_info.get("contactAdress", ""))
            # For allergens, join the texts into a single string.
            result["allergens"] = ", ".join([str(allergy.get("text", "")) for allergy in declarations.get("allergiesInformation", []) if allergy.get("text")])
            product_assortment = data.get("productAssortment", {}) or {}
            product_info = product_assortment.get("productInformation", {}) or {}
            result["brand"] = str(product_info.get("brand", ""))
            product_offer = product_assortment.get("productOffer", {}) or {}
            result["start_date"] = str(product_offer.get("startDate", ""))
            result["end_date"] = str(product_offer.get("endDate", ""))
            result["normal_price"] = product_assortment.get("normalPrice", "")
            result["offer_price"] = product_assortment.get("offerPrice", "")
            return result
        else:
            logging.error(f"GraphQL request failed for product_id {product_id}.")
            return None

    def parse_item(self, url, response, graphql_data):
        """
        Parse the product page response and insert the structured item into the parser_test collection.
        """
        sel = Selector(text=response.text)
        
        # XPATH expressions for additional extraction.
        BREADCRUMB_XPATH = '//div[@class="item"]//a[@data-navigation-item]/@data-navigation-item'
        PROMOTION_XPATH = '//p[@class="offer-runtime"]/text()'
        NUTRITIONAL_XPATH = '//img[contains(@class, "nutri-score")]/@alt'
        
        # Extract data from the product page.
        breadcrumb_list = sel.xpath(BREADCRUMB_XPATH).getall() or []
        # Only take levels 1 to 5
        breadcrumb_levels = [str(b) for b in breadcrumb_list[:5]]
        breadcrumb = ">".join(breadcrumb_levels)
        promotion = str(sel.xpath(PROMOTION_XPATH).get() or "")
        
        
        # Extract nutritional score from the alt attribute of the nutri-score image.
        nutritional = str(sel.xpath(NUTRITIONAL_XPATH).get() or "")
        nutritional_score = ""
        if nutritional:
            # Adjusted regex to capture a letter (A-E) even if the '=' is not present.
            match = re.search(r'NUTRI-SCORE\s*([A-E])', nutritional)
            if match:
                nutritional_score = match.group(1)
        
        packaging_value = str(graphql_data.get("packaging", "")) if graphql_data else ""
        grammage_quantity = ""
        grammage_unit = ""
        if packaging_value and not packaging_value.lower().startswith("per"):
            match = re.match(r"(\d+(?:\.\d+)?)\s*([a-zA-Z]+)", packaging_value)
            if match:
                grammage_quantity = str(match.group(1))
                grammage_unit = str(match.group(2))
        
        # Product unique key.
        unique_id = str(graphql_data.get("unique_id", "")) if graphql_data else ""
        
        # Organic type: set as "Organic" if any feature contains "Biologisch", otherwise "Non-Organic"
        features_list = graphql_data.get("features", []) if graphql_data else []
        organictype = "Organic" if any("Biologisch" in feature for feature in features_list) else "Non-Organic"
        
        # Price fields conversion.
        offer_price_val = graphql_data.get("offer_price", "")
        regular_price_val = graphql_data.get("normal_price", "")
        selling_price_val = offer_price_val if offer_price_val else regular_price_val
        price_was_val = offer_price_val

        regular_price = self.to_float(regular_price_val)
        selling_price = self.to_float(selling_price_val)
        price_was = self.to_float(price_was_val)
        promotion_price = self.to_float(offer_price_val)
        
        # Promotion valid dates.
        promotion_valid_from = str(graphql_data.get("start_date", "").split("T")[0]) if graphql_data else ""
        promotion_valid_upto = str(graphql_data.get("end_date", "").split("T")[0]) if graphql_data else ""
        
        # Explicit extraction of up to 6 images and filenames.
        image_urls_list = graphql_data.get("image_urls", []) if graphql_data else []
        file_name_1 = str(f"{unique_id}_1.PNG") if len(image_urls_list) > 0 else ""
        image_url_1 = str(image_urls_list[0]) if len(image_urls_list) > 0 else ""
    
        file_name_2 = str(f"{unique_id}_2.PNG") if len(image_urls_list) > 1 else ""
        image_url_2 = str(image_urls_list[1]) if len(image_urls_list) > 1 else ""
    
        file_name_3 = str(f"{unique_id}_3.PNG") if len(image_urls_list) > 2 else ""
        image_url_3 = str(image_urls_list[2]) if len(image_urls_list) > 2 else ""
    
        file_name_4 = str(f"{unique_id}_4.PNG") if len(image_urls_list) > 3 else ""
        image_url_4 = str(image_urls_list[3]) if len(image_urls_list) > 3 else ""
    
        file_name_5 = str(f"{unique_id}_5.PNG") if len(image_urls_list) > 4 else ""
        image_url_5 = str(image_urls_list[4]) if len(image_urls_list) > 4 else ""
    
        file_name_6 = str(f"{unique_id}_6.PNG") if len(image_urls_list) > 5 else ""
        image_url_6 = str(image_urls_list[5]) if len(image_urls_list) > 5 else ""
        
        item = {
            "unique_id": unique_id,
            "competitor_name": "dirk",
            "product_name": str(graphql_data.get("product_name", "")) if graphql_data else "",
            "brand": str(graphql_data.get("brand", "")) if graphql_data else "",
            "grammage_quantity": grammage_quantity,
            "grammage_unit": grammage_unit, 
            "producthierarchy_level1": breadcrumb_levels[0] if len(breadcrumb_levels) > 0 else "",
            "producthierarchy_level2": breadcrumb_levels[1] if len(breadcrumb_levels) > 1 else "",
            "producthierarchy_level3": breadcrumb_levels[2] if len(breadcrumb_levels) > 2 else "",
            "producthierarchy_level4": breadcrumb_levels[3] if len(breadcrumb_levels) > 3 else "",
            "producthierarchy_level5": breadcrumb_levels[4] if len(breadcrumb_levels) > 4 else "",
            "regular_price": regular_price,
            "selling_price": selling_price,
            "price_was": price_was,
            "promotion_price": promotion_price,
            "promotion_valid_from": promotion_valid_from,
            "promotion_valid_upto": promotion_valid_upto,
            "promotion_description": str(promotion),
            "bread_crumb": breadcrumb,
            "pdp_url": str(url),
            "product_description": str(graphql_data.get("product_description", "")) if graphql_data else "",
            "instructions": (str(graphql_data.get("instructions", "")) + " " + str(graphql_data.get("instructionsForUse", ""))).strip() if graphql_data else "",
            "storage_instructions": str(graphql_data.get("storageInstructions", "")) if graphql_data else "",
            "allergens": str(graphql_data.get("allergens", "")) if graphql_data else "",
            "nutritional_score": nutritional_score,
            "organictype": organictype,
            "file_name_1": file_name_1,
            "image_url_1": image_url_1,
            "file_name_2": file_name_2,
            "image_url_2": image_url_2,
            "file_name_3": file_name_3,
            "image_url_3": image_url_3,
            "file_name_4": file_name_4,
            "image_url_4": image_url_4,
            "file_name_5": file_name_5,
            "image_url_5": image_url_5,
            "file_name_6": file_name_6,
            "image_url_6": image_url_6,
            "ingredients": str(graphql_data.get("ingredients", "")) if graphql_data else "",
        }
        
        logging.info(f"Parsed item: {item}")
        try:
            self.parser_test_collection.insert_one(item)
            logging.info(f"Inserted item for URL: {url}")
        except Exception as e:
            logging.exception(f"Error inserting item for {url}: {e}")

    def close(self):
        logging.info("Closing database connection.")
        self.client.close()

if __name__ == '__main__':
    parser = Parser()
    parser.start()
    parser.close()
