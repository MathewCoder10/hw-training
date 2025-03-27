import requests
import re
from parsel import Selector
from pymongo import MongoClient

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
        self.crawler_collection = self.db['Crawler']
        self.parser_test_collection = self.db['parser']

    def start(self):
        # Iterate over each document (URL) in the Crawler collection.
        for document in self.crawler_collection.find():
            url = document.get("url", "")
            # Extract productId from the URL
            product_id = url.rstrip("/").split("/")[-1]
            print(f"\nProcessing URL: {url}")
            print(f"Extracted product ID: {product_id}")

            # Fetch product data via GraphQL using the dynamic product_id.
            graphql_data = self.parse_graphql(product_id)
            if graphql_data:
                print("GraphQL Product Data fetched successfully.")
            else:
                print("GraphQL query failed.")

            # Request the product page.
            try:
                page_response = requests.get(url, headers=self.headers_page)
                if page_response.status_code == 200:
                    # Parse the response and store the combined item.
                    self.parse_item(url, page_response, graphql_data)
                else:
                    print(f"Failed to retrieve product page for {url}. Status Code: {page_response.status_code}")
            except Exception as e:
                print(f"Exception fetching product page for {url}: {e}")

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
                    nutritionalInformation {{
                        standardPackagingUnit
                        soldOrPrepared
                        nutritionalValues {{
                            text
                            value
                            nutritionalSubValues {{
                                text
                                value
                            }}
                        }}
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
        try:
            response = requests.post('https://web-dirk-gateway.detailresult.nl/graphql', headers=self.headers_graphql, json=json_data)
            if response.status_code == 200:
                try:
                    json_response = response.json()
                except Exception as e:
                    print(f"Error decoding JSON: {e}")
                    return None

                # Ensure that the expected keys are present
                if not json_response or "data" not in json_response or json_response["data"] is None:
                    print("No data found in GraphQL response.")
                    return None

                data = json_response.get("data", {}).get("product", {})
                if not data:
                    print("No product data found in GraphQL response.")
                    return None

                result = {}
                result["unique_id"] = data.get("productId", "")
                result["product_name"] = data.get("headerText", "")
                result["packaging"] = data.get("packaging", "")
                description = data.get("description", "")
                additional = data.get("additionalDescription", "")
                result["product_description"] = f"{description} {additional}".strip()
                result["image_urls"] = [img.get("image", "") for img in data.get("images", []) if img.get("image")]
                result["features"] = [logo.get("description", "") for logo in data.get("logos", []) if logo.get("description")]
                declarations = data.get("declarations", {}) or {}
                result["storageInstructions"] = declarations.get("storageInstructions", "") or ""
                result["instructions"] = declarations.get("cookingInstructions", "") or ""
                result["instructionsForUse"] = declarations.get("instructionsForUse", "") or ""
                result["ingredients"] = declarations.get("ingredients", "") or ""
                contact_info = declarations.get("contactInformation", {}) or {}
                result["distributor_address"] = contact_info.get("contactAdress", "") or ""
                result["allergens"] = [allergy.get("text", "") for allergy in declarations.get("allergiesInformation", []) if allergy.get("text")]
                nutritional_info = []
                nutritional_values = declarations.get("nutritionalInformation", {}).get("nutritionalValues", []) or []
                for nv in nutritional_values:
                    nutritional_info.append((nv.get("text", ""), nv.get("value", "")))
                result["nutritional_information"] = nutritional_info
                product_assortment = data.get("productAssortment", {}) or {}
                product_info = product_assortment.get("productInformation", {}) or {}
                result["brand"] = product_info.get("brand", "") or ""
                product_offer = product_assortment.get("productOffer", {}) or {}
                result["start_date"] = product_offer.get("startDate", "") or ""
                result["end_date"] = product_offer.get("endDate", "") or ""
                result["normal_price"] = product_assortment.get("normalPrice", "") or ""
                result["offer_price"] = product_assortment.get("offerPrice", "") or ""
                return result
            else:
                print("GraphQL Error:", response.status_code, response.text)
                return None
        except Exception as e:
            print("Exception during GraphQL request:", e)
            return None

    def parse_item(self, url, response, graphql_data):
        """
        Parse the response from a URL and insert the structured item into the parser_test collection.
        This method uses XPath to extract additional fields from the product page and then merges them with
        the GraphQL data.
        """
        sel = Selector(text=response.text)
        
        # XPATH expressions for PARSER_2 extraction
        BREADCRUMB_XPATH = '//div[@class="item"]//a[@data-navigation-item]/@data-navigation-item'
        PROMOTION_XPATH = '//p[@class="offer-runtime"]/text()'
        
        # Extract data from product page
        breadcrumb = sel.xpath(BREADCRUMB_XPATH).getall() or []
        promotion = sel.xpath(PROMOTION_XPATH).get() or ""

        packaging_value = graphql_data.get("packaging", "") if graphql_data else ""
        site_shown_uom = packaging_value 
        per_unit_sizedescription = ""
        grammage_quantity = ""
        grammage_unit = ""
        if packaging_value:
            if packaging_value.lower().startswith("per"):
                per_unit_sizedescription = packaging_value
            else:
                # Check if packaging starts with digits followed by alphabets (e.g., "300 ml", "1 kg")
                match = re.match(r"(\d+(?:\.\d+)?)\s*([a-zA-Z]+)", packaging_value)
                if match:
                    grammage_quantity = match.group(1)
                    grammage_unit = match.group(2)
        
        # Product Unique Key: Append "P" to unique_id.
        unique_id = graphql_data.get("unique_id", "") if graphql_data else ""
        product_unique_key = f"{unique_id}P" if unique_id else ""
        
        # Organic Type: Check if features contain "Biologisch"
        features = graphql_data.get("features", []) if graphql_data else []
        organictype = "organic" if any("Biologisch" in feature for feature in features) else ""
        
        # Price fields: price_was and promotion_price equal to offer_price.
        offer_price = graphql_data.get("offer_price", "") if graphql_data else ""
        price_was = offer_price
        promotion_price = offer_price
        # Regular Price: set to normal_price.
        regular_price = graphql_data.get("normal_price", "") if graphql_data else ""
        # Selling Price: if offer_price exists use it, otherwise use normal_price.
        selling_price = offer_price if offer_price else regular_price 
        promotion_valid_from = graphql_data.get("start_date", "").split("T")[0] if graphql_data else ""
        promotion_valid_upto = graphql_data.get("end_date", "").split("T")[0] if graphql_data else ""

       # Product Hierarchy Levels and bread_crumb from breadcrumb.
        producthierarchy_level1 = breadcrumb[0] if len(breadcrumb) > 0 else ""
        producthierarchy_level2 = breadcrumb[1] if len(breadcrumb) > 1 else ""
        producthierarchy_level3 = breadcrumb[2] if len(breadcrumb) > 2 else ""
        producthierarchy_level4 = breadcrumb[3] if len(breadcrumb) > 3 else ""
        producthierarchy_level5 = breadcrumb[4] if len(breadcrumb) > 4 else ""
        producthierarchy_level6 = breadcrumb[5] if len(breadcrumb) > 5 else ""
        producthierarchy_level7 = breadcrumb[6] if len(breadcrumb) > 6 else ""
        bread_crumb = ">".join(breadcrumb)
        
        # New 6 Fields for images based on image_urls field.
        image_urls = graphql_data.get("image_urls", []) if graphql_data else []
        file_name_1 = f"{unique_id}_1.PNG" if len(image_urls) > 0 else ""
        image_url_1 = image_urls[0] if len(image_urls) > 0 else ""
    
        file_name_2 = f"{unique_id}_2.PNG" if len(image_urls) > 1 else ""
        image_url_2 = image_urls[1] if len(image_urls) > 1 else ""
    
        file_name_3 = f"{unique_id}_3.PNG" if len(image_urls) > 2 else ""
        image_url_3 = image_urls[2] if len(image_urls) > 2 else ""
        

        item = {
            "unique_id": unique_id,
            "competitor_name": "dirk",
            "product_name": graphql_data.get("product_name", "") if graphql_data else "",
            "brand": graphql_data.get("brand", "") if graphql_data else "",
            "grammage_quantity": grammage_quantity,
            "grammage_unit": grammage_unit, 
            "producthierarchy_level1": producthierarchy_level1,
            "producthierarchy_level2": producthierarchy_level2,
            "producthierarchy_level3": producthierarchy_level3,
            "producthierarchy_level4": producthierarchy_level4,
            "producthierarchy_level5": producthierarchy_level5,
            "producthierarchy_level6": producthierarchy_level6,
            "producthierarchy_level7": producthierarchy_level7,
            "regular_price": regular_price,
            "selling_price": selling_price,
            "price_was": price_was,
            "promotion_price": promotion_price,
            "promotion_valid_from": promotion_valid_from,
            "promotion_valid_upto": promotion_valid_upto,
            "promotion_description": promotion,
            "per_unit_sizedescription": per_unit_sizedescription,
            "bread_crumb": bread_crumb,
            "pdp_url": url,
            "product_description": graphql_data.get("product_description", "") if graphql_data else "",
            "storage_instructions": graphql_data.get("storageInstructions", "") if graphql_data else "",
            "preparationinstructions": graphql_data.get("instructions", "") if graphql_data else "",
            "instructionforuse": graphql_data.get("instructionsForUse", "") if graphql_data else "",
            "allergens": graphql_data.get("allergens", []) if graphql_data else [],
            "nutritional_information": graphql_data.get("nutritional_information", []) if graphql_data else [],
            "packaging": packaging_value,
            "organictype": organictype,
            "file_name_1": file_name_1,
            "image_url_1": image_url_1,
            "file_name_2": file_name_2,
            "image_url_2": image_url_2,
            "file_name_3": file_name_3,
            "image_url_3": image_url_3,
            "features": features,
            "distributor_address": graphql_data.get("distributor_address", "") if graphql_data else "",
            "site_shown_uom": site_shown_uom,
            "ingredients": graphql_data.get("ingredients", "") if graphql_data else "",
            "product_unique_key": product_unique_key,          
        }
        
        
        print("Parsed item:", item)
        try:
            self.parser_test_collection.insert_one(item)
            print(f"Inserted item for URL: {url}")
        except Exception as e:
            print(f"Error inserting item for {url}: {e}")

    def close(self):
        print("Closing database connection.")
        self.client.close()

if __name__ == '__main__':
    parser = Parser()
    parser.start()
    parser.close()
