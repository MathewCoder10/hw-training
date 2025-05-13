import logging
import requests
import re
from parsel import Selector
from playwright.sync_api import sync_playwright
from items import ProductParserFailedItem, ProductParserItem
from mongoengine import connect, disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CRAWLER,
    HEADERS,
    BASE_URL,
    SCRIPT_PATTERN
)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Parser:
    def __init__(self):
        # MongoDB setup
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.crawler_collection = self.database[MONGO_COLLECTION_CRAWLER]

        dynamic_script_url = self.retrieve_dynamic_script_url()
        if dynamic_script_url:
            self.release_id = self.extract_release_id(dynamic_script_url)
            if self.release_id:
                logging.info(f"Using release ID: {self.release_id}")
            else:
                logging.error("Missing release ID; aborting parser initialization.")
                # Clean up DB connection before exiting
                self.close()
                exit("Initialization failed: Sentry release ID not found.")
        else:
            logging.error("Unable to locate dynamic script URL.")
            # Clean up DB connection before exiting
            self.close()
            exit("Initialization failed: dynamic script URL not found.")
        

    def retrieve_dynamic_script_url(self):
        """Launch headless browser to fetch the page and locate the dynamic script URL."""
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(user_agent=HEADERS.get('User-Agent'))
            page.goto(BASE_URL, wait_until='domcontentloaded')
            html = page.content()
            browser.close()

        sel = Selector(text=html)
        scripts = sel.xpath('//script[@src]/@src').getall()
        matches = [src for src in scripts if SCRIPT_PATTERN.match(src)]
        return matches[0] if matches else None

    def extract_release_id(self, script_url):
        """Download the JS file and parse out the Sentry release ID."""
        response = requests.get(script_url, headers=HEADERS)
        response.raise_for_status()
        match = re.search(r't4\.SENTRY_RELEASE\s*=\s*\{id:"([0-9a-fA-F]+)"\}', response.text)
        return match.group(1) if match else None
    
    
    def start(self):
        """Iterate through each category, do the API request, then hand off to parse_items()."""
        for category in self.crawler_collection.find():
            product_url = category.get('product_url')
            product_id = product_url.rstrip('/').split('/')[-1]
            product_details_url = f'https://www.heb.com/_next/data/{self.release_id}/product-detail/{product_id}.json'
            params = {'productId': product_id}
            response = requests.get(product_details_url, params=params, headers=HEADERS)
            if response.status_code == 200:
                self.parse_items(response, product_url, product_id)
            else:
                failed_item = {}
                failed_item ['product_url'] = product_url
                failed_item ['issue'] = f"{response.status_code}"
                try:
                    ProductParserFailedItem(**failed_item).save()
                    logging.info(f"Logged failed URL for unique_id {product_id}")
                except Exception:
                    logging.exception("Failed to log URL failure")

        
    def parse_items(self, response, product_url, product_id):

        data = response.json()
        product = data["pageProps"]["product"]
        product_name = product.get("fullDisplayName",'')
        product_description = product.get("productDescription",'')
        ingredients = product.get("ingredientStatement",'')
        brand = product.get("brand", {}).get("name",'')
        unique_id = product_id
        product_unique_key = f"{product_id}P"
        pdp_url = product_url
        store = product.get("store", {})
        store_name = store.get("name",'')
        store_addressline1 = store.get("address", {}).get("streetAddress",'')
        skus = product.get("SKUs", [])
        sku  = skus[0] if skus else {}
        upc = sku.get("twelveDigitUPC",'')
        size = sku.get("customerFriendlySize", "")
        match = re.search(r'(\d+(?:\.\d+)?|\d+/\d+)\s*([a-zA-Z]+)', size)
        if match:
            raw_quantity, raw_unit = match.groups()
            grammage_quantity = raw_quantity
            grammage_unit     = raw_unit.strip()
            package_size      = f"{grammage_quantity} {grammage_unit}"

        else:
            package_size      = ""
            grammage_quantity = ""
            grammage_unit     = ""

        matches = [context_price for context_price in sku.get("contextPrices", []) if context_price.get("context") == "CURBSIDE"]
        prices  = matches[0] if matches else {}
        raw_list = prices.get("listPrice", {}).get("amount",0.0)
        raw_sale = prices.get("salePrice", {}).get("amount",0.0)
        list_price = float(raw_list)
        sale_price = float(raw_sale)
        regular_price = list_price
        selling_price = sale_price
        if list_price and sale_price and list_price != sale_price:
            price_was       = float(list_price)
            promotion_price = float(sale_price)
        else:
            price_was       = None
            promotion_price = None
             
        unit_sale = prices.get("unitSalePrice", {})
        if unit_sale:
            price_per_unit = f"{unit_sale.get('amount','')} per {unit_sale.get('unit','')}"

        coupons = product.get("coupons", [])
        if coupons:
            coupon = coupons[0]
            promotion_description = coupon.get("description", '')
            promotion_type        = coupon.get("shortDescription", '')
            promotion_valid_upto  = coupon.get("expirationDate", '')
            match = re.search(r'(\d+)%', promotion_type)
            if match:
                percentage_discount = match.group(1)  
            else:
                percentage_discount = ''

        else:
            promotion_description = ""
            promotion_type        = ""
            promotion_valid_upto  = ""
            percentage_discount   = ""
        breadcrumb                      = product.get("breadcrumbs", [])
        breadcrumb_path = " > ".join(crumb.get("title", "") for crumb in breadcrumb)
        breadcrumbs = f"{breadcrumb_path} > {product_name}"
        parts = breadcrumbs.split(" > ")
        parts += [""] * (7 - len(parts))
        parts = parts[:7]
        producthierarchy_level1 = parts[0]
        producthierarchy_level2 = parts[1]
        producthierarchy_level3 = parts[2]
        producthierarchy_level4 = parts[3]
        producthierarchy_level5 = parts[4]
        producthierarchy_level6 = parts[5]
        producthierarchy_level7 = parts[6]
       
        image_urls        = product.get("carouselImageUrls", [])
        stock = product.get("inventory", {}).get("inventoryState",'') 
        preparationInstructions     = product.get("preparationInstructions",'')
        Warning                     = product.get("safetyWarning",'')
        nutrition_labels = product.get("nutritionLabels", [])
        if nutrition_labels:
            nutrition = nutrition_labels[0]
            servings_per_pack = nutrition.get("servingsPerContainer", "")
            
            nutrients = nutrition.get("nutrients", [])
            vitamins  = nutrition.get("vitaminsAndMinerals", [])
            combined = nutrients + vitamins
            nutritional_information = ",".join(
                f"{item['title']}:{item.get('unit','')}:{item.get('percentage','')}"
                for item in combined
                if "title" in item
            )
            vitamin_titles = [v["title"] for v in vitamins if "title" in v]
            vitamin = ",".join(vitamin_titles) if vitamin_titles else ""
        else:
            servings_per_pack       = ""
            nutritional_information = ""
            vitamin                 = ""

        
        item = {}
        item['unique_id']             = unique_id
        item['competitor_name']       = 'heb'
        item['product_name']          = product_name
        item['product_unique_key']    = product_unique_key
        item['store_name']            = store_name
        item['store_addressline1']    = store_addressline1
        item['upc']                   = upc
        item['package_size']          = package_size
        item['breadcrumb']            = breadcrumbs
        item['producthierarchy_level1'] = producthierarchy_level1
        item['producthierarchy_level2'] = producthierarchy_level2
        item['producthierarchy_level3'] = producthierarchy_level3
        item['producthierarchy_level4'] = producthierarchy_level4
        item['producthierarchy_level5'] = producthierarchy_level5
        item['producthierarchy_level6'] = producthierarchy_level6
        item['producthierarchy_level7'] = producthierarchy_level7
        item['brand']                 = brand
        item['pdp_url']               = pdp_url
        item['currency']              = 'EUR'
        item['regular_price']         = regular_price
        item['selling_price']         = selling_price
        item['promotion_price']       = promotion_price
        item['price_was']             = price_was
        item['percentage_discount']   = percentage_discount
        item['promotion_description'] = promotion_description
        item['promotion_type']        = promotion_type
        item['promotion_valid_upto']  = promotion_valid_upto
        item['product_description']   = product_description
        item['grammage_quantity'] = grammage_quantity
        item['grammage_unit']         = grammage_unit
        item['price_per_unit']        = price_per_unit
        item['image_urls']            = ','.join(image_urls)
        item['instock']               = stock
        item['preparation_instructions'] = preparationInstructions
        item['warning']               = Warning
        item['servings_per_pack']     = servings_per_pack
        item['nutritional_information'] = nutritional_information
        item['vitamins']              = vitamin
        item['ingredients']           = ingredients

        logging.info(item)
        try:
            product_item = ProductParserItem(**item)
            product_item.save()
        except NotUniqueError:
            logging.warning(f"Duplicate unique_id found for ID: {unique_id}")
        except Exception as e:
            logging.exception(f"Failed to save product item for URL: {pdp_url}")


    def close(self):
        disconnect()
        logging.info("MongoDB connection closed.")

if __name__ == '__main__':
    parser = Parser()
    parser.start()
    parser.close()