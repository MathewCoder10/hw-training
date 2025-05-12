import logging
import requests
import re
from fractions import Fraction
from parsel import Selector
from playwright.sync_api import sync_playwright
from items import ProductParserFailedItem, ProductParserItem
from datetime import datetime
from mongoengine import connect, disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CRAWLER,
    MONGO_COLLECTION_PARSER,
    MONGO_COLLECTION_PARSER_URL_FAILED,
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
        self.parser_collection = self.database[MONGO_COLLECTION_PARSER]
        self.failed_urls_collection = self.database[MONGO_COLLECTION_PARSER_URL_FAILED]

        try:
            dynamic_script_url = self.retrieve_dynamic_script_url()
            if dynamic_script_url:
                self.release_id = self.extract_release_id(dynamic_script_url)
                logging.info(f"Using release ID: {self.release_id}")
            else:
                logging.error("Unable to locate dynamic script URL.")
        except Exception:
            logging.exception("Exception during initialization of release ID.")

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
        if not self.release_id:
            logging.error("Missing release ID; aborting crawl.")
            return

        for category in self.crawler_collection.find():
            product_url = category.get('product_url')
            product_id = product_url.rstrip('/').split('/')[-1]

            product_details_url = f'https://www.heb.com/_next/data/6ce703410621d0cffdc8972db42c3395d09eee6f/product-detail/{product_id}.json'
            params = {'productId': product_id}
            response = requests.get(product_details_url, params=params, headers=HEADERS)
            if response.status_code == 200:
                self.parse_items(response, product_url, product_id)
            else:
                failed_item = {}
                failed_item ['unique_id'] = product_id
                failed_item ['product_url'] = product_url
                failed_item ['issue'] = response.status_code
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
        sku = product["SKUs"][0]
        upc = sku.get("twelveDigitUPC",'')
        size_str = sku.get("customerFriendlySize", "")
        quantity, unit = parse_size(size_str)
        grammage_quantity = quantity
        grammage_unit = unit
        package_size = f"{quantity} {unit}" if quantity and unit else None
        prices = pick_context(sku["contextPrices"], "CURBSIDE")
        list_price = prices.get("listPrice", {}).get("amount")
        sale_price = prices.get("salePrice", {}).get("amount")
        regular_price = list_price
        selling_price = sale_price
        if list_price and sale_price and list_price != sale_price:
            price_was = list_price

        # Price per unit
        unit_sale = prices.get("unitSalePrice", {})
        if unit_sale:
            price_per_unit = f"{unit_sale.get('amount')} per {unit_sale.get('unit')}"

        coupons = product.get("coupons", [])
        if coupons:
            coupon = coupons[0]
            promotion_description = coupon.get("description",'')
            promotion_type = coupon.get("shortDescription",'')
            promotion_valid_upto = coupon.get("expirationDate",'')

            short_description = coupon.get("shortDescription", "")
            percent_match = re.search(r'(\d+)%', short_description)
            if percent_match:
                percentage_discount = int(percent_match.group(1))

        breadcrumb                      = product.get("breadcrumbs", [])
        producthierarchy_level1     = breadcrumb[0]["title"] if len(breadcrumb) >= 1 else ""
        producthierarchy_level2     = breadcrumb[1]["title"] if len(breadcrumb) >= 2 else ""
        producthierarchy_level3     = breadcrumb[2]["title"] if len(breadcrumb) >= 3 else ""
        producthierarchy_level4     = breadcrumb[3]["title"] if len(breadcrumb) >= 4 else ""
        producthierarchy_level5     = breadcrumb[4]["title"] if len(breadcrumb) >= 5 else ""
        producthierarchy_level6     = breadcrumb[5]["title"] if len(breadcrumb) >= 6 else ""
        producthierarchy_level7     = breadcrumb[6]["title"] if len(breadcrumb) >= 7 else ""

        # ensure last level is the product name (but only up to 7)
        last_level_index            = len(breadcrumb) + 1
        if last_level_index <= 7:
            locals()[f"producthierarchy_level{last_level_index}"] = product_name

        # build the flat breadcrumbs string, including product_name at the end
        breadcrumbs = " > ".join([breadcrumb.get("title", "") for breadcrumb in breadcrumb] + [product_name])


        images          = product.get("carouselImageUrls", [])
        file_name_1   = f"{product_id}_1.PNG"       if len(images) >= 1 else ""
        file_name_2   = f"{product_id}_2.PNG"       if len(images) >= 2 else ""
        file_name_3   = f"{product_id}_3.PNG"       if len(images) >= 3 else ""
        image_url_1   = images[0]              if len(images) >= 1 else ""
        image_url_2   = images[1]              if len(images) >= 2 else ""
        image_url_3   = images[2]              if len(images) >= 3 else ""

        preparationInstructions     = product.get("preparationInstructions",'')
        Warning                     = product.get("safetyWarning",'')

        # Nutrition Labels (first label)
        nutritionLabels             = product.get("nutritionLabels", [])
        if nutritionLabels:
            nutrition                 = nutritionLabels[0]
            servings_per_pack         = nutrition.get("servingsPerContainer",'')
            nutrients                 = nutrition.get("nutrients", [])
            vitamins                  = nutrition.get("vitaminsAndMinerals", [])
            combined_nutrition        = nutrients + vitamins
            nutritional_information   = ",".join(
                f"{n['title']}:{n.get('unit','')}:{n.get('percentage','')}"
                for n in combined_nutrition if 'title' in n
            )
            vitamins_list             = [v["title"] for v in vitamins]
        else:
            servings_per_pack         = None
            nutritional_information   = ""
            vitamins_list             = []



        item = {}
        item['unique_id']             = unique_id
        item['competitor_name']       = 'heb'
        item['product_name']          = product_name
        item['brand']                 = brand
        item['pdp_url']               = pdp_url
        item['currency']              = 'EUR'
        item['regular_price']         = regular_price
        item['selling_price']         = selling_price
        item['promotion_price']       = promotion_price
        item['price_was']             = price_was
        item['percentage_discount']   = percentage_discount
        item['product_description']   = product_description
        item['grammage_quantity']     = grammage_quantity
        item['grammage_unit']         = grammage_unit
        item['price_per_unit']        = price_per_unit
        item['image_urls']            = ','.join(image_urls)
        item['storage_instructions']  = ','.join(storage_instructions)
        item['reviews']               = reviews
        item['rating']                = rating
        item['product_code']          = product_code
        item['variants']              = ','.join(variant)
        item['manufacturer']          = manufacturer
        item['net_content']           = net_content
        item['dosage_recommendation'] = dosage_recommendation
        item['ingredients']           = ','.join(ingredients)

    def parse_size(size_str):
        """
        Extract quantity and unit from size strings like:
        "Avg. 2.0 lbs", "About 1/2 gal", "8 ct", "2.25 oz"
        """
        match = re.search(r'(\d+(?:\.\d+)?|\d+/\d+)\s*([a-zA-Z]+)', size_str)
        if not match:
            return None, None

        qty_raw, unit = match.groups()
        try:
            qty = float(Fraction(qty_raw))  # handles both "2.0" and "1/2"
        except ValueError:
            qty = None

        return qty, unit.strip()

    def pick_context(ctx_list, context="CURBSIDE"):
        for c in ctx_list:
            if c.get("context") == context:
                return c
        return {}
        


        

