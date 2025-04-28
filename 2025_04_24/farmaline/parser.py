import logging
import re
import requests
from parsel import Selector
from items import ProductParserFailedItem, ProductParserItem
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

    def start(self):
        logging.info("Parser started.")
        for crawler in self.crawler_collection.find({}):
            pdp_url = crawler.get('pdp_url')
            unique_id =  crawler.get('unique_id')
            response = requests.get(pdp_url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                self.parse_items(response, pdp_url, unique_id)
            else:
                failed_item = {}
                failed_item ['pdp_url'] = pdp_url
                failed_item ['unique_id'] = unique_id

                try:
                    product_item = ProductParserFailedItem(**failed_item)
                    product_item.save()
                    logging.info(f"Failed statuscode")
                except Exception as e:
                    logging.exception(f"Failed to insert")


    def parse_items(self, response, pdp_url, unique_id):
        selector = Selector(response.text)

        def clean_price(value):
            if not value:
                return 0.0
            cleaned = re.sub(r'[^\d,\.]', '', value).replace(',', '.')
            try:
                return float(cleaned)
            except ValueError:
                logging.warning(f"Could not convert price: {value}")
                return 0.0

        # XPATH
        NAME_XPATH = '//h1[@data-qa-id="product-title"]/text()'
        DESCRIPTION_XPATH = '//div[@data-qa-id="product-description"]//div[contains(@class,"prose") and contains(@class,"text-s")][1]/text()'
        REVIEWS_XPATH = '//a[@data-qa-id="number-of-ratings-text"]/text()'
        RATING_XPATH = '//div[@data-qa-id="product-ratings-container"]//span[contains(@class,"text-4xl")]/text()'
        VARIANT_XPATH = '//ul/li[@data-qa-id="product-variants"]//div[contains(@class,"font-bold") and contains(@class,"text-l")]/text()'
        IMAGE_URLS = '//button[starts-with(@data-qa-id, "product-image")]/picture/img/@src'
        QUANTITY_XPATH = '//div[@data-qa-id="product-attribute-package_size"]/span/text()|//div[@class="whitespace-nowrap font-bold text-dark-primary-max text-l font-medium"]/text()'
        PER_UNIT_PRICE_XPATH = '//div[@class="text-xs text-dark-primary-medium"]/text()'
        PRICE_XPATH = '//div[@data-qa-id="product-page-variant-details__display-price"]/text()|span[@data-qa-id="displayAmountField"]'
        PRICE_WAS_XPATH = '//span[@data-qa-id="product-old-price"]/text()'
        PERCENTAGE_DISCOUNT_XPATH = '//div[@data-qa-id="product-page-variant-details__display-price"]/following-sibling::div/text()'
        PRODUCT_CODE_XPATH = '//dt[normalize-space()="Productcode"]/following-sibling::dd//span/text()'
        MANUFACTURER_XPATH = '//dt[normalize-space()="Fabrikant"]/following-sibling::dd/text()'
        BRAND_XPATH = '//dl[dt[normalize-space(.)="Merk"]]/dd/a/text()'
        NET_CONTENT_XPATH = '//h5[contains(normalize-space(.), "Nettohoeveelheid")]/following-sibling::div/text()'
        DOSAGE_RECOMMENDATION_XPATH = '//h5[contains(normalize-space(.),"Aanbevolen dagelijkse dosis")]/following-sibling::div/text()'
        STORAGE_INSTRUCTIONS_XPATH = '//h5[contains(normalize-space(.),"Bewaar- en gebruiksadvies")]/following-sibling::div[1]/ul/li'
        INGREDIENTS_XPATH = '//h5[contains(normalize-space(.),"Ingrediënten")]/following-sibling::div[1]/p/text()'

        # EXTRACT
        product_name = selector.xpath(NAME_XPATH).get()
        product_description = selector.xpath(DESCRIPTION_XPATH).get()
        reviews = selector.xpath(REVIEWS_XPATH).get()
        rating = selector.xpath(RATING_XPATH).get()
        variant = selector.xpath(VARIANT_XPATH).getall()
        image_urls = selector.xpath(IMAGE_URLS).getall()
        quantity = selector.xpath(QUANTITY_XPATH).get()
        price_per_unit = selector.xpath(PER_UNIT_PRICE_XPATH).get()
        price = selector.xpath(PRICE_XPATH).get()
        price_was = selector.xpath(PRICE_WAS_XPATH).get()
        percentage_discount= selector.xpath(PERCENTAGE_DISCOUNT_XPATH).get()
        product_code = selector.xpath(PRODUCT_CODE_XPATH).get()
        manufacturer = selector.xpath(MANUFACTURER_XPATH).get()
        brand = selector.xpath(BRAND_XPATH).get()
        net_content = selector.xpath(NET_CONTENT_XPATH).get()
        dosage_recommendation = selector.xpath(DOSAGE_RECOMMENDATION_XPATH).get()
        storage_instructions = selector.xpath(STORAGE_INSTRUCTIONS_XPATH).getall()
        ingredients = selector.xpath(INGREDIENTS_XPATH).getall()

        # CLEAN
        price = clean_price(price)
        price_was = clean_price(price_was)

        if price_was:
            regular_price = price_was
            selling_price = price
            promotion_price = price
        else:
            regular_price = price
            selling_price = price
            promotion_price = 0.0

        quantity = quantity.strip() if quantity else ""  
        match = re.match(r"(.+?)\s*([A-Za-z]+)$", quantity)
        if match:
            grammage_quantity, grammage_unit = match.groups()
        else:
            grammage_quantity, grammage_unit = "", ""
        

        # Assemble final item
        item = {}
        item['unique_id']             = unique_id
        item['competitor_name']       = 'farmaline'
        item['product_name']          = product_name
        item['brand']                 = brand
        item['pdp_url']               = pdp_url
        item['currency']              = '€'
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
