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
        product_name = selector.xpath(NAME_XPATH).extract_first()
        product_description = selector.xpath(DESCRIPTION_XPATH).extract_first()
        reviews = selector.xpath(REVIEWS_XPATH).extract_first()
        rating = selector.xpath(RATING_XPATH).extract_first()
        variant = selector.xpath(VARIANT_XPATH).extract()
        image_urls = selector.xpath(IMAGE_URLS).extract()
        quantity = selector.xpath(QUANTITY_XPATH).extract_first()
        price_per_unit = selector.xpath(PER_UNIT_PRICE_XPATH).extract_first()
        price = selector.xpath(PRICE_XPATH).extract_first()
        price_was = selector.xpath(PRICE_WAS_XPATH).extract_first()
        percentage_discount= selector.xpath(PERCENTAGE_DISCOUNT_XPATH).extract_first()
        product_code = selector.xpath(PRODUCT_CODE_XPATH).extract_first()
        manufacturer = selector.xpath(MANUFACTURER_XPATH).extract_first()
        brand = selector.xpath(BRAND_XPATH).extract_first()
        net_content = selector.xpath(NET_CONTENT_XPATH).extract_first()
        dosage_recommendation = selector.xpath(DOSAGE_RECOMMENDATION_XPATH).extract_first()
        storage_instructions = selector.xpath(STORAGE_INSTRUCTIONS_XPATH).extract()
        ingredients = selector.xpath(INGREDIENTS_XPATH).extract()

        # CLEAN
        # CLEAN price
        if not price:
            price = 0.0
        else:
            cleaned = re.sub(r'[^\d,\.]', '', price).replace(',', '.')
            try:
                price = float(cleaned)
            except ValueError:
                logging.warning(f"Could not convert price: {price}")
                price = 0.0
        # CLEAN price was
        if not price_was:
            price_was = 0.0
        else:
            cleaned = re.sub(r'[^\d,\.]', '', price).replace(',', '.')
            try:
                price_was = float(cleaned)
            except ValueError:
                logging.warning(f"Could not convert price: {price}")
                price_was = 0.0

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
