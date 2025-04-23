import logging
import re
import requests
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
)

# Configure logger
logger = logging.getLogger(__name__)

class Parser:
    def __init__(self):
        # MongoDB setup
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.crawler_collection = self.database[MONGO_COLLECTION_CRAWLER]
        self.parser_collection = self.database[MONGO_COLLECTION_PARSER]
        self.failed_urls_collection = self.database[MONGO_COLLECTION_PARSER_URL_FAILED]
        # API endpoints
        self.detail_api_url = ('https://www.plus.nl/screenservices/ECP_Product_CW/ProductDetails/PDPContent/DataActionGetProductDetailsAndAgeInfo')
        self.module_version_url = ('https://www.plus.nl/moduleservices/moduleversioninfo?1745203216246')
        self.promotion_api_url = ('https://www.plus.nl/screenservices/ECP_Product_CW/ProductDetails/PDPContent/DataActionGetPromotionOffer')

        # Fetch dynamic module version
        try:
            self.module_version = self.fetch_module_version()
        except Exception:
            logger.error(
                "Could not initialize parser due to module version fetch failure."
            )
            raise

    def fetch_module_version(self):
        """Fetch module version token for detail API."""
        response = requests.get(self.module_version_url, headers=HEADERS)
        data = response.json()
        token = data.get('versionToken')
        if not token:
            raise ValueError("versionToken missing in module version response")
        logger.info(f"Fetched module version for parser: {token}")
        return token

    def start(self):
        logger.info("Parser started.")
        for crawler_doc in self.crawler_collection.find({}):
            sku = crawler_doc.get('unique_id')
            product_name = crawler_doc.get('product_name')

            # Build detail payload
            payload = {
                'versionInfo': {
                    'moduleVersion': self.module_version,
                    'apiVersion': 'j2jjJJxS4heD58kEZAYPUQ',
                },
                'viewName': 'MainFlow.ProductDetailsPage',
                'screenData': {
                    'variables': {
                        'Product': {},
                        'SKU': sku,
                        'ProductName': product_name
                    }
                }
            }

            detail_response = requests.post(self.detail_api_url, headers=HEADERS, json=payload)
            if detail_response.status_code == 200:
                self.parse_items(detail_response, sku, product_name)
            else:
                failed_item = {}
                failed_item ['unique_id'] = sku
                failed_item ['product_name'] = product_name
                failed_item ['issue'] = detail_response.status_code
                try:
                    ProductParserFailedItem(**failed_item).save()
                    logger.info(f"Logged failed URL for SKU {sku}")
                except Exception:
                    logger.exception("Failed to log URL failure")

    def promotion(self, sku, product_name, regular_price):
        """Fetch promotion info"""
        payload = {
            'versionInfo': {
                'moduleVersion': self.module_version,
                'apiVersion': 'j2jjJJxS4heD58kEZAYPUQ',
            },
            'viewName': 'MainFlow.ProductDetailsPage',
            'screenData': {
                'variables': {
                    'Product': {},
                    'SKU': sku,
                    'ProductName': product_name
                }
            }
        }

        promotion_response = requests.post(self.promotion_api_url, headers=HEADERS, json=payload)
        if promotion_response.status_code != 200:
            failed_item = {}
            failed_item ['unique_id'] = sku
            failed_item ['product_name'] = product_name
            failed_item ['issue'] = promotion_response.status_code
            try:
                ProductParserFailedItem(**failed_item).save()
                logger.info(f"Logged promotion failure for SKU {sku}")
            except Exception:
                logger.exception("Failed to log promotion failure")
            # Return defaults
            return {
                'selling_price': regular_price,
                'promotion_price': '',
                'promotion_valid_from': '',
                'promotion_valid_upto': '',
                'promotion_type': '',
                'promotion_description': ''
            }

        offer = promotion_response.json().get('data', {}).get('Offer', {})
        promotion_type = offer.get('DisplayInfo_Label', '')
        if promotion_type:
            start_date = offer.get('StartDate', '')
            end_date = offer.get('EndDate', '')
            newprice = float(offer.get('NewPrice', 0))

            if newprice == 0:
                selling_price = float(offer.get('PriceOriginal_Product', 0))
                promotion_price = 0
                promotion_description = ''
            else:
                selling_price = newprice
                promotion_price = newprice
                promotion_description = self.build_description(offer)

            return {
                'selling_price': selling_price,
                'promotion_price': promotion_price,
                'promotion_valid_from': start_date,
                'promotion_valid_upto': end_date,
                'promotion_type': promotion_type,
                'promotion_description': promotion_description
            }
        else:
            return {
                'selling_price': regular_price,
                'promotion_price': 0,
                'promotion_valid_from': '',
                'promotion_valid_upto': '',
                'promotion_type': '',
                'promotion_description': ''
            }



    def parse_items(self, response, sku , product_name):

        data = response.json().get('data', {})
        product = data.get('ProductOut', {})
        overview = response.json().get('data', {}).get('ProductOut', {}).get('Overview', {})
        brand = overview.get('Brand', '')
        slug = overview.get('Slug', '')
        pdp_url = f"https://www.plus.nl/product/{slug}" if slug else ''
        regular_price = float(overview.get('Price', 0))  
        unique_id = sku
        product_name = product_name
        promotion = self.promotion(unique_id, product_name, regular_price) #doubt
        category_level = overview.get('Categories', {}).get('List', [])
        levels = ['Home', 'Producten'] + [lvl.get('Name', '') for lvl in category_level[:3]]
        breadcrumb = ' > '.join([lvl for lvl in levels if lvl])
        instructions = product.get('InstructionsAndSuggestions', {})
        preparation_instructions = instructions.get('Instructions', {}).get('Preparation', '').strip()
        usage_instructions = instructions.get('Instructions', {}).get('Usage', '').strip()
        combined_instructions = f"{preparation_instructions} {usage_instructions}".strip()
        storage_instructions = instructions.get('Instructions', {}).get('Storage', '').strip()
        servings_per_pack = instructions.get('Suggestions', {}).get('Serving', '').strip()
        ingredients = product.get('Ingredients', '').strip()
        allergens = product.get('Allergen', {}).get('Description', '').strip()
        subtitle = overview.get('Subtitle', '')
        grammage_match = re.search(r"Per\s+\w+\s+(\d+)\s+(\w+)", subtitle)
        grammage_quantity = grammage_match.group(1) if grammage_match else ''
        grammage_unit = grammage_match.group(2) if grammage_match else ''
        price_match = re.search(r"\((.*?)\)", subtitle)
        price_per_unit = price_match.group(1) if price_match else ''
        product_description = overview.get('Meta', {}).get('Description', '')
        logos = product.get('Logos', {}).get('PDPInProductInformation', {}).get('List', [])
        nutritional_score = ''
        organic_type = 'Non-Organic'
        for logo in logos:
            name = logo.get('Name', '')
            long_description = logo.get('LongDescription', '')
            if name.startswith('Nutri-Score'):
                score = name.replace('Nutri-Score', '').strip().upper()
                if score in list('ABCDE'):
                    nutritional_score = score
                    break
            if 'Biologisch' in long_description:
                organic_type = 'Organic'
                break
        nutrients = product.get('Nutrient', {}).get('Nutrients', {}).get('List', [])
        fat_percentage = ''
        for n in nutrients:
            if n.get('ParentCode') == 'FAT' and 'meervoudig onverzadigd' in n.get('Description', '').lower():
                fat_percentage = n.get('QuantityContained', {}).get('Value', '')
                break

        # Assemble final item
        item = {}
        item['unique_id']             = unique_id
        item['competitor_name']       = 'plus'
        item['product_name']          = product_name
        item['brand']                 = brand
        item['pdp_url']               = pdp_url
        item['producthierarchy_level1']= levels[0]
        item['producthierarchy_level2']= levels[1]
        item['producthierarchy_level3']= levels[2] if len(levels)>2 else ''
        item['producthierarchy_level4']= levels[3] if len(levels)>3 else ''
        item['producthierarchy_level5']= levels[4] if len(levels)>4 else ''
        item['breadcrumb']            = breadcrumb
        item['currency']              = '€'
        item['regular_price']         = regular_price
        item['selling_price']         = promotion['selling_price']
        item['promotion_price']       = promotion['promotion_price']
        item['promotion_valid_from']  = promotion['promotion_valid_from']
        item['promotion_valid_upto']  = promotion['promotion_valid_upto']
        item['promotion_type']        = promotion['promotion_type']
        item['promotion_description'] = promotion['promotion_description']
        item['product_description']   = product_description
        item['grammage_quantity']     = grammage_quantity
        item['grammage_unit']         = grammage_unit
        item['price_per_unit']        = price_per_unit
        item['instructions']          = combined_instructions
        item['storage_instructions']  = storage_instructions
        item['servings_per_pack']     = servings_per_pack
        item['ingredients']           = ingredients
        item['nutritional_score']     = nutritional_score
        item['organictype']           = organic_type
        item['allergens']             = allergens
        item['fat_percentage']        = fat_percentage
        
        logging.info(item)
        try:
            product_item = ProductParserItem(**item)
            product_item.save()
        except NotUniqueError:
            logging.warning(f"Duplicate unique_id found")


    def build_description(self, offer):
        """Construct a human-readable promotion description."""
        title = offer.get('Title', '')
        variant = offer.get('Variant', '')
        package = offer.get('Package', '')
        start_date = offer.get('StartDate', '')
        end_date = offer.get('EndDate', '')
        formatted_start = self.format_date(start_date) if start_date else 'onbekend'
        formatted_end = self.format_date(end_date) if end_date else 'onbekend'
        return f"{title} {variant} {package} Geldig van {formatted_start} tot en met {formatted_end}".strip()

    def format_date(self, date_str):
        """Format ISO date to Dutch weekday + day + month."""
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return date_str

        weekdays = ['maandag','dinsdag','woensdag','donderdag','vrijdag','zaterdag','zondag']
        months = ['januari','februari','maart','april','mei','juni',
                  'juli','augustus','september','oktober','november','december']
        return f"{weekdays[dt.weekday()]} {dt.day} {months[dt.month-1]}"

    def close(self):
        disconnect()
        logger.info("MongoDB connection closed.")

if __name__ == '__main__':
    parser = Parser()
    parser.start()
    parser.close()
