import logging
import re
import requests
from datetime import datetime
from pymongo import MongoClient, errors
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CRAWLER,
    MONGO_COLLECTION_PARSER,
    MONGO_COLLECTION_URL_FAILED,
    HEADERS,
    DETAILS_API_URL,
    PROMOTION_API_URL,
    MODULE_VERSION_URL,
    BASE_URL,
)

# Configure logger
logger = logging.getLogger(__name__)

class Parser:
    """Parser for product details, mirroring the structure of the Crawler."""

    def __init__(self):
        # MongoDB setup
        self.mongo_client = MongoClient(MONGO_URI)
        self.database = self.mongo_client[MONGO_DB]
        self.crawler_collection = self.database[MONGO_COLLECTION_CRAWLER]
        self.parser_collection = self.database[MONGO_COLLECTION_PARSER]
        self.failed_urls_collection = self.database[MONGO_COLLECTION_URL_FAILED]

        # Ensure uniqueness on unique_id
        self.parser_collection.create_index("unique_id", unique=True)

        # Headers for HTTP requests
        self.request_headers = HEADERS.copy()
        self.request_headers['referer'] = BASE_URL

        # API endpoints
        self.detail_api_url = DETAILS_API_URL
        self.module_version_url = MODULE_VERSION_URL
        self.promotion_api_url = PROMOTION_API_URL

        # Fetch dynamic module version
        try:
            self.module_version = self.fetch_module_version()
        except Exception:
            logger.error("Could not initialize parser due to module version fetch failure.")
            raise

    def fetch_module_version(self):
        """Fetch module version token for detail API."""
        response = requests.get(self.module_version_url, headers=self.request_headers)
        response.raise_for_status()
        data = response.json()
        token = data.get('versionToken')
        if not token:
            raise ValueError("versionToken missing in module version response")
        logger.info(f"Fetched module version for parser: {token}")
        return token

    def build_payload(self, sku):
        """Construct JSON payload for detail API request."""
        return {
            'versionInfo': {
                'moduleVersion': self.module_version,
                'apiVersion': 'j2jjJJxS4heD58kEZAYPUQ',
            },
            'viewName': 'MainFlow.ProductDetailsPage',
            'screenData': {
                'variables': {
                    'Locale': 'nl-NL',
                    'SKU': sku,
                }
            }
        }

    def start(self):
        """Start parsing process: fetch details, promotions, and store results."""
        logger.info("Parser started.")
        for crawler_doc in self.crawler_collection.find({}):
            sku = crawler_doc.get('unique_id')
            pdp_url = crawler_doc.get('pdp_url')
            payload = self.build_payload(sku)

            # Detail API request
            try:
                detail_response = requests.post(
                    self.detail_api_url,
                    headers=self.request_headers,
                    json=payload
                )
                detail_response.raise_for_status()
            except requests.RequestException as err:
                logger.exception(f"Detail request failed for SKU {sku}: {err}")
                self.record_failed_url(pdp_url, status_code=str(err))
                continue

            try:
                detail_data = detail_response.json()
            except ValueError:
                logger.error(f"Invalid JSON for SKU {sku}")
                continue

            product_section = self.extract_product(detail_data)
            if not product_section:
                logger.warning(f"No product details for SKU {sku}")
                continue

            # Promotion description
            promotion_description = ''
            if crawler_doc.get('promotion_type'):
                try:
                    promo_response = requests.post(
                        self.promotion_api_url,
                        headers=self.request_headers,
                        json=payload
                    )
                    promo_response.raise_for_status()
                    promo_data = promo_response.json().get('data', {}).get('Offer', {})
                    promotion_description = self.build_description(promo_data)
                except Exception as promo_error:
                    logger.exception(f"Promotion request failed for SKU {sku}: {promo_error}")

            # Parse and store item including promotion_description
            parsed_item = self.parse_items(product_section, crawler_doc, promotion_description)

            try:
                self.parser_collection.insert_one(parsed_item)
                logger.info(f"Parsed and stored item for SKU {sku}")
            except errors.DuplicateKeyError:
                logger.warning(f"Duplicate SKU {sku}, skipping.")

    def record_failed_url(self, url, status_code=''):
        """Insert a failed URL record into the failed URLs collection."""
        record = {'pdp_url': url}
        if status_code != '':
            record['status_code'] = status_code
        try:
            self.failed_urls_collection.insert_one(record)
        except Exception:
            logger.exception("Failed to record failed URL.")

    def extract_product(self, response_json):
        """Extract the ProductOut section from the API response."""
        data_section = response_json.get('data')
        return data_section.get('ProductOut') if data_section else None

    def parse_items(self, product, crawler_doc, promotion_description=''):
        """Parse fields from product details, merge with crawler doc, and include promotion_description."""

        overview = product.get('Overview', {})
        instructions = product.get('InstructionsAndSuggestions', {}).get('Instructions', {})
        suggestions = product.get('InstructionsAndSuggestions', {}).get('Suggestions', {})
        ingredients_text = product.get('Ingredients', '').strip()
        logos = product.get('Logos', {}).get('PDPInProductInformation', {}).get('List', [])
        nutrients = product.get('Nutrient', {}).get('Nutrients', {}).get('List', [])
        allergens_text = product.get('Allergen', {}).get('Description', '').strip()
        subtitle = overview.get('Subtitle', '')
        grammage_match = re.search(r"Per\s+\w+\s+(\d+)\s+(\w+)", subtitle)
        grammage_quantity = grammage_match.group(1) if grammage_match else ''
        grammage_unit = grammage_match.group(2) if grammage_match else ''
        price_match = re.search(r"\((.*?)\)", subtitle)
        price_per_unit = price_match.group(1) if price_match else ''
        description_text = overview.get('Meta', {}).get('Description', '')
        preparation_instructions = instructions.get('Preparation', '').strip()
        usage_instructions = instructions.get('Usage', '').strip()
        combined_instructions = f"{preparation_instructions} {usage_instructions}".strip()
        storage_instructions = instructions.get('Storage', '').strip()
        servings_per_pack = suggestions.get('Serving', '').strip()
        nutritional_score = ''
        organic_type = 'Non-Organic'
        for logo in logos:
            logo_name = logo.get('Name', '')
            if logo_name.startswith('Nutri-Score'):
                score = logo_name.replace('Nutri-Score', '').strip().upper()
                if score in list('ABCDE'):
                    nutritional_score = score
                break
            if 'Biologisch' in logo.get('LongDescription', ''):
                organic_type = 'Organic'
                break
        fat_percentage = ''
        for nutrient in nutrients:
            if (
                nutrient.get('ParentCode') == 'FAT'
                and 'meervoudig onverzadigd' in nutrient.get('Description', '').lower()
            ):
                fat_percentage = nutrient.get('QuantityContained', {}).get('Value')
                break

        item = {}
        item['unique_id']             = crawler_doc.get('unique_id')
        item['competitor_name']       = crawler_doc.get('competitor_name')
        item['product_name']          = crawler_doc.get('product_name')
        item['brand']                 = crawler_doc.get('brand')
        item['pdp_url']               = crawler_doc.get('pdp_url')
        item['producthierarchy_level1']= crawler_doc.get('producthierarchy_level1')
        item['producthierarchy_level2']= crawler_doc.get('producthierarchy_level2')
        item['producthierarchy_level3']= crawler_doc.get('producthierarchy_level3')
        item['producthierarchy_level4']= crawler_doc.get('producthierarchy_level4')
        item['producthierarchy_level5']= crawler_doc.get('producthierarchy_level5')
        item['breadcrumb']            = crawler_doc.get('breadcrumb')
        item['regular_price']         = crawler_doc.get('regular_price')
        item['selling_price']         = crawler_doc.get('selling_price')
        item['promotion_price']       = crawler_doc.get('promotion_price')
        item['promotion_valid_from']  = crawler_doc.get('promotion_valid_from')
        item['promotion_valid_upto']  = crawler_doc.get('promotion_valid_upto')
        item['promotion_type']        = crawler_doc.get('promotion_type')   
        item['promotion_description'] = promotion_description
        item['product_description']   = description_text
        item['grammage_quantity']     = grammage_quantity
        item['grammage_unit']         = grammage_unit
        item['price_per_unit']        = price_per_unit
        item['instructions']          = combined_instructions
        item['storage_instructions']  = storage_instructions
        item['servings_per_pack']     = servings_per_pack
        item['ingredients']           = ingredients_text
        item['nutritional_score']     = nutritional_score
        item['organictype']           = organic_type
        item['allergens']             = allergens_text
        item['fat_percentage']        = fat_percentage

        return item

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
        self.mongo_client.close()
        logger.info("MongoDB connection closed.")

if __name__ == '__main__':
    parser = Parser()
    parser.start()
    parser.close()
