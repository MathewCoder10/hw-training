import logging
import requests
import re
import json
from parsel import Selector
from mongoengine import connect, disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCrawlerFailedItem, ProductCrawlerItem
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CATEGORY,
    HEADERS,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Crawler:
    """Crawling categories and paginated products with dynamic tokens"""

    def __init__(self):
        # Connect to MongoDB
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.category_collection = self.database[MONGO_COLLECTION_CATEGORY]

        # Fetch dynamic tokens
        try:
            self.api_key, self.visitor_id = self.fetch_target_tokens()
            logging.info("Fetched api_key and visitor_id")
        except Exception:
            logging.exception("Failed to fetch api_key or visitor_id")
            raise

        # Fetch fulfillment location
        self.location_id = self.location_fetch()

    def fetch_target_tokens(self):
        """
        Fetch apiKey and visitor_id dynamically from Target's homepage.
        """
        # Start with existing HEADERS (to include any custom User-Agent)
        headers = HEADERS.copy()
        # Ensure we accept HTML
        headers.setdefault('accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')

        resp = requests.get('https://www.target.com/', headers=headers)
        resp.raise_for_status()
        html = resp.text

        sel = Selector(text=html)

        # Extract apiKey from inline __CONFIG__ blob
        api_key = None
        cfg_blob = sel.xpath('//script[contains(text(), "__CONFIG__")]/text()').get() or ''
        m_key = re.search(r'\\"apiKey\\":\\"([0-9a-f]{40})\\"', cfg_blob)
        if m_key:
            api_key = m_key.group(1)

        # Extract visitor_id from Next.js payload or fallback
        visitor_id = None
        next_data = sel.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if next_data:
            try:
                payload = json.loads(next_data)
                def find_key(obj, target):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k.lower() == target.lower():
                                return v
                            found = find_key(v, target)
                            if found is not None:
                                return found
                    elif isinstance(obj, list):
                        for item in obj:
                            found = find_key(item, target)
                            if found is not None:
                                return found
                    return None
                visitor_id = find_key(payload, 'visitor_id')
            except json.JSONDecodeError:
                pass

        if not visitor_id:
            m_vid = re.search(r'visitor_id\\"\:\\"([^\"]+)\\"', html)
            if m_vid:
                visitor_id = m_vid.group(1)

        if not api_key:
            raise RuntimeError("Unable to locate apiKey in the page")
        if not visitor_id:
            raise RuntimeError("Unable to locate visitor_id in the page")

        return api_key, visitor_id

    def location_fetch(self):
        params = {
            'key': self.api_key,
            'zipcode': '75217',
        }
        url = 'https://api.target.com/location_fulfillment_aggregations/v1/preferred_stores'
        response = requests.get(url, params=params, headers=HEADERS)
        data = response.json()

        if data.get("preferred_stores"):
            loc_id = data["preferred_stores"][0].get("location_id")
            logging.info(f"First location_id: {loc_id}")
            return loc_id
        else:
            logging.warning("No preferred stores found.")
            return None

    def start(self):
        if not self.location_id:
            logging.error("No location_id. Exiting.")
            return

        for category in self.category_collection.find({}):
            category_id = category.get('category_id')
            offset = 0

            while True:
                params = {
                    'key': self.api_key,
                    'category': category_id,
                    'channel': 'WEB',
                    'count': '24',
                    'offset': f'{offset}',
                    'page': f'/c/{category_id}',
                    'platform': 'desktop',
                    'pricing_store_id': self.location_id,
                    'visitor_id': self.visitor_id,
                }
                paged_url = 'https://redsky.target.com/redsky_aggregations/v1/web/plp_search_v2'
                response = requests.get(paged_url, params=params, headers=HEADERS)

                if response.status_code == 200:
                    has_products = self.parse_items(response, category_id)
                    if has_products:
                        offset += 24
                    else:
                        break

                else:
                        failed_item = {}
                        failed_item ['category_id'] = category_id
                        failed_item ['issue'] = f"{response.status_code}"
                        try:
                            product_item = ProductCrawlerFailedItem(**failed_item)
                            product_item.save()
                            logging.info(f"Failed statuscode")
                        except Exception as e:
                            logging.exception(f"Failed to insert")
                        break
                

    def parse_items(self, response, category_id):
        """Parse HTML response, insert items or log failures."""
        data = response.json()
        if not data:
            print("data error")
            exit()
        products = data["data"]["search"]["products"]
        if not products:
            failed_item = {}
            failed_item ['category_id'] = category_id
            failed_item ['issue'] = 'No products'
            try:
                product_item = ProductCrawlerFailedItem(**failed_item)
                product_item.save()
                logging.info(f"No products found")
            except Exception as e:
                logging.exception(f"Failed to insert")
            return False
        # EXTRACT
        for product in products:
            # safely drill into the nested dicts
            item_info = product.get('item', {})
            enrichment = item_info.get('enrichment', {})

            # build your item dict
            item = {}

            buy_url = enrichment.get('buy_url', '') 

            # ITEM YEILD
            item['buy_url'] = buy_url

            logging.info(item)
            try:
                product_item = ProductCrawlerItem(**item)
                product_item.save()
 
            except NotUniqueError:
                logging.warning(f"Duplicate unique_id found")

        return True

    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")


if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
    crawler.close()