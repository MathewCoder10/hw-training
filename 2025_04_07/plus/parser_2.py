import re
import copy
import requests
from datetime import datetime
from pymongo import MongoClient

class Parser_2:
    def __init__(self, db_uri="mongodb://localhost:27017", db_name="plus_nl"):
        # MongoDB connection
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.crawler_collection = self.db["crawler"]
        self.parser_collection = self.db["parser"]

        # Fetch CSRF token and setup headers
        self.csrf_token = self._fetch_csrf_token()
        self.headers = {
            'accept': 'application/json',
            'content-type': 'application/json; charset=UTF-8',
            'origin': 'https://www.plus.nl',
            'referer': 'https://www.plus.nl/',
            'user-agent': 'Mozilla/5.0',
            'x-csrftoken': self.csrf_token,
        }

        # Minimal JSON payload template
        self.base_payload = {
            'versionInfo': {
                'moduleVersion': 'gA_kBEFQeNXDsqTGV6FWFQ',
                'apiVersion': 'pRmIEBbjlwOG2dJVuRaytA',
            },
            'viewName': 'MainFlow.ProductDetailsPage',
            'screenData': {
                'variables': {
                    'Locale': 'nl-NL',
                    'SKU': '',
                }
            }
        }

    def _fetch_csrf_token(self):
        js_url = 'https://www.plus.nl/scripts/OutSystems.js?H4bR29NkZ15NFYcdxJmseg'
        r = requests.get(js_url, headers={'accept': '*/*', 'referer': 'https://www.plus.nl/', 'user-agent': 'Mozilla/5.0'})
        r.raise_for_status()
        match = re.search(r'e\.AnonymousCSRFToken\s*=\s*"([^"]+)"', r.text)
        if not match:
            raise RuntimeError("Unable to extract CSRF token")
        return match.group(1)

    def start(self):
        self.parse_items()
        self.close()

    def parse_items(self):
        for doc in self.crawler_collection.find({}, {"unique_id": 1}):
            sku = doc.get("unique_id")
            if not sku:
                continue
            print(f"Processing SKU: {sku}")

            payload = copy.deepcopy(self.base_payload)
            payload['screenData']['variables']['SKU'] = sku

            try:
                resp = requests.post(
                    'https://www.plus.nl/screenservices/ECP_Product_CW/ProductDetails/PDPContent/DataActionGetPromotionOffer',
                    headers=self.headers,
                    json=payload
                )
                resp.raise_for_status()
                data = resp.json().get('data', {})
                offer = data.get('Offer', {})
            except Exception as e:
                print(f"Request error for SKU {sku}: {e}")
                continue

            desc = self._build_description(offer)
            print(desc)
            try:
                self.parser_collection.insert_one({"unique_id": sku, "promotion_description": desc})
            except Exception as e:
                print(f"Insert error for SKU {sku}: {e}")

    def build_description(self, offer):
        title = offer.get('Title', '')
        variant = offer.get('Variant', '')
        package = offer.get('Package', '')
        sd = offer.get('StartDate', '')
        ed = offer.get('EndDate', '')
        start = self.format_date(sd) if sd else 'onbekend'
        end = self.format_date(ed) if ed else 'onbekend'
        return f"{title} {variant} {package} Geldig van {start} tot en met {end}"

    def format_date(self, date_str):
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return date_str

        weekdays = ['maandag','dinsdag','woensdag','donderdag','vrijdag','zaterdag','zondag']
        months = ['januari','februari','maart','april','mei','juni',
                  'juli','augustus','september','oktober','november','december']
        return f"{weekdays[dt.weekday()]} {dt.day} {months[dt.month-1]}"

    def close(self):
        self.client.close()
        print("MongoDB connection closed.")

if __name__ == '__main__':
    Parser_2().start()
