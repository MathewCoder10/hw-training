import re
import copy
import requests
from datetime import datetime
from pymongo import MongoClient

class Parser_2:
    def __init__(self, db_uri="mongodb://localhost:27017", db_name="plus_nl"):
        """
        Initialize the parser:
          - Connect to MongoDB.
          - Setup collections.
          - Fetch CSRF token and prepare headers.
          - Initialize JSON payload template.
        """
        # MongoDB connection
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.crawler_collection = self.db["crawler"]
        self.parser_collection = self.db["parser"]

        # Fetch CSRF token and setup common headers
        self.csrf_token = self._fetch_csrf_token()
        self.headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
            'cache-control': 'no-cache',
            'content-type': 'application/json; charset=UTF-8',
            # 'cookie': '...',  # cookies omitted for brevity
            'origin': 'https://www.plus.nl',
            'outsystems-locale': 'nl-NL',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.plus.nl/product/biologisch-plus-jong-belegen-50-plakken-pak-180-g-760896',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'traceparent': '00-e68d72e49d81289f29f2c63060eb6c2b-93a551a6370e9537-01',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-csrftoken': self.csrf_token,
        }

        # Define the JSON payload template that will be used in API calls.
        self.json_template = {
            'versionInfo': {
                'moduleVersion': 'gA_kBEFQeNXDsqTGV6FWFQ',
                'apiVersion': 'pRmIEBbjlwOG2dJVuRaytA',
            },
            'viewName': 'MainFlow.ProductDetailsPage',
            'screenData': {
                'variables': {
                    'ShowMedicineSidebar': False,
                    'Product': {
                        'Overview': {
                            'Name': '',
                            'Subtitle': '',
                            'Brand': '',
                            'Slug': '',
                            'Image': {
                                'Label': '',
                                'URL': '',
                            },
                            'Meta': {
                                'Description': '',
                                'Title': '',
                            },
                            'IsNIX18': False,
                            'Price': '0',
                            'BaseUnitPrice': '',
                            'LineItem': {
                                'Id': '',
                                'Quantity': 0,
                            },
                            'IsOfflineSaleOnly': False,
                            'IsServiceItem': False,
                            'IsAvailableInStore': False,
                            'MaxOrderLimit': 0,
                        },
                    },
                    'ChannelId': '',
                    'Locale': 'nl-NL',
                    'StoreId': '0',
                    'StoreNumber': 0,
                    'CheckoutId': '44242557-d0ff-4aef-807b-05b5e1385195',
                    'OrderEditId': '',
                    'IsOrderEditMode': False,
                    'TotalLineItemQuantity': 0,
                    'ShoppingListProducts': {
                        'List': [],
                        'EmptyListItem': {
                            'SKU': '',
                            'Quantity': '0',
                        },
                    },
                    'HasDailyValueIntakePercent': False,
                    'CartPromotionDeliveryDate': '2025-04-08',
                    'LineItemQuantity': 0,
                    'IsPhone': False,
                    '_isPhoneInDataFetchStatus': 1,
                    'OneWelcomeUserId': '',
                    '_oneWelcomeUserIdInDataFetchStatus': 1,
                    'SKU': '',  # To be updated per unique_id
                    '_sKUInDataFetchStatus': 1,
                    'TotalCartItems': 0,
                    '_totalCartItemsInDataFetchStatus': 1,
                    '_productNameInDataFetchStatus': 1,
                },
            },
        }

    def _fetch_csrf_token(self):
        """
        Fetch the CSRF token dynamically from the OutSystems.js file.
        """
        js_url = 'https://www.plus.nl/scripts/OutSystems.js?H4bR29NkZ15NFYcdxJmseg'
        try:
            response = requests.get(js_url, headers={
                'accept': '*/*',
                'referer': 'https://www.plus.nl/',
                'user-agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                               '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
            })
            response.raise_for_status()
            # Extract the CSRF token using regex.
            match = re.search(r'e\.AnonymousCSRFToken\s*=\s*"([^"]+)"', response.text)
            if not match:
                raise RuntimeError("Unable to extract CSRF token from OutSystems.js")
            token = match.group(1)
            print(f"Fetched CSRF token: {token}")
            return token
        except Exception as e:
            raise RuntimeError(f"Error fetching CSRF token: {e}")

    def start(self):
        """
        Start the parsing process:
         - Process each document in the crawler collection.
         - After parsing, close the MongoDB connection.
        """
        self.parse_items()
        self.close()

    def parse_items(self):
        """
        Process each document from the crawler collection to:
         - Build the JSON payload for a given SKU.
         - Make a POST request to retrieve promotion details.
         - Parse the promotion description.
         - Store it in the parser collection.
        """
        # Query only unique_id field from documents in the crawler collection.
        documents = self.crawler_collection.find({}, {"unique_id": 1})
        for doc in documents:
            sku_value = doc.get("unique_id")
            if not sku_value:
                continue

            print(f"Processing SKU: {sku_value}")

            # Create a deep copy of the payload template.
            payload = copy.deepcopy(self.json_template)
            payload["screenData"]["variables"]["SKU"] = sku_value

            try:
                response = requests.post(
                    'https://www.plus.nl/screenservices/ECP_Product_CW/ProductDetails/PDPContent/DataActionGetPromotionOffer',
                    headers=self.headers,
                    json=payload,
                )
                response.raise_for_status()
            except Exception as e:
                print(f"Error in POST request for SKU {sku_value}: {e}")
                continue

            try:
                resp_json = response.json()
            except Exception as e:
                print(f"Failed to decode JSON for SKU {sku_value}: {e}")
                continue

            data_section = resp_json.get("data", {})
            offer_section = data_section.get("Offer", {})
            title = offer_section.get("Title", "")
            variant = offer_section.get("Variant", "")
            package = offer_section.get("Package", "")
            startdate = offer_section.get("StartDate", "")
            enddate = offer_section.get("EndDate", "")

            formatted_startdate = self.format_date(startdate) if startdate else "onbekend"
            formatted_enddate = self.format_date(enddate) if enddate else "onbekend"

            promotion_description = (
                f"{title} {variant} {package} "
                f"Geldig van {formatted_startdate} tot en met {formatted_enddate}"
            )
            print(promotion_description)

            # Store the promotion_description in the parser collection.
            parser_data = {
                "unique_id": sku_value,
                "promotion_description": promotion_description,
            }
            try:
                insert_result = self.parser_collection.insert_one(parser_data)
                print(f"Inserted document with id: {insert_result.inserted_id}")
            except Exception as e:
                print(f"Error inserting document for SKU {sku_value}: {e}")

    def format_date(self, date_str):
        """
        Format a date string ('YYYY-MM-DD') to 'weekday day month' in Dutch.
        """
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        except Exception as e:
            print(f"Date format error: {e}")
            return date_str

        # Mapping of weekday numbers to Dutch names.
        weekday_mapping = {
            0: 'maandag',
            1: 'dinsdag',
            2: 'woensdag',
            3: 'donderdag',
            4: 'vrijdag',
            5: 'zaterdag',
            6: 'zondag'
        }
        # Mapping of month numbers to Dutch names.
        month_mapping = {
            1: 'januari',
            2: 'februari',
            3: 'maart',
            4: 'april',
            5: 'mei',
            6: 'juni',
            7: 'juli',
            8: 'augustus',
            9: 'september',
            10: 'oktober',
            11: 'november',
            12: 'december'
        }

        weekday = weekday_mapping.get(date_obj.weekday(), "onbekend")
        day = date_obj.day
        month = month_mapping.get(date_obj.month, "onbekend")

        return f"{weekday} {day} {month}"

    def close(self):
        """
        Close the MongoDB connection.
        """
        self.client.close()
        print("MongoDB connection closed.")

# To use the Parser_2 class, create an instance and call the start method.
if __name__ == '__main__':
    parser = Parser_2()
    parser.start()
