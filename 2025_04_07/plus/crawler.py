import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

class Crawler:
    def __init__(self):
        # Setup MongoDB connection
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['plus_db']
        self.collection = self.db['crawler']
        
        # Ensure unique index on unique_id field
        self.collection.create_index("unique_id", unique=True)

        # Define the category slugs list
        self.category_slugs = [
            "eieren", "vrije-uitloopeieren", "biologische-eieren", "speciale-eieren",
            "boter-margarine", "halvarine-margarine", "roomboter", "bakboter-braadboter",
            "kaas", "verse-kaas-plakken", "verse-stukken-kaas", "voorverpakte-kaasplakken",
            "voorverpakte-stukken-kaas", "smeerkaas-roomkaas-zuivelspread",
            "rasp-strooikaas-flakes", "borrelkaas-buitenlandse-kaasjes", "kaas-om-mee-te-koken",
            "plantaardige-kaas", "kinderkazen"
        ]

        # Define cookies and headers
        self.cookies = {
            'SSLB': '1',
            'SSID_WA9S': 'CQCPJR0OAAAAAABwjfNnngzFJ3CN82cBAAAAAACc9bVrcI3zZwD0DJBOAQNuJCkAcI3zZwEA',
            'SSSC_WA9S': '1036.G7490486118090345630.1|85648.2696302',
            'visid_incap_1876175': 'K+ha6YedSEmhfRPvs2vTTHCN82cAAAAAQUIPAAAAAADNT0VxsPPulkg66ZV3QZUk',
            'incap_ses_217_1876175': 'TFTZJGWdgUBp3rrwofACA3CN82cAAAAAgBF8HzH0GTiC0eIsiokImQ==',
            'osVisitor': 'a4da44cb-0dbe-4f0d-8f1c-0b371119dd7a',
            'osVisit': '268d7d36-8ab5-4367-89e3-6d082112b7e2',
            'nr2Users': 'crf%3dT6C%2b9iB49TLra4jEsMeSckDMNhQ%3d%3buid%3d0%3bunm%3d',
            'nr1Users': 'lid%3dAnonymous%3btuu%3d0%3bexp%3d0%3brhs%3dXBC1ss1nOgYW1SmqUjSxLucVOAg%3d%3bhmc%3d4DwFYjtQmhXgYB3zdBmXn5G3zBA%3d',
            'nlbi_1876175': 'QGPtK9AxIHGUzqke+vsR5gAAAABfsadQhrUb5EN6qB0dc/VG',
            'baked': '2023-05-12 13:50:05',
            'plus_cookie_level': '3',
            '_gcl_au': '1.1.1694552535.1744014729',
            '_ga': 'GA1.1.1882665487.1744014722',
            'FPAU': '1.1.1694552535.1744014729',
            '_cs_c': '0',
            'nlbi_1876175_2948836': 'JgZNSJaKeneZiElp+vsR5gAAAADxrf2PZ0MuVYvy6OK6s2xB',
            'lantern': '23b19b3e-95bf-401c-920f-3a05538b9f71',
            '_cs_id': 'dce4f1a8-47f9-a257-f746-10ae3db4dde5.1744014730.1.1744015614.1744014730.1.1778178730967.1.x',
            '_cs_s': '2.T.0.9.1744017414258',
            '_ga_3KFS3VVEMB': 'GS1.1.1744014721.1.1.1744015615.0.0.874809673',
            'SSRT_WA9S': '_5DzZwADAA',
        }

        self.headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/json; charset=UTF-8',
            'origin': 'https://www.plus.nl',
            'outsystems-locale': 'nl-NL',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.plus.nl/producten/zuivel-eieren-boter/eieren',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'traceparent': '00-8c102dba70a16ca1db64d67eaa4da2fc-f970ab5cca0d68ac-01',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-csrftoken': 'T6C+9iB49TLra4jEsMeSckDMNhQ=',
        }

        # API URL
        self.base_url = 'https://www.plus.nl/screenservices/ECP_Composition_CW/ProductLists/PLP_Content/DataActionGetProductListAndCategoryInfo'

    def start(self):
        # Loop through each category slug
        for category in self.category_slugs:
            print(f"Processing category: {category}")

            # Define the JSON payload with dynamic CategorySlug
            json_data = {
                'versionInfo': {
                    'moduleVersion': 'jApIf1I3AoV74zCivjDy4Q',
                    'apiVersion': 'bYh0SIb+kuEKWPesnQKP1A',
                },
                'viewName': 'MainFlow.ProductListPage',
                'screenData': {
                    'variables': {
                        'AppliedFiltersList': {
                            'List': [],
                            'EmptyListItem': {
                                'Name': '',
                                'Quantity': '0',
                                'IsSelected': False,
                                'URL': '',
                            },
                        },
                        'LocalCategoryID': 0,
                        'LocalCategoryName': '',
                        'LocalCategoryParentId': 0,
                        'LocalCategoryTitle': '',
                        'IsLoadingMore': False,
                        'IsFirstDataFetched': False,
                        'ShowFilters': False,
                        'IsShowData': False,
                        'StoreNumber': 0,
                        'StoreChannel': '',
                        'CheckoutId': '5c3e7322-2383-42d8-8794-33c8b85a5693',
                        'IsOrderEditMode': False,
                        'ProductList_All': {
                            'List': [],
                            'EmptyListItem': {
                                'SKU': '',
                                'Brand': '',
                                'Name': '',
                                'Product_Subtitle': '',
                                'Slug': '',
                                'ImageURL': '',
                                'ImageLabel': '',
                                'MetaTitle': '',
                                'MetaDescription': '',
                                'OriginalPrice': '0',
                                'NewPrice': '0',
                                'Quantity': 0,
                                'LineItemId': '',
                                'IsProductOverMajorityAge': False,
                                'Logos': {
                                    'PLPInUpperLeft': {
                                        'List': [],
                                        'EmptyListItem': {
                                            'Name': '',
                                            'LongDescription': '',
                                            'URL': '',
                                            'Order': 0,
                                        },
                                    },
                                    'PLPAboveTitle': {
                                        'List': [],
                                        'EmptyListItem': {
                                            'Name': '',
                                            'LongDescription': '',
                                            'URL': '',
                                            'Order': 0,
                                        },
                                    },
                                    'PLPBehindSizeUnit': {
                                        'List': [],
                                        'EmptyListItem': {
                                            'Name': '',
                                            'LongDescription': '',
                                            'URL': '',
                                            'Order': 0,
                                        },
                                    },
                                },
                                'EAN': '',
                                'Packging': '',
                                'Categories': {
                                    'List': [],
                                    'EmptyListItem': {
                                        'Name': '',
                                    },
                                },
                                'IsAvailable': False,
                                'PromotionLabel': '',
                                'PromotionBasedLabel': '',
                                'PromotionStartDate': '1900-01-01',
                                'PromotionEndDate': '1900-01-01',
                                'IsFreeDeliveryOffer': False,
                                'IsOfflineSaleOnly': False,
                                'MaxOrderLimit': 0,
                                'CitrusAdId': '',
                            },
                        },
                        'PageNumber': 1,  # This will be updated dynamically
                        'SelectedSort': '',
                        'OrderEditId': '',
                        'IsListRendered': False,
                        'IsAlreadyFetch': False,
                        'IsPromotionBannersFetched': False,
                        'Period': {
                            'FromDate': '2025-04-02',
                            'ToDate': '2025-04-08',
                        },
                        'UserStoreId': '0',
                        'FilterExpandedList': {
                            'List': [],
                            'EmptyListItem': False,
                        },
                        'ItemsInCart': {
                            'List': [],
                            'EmptyListItem': {
                                'LineItemId': '',
                                'SKU': '',
                                'MainCategory': {
                                    'Name': '',
                                    'Webkey': '',
                                    'OrderHint': '0',
                                },
                                'Quantity': 0,
                                'Name': '',
                                'Subtitle': '',
                                'Brand': '',
                                'Image': {
                                    'Label': '',
                                    'URL': '',
                                },
                                'ItemTypeAttributeId': '',
                                'DepositFee': '0',
                                'Slug': '',
                                'ChannelId': '',
                                'Promotion': {
                                    'BasedLabel': '',
                                    'Label': '',
                                    'StampURL': '',
                                    'NewPrice': '0',
                                    'IsFreeDelivery': False,
                                },
                                'IsNIX18': False,
                                'Price': '0',
                                'MaxOrderLimit': 0,
                                'QuantityOfFreeProducts': 0,
                            },
                        },
                        'HideDummy': False,
                        'OneWelcomeUserId': '',
                        '_oneWelcomeUserIdInDataFetchStatus': 1,
                        'CategorySlug': category,
                        '_categorySlugInDataFetchStatus': 1,
                        'SearchKeyword': '',
                        '_searchKeywordInDataFetchStatus': 1,
                        'IsDesktop': False,
                        '_isDesktopInDataFetchStatus': 1,
                        'IsSearch': False,
                        '_isSearchInDataFetchStatus': 1,
                        'URLPageNumber': 0,
                        '_uRLPageNumberInDataFetchStatus': 1,
                        'FilterQueryURL': '',
                        '_filterQueryURLInDataFetchStatus': 1,
                        'IsMobile': False,
                        '_isMobileInDataFetchStatus': 1,
                        'IsTablet': True,
                        '_isTabletInDataFetchStatus': 1,
                        'Monitoring_FlowTypeId': 3,
                        '_monitoring_FlowTypeIdInDataFetchStatus': 1,
                        'IsCustomerUnderAge': False,
                        '_isCustomerUnderAgeInDataFetchStatus': 1,
                    },
                },
            }

            # Make the initial request to determine the total number of pages for the current category
            json_data['screenData']['variables']['PageNumber'] = 1
            response = requests.post(self.base_url, cookies=self.cookies, headers=self.headers, json=json_data)

            if response.status_code == 200:
                data = response.json()
                total_pages = data.get("data", {}).get("TotalPages", 1)
                print(f"Total Pages for {category}: {total_pages}")
            else:
                print(f"Initial request failed for category {category} with status code", response.status_code)
                total_pages = 1

            # List to hold all items for the current category
            all_items = []

            # Iterate through each page
            for page in range(1, total_pages + 1):
                print(f"Processing page {page} of {total_pages} for category {category}")
                json_data['screenData']['variables']['PageNumber'] = page
                response = requests.post(self.base_url, cookies=self.cookies, headers=self.headers, json=json_data)

                if response.status_code == 200:
                    page_data = response.json()
                    # Parse items from the response
                    items = self.parse_items(page_data, category)
                    all_items.extend(items)
                else:
                    print(f"Request for page {page} of category {category} failed with status code", response.status_code)

            # Insert each item individually; if a duplicate occurs, skip insertion.
            for item in all_items:
                try:
                    self.collection.insert_one(item)
                    print(f"Inserted item with unique_id: {item['unique_id']}")
                except DuplicateKeyError:
                    print(f"Duplicate found for unique_id: {item['unique_id']}. Skipping insertion.")
                except Exception as e:
                    print(f"Error inserting item {item['unique_id']}: {e}")

    def parse_items(self, response_json, category):
        items = []
        product_list = response_json.get("data", {}).get("ProductList", {}).get("List", [])
        for item in product_list:
            plp_str = item.get("PLP_Str", {})
            sku = plp_str.get("SKU")
            brand = plp_str.get("Brand")
            name = plp_str.get("Name")
            slug_original = plp_str.get("Slug", "")
            slug = f"https://www.plus.nl/product/{slug_original}" if slug_original else None

            # Extract Categories hierarchy
            categories = plp_str.get("Categories", {}).get("List", [])
            producthierarchy_level3 = categories[0]["Name"] if len(categories) > 0 else ""
            producthierarchy_level4 = categories[1]["Name"] if len(categories) > 1 else ""
            producthierarchy_level5 = categories[2]["Name"] if len(categories) > 2 else ""

            # Pricing logic
            original_price = plp_str.get("OriginalPrice", "0")
            new_price = plp_str.get("NewPrice", "0")
            if new_price != "0" and new_price != original_price:
                regular_price = original_price
                selling_price = new_price
                promotion_price = new_price
            else:
                regular_price = original_price
                selling_price = new_price
                promotion_price = new_price

            # Promotion details
            promotion_valid_from = plp_str.get("PromotionStartDate")
            promotion_valid_upto = plp_str.get("PromotionEndDate")
            promotion_type = plp_str.get("PromotionLabel")

            # Breadcrumb construction by joining the hierarchy levels
            breadcrumb_parts = ["Home", "Producten", producthierarchy_level3, producthierarchy_level4, producthierarchy_level5]
            breadcrumb = " > ".join([part for part in breadcrumb_parts if part])

            # Image URL
            image_urls = plp_str.get("ImageURL")

            # Construct the item dictionary with the new fields
            item_dict = {
                "unique_id": sku,
                "competitor_name": "plus",
                "product_name": name,
                "brand": brand,
                "pdp_url": slug,
                "producthierarchy_level1": "Home",
                "producthierarchy_level2": "Producten",
                "producthierarchy_level3": producthierarchy_level3,
                "producthierarchy_level4": producthierarchy_level4,
                "producthierarchy_level5": producthierarchy_level5,
                "regular_price": regular_price,
                "selling_price": selling_price,
                "promotion_price": promotion_price,
                "promotion_valid_from": promotion_valid_from,
                "promotion_valid_upto": promotion_valid_upto,
                "promotion_type": promotion_type,
                "breadcrumb": breadcrumb,
                "image_urls": image_urls,
            }
            items.append(item_dict)
        return items

    def close(self):
        self.client.close()

if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
    crawler.close()
