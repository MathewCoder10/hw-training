import re
import copy
import requests
from datetime import datetime
from pymongo import MongoClient

class Parser:
    def __init__(self, db_uri, db_name, crawler_collection_name, parser_collection_name):
        # Initialize MongoDB connection and collections.
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.crawler_coll = self.db[crawler_collection_name]
        self.parser_coll = self.db[parser_collection_name]

        # Fetch CSRF token and setup headers.
        self.csrf_token = self._fetch_csrf_token()
        self.headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
            'cache-control': 'no-cache',
            'content-type': 'application/json; charset=UTF-8',
            'outsystems-locale': 'nl-NL',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.plus.nl/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'traceparent': '00-527258064b1705846db4a2cc8f2c65bd-8e141e78141c3eec-01',
            'user-agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'),
            'x-csrftoken': self.csrf_token,
        }

        # JSON payload templates
        self.details_payload_template = {
            'versionInfo': {
                'moduleVersion': 'jApIf1I3AoV74zCivjDy4Q',
                'apiVersion': 'j2jjJJxS4heD58kEZAYPUQ',
            },
            'viewName': 'MainFlow.ProductDetailsPage',
            'screenData': {
                'variables': {
                    'SKU': '',
                    'ChannelId': '',
                    'Locale': 'nl-NL',
                    'StoreId': '0',
                    'StoreNumber': 0,
                    'CheckoutId': '5c3e7322-2383-42d8-8794-33c8b85a5693',
                    'OrderEditId': '',
                    'IsOrderEditMode': False,
                    'TotalLineItemQuantity': 0,
                    'ShoppingListProducts': {
                        'List': [],
                        'EmptyListItem': {'SKU': '', 'Quantity': '0'},
                    },
                    'HasDailyValueIntakePercent': False,
                    'CartPromotionDeliveryDate': '2025-04-07',
                    'LineItemQuantity': 0,
                    'IsPhone': False,
                    '_isPhoneInDataFetchStatus': 1,
                    'OneWelcomeUserId': '',
                    '_oneWelcomeUserIdInDataFetchStatus': 1,
                    '_sKUInDataFetchStatus': 1,
                    'TotalCartItems': 0,
                    '_totalCartItemsInDataFetchStatus': 1,
                    '_productNameInDataFetchStatus': 1,
                },
            },
        }
        self.promotion_payload_template = {
            'versionInfo': {
                'moduleVersion': 'gA_kBEFQeNXDsqTGV6FWFQ',
                'apiVersion': 'pRmIEBbjlwOG2dJVuRaytA',
            },
            'viewName': 'MainFlow.ProductDetailsPage',
            'screenData': {
                'variables': {
                    'ShowMedicineSidebar': False,
                    'Product': {'Overview': {
                        'Name': '', 'Subtitle': '', 'Brand': '',
                        'Slug': '', 'Image': {'Label': '', 'URL': ''},
                        'Meta': {'Description': '', 'Title': ''},
                        'IsNIX18': False, 'Price': '0', 'BaseUnitPrice': '',
                        'LineItem': {'Id': '', 'Quantity': 0},
                        'IsOfflineSaleOnly': False, 'IsServiceItem': False,
                        'IsAvailableInStore': False, 'MaxOrderLimit': 0,
                    }},
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
                        'EmptyListItem': {'SKU': '', 'Quantity': '0'},
                    },
                    'HasDailyValueIntakePercent': False,
                    'CartPromotionDeliveryDate': '2025-04-08',
                    'LineItemQuantity': 0,
                    'IsPhone': False,
                    '_isPhoneInDataFetchStatus': 1,
                    'OneWelcomeUserId': '',
                    '_oneWelcomeUserIdInDataFetchStatus': 1,
                    'SKU': '',
                    '_sKUInDataFetchStatus': 1,
                    'TotalCartItems': 0,
                    '_totalCartItemsInDataFetchStatus': 1,
                    '_productNameInDataFetchStatus': 1,
                },
            },
        }
        # Fallback JSON payload template when promotion fields are empty.
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
                        'ProductClassificationId': '',
                        'Categories': {
                            'List': [],
                            'EmptyListItem': {
                                'Name': '',
                            },
                        },
                        'Logos': {
                            'PDPInUpperLeft': {
                                'List': [],
                                'EmptyListItem': {
                                    'Name': '',
                                    'LongDescription': '',
                                    'URL': '',
                                    'Order': 0,
                                },
                            },
                            'PDPInProductInformation': {
                                'List': [],
                                'EmptyListItem': {
                                    'Name': '',
                                    'LongDescription': '',
                                    'URL': '',
                                    'Order': 0,
                                },
                            },
                            'PDPBehindSizeUnit': {
                                'List': [],
                                'EmptyListItem': {
                                    'Name': '',
                                    'LongDescription': '',
                                    'URL': '',
                                    'Order': 0,
                                },
                            },
                            'PDPBelowAddToCart': {
                                'List': [],
                                'EmptyListItem': {
                                    'Name': '',
                                    'LongDescription': '',
                                    'URL': '',
                                    'Order': 0,
                                },
                            },
                            'PDPAboveTitle': {
                                'List': [],
                                'EmptyListItem': {
                                    'Name': '',
                                    'LongDescription': '',
                                    'URL': '',
                                    'Order': 0,
                                },
                            },
                            'PDPInRemarks': {
                                'List': [],
                                'EmptyListItem': {
                                    'Name': '',
                                    'LongDescription': '',
                                    'URL': '',
                                    'Order': 0,
                                },
                            },
                        },
                        'Legal': {
                            'RegulatedName': '',
                            'HealthClaim': '',
                            'DrainWeight': {
                                'UoM': '',
                                'Value': 0,
                            },
                            'RequiredNotificationByLaw': '',
                            'AppointedAuthority': '',
                            'AdittionalClassification': {
                                'System': '',
                                'Trades': '',
                            },
                        },
                        'UsageDuring': {
                            'BreastFeeding': '',
                            'Pregnancy': '',
                            'SafePeriodAfterOpening': 0,
                        },
                        'Marketing': {
                            'Description': '',
                            'UniqueSellingPoint': '',
                            'Message': '',
                        },
                        'SupplierContact': {
                            'LegalContact': {
                                'Address': '',
                                'Name': '',
                            },
                            'LegalSupplier': {
                                'Address': '',
                                'Name': '',
                            },
                            'PDP_ProductMeans': {
                                'Email_List': {
                                    'List': [],
                                    'EmptyListItem': '',
                                },
                                'SocialMedia_List': {
                                    'List': [],
                                    'EmptyListItem': '',
                                },
                                'Contact_List': {
                                    'List': [],
                                    'EmptyListItem': '',
                                },
                                'WebSites_List': {
                                    'List': [],
                                    'EmptyListItem': '',
                                },
                            },
                        },
                        'Composition': '',
                        'Ingredients': '',
                        'Nutrient': {
                            'Base': {
                                'UoM': '',
                                'Value': 0,
                            },
                            'Additional': {
                                'NutricionalClaim': '',
                                'PreparedDeviation': '',
                                'ReferenceIntake': '',
                            },
                            'Nutrients': {
                                'List': [],
                                'EmptyListItem': {
                                    'TypeCode': '',
                                    'UnitCode': '',
                                    'Description': '',
                                    'ParentCode': '',
                                    'DailyValueIntakePercent': '',
                                    'QuantityContained': {
                                        'Value': '0',
                                        'UoM': '',
                                    },
                                    'SortOrder': 0,
                                },
                            },
                        },
                        'Allergen': {
                            'Warning': '',
                            'Description': '',
                        },
                        'InstructionsAndSuggestions': {
                            'Instructions': {
                                'Preparation': '',
                                'Storage': '',
                                'Usage': '',
                            },
                            'Suggestions': {
                                'Serving': '',
                            },
                        },
                        'PercentageOfAlcohol': '',
                        'Beer': {
                            'Kind': '',
                            'Taste': '',
                            'FoodAdvice': '',
                            'Description': {
                                'Long': '',
                                'Short': '',
                            },
                        },
                        'Wine': {
                            'Type': '',
                            'Quote': '',
                            'LongDescription': '',
                            'Flavour': '',
                            'GrapeVariety': '',
                            'Country': '',
                            'Region': '',
                            'WineTastingNote': {
                                'FoodAdvice': '',
                                'SmellAndTaste': '',
                                'FoodAdvices': {
                                    'List': [],
                                    'EmptyListItem': '',
                                },
                            },
                            'Awards': {
                                'List': [],
                                'EmptyListItem': '',
                            },
                        },
                        'SeaFood': {
                            'Production': {
                                'Method': '',
                            },
                            'Catch': {
                                'Areas': '',
                                'Methods': '',
                            },
                        },
                        'PetFood': {
                            'TargetConsumptionBy': '',
                            'Feed': {
                                'Instructions': '',
                                'Type': '',
                            },
                            'FoodStatetment': {
                                'Additive': '',
                                'AnalyticalConstituents': '',
                                'Composition': '',
                            },
                        },
                        'Medicine': {
                            'EAN': '',
                        },
                        'DrugStore': {
                            'Store': {
                                'Origin': '',
                                'Number': {
                                    'RVG': '',
                                    'RVH': '',
                                },
                                'Certification': {
                                    'Agency': '',
                                    'Standard': '',
                                },
                            },
                            'Dosage': {
                                'Admnistration': '',
                                'Recommendation': '',
                            },
                            'SideEffectsAndWarnings': '',
                        },
                        'HealthCare': {
                            'UsageAge': {
                                'Description': '',
                                'Max': {
                                    'UoM': '',
                                    'Value': 0,
                                },
                                'Min': {
                                    'UoM': '',
                                    'Value': 0,
                                },
                            },
                            'SunProtection': {
                                'Category': '',
                                'Factor': '',
                            },
                        },
                        'LightBulb': {
                            'BaseType': '',
                            'LampTypeCode': '',
                            'NumberOfSwitches': '',
                            'SuitableForAccentLighting': '',
                            'DeclaredPower': {
                                'UoM': '',
                                'Value': 0,
                            },
                            'EquivalentPower': {
                                'UoM': '',
                                'Value': 0,
                            },
                            'Diameter': {
                                'UoM': '',
                                'Value': 0,
                            },
                            'VisibleLight': {
                                'UoM': '',
                                'Value': 0,
                            },
                            'ColourTemperature': {
                                'Avg': {
                                    'UoM': '',
                                    'Value': 0,
                                },
                                'Max': {
                                    'UoM': '',
                                    'Value': 0,
                                },
                                'Min': {
                                    'UoM': '',
                                    'Value': 0,
                                },
                            },
                            'WarmUpTime': {
                                'UoM': '',
                                'Value': 0,
                            },
                        },
                        'Battery': {
                            'Voltage': {
                                'UoM': '',
                                'Value': '0',
                            },
                            'Capacity': {
                                'UoM': '',
                                'Value': 0,
                            },
                            'Weight': {
                                'UoM': '',
                                'Value': '0',
                            },
                            'Quantity': 0,
                            'MaterialAgency': '',
                            'Type': '',
                            'TechnologyTypes': {
                                'List': [],
                                'EmptyListItem': '',
                            },
                            'IsRechargeable': False,
                            'BuiltIn': {
                                'IsBuiltIn': False,
                                'Quantity': 0,
                            },
                        },
                        'Hazardous': {
                            'ChildSafeClosure': '',
                            'Chemical': {
                                'Identification': '',
                                'Name': '',
                                'Organisation': '',
                                'Concentration': 0,
                            },
                            'SafetyRecommendations': {
                                'List': [],
                                'EmptyListItem': {
                                    'Key': '',
                                    'Value': '',
                                },
                            },
                            'HazardDesignations': {
                                'List': [],
                                'EmptyListItem': {
                                    'Key': '',
                                    'Value': '',
                                },
                            },
                            'GHSSignal': {
                                'Symbols': '',
                                'Word': '',
                            },
                        },
                        'IsVisibleSection': {
                            'AboutThisBeer': False,
                            'AboutThisProduct': False,
                            'AboutThisWine': False,
                            'AllergieInfo': False,
                            'HandyInfo': False,
                            'Ingredients': False,
                            'LegalInfo': False,
                            'NutrionalValues': False,
                            'PreparationInstruction': False,
                            'ServingSuggestions': False,
                            'SupplierContact': False,
                            'TasteInfo': False,
                            'UsageAndStorage': False,
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
                    'CartPromotionDeliveryDate': '2025-04-09',
                    'LineItemQuantity': 0,
                    'IsPhone': True,
                    '_isPhoneInDataFetchStatus': 1,
                    'OneWelcomeUserId': '',
                    '_oneWelcomeUserIdInDataFetchStatus': 1,
                    'SKU': '',
                    '_sKUInDataFetchStatus': 1,
                    'TotalCartItems': 0,
                    '_totalCartItemsInDataFetchStatus': 1,
                    'ProductName': '',
                    '_productNameInDataFetchStatus': 1,
                },
            },
        }

        # API endpoints
        self.details_api_url = (
            'https://www.plus.nl/screenservices/ECP_Product_CW/'
            'ProductDetails/PDPContent/DataActionGetProductDetailsAndAgeInfo'
        )
        self.promotion_api_url = (
            'https://www.plus.nl/screenservices/ECP_Product_CW/'
            'ProductDetails/PDPContent/DataActionGetPromotionOffer'
        )

        self.session = requests.Session()

    def _fetch_csrf_token(self):
        """Fetch and return the CSRF token from the OutSystems.js file."""
        js_url = 'https://www.plus.nl/scripts/OutSystems.js?H4bR29NkZ15NFYcdxJmseg'
        resp = requests.get(js_url, headers={
            'accept': '*/*',
            'referer': 'https://www.plus.nl/',
            'user-agent': (
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            )
        })
        resp.raise_for_status()
        match = re.search(r'e\.AnonymousCSRFToken\s*=\s*"([^"]+)"', resp.text)
        if not match:
            raise RuntimeError("Unable to extract CSRF token from OutSystems.js")
        return match.group(1)

    def start(self):
        """Entry point: process documents and then close the MongoDB connection."""
        print("Combined parser started.")
        self.parse_items()
        self.close()

    def parse_items(self):
        cursor = self.crawler_coll.find({})
        for doc in cursor:
            unique_id = doc.get("unique_id")
            if not unique_id:
                print("Missing unique_id; skipping.")
                continue

            print(f"\nProcessing SKU: {unique_id}")

            # 1. Product Details
            details_payload = copy.deepcopy(self.details_payload_template)
            details_payload["screenData"]["variables"]["SKU"] = unique_id
            try:
                resp = self.session.post(
                    self.details_api_url,
                    headers=self.headers,
                    json=details_payload
                )
                resp.raise_for_status()
                details_json = resp.json()
            except Exception as e:
                print(f"Error fetching details for {unique_id}: {e}")
                continue

            data = details_json.get("data", details_json)
            product = data.get("ProductOut")
            if not product:
                print(f"No ProductOut for SKU {unique_id}.")
                continue

            # Parse product details.
            ov = product.get("Overview", {})
            instr = product.get("InstructionsAndSuggestions", {}).get("Instructions", {})
            sugg = product.get("InstructionsAndSuggestions", {}).get("Suggestions", {})
            ingreds = product.get("Ingredients", "").strip()
            pdp_info = product.get("Logos", {}) \
                              .get("PDPInProductInformation", {}) \
                              .get("List", [])
            nutrients = product.get("Nutrient", {}) \
                               .get("Nutrients", {}) \
                               .get("List", [])
            allergen = product.get("Allergen", {}).get("Description", "").strip()

            # A. Subtitle parsing
            subtitle = ov.get("Subtitle", "")
            m = re.search(r"Per.*?(\d+)\s+(\w+)", subtitle)
            grammage_quantity = m.group(1) if m else None
            grammage_unit     = m.group(2) if m else None
            subtitle = ov.get("Subtitle", "")
            p = re.search(r"\((.*?)\)", subtitle)
            price_per_unit    = p.group(1) if p else None
            product_description = ov.get("Meta", {}).get("Description", "")

            # B. Instructions & storage
            prep = instr.get("Preparation", "").strip()
            usage = instr.get("Usage", "").strip()
            instructions = prep + (" " + usage if usage else "")
            storage_instructions = instr.get("Storage", "").strip()

            # C. Servings
            servings_per_pack = sugg.get("Serving", "").strip()

            # D. Nutritional score
            nutritional_score = None
            for item in pdp_info:
                if item.get("Name", "").startswith("Nutri-Score"):
                    sc = item["Name"].replace("Nutri-Score", "").strip().upper()
                    if sc in list("ABCDE"):
                        nutritional_score = sc
                    break

            # E. Organic flag
            organictype = "Non-Organic"
            for item in pdp_info:
                if "Biologisch" in item.get("LongDescription", ""):
                    organictype = "Organic"
                    break

            # F. Allergens
            allergens = allergen

            # G. Fat percentage
            fat_percentage = None
            for item in nutrients:
                if item.get("ParentCode") == "FAT" and \
                   "meervoudig onverzadigd" in item.get("Description", "").lower():
                    fat_percentage = item.get("QuantityContained", {}).get("Value")
                    break

            # 2. Promotion Offer
            # First attempt using the promotion_payload_template.
            promo_payload = copy.deepcopy(self.promotion_payload_template)
            promo_payload["screenData"]["variables"]["SKU"] = unique_id
            try:
                resp2 = self.session.post(
                    self.promotion_api_url,
                    headers=self.headers,
                    json=promo_payload
                )
                resp2.raise_for_status()
                promo_json = resp2.json()
            except Exception as e:
                print(f"Error fetching promotion for {unique_id}: {e}")
                promo_json = {}

            offer = promo_json.get("data", {}).get("Offer", {})
            title   = offer.get("Title", "").strip()
            variant = offer.get("Variant", "").strip()
            package = offer.get("Package", "").strip()
            start   = offer.get("StartDate", "")
            end     = offer.get("EndDate", "")
            promo_type = offer.get("DisplayInfo_Label", "").strip()

            # Check if at least one of title, variant, or package is non-empty.
            if title or variant or package:
                sd = self.format_date(start) if start else "onbekend"
                ed = self.format_date(end) if end else "onbekend"
                promotion_description = f"{title} {variant} {package} Geldig van {sd} tot en met {ed}".strip()
                promotion_valid_from = start
                promotion_valid_upto = end
                promotion_type_final = promo_type
            else:
                # Fallback: try using the json_template.
                fallback_payload = copy.deepcopy(self.json_template)
                fallback_payload["screenData"]["variables"]["SKU"] = unique_id
                try:
                    fallback_resp = self.session.post(
                        self.promotion_api_url,
                        headers=self.headers,
                        json=fallback_payload
                    )
                    fallback_resp.raise_for_status()
                    fallback_json = fallback_resp.json()
                except Exception as e:
                    print(f"Error in fallback promotion call for SKU {unique_id}: {e}")
                    fallback_json = {}

                fallback_offer = fallback_json.get("data", {}).get("Offer", {})
                fb_title   = fallback_offer.get("Title", "").strip()
                fb_variant = fallback_offer.get("Variant", "").strip()
                fb_package = fallback_offer.get("Package", "").strip()
                fb_start   = fallback_offer.get("StartDate", "")
                fb_end     = fallback_offer.get("EndDate", "")
                fb_promo_type = fallback_offer.get("DisplayInfo_Label", "").strip()
                
                if fb_title or fb_variant or fb_package:
                    sd = self.format_date(fb_start) if fb_start else "onbekend"
                    ed = self.format_date(fb_end) if fb_end else "onbekend"
                    promotion_description = f"{fb_title} {fb_variant} {fb_package} Geldig van {sd} tot en met {ed}".strip()
                    promotion_valid_from = fb_start
                    promotion_valid_upto = fb_end
                    promotion_type_final = fb_promo_type
                else:
                    promotion_description = ""
                    promotion_valid_from = ""
                    promotion_valid_upto = ""
                    promotion_type_final = ""
            
            # Extract percentage_discount from the promotion_type field if applicable.
            match = re.search(r'(\d+)\s*%', promotion_type_final)
            if match:
                percentage_discount = match.group(1)
            else:
                percentage_discount = ""

            # 3. Combine and insert
            combined = {
                # crawler fields
                "unique_id": unique_id,
                "competitor_name": doc.get("competitor_name"),
                "product_name": doc.get("product_name"),
                "brand": doc.get("brand"),
                "pdp_url": doc.get("pdp_url"),
                "producthierarchy_level1": doc.get("producthierarchy_level1"),
                "producthierarchy_level2": doc.get("producthierarchy_level2"),
                "producthierarchy_level3": doc.get("producthierarchy_level3"),
                "producthierarchy_level4": doc.get("producthierarchy_level4"),
                "producthierarchy_level5": doc.get("producthierarchy_level5"),
                "regular_price": doc.get("regular_price"),
                "selling_price": doc.get("selling_price"),
                "price_was": doc.get("promotion_price"),
                "promotion_price": doc.get("promotion_price"),
                "promotion_valid_from": promotion_valid_from,
                "promotion_valid_upto": promotion_valid_upto,
                "promotion_type": promotion_type_final,
                "breadcrumb": doc.get("breadcrumb"),
                "image_urls": doc.get("image_urls"),
                # parsed details
                "product_description": product_description,
                "grammage_quantity": grammage_quantity,
                "grammage_unit": grammage_unit,
                "price_per_unit": price_per_unit,
                "instructions": instructions,
                "storage_instructions": storage_instructions,
                "servings_per_pack": servings_per_pack,
                "ingredients": ingreds,
                "nutritional_score": nutritional_score,
                "organictype": organictype,
                "allergens": allergens,
                "fat_percentage": fat_percentage,
                "promotion_description": promotion_description,
                "percentage_discount": percentage_discount,
                "currency":"€"
            }

            # --- Add image related fields ---
            image_urls_field = combined.get("image_urls")
            # Normalize the image_urls field to a list, filtering out empty strings.
            if isinstance(image_urls_field, list):
                image_urls_list = [url for url in image_urls_field if url.strip()]
            elif isinstance(image_urls_field, str) and image_urls_field.strip():
                image_urls_list = [image_urls_field]
            else:
                image_urls_list = []

            # Populate up to 6 image fields.
            for i in range(6):
                file_key = f"file_name_{i + 1}"
                url_key = f"image_url_{i + 1}"
                if i < len(image_urls_list):
                    combined[file_key] = f"{unique_id}_{i + 1}.PNG"
                    combined[url_key] = image_urls_list[i]
                else:
                    combined[file_key] = ""
                    combined[url_key] = ""
            # --- End of image fields addition ---

            try:
                self.parser_coll.insert_one(combined)
                print(f"Stored SKU {unique_id}")
            except Exception as e:
                print(f"Insert error for {unique_id}: {e}")

    def format_date(self, date_str):
        """
        Format a date ('YYYY-MM-DD') into 'weekday day month' in Dutch.
        """
        try:
            d = datetime.strptime(date_str, '%Y-%m-%d')
        except Exception:
            return date_str

        weekdays = {
            0: 'maandag', 1: 'dinsdag', 2: 'woensdag',
            3: 'donderdag', 4: 'vrijdag', 5: 'zaterdag', 6: 'zondag'
        }
        months = {
            1: 'januari', 2: 'februari', 3: 'maart', 4: 'april',
            5: 'mei', 6: 'juni', 7: 'juli', 8: 'augustus',
            9: 'september', 10: 'oktober', 11: 'november', 12: 'december'
        }
        wd = weekdays.get(d.weekday(), 'onbekend')
        mo = months.get(d.month, 'onbekend')
        return f"{wd} {d.day} {mo}"

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    parser = Parser(
        db_uri="mongodb://localhost:27017/",
        db_name="plus_nl",
        crawler_collection_name="crawler_new_checked_final",
        parser_collection_name="parser_final_collection"
    )
    parser.start()
