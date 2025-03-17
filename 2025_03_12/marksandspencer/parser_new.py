import json
import re
import requests
import uuid
from parsel import Selector
from pymongo import MongoClient
from urllib.parse import urlparse, parse_qs

class Parser:
    def __init__(self):
        # MongoDB connection and collection setup.
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client['mands']
        self.crawler_collection = self.db['crawler']
        self.parser_collection = self.db['parser_new']
        
        # Social proof API headers and endpoint.
        self.social_headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'text/plain;charset=UTF-8',
            'origin': 'https://www.marksandspencer.com',
            'pragma': 'no-cache',
            'referer': 'https://www.marksandspencer.com/',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.social_api_url = 'https://api.taggstar.com/api/v2/key/marksandspencercom/product/visit'
        
        # Request headers for product page scraping.
        self.scrape_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            # 'referer' will be set dynamically per request.
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
    
    def start(self):
        """
        Iterate over all documents in the crawler collection,
        process each product URL, and insert the parsed records into the parser collection.
        """
        cursor = self.crawler_collection.find({})
        for document in cursor:
            self.parse_items(document)
    
    def parse_items(self, document):
        """
        Processes a single document from the crawler collection:
        - Retrieves the product URL and color parameter.
        - Scrapes the product details.
        - Calls the Social Proof API.
        - Extracts product variants and flattens each SKU record.
        - Inserts the flattened record into the parser collection.
        """
        # Retrieve full product URL and parse the color parameter.
        full_product_url = document.get("product_url")
        if not full_product_url:
            return
        
        pdp_url = full_product_url
        parsed_url = urlparse(full_product_url)
        query_params = parse_qs(parsed_url.query)
        color_param = query_params.get("color", [None])[0]
        
        # Get the unique product id.
        u_id = document.get("u_id", "")
        
        print("Processing URL:", pdp_url)
        print("Color parameter:", color_param)
        
        # Set referer dynamically.
        self.scrape_headers['referer'] = full_product_url
        params = {'color': color_param} if color_param else {}
        
        try:
            response = requests.get(pdp_url, params=params, headers=self.scrape_headers, timeout=10)
        except Exception as e:
            print(f"Error fetching URL {pdp_url}: {e}")
            return
        
        if response.status_code != 200:
            print(f"Non-200 response for URL {pdp_url}: {response.status_code}")
            return
        
        selector = Selector(response.text)
        
        # --- Extract common product details ---
        common_details = {
            "gender": "Women",
            "brand": selector.xpath('//p[@class="media-0_textSm__Q52Mz brand-title_title__u6Xx5 media-0_strong__aXigV"]//text()').get(),
            "product_name": selector.xpath('//h1[@class="media-0_headingSm__aysOm"]//text()').get(),
        }
        rating_text = selector.xpath("//div[contains(@class, 'star-rating_wrapper__QmDBj')]/@aria-label").get()
        match = re.search(r'(\d+\.\d+)', rating_text) if rating_text else None
        common_details["rating"] = match.group(1) if match else "N/A"
        review_text = selector.xpath('//span[@class="media-0_textSm__Q52Mz media-0_strong__aXigV"]//text()').get()
        common_details["review"] = re.search(r'\d+', review_text).group() if review_text and re.search(r'\d+', review_text) else "0"
        
        image_urls = selector.xpath("//ul[@class='image-gallery_slides__N8x_w']//img/@src").getall()
        if len(image_urls) >= 3:
            common_details["image_url_1"] = image_urls[0]
            common_details["image_url_2"] = image_urls[1]
            common_details["image_url_3"] = image_urls[2]
        else:
            common_details["image_url_1"] = image_urls[0] if len(image_urls) > 0 else None
            common_details["image_url_2"] = image_urls[1] if len(image_urls) > 1 else None
            common_details["image_url_3"] = image_urls[2] if len(image_urls) > 2 else None

        common_details["competitor_product_key"] = selector.xpath('(//p[@class="media-0_textXs__ZzHWu"]//text())[2]').get()
        common_details["product_description"] = selector.xpath("//p[text()='About this style']/following-sibling::p[1]/text()").get()
        common_details["care_instructions"] = selector.xpath("//p[text()='Care']/following-sibling::div//p[@class='media-0_textSm__Q52Mz product-details_careText__48dt5']/text()").getall()
        common_details["material_composition"] = selector.xpath("//p[text()='Composition']/following-sibling::p[1]/text()").get()
        common_details["style"] = selector.xpath("//p[normalize-space(text())='Fit and style' or normalize-space(text())='Style']/following-sibling::div//p[contains(@class, 'product-details_dimension')]/text()").getall()
        aria_labels = selector.xpath("//div[@class='colour-swatch-list_wrapper__4kdoV']//label/@aria-label").getall()
        common_details["variant_color"] = [label.split(" colour option")[0] for label in aria_labels]
        breadcrumb_items = selector.xpath("//nav[@aria-label='breadcrumb']//a/text()").getall()
        common_details["breadcrumb"] = "/".join(breadcrumb_items)
        common_details["producthierarchy_level1"] = breadcrumb_items[0] if len(breadcrumb_items) > 0 else ''
        common_details["producthierarchy_level2"] = breadcrumb_items[1] if len(breadcrumb_items) > 1 else ''
        common_details["producthierarchy_level3"] = breadcrumb_items[2] if len(breadcrumb_items) > 2 else ''
        common_details["producthierarchy_level4"] = breadcrumb_items[3] if len(breadcrumb_items) > 3 else ''
        common_details["producthierarchy_level5"] = breadcrumb_items[4] if len(breadcrumb_items) > 4 else ''

        script_content = selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if not script_content:
            print(f"No __NEXT_DATA__ script found for URL: {pdp_url}")
            return

        try:
            data = json.loads(script_content)
        except json.JSONDecodeError as e:
            print(f"JSON decode error for URL {pdp_url}: {e}")
            return

        variants_data = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("variants", [])
        common_details["size_guide"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("linkToSizeGuide", '')
        common_details["clothing_fit"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("fit", '')
        common_details["instructions"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("usageInstructions", None)
        common_details["features"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("productfeaturesInnovations", None)
        common_details["body_fit"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("fit", '')
        
        # --- Retrieve Social Proof Label via API ---
        social_proof_label_original = document.get("social_proof_label", "")
        if not u_id:
            new_social_proof_label = social_proof_label_original
        else:
            social_proof_product_id = "P" + u_id
            random_visitor_id = str(uuid.uuid4())
            random_session_id = str(uuid.uuid4())
            data_payload = f'''{{
    "product": {{"id": "{social_proof_product_id}"}},
    "visitor": {{"id": "{random_visitor_id}", "sessionId": "{random_session_id}"}},
    "experience": {{"id": "treatment-v9"}}
}}'''
            try:
                sp_response = requests.post(self.social_api_url, headers=self.social_headers, data=data_payload, timeout=10)
                sp_response.raise_for_status()
                sp_json = sp_response.json()
            except Exception as e:
                print(f"Error calling social proof API for product {social_proof_product_id}: {e}")
                sp_json = {}

            social_proof_messages = []
            for proof in sp_json.get('socialProof', []):
                for msg in proof.get('messages', []):
                    message_html = msg.get('message', '')
                    sel = Selector(text=message_html)
                    desktop_text_list = sel.css("span.tagg-desktop *::text").getall()
                    desktop_text = " ".join(desktop_text_list).strip()
                    social_proof_messages.append(desktop_text)
            new_social_proof_label = (social_proof_label_original + " " + " ".join(social_proof_messages)).strip()
        
        # --- Build list of product variants ---
        if variants_data:
            primary_sizes_set = set()
            secondary_sizes_set = set()
            for variant in variants_data:
                for sku in variant.get("skus", []):
                    primary = sku.get("size", {}).get("primarySize")
                    secondary = sku.get("size", {}).get("secondarySize")
                    if primary:
                        primary_sizes_set.add(primary)
                    if secondary:
                        secondary_sizes_set.add(secondary)
            if secondary_sizes_set:
                product_variants = [f"{p}_{s}" for p in sorted(primary_sizes_set) for s in sorted(secondary_sizes_set)]
            else:
                product_variants = sorted(list(primary_sizes_set))
        else:
            product_variants = []
        
        # --- Process each SKU and insert flattened record into MongoDB ---
        matching_variant_found = False
        selected_color = (selector.xpath('//span[@id="selected-colour-option-text"]/text()').get() or "").strip()
        
        for variant in variants_data:
            variant_color = variant.get("colour")
            if variant_color != selected_color:
                continue
            matching_variant_found = True
            for sku in variant.get("skus", []):
                flat_record = {}
                # Add fields from the crawler document and common details.
                flat_record["pdp_url"] = pdp_url
                flat_record["color"] = color_param
                flat_record["u_id"] = u_id
                flat_record.update(common_details)
                
                # Add SKU-specific fields.
                flat_record["sku_id"] = sku.get("id")
                primary_size = sku.get("size", {}).get("primarySize")
                secondary_size = sku.get("size", {}).get("secondarySize")
                if primary_size or secondary_size:
                    flat_record["primary_size"] = primary_size
                    flat_record["secondary_size"] = secondary_size
                    flat_record["size"] = f"{primary_size}_{secondary_size}" if primary_size and secondary_size else (primary_size if primary_size else "")
                
                flat_record["competitor_name"] = "marksandspencer"
                flat_record["Quantity"] = sku.get("inventory", {}).get("quantity")
                flat_record["Quantity On Hand"] = sku.get("inventory", {}).get("quantityOnHand")
                flat_record["currency"] = sku.get("price", {}).get("currencyPrefix")
                flat_record["Current Price"] = sku.get("price", {}).get("currentPrice")
                flat_record["Previous Price"] = sku.get("price", {}).get("previousPrice")
                flat_record["Unit Price"] = sku.get("price", {}).get("unitPrice")
                flat_record["Instock"] = "outofstock" if sku.get("inventory", {}).get("quantity", 0) < 1 else "instock"
                flat_record["Selling Price"] = sku.get("price", {}).get("currentPrice")
                if sku.get("price", {}).get("previousPrice") is not None:
                    flat_record["Regular Price"] = sku.get("price", {}).get("previousPrice")
                    flat_record["Promotion Price"] = sku.get("price", {}).get("currentPrice")
                else:
                    flat_record["Regular Price"] = sku.get("price", {}).get("currentPrice")
                    flat_record["Promotion Price"] = ""
                flat_record["variants"] = product_variants
                flat_record["social_proof_label"] = new_social_proof_label

                try:
                    self.parser_collection.insert_one(flat_record)
                except Exception as e:
                    print(f"Error inserting record for sku {flat_record.get('sku_id')}: {e}")
        
        if not matching_variant_found:
            print(f"No matching variant for selected color '{selected_color}' found for URL {pdp_url}. Skipping this URL.")
    
    def close(self):
        """Close the MongoDB connection."""
        self.client.close()
        print("MongoDB connection closed.")

# To run the parser:
if __name__ == "__main__":
    parser = Parser()
    try:
        parser.start()
    finally:
        parser.close()
