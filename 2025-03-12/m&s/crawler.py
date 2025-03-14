import re
import requests
import uuid
import time
from parsel import Selector
from pymongo import MongoClient

class Crawler:
    def __init__(self):
        # Set up a single MongoDB connection.
        self.client = MongoClient('mongodb://localhost:27017/')
        
        # Use the "mands" database.
        self.db = self.client['mands']
        
        # For retrieving category URLs.
        self.category_collection = self.db['category']
        
        # For storing product data.
        self.collection = self.db['crawler']
        
        # Base URL for relative links.
        self.base_url = 'https://www.marksandspencer.com'
        
        # Common headers for requests.
        self.get_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        
        self.post_headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'text/plain;charset=UTF-8',
            'origin': 'https://www.marksandspencer.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.marksandspencer.com/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
    
    def start(self):
        """Fetch all category URLs from the category collection and start crawling each category."""
        categories = list(self.category_collection.find({ "category_url": { "$exists": True } }))
        if not categories:
            raise ValueError("No category URLs found in the category collection.")
        
        for cat in categories:
            category_url = cat["category_url"]
            print(f"\n[LOG] Starting category: {category_url}")
            current_url = category_url
            page_number = 1  # Reset page counter for each category
            
            while current_url:
                current_url = self.parse_items(current_url, category_url, page_number)
                if current_url:
                    page_number += 1
                    time.sleep(2)  # Pause to avoid flooding the server.
            print(f"[LOG] Completed category: {category_url}")
        
        print("\n[LOG] Completed crawling all categories.")
    
    def parse_items(self, current_url, category_url, page_number):
        """Fetch and parse product items from a page, process social proof, and store data.
        
        Returns the next page URL (absolute) if available; otherwise, returns None.
        """
        print(f"\n[LOG] Starting to process page {page_number}: {current_url}")
        try:
            response = requests.get(current_url, headers=self.get_headers, timeout=10)
            response.raise_for_status()
            print(f"[LOG] Successfully fetched page {page_number}.")
        except requests.RequestException as e:
            print(f"[ERROR] Request failed for page {page_number}: {e}")
            return None

        selector = Selector(text=response.text)
        
        # Extract product details.
        product_urls = selector.xpath(
            "//div[contains(@class, 'product-list_col__W5sgI')]//a[contains(@class, 'product-card_cardWrapper__GVSTY')]/@href"
        ).getall()
        product_names = selector.xpath(
            "//div[contains(@class, 'product-list_col__W5sgI')]//a[contains(@class, 'product-card_cardWrapper__GVSTY')]//h2/text()"
        ).getall()
        data_tagg_list = selector.xpath(
            "//div[contains(@class, 'product-list_col__W5sgI')]/@data-tagg"
        ).getall()
        
        unique_ids = []
        for tag in data_tagg_list:
            match = re.search(r'\d+', tag)
            if match:
                unique_ids.append(match.group())
            else:
                unique_ids.append("")
        
        if not product_urls or not product_names or not unique_ids:
            print(f"[WARN] No products found on page {page_number}. Moving to next category.")
            return None

        print(f"[LOG] Found {len(product_names)} products on page {page_number}.")
        for uid in unique_ids:
            print(f"[LOG] Fetched product UID: {uid}")
        
        # Collect products; store each product individually.
        products = []
        for name, url, uid in zip(product_names, product_urls, unique_ids):
            products.append({
                "product_name": name.strip(),
                "product_url": self.base_url + url,
                "u_id": uid,
                "social_proof_label": ""
            })
        
        # Prepare the Social Proof API call with unique product IDs.
        unique_product_ids = list(set([p["u_id"] for p in products if p["u_id"]]))
        product_list = ["P" + uid for uid in unique_product_ids]
        visitor_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        payload = {
            "category": "Women",
            "products": product_list,
            "visitor": {"id": visitor_id, "sessionId": session_id},
            "experience": {"id": "treatment-v9"},
        }
        post_url = 'https://api.taggstar.com/api/v2/key/marksandspencercom/category/visit'
        
        print(f"[LOG] Starting social proof extraction for page {page_number}.")
        try:
            post_response = requests.post(post_url, headers=self.post_headers, json=payload, timeout=10)
            post_response.raise_for_status()
            json_data = post_response.json()
            print(f"[LOG] Social proof data received for page {page_number}.")
        except requests.RequestException as e:
            print(f"[ERROR] Social proof API request failed on page {page_number}: {e}")
            json_data = {}

        # Build a mapping of UID to social proof label.
        social_proof_map = {}
        for product in json_data.get("socialProof", []):
            product_id = product.get("product", {}).get("id")
            combined_messages = []
            for msg in product.get("messages", []):
                sel_msg = Selector(text=msg.get("message", ""))
                desktop_text_list = sel_msg.xpath("//span[contains(@class, 'tagg-desktop')]//text()").getall()
                if desktop_text_list:
                    message_text = " ".join(desktop_text_list)
                else:
                    message_text = sel_msg.xpath("string()").get()
                message_text = " ".join(message_text.split())
                if message_text and message_text not in combined_messages:
                    combined_messages.append(message_text)
            if product_id and product_id.startswith("P"):
                uid_key = product_id[1:]
                social_proof_map[uid_key] = " ".join(combined_messages)
                print(f"[LOG] Processed social proof for product P{uid_key} on page {page_number}.")

        # Update each product with its corresponding social proof label.
        for product in products:
            uid = product["u_id"]
            if uid in social_proof_map:
                product["social_proof_label"] = social_proof_map[uid]

        # Insert products into MongoDB.
        print(f"[LOG] Inserting product data for page {page_number} into MongoDB.")
        for product in products:
            try:
                self.collection.insert_one(product)
                print(f"[LOG] Inserted document for product P{product['u_id']}.")
            except Exception as e:
                print(f"[ERROR] Error inserting document for product P{product['u_id']}: {e}")

        # Pagination: Check for the next page.
        next_page = selector.xpath(
            "//a[contains(@class, 'pagination_trigger__YEwyN') and @aria-label='Next page']/@href"
        ).get()
        
        if next_page:
            next_page_url = self.base_url + next_page
            print(f"[LOG] Moving to next page: {next_page_url}")
            return next_page_url
        else:
            return None
    
    def close(self):
        """Close the MongoDB connection."""
        self.client.close()
        print("[LOG] MongoDB connection closed.")


# To run the crawler:
if __name__ == "__main__":
    crawler = Crawler()
    try:
        crawler.start()
    finally:
        crawler.close()
