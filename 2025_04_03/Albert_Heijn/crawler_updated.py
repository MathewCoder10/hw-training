import re
from parsel import Selector
from curl_cffi import requests
from pymongo import MongoClient, errors

class Crawler:
    def __init__(self, mongo_url='mongodb://localhost:27017/', db_name='Albert_Heijn_db', collection_name='crawler'):
        # MongoDB setup: initialize connection and collection.
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        
        # Create a unique index on the 'url' field.
        self.collection.create_index("url", unique=True)
        
        # Define HTTP headers.
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'referer': 'https://www.ah.nl',
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
        
        # List of category URLs to crawl.
        self.category_urls = [
            "https://www.ah.nl/producten/1192/kaas",
            "https://www.ah.nl/producten/2326/kwark",
            "https://www.ah.nl/producten/18543/plantaardige-kaas",
            "https://www.ah.nl/producten/2335/eieren",
            "https://www.ah.nl/producten/951/boter-en-margarine"
        ]
        self.base_url = "https://www.ah.nl"

    def start(self):
        # Start the crawling process over each category.
        for category_url in self.category_urls:
            print(f"\nProcessing category: {category_url}")
            page = 0
            while True:
                params = {'page': str(page)}
                response = requests.get(category_url, params=params, headers=self.headers)
                print(f"Fetching {category_url} page {page} (Status: {response.status_code})")
                
                if response.status_code != 200:
                    break
                
                # Parse items on the current page.
                continue_crawl = self.parse_items(response.text, category_url, page)
                if not continue_crawl:
                    break
                
                page += 1

    def parse_items(self, html_text, category_url, page):
        # Extract product URLs and pagination info from the HTML.
        sel = Selector(text=html_text)
        urls = sel.xpath("//div[@class='header_root__ilMls']/a/@href").getall()
        
        # Attempt to extract pagination indicator.
        text = sel.xpath("//div[contains(@class, 'load-more_root')]//span/text()").get()
        number = re.search(r"(\d+)\s+van\s+de\s+\d+\s+resultaten", text).group(1) if text else None
        print("Pagination indicator:", number)
        
        if not urls:
            print("No products found on this page. Ending crawl for category:", category_url)
            return False
        
        # Prepend base_url if needed.
        full_urls = []
        for url in urls:
            full_url = url if url.startswith("https://") else self.base_url + url
            full_urls.append(full_url)
        
        # Insert URLs into MongoDB, ensuring uniqueness.
        for full_url in full_urls:
            # Extract the product id from the URL using regex.
            id_match = re.search(r"wi(\d+)", full_url)
            product_id = id_match.group(1) if id_match else None
            
            document = {
                'url': full_url,
                'page': page,
                'category': category_url,
                'id': product_id
            }
            
            try:
                self.collection.insert_one(document)
                print(f"Inserted: {full_url} with id: {product_id}")
            except errors.DuplicateKeyError:
                print(f"Duplicate found, skipping: {full_url}")
                
        print(f"Completed {category_url} page {page} with {len(full_urls)} URLs processed.")
        
        # If pagination indicator is missing, end the crawl for the current category.
        if number is None:
            print("No pagination info found. Ending crawl for category:", category_url)
            return False
        
        return True

    def close(self):
        # Close the MongoDB connection.
        self.client.close()
        print("MongoDB connection closed.")

if __name__ == '__main__':
    crawler = Crawler()
    crawler.start()
    crawler.close()
