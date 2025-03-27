import logging
import requests
from parsel import Selector
from pymongo import MongoClient, errors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class Category:
    def __init__(self):
        # Set the base URL and headers for the request.
        self.base_url = "https://www.dirk.nl"
        self.url = f"{self.base_url}/boodschappen/zuivel-kaas"
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'priority': 'u=0, i',
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
        # Initialize MongoDB connection
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['dirk']
        self.collection = self.db['category_diary']
        # Create a unique index on the "url" field to ensure no duplicates
        self.collection.create_index("url", unique=True)
        # This will hold the parsed Selector if the request is successful
        self.selector = None

    def start(self):
        # Retrieve the webpage content using requests
        response = requests.get(self.url, headers=self.headers)
        if response.status_code == 200:
            self.selector = Selector(response.text)
            logging.info("Page retrieved successfully.")
        else:
            logging.error(f"Failed to retrieve the page. Status Code: {response.status_code}")
    
    def parse_items(self):
        # Ensure that the Selector is available
        if not self.selector:
            logging.error("No page content to parse. Please run start() first.")
            return
        
        # Extract the relative URLs using XPath
        category_links = self.selector.xpath("//div[@class='right']//a/@href").getall()
        # Add the base URL in front of each link
        full_urls = [self.base_url + link for link in category_links]
        
        # Insert the URLs into MongoDB only if they don't already exist
        for url in full_urls:
            try:
                # Use update with upsert to ensure uniqueness
                self.collection.update_one(
                    {"url": url},
                    {"$setOnInsert": {"url": url}},
                    upsert=True
                )
                logging.info(f"Inserted or already exists: {url}")
            except errors.DuplicateKeyError:
                logging.warning(f"Duplicate URL found, skipping: {url}")
        logging.info("All URLs have been processed for MongoDB storage.")
    
    def close(self):
        # Close the MongoDB connection
        self.client.close()
        logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    cat = Category()
    cat.start()
    cat.parse_items()
    cat.close()
