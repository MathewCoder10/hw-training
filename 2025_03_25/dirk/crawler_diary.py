import logging
import requests
from parsel import Selector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class Crawler:
    def __init__(self):
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
        self.base_url = "https://www.dirk.nl"
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['dirk']
        # Use "category_diary" collection for fetching category URLs
        self.category_collection = self.db['category_diary']
        # "crawler_diary" collection for storing fetched product/category links.
        self.crawler_collection = self.db['crawler_diary']
        # Create unique index on the "url" field in crawler_collection to avoid duplicate entries.
        self.crawler_collection.create_index("url", unique=True)

    def start(self):
        # Fetch all category documents from category_diary
        categories = self.category_collection.find()
        for category in categories:
            # Assume the category URL is stored under the key 'url'
            url = category.get("url", "")
            logging.info(f"Fetching product/category links from: {url}")
            self.parse_items(url)

    def parse_items(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            selector = Selector(response.text)
            # Extract links from the product cards section
            links = selector.xpath("//div[@class='product-cards']//a[@class='top']/@href").getall()
            for link in links:
                # Prepend base_url if not present
                if not link.startswith(self.base_url):
                    full_link = self.base_url + link
                else:
                    full_link = link

                crawler_data = {'url': full_link}
                try:
                    self.crawler_collection.insert_one(crawler_data)
                    logging.info(f"Stored URL in Crawler: {full_link}")
                except DuplicateKeyError:
                    logging.warning(f"URL already exists in Crawler: {full_link}")
        else:
            logging.error(f"Failed to retrieve the page at {url}. Status Code: {response.status_code}")

    def close(self):
        logging.info("Closing database connection.")
        self.client.close()

if __name__ == '__main__':
    crawler = Crawler()
    crawler.start()
    crawler.close()
