import requests
from parsel import Selector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

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
        # Collections: "Subcategory" for fetching URLs and "Crawler" for storing fetched links.
        self.subcategory_collection = self.db['Subcategory']
        self.crawler_collection = self.db['Crawler']
        # Create unique index on the "url" field in Crawler to avoid duplicate entries.
        self.crawler_collection.create_index("url", unique=True)

    def start(self):
        # Fetch all subcategory documents
        subcategories = self.subcategory_collection.find()
        for subcat in subcategories:
            # Assume the subcategory URL is stored under the key 'subcategory_url'
            url = subcat.get("subcategory_url", "")
            if not url.startswith(self.base_url):
                url = self.base_url + url
            print(f"Fetching product/category links from: {url}")
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
                    print(f"Stored URL in Crawler: {full_link}")
                except DuplicateKeyError:
                    print(f"URL already exists in Crawler: {full_link}")
        else:
            print(f"Failed to retrieve the page at {url}. Status Code: {response.status_code}")

    def close(self):
        print("Closing database connection.")
        self.client.close()

if __name__ == '__main__':
    crawler = Crawler()
    crawler.start()
    crawler.close()
