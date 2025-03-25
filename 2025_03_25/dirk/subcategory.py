import requests
from parsel import Selector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

class Subcategory:
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
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['dirk']
        self.category_collection = self.db['Category']
        self.subcategory_collection = self.db['Subcategory']
        # Create a unique index on the "subcategory_url" field
        self.subcategory_collection.create_index("subcategory_url", unique=True)

    def start(self):
        base_url = "https://www.dirk.nl"
        categories = self.category_collection.find()
        for category in categories:
            # Assuming category documents have the URL stored in the 'url' key.
            url = category.get("url", "")
            if not url.startswith(base_url):
                url = base_url + url
            print(f"Fetching subcategories from: {url}")
            self.scrape_subcategories(url)

    def scrape_subcategories(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            selector = Selector(response.text)
            subcategory_links = selector.xpath("//div[@class='right']//a/@href").getall()
            base_url = "https://www.dirk.nl"
            for link in subcategory_links:
                if not link.startswith(base_url):
                    full_link = base_url + link
                else:
                    full_link = link

                subcategory_data = {
                    'category_url': url,
                    'subcategory_url': full_link
                }
                try:
                    self.subcategory_collection.insert_one(subcategory_data)
                    print(f"Stored Subcategory URL: {full_link}")
                except DuplicateKeyError:
                    print(f"Subcategory URL already exists: {full_link}")
        else:
            print(f"Failed to retrieve subcategories. Status Code: {response.status_code}")

    def close(self):
        print("Closing database connection.")
        self.client.close()

if __name__ == '__main__':
    scraper = Subcategory()
    scraper.start()
    scraper.close()
