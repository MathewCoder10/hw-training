import requests
from parsel import Selector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

class CategoryScraper:
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
        self.collection = self.db['Category']
        # Create a unique index on the "url" field to ensure no duplicates.
        self.collection.create_index("url", unique=True)

    def start(self):
        print("Sending request to website...")
        response = requests.get('https://www.dirk.nl/boodschappen', headers=self.headers)
        if response.status_code == 200:
            print("Request successful. Parsing items...")
            self.parse_items(response.text)
        else:
            print(f"Failed to retrieve the page. Status Code: {response.status_code}")

    def parse_items(self, html_content):
        selector = Selector(html_content)
        category_links = selector.xpath("//article[@data-section='departments']//a/@href").getall()
        
        base_url = "https://www.dirk.nl"
        for link in category_links:
            if not link.startswith(base_url):
                full_link = base_url + link
            else:
                full_link = link

            category_data = {'url': full_link}
            try:
                self.collection.insert_one(category_data)
                print(f"Stored URL: {full_link}")
            except DuplicateKeyError:
                print(f"URL already exists: {full_link}")

    def close(self):
        print("Closing database connection.")
        self.client.close()

if __name__ == '__main__':
    scraper = CategoryScraper()
    scraper.start()
    scraper.close()
