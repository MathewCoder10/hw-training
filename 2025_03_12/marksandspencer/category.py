import requests
from parsel import Selector
from pymongo import MongoClient

class Category:
    def __init__(self):
        self.headers = {
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
        self.base_url = 'https://www.marksandspencer.com'
        self.url = self.base_url
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['mands']
        self.collection = self.db['category']
        self.categories = []

    def start(self):
        response = requests.get(self.url, headers=self.headers)
        if response.status_code == 200:
            selector = Selector(text=response.text)
            category_urls = selector.xpath("//div[@data-tab-id='SC_Level_1_1'][.//p[normalize-space()='Women']]//a[normalize-space()='Clothing']/parent::div/following-sibling::ul/li/a/@href").getall()
            category_names = selector.xpath("//div[@data-tab-id='SC_Level_1_1'][.//p[normalize-space()='Women']]//a[normalize-space()='Clothing']/parent::div/following-sibling::ul/li/a/text()").getall()
            for name, category in zip(category_names, category_urls):
                self.parse_items(name.strip(), category)
        else:
            print(f"Failed to fetch page, status code: {response.status_code}")

    def parse_items(self, name, category):
        category_full_url = self.base_url + category
        category_url = category_full_url.split('#')[0]  # Remove URL fragment if any
        self.categories.append({
            'category_name': name,
            'category_full_url': category_full_url,
            'category_url': category_url
        })

    def close(self):
        if self.categories:
            self.collection.insert_many(self.categories)
        self.client.close()
        for category in self.categories:
            print(category)

if __name__ == "__main__":
    category = Category()
    category.start()
    category.close()
