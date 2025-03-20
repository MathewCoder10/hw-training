import requests
import logging
from parsel import Selector
from urllib.parse import urljoin
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class Crawler:
    def __init__(self):
        # Set up cookies and headers
        self.cookies = {
            '_fbp': 'fb.1.1742376208685.752910496336488466',
            '_gcl_au': '1.1.784343115.1742376209',
            'notice_behavior': 'implied,us',
            '_ga': 'GA1.1.2064516614.1742376221',
            'LanguagePreference': 'eng',
            'sir_mp': 'is_mp=1|mp_agent=180-a-df21030305521031362',
            'ASP.NET_SessionId': 'nnuierbn35w5uqhoe0qfnlec',
            'ResultsPerPage': '%7b%27sales%7cAgentDetails%27%3a+%2715%27%7d',
            'LastLocationGetter': '{"data":{"SeoPart":"/int"}}',
            'TAsessionID': '5718afbf-418f-4a7d-a9b8-8408a4b142b8|NEW',
            '_ga_07J12X0FK6': 'GS1.1.1742383940.2.0.1742383940.60.0.0',
            'aws-waf-token': 'f1979b59-daab-485a-8d74-c21deb1dbb7a:BgoAox9EFJUGAAAA:kvrcH4ND+xhAhIeMK9862jhMyF/MFR4no3QiihFdvjyqrWtrsu89Ycq7AlGpgd9XSxwNz5Y+OGFuNaMRHK0Q+LHXhlkkVqNrR32MW6/B7BMWdghji16XJvm6QuabT0gg+V6vDh3FgmqlgHqH4IriAx0g0QYDgIm7nV4e74WuyqmHDwM9hmJCKQ3cnrOa5kOAaWnXHI/Kb3KUB6NhHODcLrB8YA==',
        }
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'referer': 'https://www.sothebysrealty.com/eng/associates/int',
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
        
        # MongoDB connection and collection
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['sothebysrealty_db']
        self.collection = self.db['crawler']
        
        # Create a unique index on the "url" field
        self.collection.create_index("url", unique=True)
        
        # Base URL and starting URL
        self.base_url = 'https://www.sothebysrealty.com'
        self.start_url = f'{self.base_url}/eng/associates/int'
        self.final_page_num = 1

    def start(self):
        # Get the final page number from the starting URL
        response = requests.get(self.start_url, cookies=self.cookies, headers=self.headers)
        sel = Selector(response.text)

        # XPATH expression to extract final page number
        FINAL_PAGE_TEXT_XPATH = '//div[contains(@class, "pagination-container")]//a[not(contains(@aria-label, "Prev")) and not(contains(@aria-label, "Next"))][last()]/@aria-label'
        final_page_text = sel.xpath(FINAL_PAGE_TEXT_XPATH).extract_first()

        self.final_page_num = int(final_page_text.split()[-1]) if final_page_text else 1
        logging.info("Final page number found: %s", self.final_page_num)

        # Loop through each page and process items
        for page in range(1, self.final_page_num + 1):
            url = self.start_url if page == 1 else f'{self.base_url}/eng/associates/int/{page}-pg'
            logging.info("Scraping page: %s", url)
            
            response = requests.get(url, cookies=self.cookies, headers=self.headers)
            sel = Selector(response.text)
            profile_urls = self.parse_items(sel)
            
            # Store each profile URL in MongoDB
            for agent_url in profile_urls:
                full_url = urljoin(self.base_url, agent_url)
                result = self.collection.update_one(
                    {'url': full_url},
                    {'$set': {'url': full_url}},
                    upsert=True
                )
                if result.matched_count > 0:
                    logging.info("Duplicate URL found and updated: %s", full_url)
                else:
                    logging.info("Storing URL: %s", full_url)
        
        # Close MongoDB connection after scraping is done
        self.close()

    def parse_items(self, selector):
        # Extract profile URLs from the page
        return selector.xpath(
            '//div[contains(@class, "m-agent-item-results__card")]//a[contains(@class, "m-agent-item-results__card-photo")]/@href'
        ).getall()

    def close(self):
        # Close MongoDB connection
        self.client.close()
        logging.info("Scraping completed and MongoDB connection closed.")

if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
