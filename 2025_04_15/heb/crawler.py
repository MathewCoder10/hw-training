import requests
import json
from pymongo import MongoClient, errors

class Crawler:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="heb_db"):
        # MongoDB connection and collections
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.category_collection = self.db.category_28
        self.crawler_collection = self.db.crawler_full_new_28
        self.crawler_collection.create_index("product_url", unique=True)
        
        # Cursor for category documents (with no_cursor_timeout)
        self.categories_cursor = self.category_collection.find(
            {},
            {"parentId": 1, "childId": 1},
            no_cursor_timeout=True
        )
        
        # In-memory set for unique URLs
        self.unique_urls = set()
        
        # Base configuration for cookies and headers
#         self.cookies = {
#     'HEB_AMP_DEVICE_ID': 'h-c79505c6-24a7-40ab-8a0b-375237ea6554',
#     'USER_SELECT_STORE': 'false',
#     'CURR_SESSION_STORE': '92',
#     'sessionContext': 'curbside',
#     'sst': 'hs:sst:rJrIe7XfO-oPOYCibAuQ7',
#     'sst.sig': 'bIAQNyLEqGBSUAamjiiDbxVCD12Tqt2C67LyPsJZrdg',
#     'visid_incap_2302070': 'U4Tc0q+HQf6I3cteZ8RkayBuCGgAAAAAQUIPAAAAAABj5+GlaiBhBfVGxBmQB917',
#     'AMP_MKTG_760524e2ba': 'JTdCJTdE',
#     '_ga': 'GA1.1.652251946.1745382952',
#     '_gcl_au': '1.1.417985956.1745382962',
#     'incap_ses_770_2302070': 'mut4f5fHUjjAkwcwIZevCsR+CGgAAAAAkt50kwNo5vdf2XcndgeOLA==',
#     'DYN_USER_ID': '19696140589',
#     'DYN_USER_CONFIRM': '82ea587baf1b374715e209a41a19ccf9',
#     'JSESSIONID': 'bNk4TJVUKFdCRkYiB3nTebIHyTWCSMDjWDQWkqrh',
#     'incap_ses_706_2302070': '7cFlXtCPohOVF2BwgTfMCbJXCmgAAAAAJm5RNCPD18xXgc+pqCbWUw==',
#     'reese84': '3:EYDJyhCTlV5E8SBDe+sIMw==:ZJuoBJv+UqnD+QlpyJMb4fsW/rEk9Rdmz6K4jhlR7xBfpnB2h0WBOcAHAtV9U5BrXDcN1igW1GhDRFan2Ecu/qlqfENtfTA5TityxlZlnJw+rLwOqEZOyNAzM3uENvOqVHrj7D8htAoXfFpGuWMuemJoONQa6htsAddLRr9dnBfajGH3LNENWtNG6bPWz9Y3E7ElWka1ryWlmgf9SBtHvhsyj5ScF3H02IPhD8vvxcsu+QG/2wcz8F3BFRr136x8FPigQH4qgUmpb3o8522tC0O/ra4lUjkPHTHm7pUgmp7Rbj5p7J4Q/he41nkM2uLtPsE1R1ypHQBUMugvLiPiI2MZicC+41o/8+/mvRiEfPp7U0TT0H4Avml75srkoq5whNPAj28njiRl5wDyYrSEK+/5BKg7Co84OqO+zNc+WZrmxXaNrsUsHFVFdlmkyENWQjo/v9TUSwZWe52T7bgQ61FluKLkjhdSo2FsO+DE4An5zTe0/blQqEku6KJjswyE:PCbmSZP8sjWfVhlfmHyKaaCjuIH6SbC0lgYcS2H8g50=',
#     'AWSALB': 'YE1arMyqsEJEHpPef0kGACFx4IdmhYp5A3lyVPoth/GP4lhsGRToRAnoFDu0Y+Lt3Hai6RBesXuFKb19Ndx0mRqwZGxdhZo/IqegHOS/V2P2gFXEqXgEqS15IyLk',
#     'OptanonConsent': 'isGpcEnabled=0&datestamp=Thu+Apr+24+2025+20%3A55%3A15+GMT%2B0530+(India+Standard+Time)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=911b8bed-d060-439d-950a-6dd838411dd7&interactionCount=0&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1&AwaitingReconsent=false',
#     '_ga_WKSH6HYPT4': 'GS1.1.1745508277.3.1.1745508317.0.0.0',
#     'AMP_760524e2ba': 'JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJoLWM3OTUwNWM2LTI0YTctNDBhYi04YTBiLTM3NTIzN2VhNjU1NCUyMiUyQyUyMnNlc3Npb25JZCUyMiUzQTE3NDU1MDgyNzU3MTIlMkMlMjJvcHRPdXQlMjIlM0FmYWxzZSUyQyUyMmxhc3RFdmVudFRpbWUlMjIlM0ExNzQ1NTA4MzE3ODU2JTJDJTIybGFzdEV2ZW50SWQlMjIlM0E1MyUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMCU3RA==',
# }
        self.base_headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-nextjs-data': '1',
        }
        self.domain_prefix = "https://www.heb.com"

    def start(self):
        """Iterate over categories and process each one."""
        try:
            for cat in self.categories_cursor:
                parentId = cat.get("parentId")
                childId = cat.get("childId")
                if not parentId or not childId:
                    print("Skipping a document without both parentId and childId.")
                    continue

                print(f"Processing category with parentId: {parentId}, childId: {childId}")
                # Construct referer and base URL for the category
                headers = self.base_headers.copy()
                headers["referer"] = f"https://www.heb.com/category/shop/fruit-vegetables/fruit/{parentId}/{childId}"
                base_url = f"https://www.heb.com/_next/data/93b02a8323a2031ffa489d005bf4883737edaba8/category/shop/{parentId}/{childId}.json"
                

                # Process pages for the given category
                self.parse_items(parentId, childId, base_url, headers)
        finally:
            self.close()

    def parse_items(self, parentId, childId, base_url, headers):
        """Paginate through a category's pages and store product URLs."""
        page = 1
        while True:
            params = {
                'page': str(page),
                'sct': '_H4sIAAAAAAAA/xWNuw7CIBSG34W5TQCx9HRTdGhcmhonY0wLxHYB5JJojO/ucfyv34dE7/Non8Wm3BvSEbud5MykqRs9sxq0gBo42FqylkuYKJdCk4qk7KP9D4CjWHwIq3so77J9ZXxRl3F/7g/H+9Cr02XAARLiGxMBlLYcjTWlYs0O20wK0VDYCCYkVCRMKQ8l6mVKVvnisEGREbxLyDRD9KbonEh3vX1/fHfqQcAAAAA=',
                'parentId': str(parentId),
                'childId': str(childId),
            }

            print(f"Fetching page {page} for category {parentId}/{childId}...")
            response = requests.get(base_url, params=params, headers=headers)
            
            if response.status_code != 200:
                print(f"Request failed with status code: {response.status_code} for {parentId}/{childId} page {page}")
                break

            try:
                data = response.json()
                # Extract product URLs from visual components
                components = data.get("pageProps", {}).get("layout", {}).get("visualComponents", [])
                for component in components:
                    for item in component.get("items", []):
                        url = item.get("productPageURL")
                        if url:
                            # Prepend domain prefix if missing
                            if not url.startswith(self.domain_prefix):
                                url = self.domain_prefix + url
                            
                            # Check for duplicates in memory
                            if url in self.unique_urls:
                                continue

                            try:
                                self.crawler_collection.insert_one({
                                    "parentId": parentId,
                                    "childId": childId,
                                    "product_url": url
                                })
                                self.unique_urls.add(url)
                                print(f"Stored URL: {url}")
                            except errors.DuplicateKeyError:
                                print(f"URL already exists in DB: {url}")

                # Determine if there is a next page
                next_page = data.get("pageProps", {}).get("_head", {}).get("next")
                if not next_page:
                    print(f"No next page found for category {parentId}/{childId}; moving to next category.")
                    break

                page += 1

            except json.JSONDecodeError:
                print(f"Invalid JSON received for category {parentId}/{childId} page {page}.")
                break

    def close(self):
        """Close the MongoDB cursor and client connection."""
        self.categories_cursor.close()
        self.client.close()


if __name__ == '__main__':
    crawler = Crawler()
    crawler.start()
    print("All product URLs have been processed and stored in the 'crawler_full_new' collection.")
