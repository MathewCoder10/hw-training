import requests
import json
from pymongo import MongoClient, errors

class Crawler:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="heb_db"):
        # MongoDB connection and collections
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.category_collection = self.db.category
        self.crawler_collection = self.db.crawler_full_new
        
        # Ensure unique index on product_url if needed (uncomment if required)
        # self.crawler_collection.create_index("product_url", unique=True)
        
        # Cursor for category documents (with no_cursor_timeout)
        self.categories_cursor = self.category_collection.find(
            {},
            {"parentId": 1, "childId": 1},
            no_cursor_timeout=True
        )
        
        # In-memory set for unique URLs
        self.unique_urls = set()
        
        # Base configuration for cookies and headers
        self.cookies = {
            'HEB_AMP_DEVICE_ID': 'h-80dbe4dd-e046-4ec3-8ae4-2e60abfcb7ad',
            'USER_SELECT_STORE': 'false',
            'CURR_SESSION_STORE': '92',
            'sst': 'hs:sst:ANwwI-vkN3Rm81SJ9eobq',
            'sst.sig': 'CXUmXpu_OrqVnsOuAxpvoPuW4uOQkjRpP16tDygGVG4',
            'visid_incap_2302070': 'x1xKLuErSbmXQLA42R7/OkaW62cAAAAAQUIPAAAAAABbfEdUQA9C/v1QRTPMfTa6',
            '_ga': 'GA1.1.68092898.1743492693',
            '_gcl_au': '1.1.692784280.1743492699',
            'sessionContext': 'curbside',
            'incap_ses_769_2302070': 'NzNLVBEsigbv3sw0oAmsCinv+2cAAAAAvnxIBokflRtE5RTvycK9GQ==',
            'incap_ses_463_2302070': 'PQyEJWTXwkERH/mRQehsBntj/GcAAAAAanW6QhgS9CO0OW5uX2jcIA==',
            'incap_ses_6551_2302070': 'SzRwBbq96U9uatBhJdTpWryY/GcAAAAAjKaXnq/C/Pg5PxY4S0ygkw==',
            'incap_ses_710_2302070': 'GCsUSBT9FnLABXKAh23aCbLa/GcAAAAA9VIOO7iJXrPUDMWXA7UrHw==',
            'reese84': '3:izY5a4tS4ntNq0UrIHVMDg==:A8AhJ3WYaUlFxhlfU8PMNBQ0YQIO2N3FI8pAk9oi4OBIRm4UTPs/st2vwJEWz9wzjN0PN6KYcCQEklnWlFuAXYz3x5MdbXtHYBZKvdgVR/vOMg5hs19TdeB9zRghdEKTOxV0b8+3E5Y8nWLfrraOggtECwEY/ybTO9TW3y2SHp5EjyIGQtz2jirEeHldQOlINqanoCSyRwoge5aBgrmkS99VSbjchJK94dQBb0qoSH0ZlBa9XIhuh+6tz9EN9I0Y71VOwQA9HAhdkJYywf/ofKIvN6sphdb6z/SrbdjY9Z0Cdnl7UurVtOwsionOIA5yA0f/ssNYvF6PyF7JAEOXoKuNx/2xJI4OLxb6GjQNITSZn4r7Xev2D6upp8G5l1kj2AqvNlFiSHVQSJ0dEeRTA4yt3tdWHfSuD6/UB4e7jk5fiRYC+CXsRbiUVALRJNS573wAjwvAi50tOrVXLD6XWQ==:GEFDXqGQIyLtxUOSnI+YNzUNeF88oIKQWNtlMlbx5Gw=',
            'incap_ses_426_2302070': 'Kg2sfyrLTC+nqgFs8XTpBWHb/GcAAAAAYqbWlogOEECK5ommVnQc/Q==',
            'DYN_USER_ID': '19653205630',
            'DYN_USER_CONFIRM': '9b7d2e03882777d1922b1b59f2baface',
            'JSESSIONID': 'R0GicBAi7uO_96fgVi5JJotnnv8L4mXnLL2Fwtfv',
            'incap_ses_407_2302070': 'up9WbksuX12a7dSFjfSlBXPb/GcAAAAAM8w/Bp3AGFSinEZknsTJLA==',
            'AMP_MKTG_760524e2ba': 'JTdCJTIycmVmZXJyZXIlMjIlM0ElMjJodHRwcyUzQSUyRiUyRnd3dy5nb29nbGUuY29tJTJGJTIyJTJDJTIycmVmZXJyaW5nX2RvbWFpbiUyMiUzQSUyMnd3dy5nb29nbGUuY29tJTIyJTdE',
            'AWSALB': 'IHxQMAdGs9/orWoQRjAsI8j9Avd9W8E4zexX2u/2e6F/nXfN7GR9IN0NZXieBTm89HAN3LFfgTeWG/whF0KqhCpXoydihsafTD7g549eGOq6jAYGI6EwcfwEPdqk',
            '_ga_WKSH6HYPT4': 'GS1.1.1744624502.5.1.1744624514.0.0.0',
            'OptanonConsent': 'isGpcEnabled=0&datestamp=Mon+Apr+14+2025+15%3A25%3A17+GMT%2B0530+(India+Standard+Time)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=9b5ea69b-7b99-4bc3-8233-9673fa0a6f5f&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1&AwaitingReconsent=false',
            'AMP_760524e2ba': 'JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJoLTgwZGJlNGRkLWUwNDYtNGVjMy04YWU0LTJlNjBhYmZjYjdhZCUyMiUyQyUyMnNlc3Npb25JZCUyMiUzQTE3NDQ2MjQ1MDA4MTMlMkMlMjJvcHRPdXQlMjIlM0FmYWxzZSUyQyUyMmxhc3RFdmVudFRpbWUlMjIlM0ExNzQ0NjI0NTI2MDg3JTJDJTIybGFzdEV2ZW50SWQlMjIlM0ExNDAlMkMlMjJwYWdlQ291bnRlciUyMiUzQTAlN0Q=',
        }
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
                base_url = f"https://www.heb.com/_next/data/22b50a38e0e0961393883f53de7dad4818a32bee/category/shop/{parentId}/{childId}.json"
                
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
            response = requests.get(base_url, params=params, cookies=self.cookies, headers=headers)
            
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
