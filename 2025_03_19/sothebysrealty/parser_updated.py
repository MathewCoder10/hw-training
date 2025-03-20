import logging
import json
import requests
from parsel import Selector
from pymongo import MongoClient

class Parser:
    """Parser class to fetch URLs from the database, request pages, parse data, and store results."""

    def __init__(self):
        # Set up cookies and headers
        self.cookies = {
    'LanguagePreference': 'eng',
    'Currency': 'USD',
    'UnitSystem': 'Imperial',
    'notice_behavior': 'implied,us',
    'currentSearchQuery': 'int%2F180-a-df21030305521031362-agentid',
    '_fbp': 'fb.1.1742365922548.199578295208308822',
    '_gcl_au': '1.1.1031560927.1742365923',
    '_ga': 'GA1.1.953117992.1742365923',
    '_ga_0QLKQS5MQZ': 'GS1.1.1742365922.1.1.1742366437.0.0.0',
    'ASP.NET_SessionId': 'ennoypuy5li41elqxqgimxyc',
    'ResultsPerPage': '%7b%27sold%7cAgentDetails%27%3a+%2712%27%2c+%27sales%7cAgentDetails%27%3a+%2715%27%7d',
    'aws-waf-token': 'b78885a9-1d2a-4899-bf3a-9300985a467a:EwoA3/9H9f1MAAAA:1nIUuomn1NO4TZpR7EDhKWLJZuqThILgU6jJjctO2HIS09ESwtw5qJBVv28dioZWSPqdtFYhFcx9MRuu5Yq5R/bj2D2PBBNO++w8n96An3sWUmsM7ubjWCE37tNb5EWuc09V7ohqvVMAQKYZTGh8EZcpoCeACOBnvw07I4pWbCxG1CC0x+fTiyLy4C7ZWqOCt7ooXeQ=',
    'LastLocationGetter': '{"data":{"SeoPart":"/180-a-560-4026143"}}',
    '_ga_07J12X0FK6': 'GS1.1.1742465326.7.1.1742466200.60.0.0',
    'TAsessionID': '6cc62e2e-4271-4fb6-9de1-d62c54f35d91|NEW',
    'sir_mp': 'is_mp=1|mp_agent=180-a-560-4026143',
        }
        self.headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.sothebysrealty.com/eng/associate/180-a-560-4026143/carmen-baskind',
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
        
        # MongoDB connection and collections
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['sothebysrealty_db']
        # The 'crawler' collection stores the URLs to be processed
        self.crawler_collection = self.db['crawler']
        # The 'parser' collection is where parsed items are stored
        self.parser_collection = self.db['parser']

    def start(self):
        """Fetch URLs from the 'crawler' collection and process them"""
        metas = list(self.crawler_collection.find({}))
    
        for meta in metas:
            url = meta.get('url')
            if not url:
                continue  # Skip if URL is missing
            
            try:
                response = requests.get(url, headers=self.headers, cookies=self.cookies)
            except Exception as e:
                logging.error(f"Error fetching {url}: {e}")
                continue

            if response.status_code == 200:
                self.parse_item(url, response)
            else:
                logging.error(f"Non-200 response for {url}: {response.status_code}")

    def parse_item(self, url, response):
        """Parse the response from a URL and insert the structured item into the parser collection."""
        sel = Selector(text=response.text)

        # XPATH expressions
        AGENT_NAME_XPATH = '//h1[contains(@class, "Hero__agent-name")]/text()'
        OFFICE_NAME_XPATH = '//div[@class="m-listing-contact-info__office-name"]/a/text()'
        TITLE_XPATH = '//div[contains(@class, "agent__agent-title u-text-uppercase")]/text()'
        DESCRIPTION_XPATH = '//h4[contains(@class, "agent__long-video-description")]/text()'
        LANGUAGES_XPATH = '//p[normalize-space(.)="Languages"]/following-sibling::h3/text()'
        IMAGE_XPATH = '//div[contains(@class, "Hero__agent-image")]//img/@src'
        EMAIL_XPATH = '//div[@class="m-listing-contact-info__agent-email"]/a/@href'
        WEBSITE_XPATH = '//a[contains(text(), "Personal Website")]/@href'
        PHONE_XPATH = '//li[@class="m-listing-contact-info__agent-phone"]/a/span/text()'
        SOCIAL_XPATH = '//ul[contains(@class, "m-listing-contact-info__agent-networks")]/li/a'
        ADDRESS_XPATH = '//script[@id="__NEXT_DATA__"]/text()'
        
        # EXTRACT DATA
        agent_name = sel.xpath(AGENT_NAME_XPATH).extract_first()
        office_name = sel.xpath(OFFICE_NAME_XPATH).extract_first()
        title = sel.xpath(TITLE_XPATH).extract_first()
        description = sel.xpath(DESCRIPTION_XPATH).extract_first()
        languages = sel.xpath(LANGUAGES_XPATH).extract()
        image_url = sel.xpath(IMAGE_XPATH).extract_first()
        email = sel.xpath(EMAIL_XPATH).extract_first()
        website = sel.xpath(WEBSITE_XPATH).extract_first()
        phone_numbers = sel.xpath(PHONE_XPATH).extract()
        anchors = sel.xpath(SOCIAL_XPATH)
        next_data = sel.xpath(ADDRESS_XPATH).extract_first()
        
        # Process phone numbers
        agent_phone_numbers, office_phone_numbers = self.extract_phone_numbers(phone_numbers)

        # Split the agent's full name
        name_parts = self.split_agent_name(agent_name or "")

        # Process embedded JSON address if available
        address_data = {}
        if next_data:
            try:
                data = json.loads(next_data)
                modules = (data.get("props", {})
                             .get("pageProps", {})
                             .get("initialState", {})
                             .get("PageStore", {})
                             .get("modules", []))
                addr_obj = self.find_address_with_country(modules)
                if addr_obj:
                    address_data = self.format_address(addr_obj)
            except Exception as e:
                logging.error(f"Error parsing address JSON: {e}")

        # Process social links
        social_links = {}
        for link in anchors:
            text = link.xpath("normalize-space(text())").get()
            href = link.xpath("@href").get()
            if text and href:
                social_links[text] = href

        # Map social links to the desired keys
        facebook_url = social_links.get("Facebook", "")
        twitter_url = social_links.get("X", social_links.get("Twitter", ""))
        linkedin_url = social_links.get("LinkedIn", "")
        other_urls = [v for k, v in social_links.items() if k not in ("Facebook", "X", "Twitter", "LinkedIn")]
        social = {
            "facebook_url": facebook_url,
            "twitter_url": twitter_url,
            "linkedin_url": linkedin_url,
            "other_urls": other_urls
        }

        # Build the final item
        item = {
            "first_name": name_parts.get("first_name"),
            "middle_name": name_parts.get("middle_name"),
            "last_name": name_parts.get("last_name"),
            "office_name": office_name,
            "title": title,
            "description": description,
            "languages": languages,
            "image_url": image_url,
            "address": address_data.get("address", ""),
            "city": address_data.get("city", ""),
            "state": address_data.get("state", ""),
            "country": address_data.get("country", ""),
            "zipcode": address_data.get("zipcode", ""),
            "office_phone_numbers": office_phone_numbers,
            "agent_phone_numbers": agent_phone_numbers,
            "email": email,
            "website": website,
            "social": social,
            "profile_url": url
        }

        logging.info(item)
        try:
            self.parser_collection.insert_one(item)
        except Exception as e:
            logging.error(f"Error inserting item into parser collection: {e}")

    @staticmethod
    def extract_phone_numbers(phone_list):
        """Extract agent and office phone numbers from a list of strings."""
        agent_numbers = []
        office_numbers = []
        for entry in phone_list:
            entry = entry.strip()
            if entry.startswith("M:"):
                number = entry.split("M:")[1].strip()
                agent_numbers.append(number)
            elif entry.startswith("O:"):
                number = entry.split("O:")[1].strip()
                office_numbers.append(number)
        return agent_numbers, office_numbers

    @staticmethod
    def split_agent_name(full_name):
        """
        Splits the full name into first, middle, and last names.
        - If only one word is present, it's the first name.
        - If two words are present, they are first and last.
        - If three or more words are present, the first word is first name, the second is middle name,
          and the rest form the last name.
        """
        parts = full_name.strip().split()
        if len(parts) == 0:
            return {"first_name": "", "middle_name": "", "last_name": ""}
        elif len(parts) == 1:
            return {"first_name": parts[0], "middle_name": "", "last_name": ""}
        elif len(parts) == 2:
            return {"first_name": parts[0], "middle_name": "", "last_name": parts[1]}
        else:
            first_name = parts[0]
            middle_name = parts[1]
            last_name = " ".join(parts[2:])
            return {"first_name": first_name, "middle_name": middle_name, "last_name": last_name}

    @staticmethod
    def find_address_with_country(modules):
        """
        Recursively search for a dictionary in modules that contains an "addrcountry" key.
        """
        if isinstance(modules, list):
            for module in modules:
                result = Parser.find_address_with_country(module)
                if result:
                    return result
        elif isinstance(modules, dict):
            if "addrcountry" in modules:
                return modules
            for key, value in modules.items():
                result = Parser.find_address_with_country(value)
                if result:
                    return result
        return {}

    @staticmethod
    def format_address(addr_obj):
        """
        Extract common address fields and combine parts as needed.
        Returns a unified dictionary with keys: address, city, state, country, zipcode.
        """
        main_addr = addr_obj.get("AddrDisplayMemberSerialization", {}).get("_text", "").strip()
        additional = addr_obj.get("address", {}).get("_cdata", "").strip()
        full_address = f"{main_addr} {additional}".strip() if additional else main_addr

        city = addr_obj.get("addrcity", {}).get("_text", "").strip()
        state = addr_obj.get("addrstateabbr", {}).get("_text", "").strip()
        if not state:
            state = addr_obj.get("addrstate", {}).get("_text", "").strip()
        country = addr_obj.get("addrcountry", {}).get("_text", "").strip()
        zipcode = addr_obj.get("addrzip", {}).get("_text", "").strip()
        return {
            "address": full_address,
            "city": city,
            "state": state,
            "country": country,
            "zipcode": zipcode
        }

    def close(self):
        """Close the MongoDB connection"""
        self.client.close()

if __name__ == "__main__":
    parser_obj = Parser()
    parser_obj.start()
    parser_obj.close()
