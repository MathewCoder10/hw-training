import requests
import time
import random
from parsel import Selector
from pymongo import MongoClient
from playwright.sync_api import sync_playwright

# MongoDB connection setup
client = MongoClient("mongodb://localhost:27017/")
db = client.sotheby_db

# Collections for errors and agent pages (terminal pages)
error_crawler = db.error_crawler
agent_crawler = db.agent_crawler

# Your cookies and headers (initial token value may expire)
cookies = {
    '_gcl_au': '1.1.680296884.1742468533',
    '_ga_07J12X0FK6': 'GS1.1.1742470714.2.0.1742470714.60.0.0',
    '_ga': 'GA1.1.1345707739.1742468533',
    'LanguagePreference': 'eng',
    '_fbp': 'fb.1.1742468534722.421802748275022315',
    'Currency': 'USD',
    'UnitSystem': 'Imperial',
    'notice_behavior': 'implied,eu',
    'LastLocationGetter': '{"data":{"SeoPart":"/180-a-df2211071327109171"}}',
    'ASP.NET_SessionId': 'o4r4aepwynp4x1gl3ppksjyt',
    'ResultsPerPage': '%7b%27sold%7cAgentDetails%27%3a+%2712%27%2c+%27sales%7cAgentDetails%27%3a+%2715%27%7d',
    'sir_mp': 'is_mp=1|mp_agent=180-a-df2211071327109171',
    'aws-waf-token': '79c92352-9a3e-4d68-97d0-24f804a3973c:EgoAkuIwSFCsAAAA:Kd04uPin2TI58PpJt15yrV8IOPHucEgUTzZyDXlUzr+NRIbvZhb7valD5EOF/v/pHKD9ViYe03pNEJ9/0jwDs04QY8yzw+WYqx0FmbapA/Oj1FgPgR7zkhBWZIkWCG1jdlpvKp810aPaKCM+QsHsbIywiXKssAtxgbARCMj9Bifa/Otvw4IE3qcLH5+DE1ndmexT',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.sothebysrealty.com/eng/sitemap/associates',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=0, i',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}

# XPath for first-level filtering (USA or CAN URLs)
XPATH_FILTERED_LINKS = ("//div[@class='sitemap__container--links']//div/a["
                        "substring(@href, string-length(@href)-string-length('usa')+1)='usa' "
                        "or substring(@href, string-length(@href)-string-length('can')+1)='can']/@href")

# XPath to extract all URLs for subsequent crawls
XPATH_ALL_LINKS = "//div[@class='sitemap__container--links']//div/a/@href"

def fetch_new_aws_waf_token(url):
    """
    Uses Playwright to open the given URL and extract the updated aws-waf-token cookie.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto(url, timeout=15000)  # adjust timeout if needed
        # Allow some time for cookies to be set
        page.wait_for_timeout(3000)
        cookies_list = context.cookies()
        browser.close()
    
    # Find the aws-waf-token cookie among the cookies
    token_cookie = next((cookie for cookie in cookies_list if cookie['name'] == 'aws-waf-token'), None)
    if token_cookie:
        return token_cookie['value']
    else:
        raise Exception("aws-waf-token not found in cookies.")

def process_urls(source_collection, target_collection, level, base_url):
    """
    Process URLs from source_collection and store results in target_collection.
    - Level 1: Use the filtered XPath (USA and CAN).
    - Subsequent levels: Extract all URLs.
    """
    for doc in source_collection.find({}):
        url = doc.get("url")
        print(f"Processing: {url}")
        
        try:
            response = requests.get(url, cookies=cookies, headers=headers, timeout=10)
        except Exception as e:
            error_crawler.insert_one({"url": url, "error": str(e)})
            continue
        
        # If token might be expired (e.g., status 202), update the token and retry
        if response.status_code == 202:
            print("Received 202 status code. Refreshing AWS WAF token...")
            try:
                new_token = fetch_new_aws_waf_token(base_url)
                cookies['aws-waf-token'] = new_token
                print("Token refreshed. Retrying request...")
                response = requests.get(url, cookies=cookies, headers=headers, timeout=10)
            except Exception as e:
                error_crawler.insert_one({"url": url, "error": f"Token refresh failed: {str(e)}"})
                continue
        
        if response.status_code != 200:
            error_crawler.insert_one({"url": url, "status": response.status_code})
            continue
        
        sel = Selector(response.text)
        xpath_to_use = XPATH_FILTERED_LINKS if level == 1 else XPATH_ALL_LINKS
        extracted_urls = sel.xpath(xpath_to_use).extract()
        # Prepend "https:" if necessary
        extracted_urls = ["https:" + u if u.startswith("//") else u for u in extracted_urls]
        
        if extracted_urls:
            for new_url in extracted_urls:
                if not target_collection.find_one({"url": new_url}):
                    target_collection.insert_one({"url": new_url})
        else:
            agent_crawler.insert_one({"url": url})
        
        # Random delay to avoid overwhelming the server
        time_delay = random.uniform(1, 3)
        print(f"Sleeping for {time_delay:.2f} seconds...")
        time.sleep(time_delay)

def dynamic_crawl(start_url, max_levels=10):
    """
    Perform a multi-level crawl starting with the initial URL.
    Level 1 stores only 'usa' and 'can' URLs. 
    Subsequent levels store all found links.
    """
    level = 1
    start_collection_name = f"crawler_{level}"
    start_collection = db[start_collection_name]
    start_collection.delete_many({})
    start_collection.insert_one({"url": start_url})
    
    # Use the start_url as the base URL for token refresh purposes
    base_url = start_url

    while level < max_levels:
        current_collection = db[f"crawler_{level}"]
        next_collection = db[f"crawler_{level + 1}"]
        next_collection.delete_many({})
        print(f"\n=== Crawling Level {level} to Level {level+1} ===")
        process_urls(current_collection, next_collection, level, base_url)
        
        count_new = next_collection.count_documents({})
        print(f"Found {count_new} new URLs at level {level+1}")
        if count_new == 0:
            print("No new URLs found. Ending crawl.")
            break
        level += 1

# Start the crawl with the initial URL
initial_url = "https://www.sothebysrealty.com/eng/sitemap/associates"
dynamic_crawl(initial_url, max_levels=10)
