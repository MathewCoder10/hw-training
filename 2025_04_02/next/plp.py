import requests
import pymongo
import json

# Connect to MongoDB (adjust the connection string as needed)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["next_db"]
crawler_col = db["category"]
parser_col = db["plp_update"]

# Prepare headers for the API request
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.next.co.uk/shop/gender-women-productaffiliation-clothing/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'x-next-correlation-id': '599a4450-0fa8-11f0-9e91-118e8403de73',
    'x-next-language': 'en',
    'x-next-persona': 'DPlatform',
    'x-next-realm': 'next',
    'x-next-realm-entrypoint': 'next',
    'x-next-session-id': '',
    'x-next-siteurl': 'https://www.next.co.uk',
    'x-next-tabbed-navigation': '',
    'x-next-territory': 'GB',
    'x-next-viewport-size': 'desktop, desktop',
}

# Define the base URL template with a placeholder for dynamic criteria
base_url = (
    "https://www.next.co.uk/products-fragment?criteria={}&providerArgs=_br_var_2&start=0&pagesize=84"
    "&contextid=uid%3D4319034634127:v%3D13.0:ts%3D1743575630169:hc%3D17&type=Category&fields="
    "items,filters,totalResults,sorting,title,relaxedQuery,includedComponents,searchCategory,"
    "templateOverride,searchBanner&segment=unclassified&pageLoadTrigger=FILTER&sliceSize=12&"
    "productSummaryTemplateName=&imageRatio=&searchTerm=&showSearchProviderRequestUrl=false"
)

# Iterate through each document in the crawler collection
for crawler_doc in crawler_col.find({}):
    original_url = crawler_doc.get("url", "")
    if not original_url:
        print("Skipping document without a URL field.")
        continue

    # Clean the URL by removing "https://"
    cleaned_url = original_url.replace("https://", "")
    # Build dynamic criteria from the cleaned URL (adjust the format as needed)
    criteria = cleaned_url

    # Build the full request URL
    request_url = base_url.format(criteria)
    print(f"Processing URL: {original_url}")
    
    # Make the GET request
    response = requests.get(request_url, headers=headers)
    print("Status Code:", response.status_code)

    if response.status_code == 200:
        try:
            decoder = json.JSONDecoder()
            json_text = response.text
            data, idx = decoder.raw_decode(json_text)
            remainder = json_text[idx:].strip()
            if remainder:
                print("Extra data found after the first JSON object.")
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
            continue

        # Extract totalResults and facets from the response
        total_results = data.get("totalResults", 0)
        facets = data.get("facets", {})

        # Get additional fields from the crawler document
        breadcrumb = crawler_doc.get("breadcrumb")
        category = crawler_doc.get("category")

        # For each facet, insert a document into the parser collection
        for facet_key, facet_info in facets.items():
            filter_type = facet_info.get("n")
            filter = facet_info.get("v")
            count = facet_info.get("c")

            parser_document = {
                "url": original_url,
                "breadcrumb": breadcrumb,
                "category_name": category,
                "count": count,
                "filter_type": filter_type,   
                "filter" : filter,               
                "totalResults": total_results      
            }
            parser_col.insert_one(parser_document)
        print("Data inserted into parser collection successfully for this URL.")
    else:
        print("Request failed with status code", response.status_code)
