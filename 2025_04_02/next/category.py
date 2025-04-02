import requests
from pymongo import MongoClient

# Define the headers for the GET request.
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.next.co.uk/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'x-next-correlation-id': 'a6054020-0f8e-11f0-a918-718824c661ec',
    'x-next-language': 'en',
    'x-next-persona': 'DPlatform',
    'x-next-realm': 'next',
    'x-next-session-id': '',
    'x-next-siteurl': 'https://www.next.co.uk',
    'x-next-territory': 'GB',
    'x-next-viewport-size': 'desktop, desktop',
}

# Make the GET request.
response = requests.get('https://www.next.co.uk/secondary-items/home/women', headers=headers)
print("Response status code:", response.status_code)

# Parse the response as JSON.
data = response.json()

# Define the target titles we are interested in.
TARGET_TITLES = [
    "CLOTHING",
    "DRESSES",
    "WORKWEAR & TAILORING",
    "LINGERIE & NIGHTWEAR",
    "ACCESSORIES",
    "FOOTWEAR",
    "BEAUTY",
    "SHOP BY SIZE TYPE",
    "LUXURY BRANDS",
    "SHOP BY BRAND"
]

def extract_target_items(data):
    """
    Recursively traverse the JSON structure.
    If a dictionary contains a 'title' key with a value that is one of the target titles,
    add that dictionary to the results.
    """
    results = []
    if isinstance(data, dict):
        if "title" in data and isinstance(data["title"], str) and data["title"] in TARGET_TITLES:
            results.append(data)
        for value in data.values():
            results.extend(extract_target_items(value))
    elif isinstance(data, list):
        for item in data:
            results.extend(extract_target_items(item))
    return results

# Extract dictionaries with the target "title" values.
target_items = extract_target_items(data)

# MongoDB connection parameters (adjust connection string as needed)
client = MongoClient("mongodb://localhost:27017/")
db = client["next_db"]
collection = db["category"]

# Base URL to prepend to every subcategory target.
base_url = "https://www.next.co.uk"

# Prepare list to hold MongoDB documents.
documents = []

# Each item in target_items represents a main category.
for main_category_dict in target_items:
    main_category = main_category_dict.get("title")
    subitems = main_category_dict.get("items", [])
    # For each subcategory, create a document.
    for item in subitems:
        subcategory = item.get("title")
        target_url = item.get("target", "")
        # Build the MongoDB document.
        doc = {
            "breadcrumb": f"{main_category}>{subcategory}",
            "url": base_url + target_url,
            "category": subcategory
        }
        documents.append(doc)

# Insert documents into MongoDB.
if documents:
    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} records into the 'category' collection.")
else:
    print("No documents found to insert.")
