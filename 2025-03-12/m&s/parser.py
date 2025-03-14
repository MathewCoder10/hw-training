import json
import re
import requests
from parsel import Selector

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.marksandspencer.com/l/women/knitwear/cardigans',
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

params = {'color': 'OATMEAL'}

response = requests.get(
    'https://www.marksandspencer.com/textured-button-front-cardigan/p/clp60738484',
    params=params,
    headers=headers,
)
print("Status Code:", response.status_code)

selector = Selector(response.text)

gender = "Women"
print(f"gender:{gender}")

brand = selector.xpath('//p[@class="media-0_textSm__Q52Mz brand-title_title__u6Xx5 media-0_strong__aXigV"]//text()').get()
print(f"brand:{brand}")

product_name = selector.xpath('//h1[@class="media-0_headingSm__aysOm"]//text()').get()
print(f"product_name:{product_name}")

rating_text = selector.xpath("//div[contains(@class, 'star-rating_wrapper__QmDBj')]/@aria-label").get()
match = re.search(r'(\d+\.\d+)', rating_text)  
rating_value = match.group(1) if match else "N/A"
print(f"rating:{rating_value}")

review_text = selector.xpath('//span[@class="media-0_textSm__Q52Mz media-0_strong__aXigV"]//text()').get()
review_digit = re.search(r'\d+', review_text).group()
print(f"review:{review_digit}")

image_urls = selector.xpath("//ul[@class='image-gallery_slides__N8x_w']//img/@src").getall()

# Ensure there are at least three URLs available
if len(image_urls) >= 3:
    image_url_1 = image_urls[0]
    image_url_2 = image_urls[1]
    image_url_3 = image_urls[2]
else:
    image_url_1 = image_urls[0] if len(image_urls) > 0 else None
    image_url_2 = image_urls[1] if len(image_urls) > 1 else None
    image_url_3 = image_urls[2] if len(image_urls) > 2 else None

print(f"image_url_1:{image_url_1}")
print(f"image_url_2:{image_url_2}")
print(f"image_url_3:{image_url_3}")

product_code = selector.xpath('(//p[@class="media-0_textXs__ZzHWu"]//text())[2]').get()
print(f"competitor_product_key:{product_code}")

product_details = selector.xpath("//p[text()='About this style']/following-sibling::p[1]/text()").get()
print(f"product_description:{product_details}")

care = selector.xpath("//p[text()='Care']/following-sibling::div//p[@class='media-0_textSm__Q52Mz product-details_careText__48dt5']/text()").getall()
print(f"care_instructions:{care}")

material_composition = selector.xpath("//p[text()='Composition']/following-sibling::p[1]/text()").get()
print(f"material_composition:{material_composition}")

fit_and_style = selector.xpath("//p[normalize-space(text())='Fit and style' or normalize-space(text())='Style']/following-sibling::div//p[contains(@class, 'product-details_dimension')]/text()").getall()
print(f"style:{fit_and_style}")

breadcrumb_items = selector.xpath("//nav[@aria-label='breadcrumb']//a/text()").getall()
print("breadcrumb:", "/".join(breadcrumb_items))

producthierarchy_level1 = breadcrumb_items[0] if len(breadcrumb_items) > 0 else ''
producthierarchy_level2 = breadcrumb_items[1] if len(breadcrumb_items) > 1 else ''
producthierarchy_level3 = breadcrumb_items[2] if len(breadcrumb_items) > 2 else ''
producthierarchy_level4 = breadcrumb_items[3] if len(breadcrumb_items) > 3 else ''
producthierarchy_level5 = breadcrumb_items[4] if len(breadcrumb_items) > 4 else ''

print("producthierarchy_level1:", producthierarchy_level1)
print("producthierarchy_level2:", producthierarchy_level2)
print("producthierarchy_level3:", producthierarchy_level3)
print("producthierarchy_level4:", producthierarchy_level4)
print("producthierarchy_level5:", producthierarchy_level5)

script_content = selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
data = json.loads(script_content)

# Navigate to the correct path
variants = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("variants", [])
size_guide = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("linkToSizeGuide", '')
print(size_guide)
fit = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("fit", '')
print(f"clothing_fit:{fit}")
instructions = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("usageInstructions", '')
print(f"instructions:{instructions}")
features = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("productfeaturesInnovations", '')
print(f"features:{features}")

# Function to remove HTML tags using regex
def clean_html(raw_html):
    clean_text = re.sub(r"<[^>]+>", " ", raw_html)  # Remove HTML tags
    return re.sub(r"\s+", " ", clean_text).strip()  # Remove extra spaces

# Extract details for each variant SKU
# Extract details for each variant SKU
for variant in variants:
    product_id = variant.get("id")
    colour = variant.get("colour")
    skus = variant.get("skus", [])
    
    for sku in skus:
        sku_id = sku.get("id")
        primary_size = sku.get("size", {}).get("primarySize")
        secondarySize = sku.get("size", {}).get("secondarySize")
        quantity = sku.get("inventory", {}).get("quantity")
        quantity_on_hand = sku.get("inventory", {}).get("quantityOnHand")
        currency_prefix = sku.get("price", {}).get("currencyPrefix")
        current_price = sku.get("price", {}).get("currentPrice")
        previous_price = sku.get("price", {}).get("previousPrice")
        unit_price = sku.get("price", {}).get("unitPrice")
        offer_text = sku.get("price", {}).get("offerText")
        
        # Determine instock status based on quantity
        instock = "outofstock" if quantity < 1 else "instock"
        
        # Set selling, regular, and promotion prices based on previous_price
        selling_price = current_price
        if previous_price is not None:
            regular_price = previous_price
            promotion_price = selling_price
        else:
            regular_price = selling_price
            promotion_price = ""
        
        # Extract promotions separately
        promotions = sku.get("price", {}).get("promotions", [])
        promotions_name = []
        promotions_description = []
        promotions_long_description = []

        for promo in promotions:
            promo_name = promo.get("name", "N/A")
            promo_description = promo.get("description", "N/A")
            promo_long_description = promo.get("longDescription", "N/A")

            # Clean HTML content using regex
            clean_long_description = clean_html(promo_long_description)

            promotions_name.append(promo_name)
            promotions_description.append(promo_description)
            promotions_long_description.append(clean_long_description)

        print(f"unique_id: {product_id}")
        print(f"SKU ID: {sku_id}")
        # print(f"color: {colour}")
        print(f"primary_size: {primary_size}")
        print(f"secondary_size: {secondarySize}") 
        print(f"Quantity: {quantity}")
        print(f"Quantity On Hand: {quantity_on_hand}")
        print(f"currency: {currency_prefix}")
        print(f"Current Price: {current_price}")
        print(f"Previous Price: {previous_price}")
        print(f"Unit Price: {unit_price}")
        print(f"Offer Text: {offer_text}")
        print(f"Instock: {instock}")
        print(f"Selling Price: {selling_price}")
        print(f"Regular Price: {regular_price}")
        print(f"Promotion Price: {promotion_price}")

        # Print separate fields for promotions
        # print(f"Promotions Name: {', '.join(promotions_name) if promotions_name else 'N/A'}")
        # print(f"Promotions Description: {', '.join(promotions_description) if promotions_description else 'N/A'}")
        # print(f"Promotions Long Description: {', '.join(promotions_long_description) if promotions_long_description else 'N/A'}")
        
        print("-" * 80)

