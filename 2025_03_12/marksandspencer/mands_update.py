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

brand = selector.xpath('//p[@class="media-0_textSm__Q52Mz brand-title_title__u6Xx5 media-0_strong__aXigV"]//text()').get()
print(brand)

product_name = selector.xpath('//h1[@class="media-0_headingSm__aysOm"]//text()').get()
print(product_name)

rating_text = selector.xpath("//div[contains(@class, 'star-rating_wrapper__QmDBj')]/@aria-label").get()
match = re.search(r'(\d+\.\d+)', rating_text)  
rating_value = match.group(1) if match else "N/A"
print(rating_value)

review = selector.xpath('//span[@class="media-0_textSm__Q52Mz media-0_strong__aXigV"]//text()').get()
print(review)

image_urls = selector.xpath("//ul[@class='image-gallery_slides__N8x_w']//img/@src").getall()
print(image_urls)

product_code = selector.xpath('(//p[@class="media-0_textXs__ZzHWu"]//text())[2]').get()
print(product_code)

product_details = selector.xpath("//p[text()='About this style']/following-sibling::p[1]/text()").get()
print(product_details)

care = selector.xpath("//p[text()='Care']/following-sibling::div//p[@class='media-0_textSm__Q52Mz product-details_careText__48dt5']/text()").getall()
print(care)

fit_and_style = selector.xpath("//p[text()='Fit and style']/following-sibling::div//p[@class='media-0_textSm__Q52Mz product-details_dimension__nPlAW']/text()").getall()
print(fit_and_style)

item_details = selector.xpath("//p[text()='Item details']/following-sibling::div//p[@class='media-0_textSm__Q52Mz product-details_dimension__nPlAW']/text()").getall()
print(item_details)

material_composition = selector.xpath("//p[text()='Composition']/following-sibling::p[1]/text()").get()
print(material_composition)

script_content = selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
data = json.loads(script_content)

# # Navigate to the correct path
variants = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("variants", [])
size_guide = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("linkToSizeGuide",'')
print(size_guide)
fit = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("fit",'')
print(fit)
instructions = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("usageInstructions",'')
print(instructions)
competitor_product_key = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("strokeId",'')
print(competitor_product_key)

# Function to remove HTML tags using regex
def clean_html(raw_html):
    clean_text = re.sub(r"<[^>]+>", " ", raw_html)  # Remove HTML tags
    return re.sub(r"\s+", " ", clean_text).strip()  # Remove extra spaces

# Extract details for each variant SKU
for variant in variants:
    product_id = variant.get("id")
    colour = variant.get("colour")
    skus = variant.get("skus", [])
    
    for sku in skus:
        sku_id = sku.get("id")
        primary_size = sku.get("size", {}).get("primarySize")
        quantity = sku.get("inventory", {}).get("quantity")
        quantity_on_hand = sku.get("inventory", {}).get("quantityOnHand")
        currency_prefix = sku.get("price", {}).get("currencyPrefix")
        current_price = sku.get("price", {}).get("currentPrice")
        previous_price = sku.get("price", {}).get("previousPrice")
        unit_price = sku.get("price", {}).get("unitPrice")
        offer_text = sku.get("price", {}).get("offerText")

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

        print(f"Product ID: {product_id}")
        print(f"SKU ID: {sku_id}")
        print(f"Colour: {colour}")
        print(f"Primary Size: {primary_size}")
        print(f"Quantity: {quantity}")
        print(f"Quantity On Hand: {quantity_on_hand}")
        print(f"Currency Prefix: {currency_prefix}")
        print(f"Current Price: {current_price}")
        print(f"Previous Price: {previous_price}")
        print(f"Unit Price: {unit_price}")
        print(f"Offer Text: {offer_text}")

        # Print separate fields for promotions
        print(f"Promotions Name: {', '.join(promotions_name) if promotions_name else 'N/A'}")
        print(f"Promotions Description: {', '.join(promotions_description) if promotions_description else 'N/A'}")
        print(f"Promotions Long Description: {', '.join(promotions_long_description) if promotions_long_description else 'N/A'}")
        

        print("-" * 80)

