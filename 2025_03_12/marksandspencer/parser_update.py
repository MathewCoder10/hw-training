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

# Extract common details from the page
common_details = {
    "gender": "Women",
    "brand": selector.xpath('//p[@class="media-0_textSm__Q52Mz brand-title_title__u6Xx5 media-0_strong__aXigV"]//text()').get(),
    "product_name": selector.xpath('//h1[@class="media-0_headingSm__aysOm"]//text()').get(),
}

rating_text = selector.xpath("//div[contains(@class, 'star-rating_wrapper__QmDBj')]/@aria-label").get()
match = re.search(r'(\d+\.\d+)', rating_text)
common_details["rating"] = match.group(1) if match else "N/A"

review_text = selector.xpath('//span[@class="media-0_textSm__Q52Mz media-0_strong__aXigV"]//text()').get()
common_details["review"] = re.search(r'\d+', review_text).group()

image_urls = selector.xpath("//ul[@class='image-gallery_slides__N8x_w']//img/@src").getall()
if len(image_urls) >= 3:
    common_details["image_url_1"] = image_urls[0]
    common_details["image_url_2"] = image_urls[1]
    common_details["image_url_3"] = image_urls[2]
else:
    common_details["image_url_1"] = image_urls[0] if len(image_urls) > 0 else None
    common_details["image_url_2"] = image_urls[1] if len(image_urls) > 1 else None
    common_details["image_url_3"] = image_urls[2] if len(image_urls) > 2 else None

common_details["competitor_product_key"] = selector.xpath('(//p[@class="media-0_textXs__ZzHWu"]//text())[2]').get()
common_details["product_description"] = selector.xpath("//p[text()='About this style']/following-sibling::p[1]/text()").get()
common_details["care_instructions"] = selector.xpath("//p[text()='Care']/following-sibling::div//p[@class='media-0_textSm__Q52Mz product-details_careText__48dt5']/text()").getall()
common_details["material_composition"] = selector.xpath("//p[text()='Composition']/following-sibling::p[1]/text()").get()
common_details["style"] = selector.xpath("//p[normalize-space(text())='Fit and style' or normalize-space(text())='Style']/following-sibling::div//p[contains(@class, 'product-details_dimension')]/text()").getall()

aria_labels = selector.xpath("//div[@class='colour-swatch-list_wrapper__4kdoV']//label/@aria-label").getall()
common_details["variant_color"] = [label.split(" colour option")[0] for label in aria_labels]

breadcrumb_items = selector.xpath("//nav[@aria-label='breadcrumb']//a/text()").getall()
common_details["breadcrumb"] = "/".join(breadcrumb_items)
common_details["producthierarchy_level1"] = breadcrumb_items[0] if len(breadcrumb_items) > 0 else ''
common_details["producthierarchy_level2"] = breadcrumb_items[1] if len(breadcrumb_items) > 1 else ''
common_details["producthierarchy_level3"] = breadcrumb_items[2] if len(breadcrumb_items) > 2 else ''
common_details["producthierarchy_level4"] = breadcrumb_items[3] if len(breadcrumb_items) > 3 else ''
common_details["producthierarchy_level5"] = breadcrumb_items[4] if len(breadcrumb_items) > 4 else ''

script_content = selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
data = json.loads(script_content)
variants_data = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("variants", [])
common_details["size_guide"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("linkToSizeGuide", '')
common_details["clothing_fit"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("fit", '')
common_details["instructions"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("usageInstructions", '')
common_details["features"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("productfeaturesInnovations", '')
common_details["body_fit"] = data.get("props", {}).get("pageProps", {}).get("productDetails", {}).get("attributes", {}).get("fit", '')
# Helper function to remove HTML tags
def clean_html(raw_html):
    clean_text = re.sub(r"<[^>]+>", " ", raw_html)
    return re.sub(r"\s+", " ", clean_text).strip()

# Retrieve the selected color from the page.
color_selected = selector.xpath('//span[@id="selected-colour-option-text"]/text()').get()
if color_selected:
    color_selected = color_selected.strip()

# Group SKUs by product unique_id (only process the first variant that matches the selected color).
grouped_products = {}
variant_found = False

for variant in variants_data:
    if variant_found:
        break  # Only process the first matching variant

    variant_color = variant.get("colour")
    if variant_color != color_selected:
        continue  # Skip variants that do not match the selected color

    variant_found = True  # Mark that we found a matching variant

    product_id = variant.get("id")
    skus = variant.get("skus", [])
    
    if product_id not in grouped_products:
        grouped_products[product_id] = common_details.copy()
        grouped_products[product_id]["skus"] = []
    
    # Initialize sets to collect primary and secondary sizes for variants.
    primary_sizes_set = set()
    secondary_sizes_set = set()

    for sku in skus:
        sku_info = {}
        sku_info["SKU ID"] = sku.get("id")
        sku_info["color"] = variant_color
        
        primary_size = sku.get("size", {}).get("primarySize")
        secondary_size = sku.get("size", {}).get("secondarySize")
        sku_info["primary_size"] = primary_size
        sku_info["secondary_size"] = secondary_size
        
        sku_info["competitor_name"] = "marksandspencer"
        if primary_size and secondary_size:
            size_value = f"{primary_size}_{secondary_size}"
        else:
            size_value = primary_size if primary_size else ""
        sku_info["size"] = size_value
        
        # Accumulate primary and secondary sizes.
        if primary_size:
            primary_sizes_set.add(primary_size)
        if secondary_size:
            secondary_sizes_set.add(secondary_size)
        
        sku_info["Quantity"] = sku.get("inventory", {}).get("quantity")
        sku_info["Quantity On Hand"] = sku.get("inventory", {}).get("quantityOnHand")
        sku_info["currency"] = sku.get("price", {}).get("currencyPrefix")
        sku_info["Current Price"] = sku.get("price", {}).get("currentPrice")
        sku_info["Previous Price"] = sku.get("price", {}).get("previousPrice")
        sku_info["Unit Price"] = sku.get("price", {}).get("unitPrice")
        sku_info["Offer Text"] = sku.get("price", {}).get("offerText")
        
        quantity = sku.get("inventory", {}).get("quantity", 0)
        sku_info["Instock"] = "outofstock" if quantity < 1 else "instock"
        selling_price = sku.get("price", {}).get("currentPrice")
        previous_price = sku.get("price", {}).get("previousPrice")
        sku_info["Selling Price"] = selling_price
        if previous_price is not None:
            sku_info["Regular Price"] = previous_price
            sku_info["Promotion Price"] = selling_price
        else:
            sku_info["Regular Price"] = selling_price
            sku_info["Promotion Price"] = ""
        
        promotions = sku.get("price", {}).get("promotions", [])
        promo_names = []
        promo_descriptions = []
        promo_long_descriptions = []
        for promo in promotions:
            promo_name = promo.get("name", "N/A")
            promo_description = promo.get("description", "N/A")
            promo_long_description = promo.get("longDescription", "N/A")
            promo_long_descriptions.append(clean_html(promo_long_description))
            promo_names.append(promo_name)
            promo_descriptions.append(promo_description)
        
        grouped_products[product_id]["skus"].append(sku_info)
    
    # Build the "variants" field based on the collected primary and secondary sizes.
    if secondary_sizes_set:
        # If secondary sizes exist, create combinations.
        variants_list = [f"{p}_{s}" for p in sorted(primary_sizes_set) for s in sorted(secondary_sizes_set)]
    else:
        variants_list = list(primary_sizes_set)
    
    grouped_products[product_id]["variants"] = variants_list

# Finally, print the grouped product for the first matching variant.
for product in grouped_products.values():
    print(json.dumps(product, indent=2))
