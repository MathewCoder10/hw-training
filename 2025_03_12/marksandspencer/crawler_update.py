import requests
from parsel import Selector

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

base_url = 'https://www.marksandspencer.com'
# category_url can be fetched dynamically from your MongoDB collection, here we hardcode it for demonstration:
category_url = 'https://www.marksandspencer.com/l/women/knitwear/cardigans'
current_url = category_url

while current_url:
    response = requests.get(current_url, headers=headers)
    selector = Selector(text=response.text)

    # Extract product details
    product_urls = selector.xpath(
        "//div[contains(@class, 'product-list_col__W5sgI')]//a[contains(@class, 'product-card_cardWrapper__GVSTY')]/@href"
    ).getall()
    product_names = selector.xpath(
        "//div[contains(@class, 'product-list_col__W5sgI')]//a[contains(@class, 'product-card_cardWrapper__GVSTY')]//h2/text()"
    ).getall()
    u_ids = selector.xpath(
        "//div[contains(@class, 'product-list_col__W5sgI')]/@id"
    ).getall()

    # Print product details
    print("u IDs:", u_ids)
    print("Product Names:", product_names)
    for product in product_urls:
        product_url = base_url + product
        print("Product URL:", product_url)

    # Look for the next page link using the class and aria-label attributes.
    next_page = selector.xpath(
        "//a[contains(@class, 'pagination_trigger__YEwyN') and @aria-label='Next page']/@href"
    ).get()
    
    if next_page:
        current_url = base_url + next_page
        print("\nMoving to next page:", current_url)
    else:
        # No further pages available
        print("\nNo further pages found.")
        break
