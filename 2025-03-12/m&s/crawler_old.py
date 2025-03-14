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


category_url = 'https://www.marksandspencer.com/l/women/knitwear/cardigans'
response = requests.get(category_url,headers=headers)
# print(response.status_code)
selector = Selector(text=response.text)
product_urls = selector.xpath("//div[contains(@class, 'product-list_col__W5sgI')]//a[contains(@class, 'product-card_cardWrapper__GVSTY')]/@href").getall()
product_name = selector.xpath("//div[contains(@class, 'product-list_col__W5sgI')]//a[contains(@class, 'product-card_cardWrapper__GVSTY')]//h2/text()").getall()
unique_id = selector.xpath("//div[contains(@class, 'product-list_col__W5sgI')]/@data-tagg").getall()
print(unique_id)
print(product_name)
# Print the full URL for each product link
base_url = 'https://www.marksandspencer.com'
for product in product_urls:
    product_url = base_url + product
    print(product_url)