import requests
from parsel import Selector


headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

##############################CATEGORY##############################
#Response to get main category urls.
response = requests.get('https://www.farmaline.be/nl/', headers=headers)
print(response.status_code)
selector = Selector(text=response.text)

category = selector.xpath('//h3[normalize-space(.)="Onze categorieën"]/following-sibling::menu[@id="categories-menu"]//a/@href').getall()
print(category)
#Response to get all products from the main category urls.
response = requests.get('https://www.farmaline.be/nl/beauty-lichaamsverzorging/', headers=headers)
selector = Selector(text=response.text)

href = selector.xpath('//div[@class="o-PageHeading__all-products-link"]/a[@class="a-link--underline"]/@href').getall()
print(href)
#Response to get subcategory category urls.
response = requests.get('https://www.farmaline.be/nl/duurzaamheid/', headers=headers)
print(response.status_code)
selector = Selector(text=response.text)

href = selector.xpath('//a[@class="a-Button--primary" and normalize-space(.)="Ontdek nu"]/@href').getall()
print(href)


##############################CRAWLER##############################
response = requests.get('https://www.farmaline.be/nl/beauty-lichaamsverzorging/all-from-category/', headers=headers,)
print(response.status_code)
selector = Selector(text=response.text)

pdp_url = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//a[@class="o-SearchProductListItem__image"]/@href').get()
print(pdp_url)
image_url = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//img[@class="a-ResponsiveImage__img a-fullwidth-image"]/@src').get()
print(image_url)
product_name = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//img[@class="a-ResponsiveImage__img a-fullwidth-image"]/@alt').get()
print(product_name)
rating = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//span[@class="a-visuallyhidden" and contains(., "van")]/text()').get()
print(rating)
review = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//span[contains(@class,"m-StarRating__rating-count-text")]/text()').get()
print(review)
quantity = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//span[@class="a-h4-tiny u-font-weight--bold o-SearchProductListItem__info__items"]/text()').get()
print(quantity)
product_code = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//li[span[contains(., "Productcode")]]/span/text()').get()
print(product_code)
instock = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//p[@data-qa-id="product-status-qa-id"]/span[2]/text()').get()
print(instock)
product_description = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//p[@class="a-h4-tiny o-SearchProductListItem__content__body__text"]/text()').get()
print(product_description)
percentage_discount = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//span[@class="a-CircleBadge__inner"]/span/text()').get()
print(percentage_discount)
regular_price = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//p[@data-qa-id="entry-price"]//span/text()').get()
print(regular_price)
per_unit_price = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//p[contains(@class,"unitPricing")]/text()').get()
print(per_unit_price)
price_was = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//span[contains(@class,"o-SearchProductListItem__prices__list-price")]/span/text()').get()
print(price_was)
unique_id = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//form//input[@name="product"]/@value').get()
print(unique_id)
varients = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]//ul[contains(@class,"variants-list")]/li/button/text()').getall()
print(varients)

##############################PARSER##############################

response = requests.get('https://www.farmaline.be/nl/Supplementen/BE09806414/etixx-isotonic-drink-lemon-1-1-gratis-promo.htm',headers=headers)
print(response.status_code)
selector = Selector(text=response.text)

product_name = selector.xpath('//h1[@data-qa-id="product-title"]/text()').get()
print(product_name)
reviews = selector.xpath('//a[@data-qa-id="number-of-ratings-text"]/text()').get()
print(reviews)
rating = selector.xpath('//div[@data-qa-id="product-ratings-container"]//span[contains(@class,"text-4xl")]/text()').get()
print(rating)
variant = selector.xpath('//ul/li[@data-qa-id="product-variants"]//div[contains(@class,"font-bold") and contains(@class,"text-l")]/text()').getall()
print(variant)
image_urls = selector.xpath('//button[starts-with(@data-qa-id, "product-image")]/picture/img/@src').getall()
print(image_urls)
quantity = selector.xpath('//div[@class="whitespace-nowrap font-bold text-dark-primary-max text-l font-medium"]/text()').get()
print(quantity)
per_unit_price = selector.xpath('//div[@class="text-xs text-dark-primary-medium"]/text()').get()
print(per_unit_price)
selling_price = selector.xpath('//div[@data-qa-id="product-page-variant-details__display-price"]/text()').get()
print(selling_price)
regular_price = selector.xpath('//span[@data-qa-id="product-old-price"]/text()').get()
print(regular_price)
percentage_discount= selector.xpath('//div[@data-qa-id="product-page-variant-details__display-price"]/following-sibling::div/text()').get()
print(percentage_discount)
product_code = selector.xpath('//dt[normalize-space()="Productcode"]/following-sibling::dd//span/text()').get()
print(product_code)
manufacturer = selector.xpath('//dt[normalize-space()="Fabrikant"]/following-sibling::dd/text()').get()
print(manufacturer)
brand = selector.xpath('//dl[dt[normalize-space(.)="Merk"]]/dd/a/text()').get()
print(brand)
manufacturer_address = selector.xpath('//h5[contains(normalize-space(.), "Gegevens fabrikant")]/following-sibling::div[1]/text()').getall()
print(manufacturer_address)
net_quantity = selector.xpath('//h5[contains(normalize-space(.), "Nettohoeveelheid")]/following-sibling::div/text()').get()
print(net_quantity)
dosage_recommendation = selector.xpath('//h5[contains(normalize-space(.),"Aanbevolen dagelijkse dosis")]/following-sibling::div/text()').get()
print(dosage_recommendation)
storage_instructions = selector.xpath('//h5[contains(normalize-space(.),"Bewaar- en gebruiksadvies")]/following-sibling::div[1]/ul/li').getall()
print(f"storage_instructions:{storage_instructions}")
ingredients = selector.xpath('//h5[contains(normalize-space(.),"Ingrediënten")]/following-sibling::div[1]/p/text()').getall()
print(f"ingredients:{ingredients}")


# ##############################FINDINGS##############################
# The nutritional_information field is visible, but its structure varies across different products, making extraction complicated.
# eg:https://www.farmaline.be/nl/fitness/upmCDF2AU/vitastrong-pure-hydro-protein-chocolade.htm
# https://www.farmaline.be/nl/Supplementen/BE04351813/soria-natural-fostprint-plus-nieuwe-formule.htm
# https://www.farmaline.be/nl/Supplementen/BE04373239/selenium-ace-d-zn-30-tabletten-gratis-promo.htm
# https://www.farmaline.be/nl/Supplementen/BE04505541/etixx-magnesium-2000-aa.htm
# The structure of the duurzaamheid category is different from other categories.

