import requests
from parsel import Selector

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'api_key': '6d3a42a3-6d93-4f98-838d-bcc0ab2307fd',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://www.dirk.nl',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.dirk.nl/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

json_data = {
    'query': 'query  {product(productId: 108146)  {productId\ndepartment\nheaderText\npackaging\ndescription\nadditionalDescription\nimages  {image\nrankNumber\nmainImage}\nlogos  {description\nimage}\ndeclarations  {storageInstructions\ncookingInstructions\ninstructionsForUse\ningredients\ncontactInformation  {contactName\ncontactAdress}\nnutritionalInformation  {standardPackagingUnit\nsoldOrPrepared\nnutritionalValues  {text\nvalue\nnutritionalSubValues  {text\nvalue}}}\nallergiesInformation  {text}}\nproductAssortment(storeId: 66)  {productId\nnormalPrice\nofferPrice\nisSingleUsePlastic\nsingleUsePlasticValue\nstartDate\nendDate\nproductOffer  {textPriceSign\nendDate\nstartDate\ndisclaimerStartDate\ndisclaimerEndDate}\nproductInformation  {productId\nheaderText\nsubText\npackaging\nimage\ndepartment\nwebgroup\nbrand\nlogos  {description\nimage}}}}}',
    'variables': {},
}

#################################################CATEGORY#################################################

response = requests.get('https://www.dirk.nl/boodschappen', headers=headers)

if response.status_code == 200:
    selector = Selector(response.text)
    category_links = selector.xpath("//article[@data-section='departments']//a/@href").getall()
    print("Category Links:")
    for link in category_links:
        print(link)
else:
    print(f"Failed to retrieve the page. Status Code: {response.status_code}")

#################################################SUBCATEGORY#################################################

response = requests.get('https://www.dirk.nl/boodschappen/aardappelen-groente-fruit', headers=headers)

if response.status_code == 200:
    selector = Selector(response.text)
    category_links = selector.xpath("//div[@class='right']//a/@href").getall()
    print("Category Links:")
    for link in category_links:
        print(link)
else:
    print(f"Failed to retrieve the page. Status Code: {response.status_code}")

################################################CRAWLER#################################################

response = requests.get('https://www.dirk.nl/boodschappen/aardappelen-groente-fruit/aardappelen', headers=headers,)

if response.status_code == 200:
    selector = Selector(response.text)
    bread_crumb = selector.xpath('//div[@class="breadcrumbs container"]//a/@data-navigation-item').getall()
    print("breadcrumb:",bread_crumb)
    category_links = selector.xpath("//div[@class='product-cards']//a[@class='top']/@href").getall()
    print("Category Links:")
    for link in category_links:
        print(link)
else:
    print(f"Failed to retrieve the page. Status Code: {response.status_code}")


#################################################PARSER_1#################################################

response = requests.post('https://web-dirk-gateway.detailresult.nl/graphql', headers=headers, json=json_data)

if response.status_code == 200:
    data = response.json().get("data", {}).get("product", {})

    result = {}

    # unique_id: content of "productId"
    result["unique_id"] = data.get("productId")

    # packaging: content of "packaging"
    result["packaging"] = data.get("packaging")

    # product_description: combination of "description" and "additionalDescription"
    description = data.get("description", "")
    additional = data.get("additionalDescription", "")
    result["product_description"] = f"{description} {additional}".strip()

    # image_urls: extract the "image" field from each item in "images"
    result["image_urls"] = [img.get("image") for img in data.get("images", []) if img.get("image")]

    # features: extract the "description" field from each item in "logos"
    result["features"] = [logo.get("description") for logo in data.get("logos", []) if logo.get("description")]

    declarations = data.get("declarations", {})

    # storageInstructions: content inside "storageInstructions"
    result["storageInstructions"] = declarations.get("storageInstructions", [])

    # instructionsForUse: content inside "instructionsForUse"
    result["instructionsForUse"] = declarations.get("instructionsForUse", [])

    # instructions: content inside "cookingInstructions"
    result["instructions"] = declarations.get("cookingInstructions", [])

    # preparationinstructions: using "cookingInstructions" (same as instructions)
    result["preparationinstructions"] = declarations.get("cookingInstructions", [])

    # ingredients: content inside "ingredients"
    result["ingredients"] = declarations.get("ingredients")

    # distributor_address: content of "contactAdress" inside "contactInformation"
    contact_info = declarations.get("contactInformation", {})
    result["distributor_address"] = contact_info.get("contactAdress")

    # allergens: extract the "text" field from each item in "allergiesInformation"
    result["allergens"] = [allergy.get("text") for allergy in declarations.get("allergiesInformation", []) if allergy.get("text")]

    # nutritional_information: extract only the text and value fields as tuples (ignoring any subvalues)
    nutritional_info = []
    nutritional_values = declarations.get("nutritionalInformation", {}).get("nutritionalValues", [])
    for nv in nutritional_values:
        nutritional_info.append((nv.get("text"), nv.get("value")))
    result["nutritional_information"] = nutritional_info

    # Access productAssortment section
    product_assortment = data.get("productAssortment", {})

    # brand: content inside "brand" inside "productInformation"
    product_info = product_assortment.get("productInformation", {})
    result["brand"] = product_info.get("brand")

    # start_date and end_date: content inside "productOffer"
    product_offer = product_assortment.get("productOffer", {})
    result["start_date"] = product_offer.get("startDate")
    result["end_date"] = product_offer.get("endDate")

    # normal_price: content inside "normalPrice"
    result["normal_price"] = product_assortment.get("normalPrice")
    # offer_price: content inside "offerPrice"
    result["offer_price"] = product_assortment.get("offerPrice")

    print(result)
else:
    print("Error:", response.status_code, response.text)


#################################################PARSER_2#################################################

url = 'https://www.dirk.nl/boodschappen/aardappelen-groente-fruit/aardappelen/1%20de%20beste%20aardappelen%20kruimig/108146'
response = requests.get(url, headers=headers)

if response.status_code == 200:
    selector = Selector(response.text)
    
    # Standard extraction using XPath
    bread_crumb = selector.xpath('//div[@class="item"]//a[@data-navigation-item]/@data-navigation-item').getall()
    print("Breadcrumb:", bread_crumb)
    
    promotion_discription = selector.xpath('//p[@class="offer-runtime"]/text()').getall()
    print("promotion:",promotion_discription)

#################################################FINDINGS##################################################
# Need vpn to access the site.
# Have AWS CloudFront blocking.
# In the pdp page we can get the data using api but api doesnot contain breadcrumb so we need to generate braedcrumb by adding the name of the product with the existing braedcrumb from the crawler.
# All products are available in boodschappen.


