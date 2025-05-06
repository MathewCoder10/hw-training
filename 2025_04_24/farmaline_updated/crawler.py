import logging
import requests
import re
from parsel import Selector
from mongoengine import connect, disconnect
from mongoengine.connection import get_db
from mongoengine.errors import NotUniqueError
from items import ProductCrawlerFailedItem, ProductCrawlerItem
from settings import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CATEGORY,
    MONGO_COLLECTION_CRAWLER,
    MONGO_COLLECTION_CRAWLER_URL_FAILED,
    HEADERS,
    BASE
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Crawler:
    """Crawling categories and paginated products"""

    def __init__(self):
        # Connect to MongoDB
        connect(db=MONGO_DB, host=MONGO_URI)
        self.database = get_db()
        self.category_collection = self.database[MONGO_COLLECTION_CATEGORY]
        self.crawler_collection = self.database[MONGO_COLLECTION_CRAWLER]
        self.failed_urls_collection = self.database[MONGO_COLLECTION_CRAWLER_URL_FAILED]

    def start(self):
        """Request start categories and iterate through paginated pages"""
        for category in self.category_collection.find({}):
            base_url = category.get('category_url')
            logging.info(f"Starting pagination for category: {base_url}")
            page = 1

            while True:
                # Build URL with page parameter
                paged_url = f"{base_url}?page={page}"
                logging.info(f"Fetching page {page}: {paged_url}")
                
                response = requests.get(paged_url, headers=HEADERS, timeout=10)


                if response.status_code == 200:

                    products = self.parse_items(response, category, page)
                    if products:
                        page= page+1
                    else:
                        break

                else:
                    failed_item = {}
                    failed_item ['category_url'] = paged_url

                    try:
                        product_item = ProductCrawlerFailedItem(**failed_item)
                        product_item.save()
                        logging.info(f"Failed statuscode")
                    except Exception as e:
                        logging.exception(f"Failed to insert")
                

    def parse_items(self, response, page_url):
        """Parse HTML response, insert items or log failures."""

        
        selector = Selector(response.text)

        # XPATH
        PRODUCT_LIST_XPATH = '//li[@data-clientside-hook="FilteredProductListItem"]'
        URL_XPATH = './/a[@class="o-SearchProductListItem__image"]/@href'
        IMAGE_XPATH = './/img[@class="a-ResponsiveImage__img a-fullwidth-image"]/@src'
        NAME_XPATH = './/img[@class="a-ResponsiveImage__img a-fullwidth-image"]/@alt'
        RATING_XPATH = './/span[@class="a-visuallyhidden" and contains(., "van")]/text()'
        REVIEW_XPATH = './/span[contains(@class,"m-StarRating__rating-count-text")]/text()'
        QUANTITY_XPATH = './/span[@class="a-h4-tiny u-font-weight--bold o-SearchProductListItem__info__items"]/text()'
        CODE_XPATH = './/li[span[contains(., "Productcode")]]/span/text()'
        PRODUCT_DESCRIPTION_XPATH = './/p[@class="a-h4-tiny o-SearchProductListItem__content__body__text"]/text()'
        PERCENTAGE_DISCOUNT_XPATH = './/span[@class="a-CircleBadge__inner"]/span/text()'
        PRICE_XPATH = './/p[@data-qa-id="entry-price"]//span/text()'
        PER_UNIT_PRICE_XPATH = './/p[contains(@class,"unitPricing")]/text()'
        PRICE_WAS_XPATH = './/span[contains(@class,"o-SearchProductListItem__prices__list-price")]/span/text()'
        UNIQUE_ID_XPATH = './/form//input[@name="product"]/@value'


        products_list = selector.xpath(PRODUCT_LIST_XPATH)

        if products_list:
            for li in products_list:
                pdp_url = li.xpath(URL_XPATH).extract_first()
                image_url = li.xpath(IMAGE_XPATH).extract_first()
                product_name = li.xpath(NAME_XPATH).extract_first()
                rating = li.xpath(RATING_XPATH).extract_first()
                review = li.xpath(REVIEW_XPATH).extract_first()
                quantity = li.xpath(QUANTITY_XPATH).extract_first()
                product_code = li.xpath(CODE_XPATH).extract_first()
                product_description= li.xpath(PRODUCT_DESCRIPTION_XPATH).extract_first()
                percentage_discount= li.xpath(PERCENTAGE_DISCOUNT_XPATH).extract_first()
                price= li.xpath(PRICE_XPATH).extract_first()
                per_unit_price= li.xpath(PER_UNIT_PRICE_XPATH).extract_first()
                price_was= li.xpath(PRICE_WAS_XPATH).extract_first()
                unique_id= li.xpath(UNIQUE_ID_XPATH).extract_first()

                # CLEAN
                if not price:
                    price = 0.0
                else:
                    cleaned = re.sub(r'[^\d,\.]', '', price).replace(',', '.')
                    try:
                        price = float(cleaned)
                    except ValueError:
                        logging.warning(f"Could not convert price: {price}")
                        price = 0.0
                if pdp_url:
                    pdp_url = f"{BASE}{pdp_url}"

                # ITEM YEILD
                item = {}
                item['pdp_url'] = pdp_url
                item['image_url'] = image_url
                item['product_name'] = product_name
                item['rating'] = rating
                item['review'] = review
                item['quantity'] = quantity
                item['product_code'] = product_code
                item['product_description'] = product_description
                item['percentage_discount'] = percentage_discount
                item['price'] = price
                item['per_unit_price'] = per_unit_price
                item['price_was'] = price_was
                item['unique_id'] = unique_id

                logging.info(item)
                try:
                    product_item = ProductCrawlerItem(**item)
                    product_item.save()
                except NotUniqueError:
                    logging.warning(f"Duplicate unique_id found")

            return True    

        else:
            failed_item = {}
            failed_item ['category_url'] = page_url

            try:
                product_item = ProductCrawlerFailedItem(**failed_item)
                product_item.save()
                logging.info(f"Failed statuscode")
            except Exception as e:
                logging.exception(f"Failed to insert")

            return False

    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")


if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
    crawler.close()
