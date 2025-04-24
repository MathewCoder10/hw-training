import logging
import re
import requests
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

                try:
                    response = requests.get(paged_url, headers=HEADERS, timeout=10)
                except requests.RequestException as e:
                    logging.error(f"Request failed for {paged_url}: {e}")
                    self._log_failure(paged_url)
                    break

                if response.status_code != 200:
                    logging.warning(f"Non-200 status code {response.status_code} for {paged_url}")
                    self._log_failure(paged_url)
                    break

                has_items = self.parse_items(response, paged_url)
                if not has_items:
                    logging.info(f"No more items found at {paged_url}. Stopping pagination.")
                    break

                page += 1

    def parse_items(self, response, page_url):
        """Parse HTML response, insert items or log failures."""
        selector = Selector(response.text)
        products_list = selector.xpath('//li[@data-clientside-hook="FilteredProductListItem"]')

        if not products_list:
            self._log_failure(page_url)
            return False

        for li in products_list:
            # raw extraction
            price_raw = li.xpath('.//p[@data-qa-id="entry-price"]//span/text()').get()
            price_was_raw = li.xpath('.//span[contains(@class,"o-SearchProductListItem__prices__list-price")]/span/text()').get()

            item_data = {
                'pdp_url': li.xpath('.//a[@class="o-SearchProductListItem__image"]/@href').get(),
                'image_url': li.xpath('.//img[contains(@class, "a-ResponsiveImage__img")]/@src').get(),
                'product_name': li.xpath('.//img[contains(@class, "a-ResponsiveImage__img")]/@alt').get(),
                'rating': li.xpath('.//span[@class="a-visuallyhidden" and contains(text(), "van")]/text()').get(),
                'review': li.xpath('.//span[contains(@class,"m-StarRating__rating-count-text")]/text()').get(),
                'quantity': li.xpath('.//span[contains(@class, "o-SearchProductListItem__info__items")]/text()').get(),
                'product_code': li.xpath('.//li[span[contains(text(), "Productcode")]]/span/text()').get(),
                'product_description': li.xpath('.//p[contains(@class, "o-SearchProductListItem__content__body__text")]/text()').get(),
                'percentage_discount': li.xpath('.//span[@class="a-CircleBadge__inner"]/span/text()').get(),
                'price': price_raw,
                'per_unit_price': li.xpath('.//p[contains(@class,"unitPricing")]/text()').get(),
                'price_was': price_was_raw,
                'unique_id': li.xpath('.//form//input[@name="product"]/@value').get(),
            }

            # normalize and parse numeric prices
            item_data['price'] = self._parse_price(item_data.get('price'))
            item_data['price_was'] = self._parse_price(item_data.get('price_was'))

            logging.info(f"Extracted item: {item_data.get('unique_id')} (price={item_data['price']}, was={item_data['price_was']})")
            try:
                product_item = ProductCrawlerItem(**item_data)
                product_item.save()
            except NotUniqueError:
                logging.warning(f"Duplicate unique_id skipped: {item_data.get('unique_id')}")
            except Exception as e:
                logging.exception(f"Failed to save item {item_data.get('unique_id')}: {e}")

        return True

    def _parse_price(self, price_str):
        """Convert a price string (e.g. '€9,99') to float."""
        if not price_str:
            return None
        # Strip out any non-numeric characters except comma and dot
        cleaned = re.sub(r"[^0-9,\.]", "", price_str)
        # If European format ('9,99'), convert single comma to dot
        if cleaned.count(',') == 1 and cleaned.count('.') == 0:
            cleaned = cleaned.replace(',', '.')
        else:
            # Remove thousands separators
            cleaned = cleaned.replace(',', '')
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _log_failure(self, url):
        """Helper to log and record failed URLs"""
        try:
            ProductCrawlerFailedItem(category_url=url).save()
            logging.info(f"Logged failure for URL: {url}")
        except Exception:
            logging.exception(f"Failed to record failure for URL: {url}")

    def close(self):
        """Close MongoDB connection"""
        disconnect()
        logging.info("MongoDB connection closed.")


if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
    crawler.close()
