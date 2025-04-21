from mongoengine import connect, DynamicDocument, StringField, FloatField
from settings import (
    MONGO_URI,       
    MONGO_DB,         
    MONGO_COLLECTION_CRAWLER,
    MONGO_COLLECTION_URL_FAILED,
)

# Establish a connection to the local MongoDB instance
connect(db=MONGO_DB, host=MONGO_URI, alias='default')

class ProductCategoryItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""
    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_URL_FAILED }
    category = StringField()
    page = StringField()
    status_code = StringField()

class ProductCrawlerItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""
    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CRAWLER}
    unique_id = StringField(required=True)
    competitor_name = StringField()
    product_name = StringField()
    brand = StringField()
    pdp_ur = StringField()
    producthierarchy_level1 = StringField()
    producthierarchy_level2 = StringField()
    producthierarchy_level3 = StringField()
    producthierarchy_level4 = StringField()
    producthierarchy_level5 = StringField()
    breadcrumb = StringField()
    regular_price = FloatField()
    selling_price = FloatField()
    image_urls = StringField()
    category = StringField()


