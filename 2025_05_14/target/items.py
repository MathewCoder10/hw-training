from mongoengine import connect, DynamicDocument, StringField, FloatField, IntField
from settings import (
    MONGO_URI,       
    MONGO_DB,         
    MONGO_COLLECTION_CATEGORY,
    MONGO_COLLECTION_CATEGORY_URL_FAILED,
    MONGO_COLLECTION_CRAWLER,
    MONGO_COLLECTION_CRAWLER_URL_FAILED,
    MONGO_COLLECTION_PARSER_URL_FAILED,
    MONGO_COLLECTION_PARSER
)

# Establish a connection to the local MongoDB instance
connect(db=MONGO_DB, host=MONGO_URI, alias='default')

class ProductCategoryItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CATEGORY}
    category_id  = StringField(required=True, unique = True)
    url = StringField(required=True)

class ProductCategoryFailedItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CATEGORY_URL_FAILED}
    url = StringField()
    issue = StringField()

class ProductCrawlerFailedItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CRAWLER_URL_FAILED }
    category = StringField()
    category_id = StringField()
    issue = StringField()

class ProductCrawlerItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CRAWLER}
    product_url = StringField(required=True, unique = True)
    parent_id = StringField()
    child_id = StringField()
    
    
class ProductParserFailedItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_PARSER_URL_FAILED}
    product_url  = StringField(required=True)
    issue = StringField()

class ProductParserItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_PARSER}
    unique_id                 = StringField(required=True, unique = True)
    competitor_name           = StringField()
    product_name              = StringField()
    product_unique_key        = StringField()
    store_name                = StringField()
    store_addressline1        = StringField()
    upc                       = StringField()
    package_size              = StringField()
    producthierarchy_level1   = StringField()
    producthierarchy_level2   = StringField()
    producthierarchy_level3   = StringField()
    producthierarchy_level4   = StringField()
    producthierarchy_level5   = StringField()
    producthierarchy_level6   = StringField()
    producthierarchy_level7   = StringField()
    breadcrumb                = StringField()
    brand                     = StringField()
    pdp_url                   = StringField()
    currency                  = StringField()
    regular_price             = FloatField()
    selling_price             = FloatField()
    promotion_price           = FloatField()
    price_was                 = FloatField()
    percentage_discount       = StringField()
    promotion_description     = StringField()
    promotion_type            = StringField()
    promotion_valid_upto      = StringField()
    product_description       = StringField()
    grammage_quantity         = StringField()
    grammage_unit             = StringField()
    price_per_unit            = StringField()
    image_urls                = StringField()
    instock                   = StringField()
    preparation_instructions  = StringField()
    warning                   = StringField()
    servings_per_pack         = StringField()
    inutritional_information  = StringField()
    vitamins                  = StringField()






