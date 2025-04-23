from mongoengine import connect, DynamicDocument, StringField, FloatField, IntField
from settings import (
    MONGO_URI,       
    MONGO_DB,         
    MONGO_COLLECTION_CATEGORY,
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
    unique_id = StringField(required=True, unique = True)
    product_name = StringField()

class ProductCrawlerFailedItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CRAWLER_URL_FAILED }
    category = StringField()
    page = IntField()
    issue = StringField()

class ProductCrawlerItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CRAWLER}
    unique_id = StringField(required=True, unique = True)
    product_name = StringField()
    
class ProductParserFailedItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_PARSER_URL_FAILED}
    unique_id = StringField(required=True)
    product_name  = StringField()
    issue = StringField()

class ProductParserItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_PARSER}
    unique_id                 = StringField(required=True, unique = True)
    competitor_name           = StringField()
    product_name              = StringField()
    brand                     = StringField()
    pdp_url                   = StringField()
    producthierarchy_level1   = StringField()
    producthierarchy_level2   = StringField()
    producthierarchy_level3   = StringField()
    producthierarchy_level4   = StringField()
    producthierarchy_level5   = StringField()
    breadcrumb                = StringField()
    currency                  = StringField()
    regular_price             = FloatField()
    selling_price             = FloatField()
    promotion_price           = FloatField()
    promotion_valid_from      = StringField()
    promotion_valid_upto      = StringField()
    promotion_type            = StringField()
    promotion_description     = StringField()
    product_description       = StringField()
    grammage_quantity         = StringField()
    grammage_unit             = StringField()
    price_per_unit            = StringField()
    instructions              = StringField()
    storage_instructions      = StringField()
    servings_per_pack         = StringField()
    ingredients               = StringField()
    nutritional_score         = StringField()
    organictype               = StringField()
    allergens                 = StringField()
    fat_percentage            = StringField()






