from mongoengine import connect, DynamicDocument, StringField, FloatField
from settings import (
    MONGO_URI,       
    MONGO_DB,  
    MONGO_COLLECTION_CATEGORY,       
    MONGO_COLLECTION_CRAWLER,
    MONGO_COLLECTION_PARSER,
    MONGO_COLLECTION_CATEGORY_URL_FAILED, 
    MONGO_COLLECTION_CRAWLER_URL_FAILED,
    MONGO_COLLECTION_PARSER_URL_FAILED
)

# Establish a connection to the local MongoDB instance
connect(db=MONGO_DB, host=MONGO_URI, alias='default')

class ProductCategoryItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CATEGORY}
    category_url = StringField(unique = True)

class ProductCategoryFailedItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CATEGORY_URL_FAILED}
    category_url = StringField()

class ProductCrawlerItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CRAWLER}
    pdp_url = StringField(unique = True)
    image_url = StringField()
    product_name = StringField()
    rating = StringField()
    review = StringField()
    quantity = StringField()
    product_code = StringField()
    product_description = StringField()
    percentage_discount = StringField()
    price = FloatField()
    per_unit_price = StringField()
    price_was = FloatField()
    unique_id = StringField()
    varients = StringField()

class ProductCrawlerFailedItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_CRAWLER_URL_FAILED }
    category_url = StringField()  

class ProductParserItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_PARSER}
    unique_id                 = StringField(unique = True)
    competitor_name           = StringField()
    product_name              = StringField()
    brand                     = StringField()
    pdp_url                   = StringField(unique = True)
    currency                  = StringField()    
    regular_price             = FloatField()
    selling_price             = FloatField()
    promotion_price           = FloatField()
    price_was                 = FloatField()
    percentage_discount       = StringField()
    product_description       = StringField()
    grammage_quantity         = StringField()
    grammage_unit             = StringField()
    price_per_unit            = StringField()
    image_urls                = StringField() 
    storage_instructions      = StringField()
    reviews                   = StringField()
    rating                    = StringField()
    product_code              = StringField()
    variants                  = StringField()
    manufacturer              = StringField()
    manufacturer_address      = StringField()
    net_content               = StringField()
    dosage_recommendation     = StringField()
    ingredients               = StringField()
    features                  = StringField()

class ProductParserFailedItem(DynamicDocument):
    """Initializing URL fields and their Data Types."""

    meta = {"db_alias": "default", "collection": MONGO_COLLECTION_PARSER_URL_FAILED}
    pdp_url = StringField(unique = True)
    unique_id = StringField()







