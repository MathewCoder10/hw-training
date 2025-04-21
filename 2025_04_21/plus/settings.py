from datetime import datetime
import logging
import pytz
from dateutil.relativedelta import relativedelta, TU
from pymongo import MongoClient

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Basic details
PROJECT = "plus"
CLIENT_NAME = "plus"
PROJECT_NAME = "plus"
FREQUENCY = "onetime"
BASE_URL = "https://www.plus.nl"

# Date calculations
todayDate = datetime.now(pytz.timezone("Asia/Kolkata"))
lastTue = todayDate + relativedelta(weekday=TU(1))
iteration = lastTue.strftime("%Y_%m")
db_monthly = iteration
scrapydate = todayDate.strftime("%Y-%m-%d")
FILE_NAME = f"plus_{iteration}"

# MongoDB Connection (Localhost)
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = f"plus_{iteration}"
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# Collections
MONGO_COLLECTION_CRAWLER = f"{PROJECT_NAME}_crawler"
MONGO_COLLECTION_PARSER = f"{PROJECT_NAME}_parser"
MONGO_COLLECTION_URL_FAILED = f"{PROJECT_NAME}_url_failed"

# Headers for API requests
HEADERS = {
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json; charset=UTF-8',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'x-csrftoken': 'T6C+9iB49TLra4jEsMeSckDMNhQ=',
}

# Category slugs to crawl
CATEGORY_SLUGS = [
    'eieren', 'vrije-uitloopeieren', 'biologische-eieren', 'speciale-eieren',
    'boter-margarine', 'halvarine-margarine', 'roomboter', 'bakboter-braadboter',
    'kaas', 'verse-kaas-plakken', 'verse-stukken-kaas', 'voorverpakte-kaasplakken',
    'voorverpakte-stukken-kaas', 'smeerkaas-roomkaas-zuivelspread',
    'rasp-strooikaas-flakes', 'borrelkaas-buitenlandse-kaasjes',
    'kaas-om-mee-te-koken', 'plantaardige-kaas', 'kinderkazen'
]

# Base API endpoint for product lists
BASE_API_URL = (
    'https://www.plus.nl/screenservices/ECP_Composition_CW/'
    'ProductLists/PLP_Content/DataActionGetProductListAndCategoryInfo'
)

DETAILS_API_URL = (
    'https://www.plus.nl/screenservices/ECP_Product_CW/'
    'ProductDetails/PDPContent/DataActionGetProductDetailsAndAgeInfo'
)
PROMOTION_API_URL = (
    'https://www.plus.nl/screenservices/ECP_Product_CW/'
    'ProductDetails/PDPContent/DataActionGetPromotionOffer'
)

# URL to fetch dynamic module version
MODULE_VERSION_URL = (
    'https://www.plus.nl/moduleservices/moduleversioninfo?1745203216246'
)

# JSON payload template (moduleVersion will be set dynamically in crawler)
JSON_TEMPLATE = {
    'versionInfo': {
        'moduleVersion': '', 
        'apiVersion': 'bYh0SIb+kuEKWPesnQKP1A',
    },
    'viewName': 'MainFlow.ProductListPage',
    'screenData': {
        'variables': {
            'PageNumber': '',
            'CategorySlug': '',
        }
    }
}