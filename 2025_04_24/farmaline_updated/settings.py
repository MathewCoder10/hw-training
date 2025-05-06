import datetime
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
PROJECT = "farmaline"
CLIENT_NAME = "farmaline"
PROJECT_NAME = "farmaline"
FREQUENCY = "onetime"
BASE_URL = "https://www.farmaline.be/nl"
BASE = "https://www.farmaline.be"

# Date calculations
EXTRACTION_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
todayDate = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
lastTue = todayDate + relativedelta(weekday=TU(1))
iteration = lastTue.strftime("%Y_%m_%d") 
db_monthly = iteration
scrapydate = todayDate.strftime("%Y-%m-%d")
FILE_NAME = f"farmaline_{iteration}.csv"

# MongoDB Connection (Localhost)
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = f"farmaline_07"
# MONGO_DB = f"farmaline_{iteration}"
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# Collections
MONGO_COLLECTION_CATEGORY = f"{PROJECT_NAME}_category"
MONGO_COLLECTION_CRAWLER = f"{PROJECT_NAME}_crawler"
MONGO_COLLECTION_PARSER = f"{PROJECT_NAME}_parser"
MONGO_COLLECTION_CATEGORY_URL_FAILED = f"{PROJECT_NAME}_category_url_failed"
MONGO_COLLECTION_CRAWLER_URL_FAILED = f"{PROJECT_NAME}_crawler_url_failed"
MONGO_COLLECTION_PARSER_URL_FAILED = f"{PROJECT_NAME}_parser_url_failed"

# Headers for API requests
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ml;q=0.8',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}



