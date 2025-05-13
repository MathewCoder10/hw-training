import datetime
import logging
import pytz
import re
from dateutil.relativedelta import relativedelta, TU
from pymongo import MongoClient

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Basic details
PROJECT = "heb"
CLIENT_NAME = "heb"
PROJECT_NAME = "heb"
FREQUENCY = "onetime"
BASE_URL = "https://www.heb.com"


# Date calculations
EXTRACTION_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
todayDate = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
lastTue = todayDate + relativedelta(weekday=TU(1))
iteration = lastTue.strftime("%Y_%m_%d") 
db_monthly = iteration
scrapydate = todayDate.strftime("%Y-%m-%d")
FILE_NAME = f"heb_{iteration}.csv"


# MongoDB Connection (Localhost)
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = f"heb_{iteration}"
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
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

HASH_VALUE = '0e669423cef683226cb8eb295664619c8e0f95945734e0a458095f51ee89efb3'
SCRIPT_PATTERN = re.compile(r"https://cx.static.heb.com/_next/static/chunks/pages/_app-[0-9a-f]+\.js")


