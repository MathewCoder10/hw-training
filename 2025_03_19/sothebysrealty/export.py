import csv
from pymongo import MongoClient
from settings import FILE_NAME_FULLDUMP, MONGO_DB, MONGO_COL_DATA

csv_headers = [
    "first_name",
    "middle_name",
    "last_name",
    "office_name",
    "title",
    "description",
    "languages",
    "image_url",
    "address",
    "city",
    "state",
    "country",
    "zipcode",
    "office_phone_numbers",
    "agent_phone_numbers",
    "email",
    "website",
    "social",
    "profile_url"
]

def clean_email(email):
    """Remove mailto: prefix from email if present."""
    if email and isinstance(email, str) and email.lower().startswith("mailto:"):
        return email[len("mailto:"):]
    return email

def clean_social_field(social):
    """
    If the social field is a dictionary with empty values for 'facebook_url',
    'twitter_url', 'linkedin_url' and an empty list for 'other_urls', return an empty dict.
    Otherwise, return the original social field.
    """
    if isinstance(social, dict):
        facebook_empty = social.get('facebook_url', '') == ''
        twitter_empty = social.get('twitter_url', '') == ''
        linkedin_empty = social.get('linkedin_url', '') == ''
        other_empty = isinstance(social.get('other_urls', []), list) and not social.get('other_urls', [])
        if facebook_empty and twitter_empty and linkedin_empty and other_empty:
            return {}
    return social

class Export:
    """PostProcessing - Export to CSV"""

    def __init__(self, writer):
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.mongo_db = self.mongo_client[MONGO_DB]
        self.mongo_col = self.mongo_db[MONGO_COL_DATA]
        self.writer = writer

    def start(self):
        """Start Function"""
        self.writer.writerow(csv_headers)

        for product in self.mongo_col.find():
            # Retrieve fields from the product document.
            first_name = product.get("first_name", '')
            middle_name = product.get("middle_name", '')
            last_name = product.get("last_name", '')
            office_name = product.get("office_name", '')
            title = product.get("title", '')
            description = product.get("description", '')
            languages = product.get("languages", '')
            image_url = product.get("image_url", '')
            address = product.get("address", '')
            city = product.get("city", '')
            state = product.get("state", '')
            country = product.get("country", '')
            zipcode = product.get("zipcode", '')
            # Leave the list fields as they are (even if they are empty lists).
            office_phone_numbers = product.get("office_phone_numbers", '')
            agent_phone_numbers = product.get("agent_phone_numbers", '')
            email = clean_email(product.get("email", ''))
            website = product.get("website", '')
            social = clean_social_field(product.get("social", ''))
            profile_url = product.get("profile_url", '')

            data = [
                first_name,
                middle_name,
                last_name,
                office_name,
                title,
                description,
                languages,
                image_url,
                address,
                city,
                state,
                country,
                zipcode,
                office_phone_numbers,
                agent_phone_numbers,
                email,
                website,
                social,
                profile_url
            ]

            self.writer.writerow(data)


if __name__ == "__main__":
    with open(FILE_NAME_FULLDUMP, "a", encoding="utf-8") as file:
        writer_file = csv.writer(file, delimiter=",", quotechar='"')
        export = Export(writer_file)
        export.start()
