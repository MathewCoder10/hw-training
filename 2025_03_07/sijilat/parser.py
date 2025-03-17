import requests
from pymongo import MongoClient

class Parser:
    @staticmethod
    def get_token():
        auth_headers = {
            'accept': '*/*',
            'accept-language': 'en-GB,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.sijilat.bh',
            'priority': 'u=1, i',
            'referer': 'https://www.sijilat.bh/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        data = {
            'username': 'sijilat',
            'password': 'af3ba08ce647fce9b2af578f43ad44905e6e74181f30abb8e43cb0ef3cf2e371',
            'grant_type': 'password',
        }
        response = requests.post('https://api.sijilat.bh/token', headers=auth_headers, data=data)
        if response.status_code == 200:
            token = response.json().get('access_token')
            return token
        else:
            raise Exception(f"Failed to acquire token: {response.status_code} {response.text}")

    @staticmethod
    def dict_to_string(d):
        """
        Converts a dictionary into a string with each key-value pair formatted as:
        'KEY': 'VALUE'
        If a value is None, it outputs an empty string.
        """
        return ", ".join(f"'{key}': {repr(value) if value is not None else repr('')}" for key, value in d.items())

    def __init__(self):
        # Connect to MongoDB and select the database
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['sijilat']
        # Source collection where crawler stores records
        self.crawler_collection = self.db['crawler']
        # Target collection for parsed data
        self.parser_collection = self.db['parser_active']
        self.token = self.get_token()

    def parse_items(self):
        documents = self.crawler_collection.find({})
        for doc in documents:
            # Process only if Status field of the document is "ACTIVE"
            if doc.get("Status") != "ACTIVE":
                continue

            # Extract cr_no and branch_no by splitting the combined "CR No." field.
            combined = doc.get("CR No.", "")
            if combined and "-" in combined:
                parts = combined.split("-")
                cr_no = parts[0].strip() 
                branch_no = parts[1].strip() if len(parts) > 1 else ""
            else:
                continue

            if not cr_no or not branch_no:
                continue

            # Build the display URL for this record.
            url = f"https://www.sijilat.bh/public-search-cr/search-cr-3.aspx?cr_no={cr_no}&branch_no={branch_no}"
            
            json_payload = {
                'cr_no': cr_no,
                'branch_no': branch_no,
                'cult_lang': 'EN',
                'Input_CULT_LANG': 'EN',
                'CULT_LANG': 'EN',
                'cultLang': 'EN',
                'CurrentMenuTyp': 'A',
                'CurrentMenu_Type': 'A',
                'MENU_TYPE': 'A',
                'cpr_no': '',
                'CPR_NO_LOGIN': '',
                'CPR_GCC_NO': '',
                'CPR_OR_GCC_NO': '',
                'Login_CPR_No': '',
                'Login_CPR': '',
                'APPCNT_CPR_NO': '',
                'cprno': '',
                'LOGIN_PB_NO': '',
                'PB_NO': '',
                'Input_PB_NO': '',
                'SESSION_ID': 'mqnj2ee1w4irewhpeyyjbuxc',
            }
            
            headers = {
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'authorization': f'Bearer {self.token}',
                'cache-control': 'no-cache',
                'content-type': 'application/json; charset=UTF-8',
                'origin': 'https://www.sijilat.bh',
                'pragma': 'no-cache',
                'priority': 'u=1, i',
                'referer': 'https://www.sijilat.bh/',
                'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Linux"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            }
            
            try:
                response = requests.post('https://api.sijilat.bh/api/CRdetails/CompleteCRDetails',
                                         headers=headers, json=json_payload)
                if response.status_code == 401:
                    self.token = self.get_token()
                    headers['authorization'] = f'Bearer {self.token}'
                    response = requests.post('https://api.sijilat.bh/api/CRdetails/CompleteCRDetails',
                                             headers=headers, json=json_payload)
                if response.status_code != 200:
                    continue
            except Exception:
                continue

            try:
                data = response.json()
            except Exception:
                continue

            json_data_content = data.get("jsonData", {})
            if isinstance(json_data_content, list):
                json_data_content = json_data_content[0] if len(json_data_content) > 0 else {}

            company_summary     = json_data_content.get("company_summary", {})
            commercial_address  = json_data_content.get("commercialAddress", {})
            business_activities = json_data_content.get("businessActivities") or []
            shareholders        = json_data_content.get("shareholdersAndPartners") or []
            
            formatted_company_summary = self.dict_to_string(company_summary)
            formatted_commercial_address = self.dict_to_string(commercial_address)
            
            # Format the business activities list as a comma-separated string; if empty, use an empty string.
            formatted_business_activities_list = [self.dict_to_string(act) for act in business_activities]
            formatted_business_activities = ", ".join(formatted_business_activities_list) if formatted_business_activities_list else ""
            
            # Format the shareholders list as a comma-separated string; if empty, use an empty string.
            formatted_shareholders_list = [self.dict_to_string(sh) for sh in shareholders]
            formatted_shareholders = ", ".join(formatted_shareholders_list) if formatted_shareholders_list else ""
            
            parsed_doc = {
                "cr_no": cr_no,
                "branch_no": branch_no,
                "url": url,
                "company_summary": formatted_company_summary,
                "commercial_address": formatted_commercial_address,
                "business_activities": formatted_business_activities,
                "shareholders": formatted_shareholders,
            }
            
            self.parser_collection.insert_one(parsed_doc)

    def close(self):
        self.client.close()

if __name__ == "__main__":
    parser = Parser()
    try:
        parser.parse_items()
    finally:
        parser.close()
