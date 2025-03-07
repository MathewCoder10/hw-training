import requests
import time
from pymongo import MongoClient

class Crawler:
    def __init__(self):
        """Initialize MongoDB connection and fetch the authentication token."""
        # Connect to MongoDB (using the collection 'crawler_last' as in your snippet)
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['sijilat']
        self.collection = self.db['crawler']
        
        # Get the authentication token using the provided headers and payload
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
            self.token = response.json().get('access_token')
            print("Token acquired successfully")
        else:
            raise Exception(f"Failed to acquire token: {response.status_code} {response.text}")

        # Counters for total expected records and records inserted
        self.total_expected = None  # Will be set from the first API response
        self.inserted_count = 0

    def start(self):
        """Handle the crawling process with pagination, token refresh, and total records check."""
        # Define the base payload for the API request as provided
        json_data = {
            'draw': 2,
            'columns': [
                {'data': 'CR_NO', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
                {'data': 'CR_LNM', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
                {'data': 'CR_ANM', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
                {'data': 'CM_TYP_DESC', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
                {'data': 'REG_DATE', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
                {'data': 'EXPIRE_DATE', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
                {'data': 'STATUS', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
                {'data': 'ACTIVITIES', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
                {'data': 'SECTOR', 'name': '', 'searchable': True, 'orderable': False, 'search': {'value': '', 'regex': False}},
            ],
            'order': [],
            'start': 0,
            'length': 10,
            'search': {'value': '', 'regex': False},
            'CR_NO': '',
            'CR_LNM': '',
            'CR_ANM': '',
            'CM_TYP_CD': '',
            'STATUS': '',
            'CR_MUNCP_CD': '',
            'CR_BLOCK': '',
            'CM_NAT_CD': '',
            'FIRST_LEV': '',
            'PERSON_LNM': '',
            'PERSON_ANM': '',
            'PTNER_NAT_CD': '',
            'REG_DATE_FROM': '',
            'REG_DATE_TO': '',
            'CR_ROAD': '',
            'CR_FLAT': '',
            'CR_BULD': '',
            'PSPORT_NO': '',
            'PTNER_CR_NO': '',
            'VCR_YN': '',
            'ISIC4_CD': "'681','682-1','682-2'",
            'CurrentMenuType': 'A',
            'cpr_no': '',
            'CULT_LANG': 'EN',
            'PaginationParams': {
                'Page': 1,
                'ItemPerPage': 10,
            },
        }
        
        page = 1
        max_retries = 3
        while True:
            json_data["PaginationParams"]["Page"] = page
            
            headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en;q=0.9',
                'authorization': f'Bearer {self.token}',
                'content-type': 'application/json; charset=UTF-8',
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
            
            retry_count = 0
            while retry_count < max_retries:
                response = requests.post('https://api.sijilat.bh/api/CRdetails/AdvanceSearchCR_Paging',
                                         headers=headers, json=json_data)
                if response.status_code in (500, 504):
                    retry_count += 1
                    print(f"Server error ({response.status_code}) on page {page}. Retrying {retry_count}/{max_retries} ...")
                    time.sleep(5)
                else:
                    break
            
            if response.status_code == 401:
                print(f"Unauthorized access on page {page}. Refreshing token...")
                self.token = self.__class__.get_token_static()
                if not self.token:
                    print("Failed to refresh token.")
                    break
                headers['authorization'] = f'Bearer {self.token}'
                response = requests.post('https://api.sijilat.bh/api/CRdetails/AdvanceSearchCR_Paging',
                                         headers=headers, json=json_data)
            
            if response.status_code != 200:
                print("Failed to retrieve data on page", page)
                break
            
            try:
                data = response.json()
            except ValueError:
                print("Response content is not valid JSON on page", page)
                break
            
            # Check total expected records from the first API response
            if self.total_expected is None:
                total_str = data.get("jsonData", {}).get("Total_Records", "0")
                try:
                    self.total_expected = int(total_str)
                except ValueError:
                    self.total_expected = 0
                print(f"Total expected records: {self.total_expected}")
            
            cr_list = data.get("jsonData", {}).get("CR_list", [])
            if not cr_list:
                print("No more records found on page", page)
                break
            
            for record in cr_list:
                self.parse_item(record)
                self.inserted_count += 1
                if self.inserted_count >= self.total_expected:
                    print("Inserted record count matches/exceeds total expected records. Stopping crawling.")
                    return
            page += 1

    def parse_item(self, record):
        """Extract required fields and insert a document into MongoDB."""
        cr_no = record.get("CR_NO", "")
        branch_no = record.get("BRANCH_NO", "")
        combined_cr_no = f"{cr_no}-{branch_no}"
        
        document = {
            "CR No.": combined_cr_no,
            "Commercial Name (EN)": record.get("CR_LNM", ""),
            "Commercial Name (AR)": record.get("CR_ANM", ""),
            "Company Type": record.get("CM_TYP_DESC", ""),
            "Reg. Date": record.get("REG_DATE", ""),
            "Exp. Date": record.get("EXPIRE_DATE", ""),
            "Status": record.get("STATUS", ""),
            "Activities": record.get("ACTIVITIES", "")
        }
        self.collection.insert_one(document)
        print("Inserted document:", document)

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()
        print("Database connection closed.")
    
    @staticmethod
    def get_token_static():
        """Static method to get a new token (used when token refresh is needed)."""
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
            print("Token refreshed successfully")
            return token
        else:
            print("Failed to refresh token:", response.status_code, response.text)
            return None

if __name__ == "__main__":
    crawler = Crawler()
    try:
        crawler.start()
    finally:
        crawler.close()
