import requests
from pymongo import MongoClient, errors

class Crawler:
    def __init__(self, mongo_uri='mongodb://localhost:27017/'):
        # API configuration
        self.base_url = 'https://api.reelly.io/api:sk5LT7jx/projectsExternalSearch'
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://soft.reelly.io',
            'referer': 'https://soft.reelly.io/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        
        # MongoDB connection
        self.client = MongoClient(mongo_uri)
        self.db = self.client['softreelly_db']
        self.collection = self.db['crawler']
        # Create a unique index on "id"
        self.collection.create_index("id", unique=True)

    def start(self):
        current_page = 1
        while True:
            params = {
                'page': str(current_page)
            }
            response = requests.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                
                if not items:
                    print(f"No items found on page {current_page}. Ending crawl.")
                    break

                # Use the generator to process and insert each item one by one
                for extracted_item in self.parse_items(items):
                    try:
                        self.collection.insert_one(extracted_item)
                        print(f"Page {current_page}: Inserted item with id {extracted_item['id']}.")
                    except errors.DuplicateKeyError:
                        print(f"Page {current_page}: Duplicate item with id {extracted_item['id']} skipped.")
                    except Exception as e:
                        print(f"Page {current_page}: Error inserting item with id {extracted_item['id']}:", str(e))

                # Determine if more pages exist using 'pageTotal' or 'nextPage'
                page_total = data.get('pageTotal')
                if page_total and current_page >= page_total:
                    print("Reached the last page.")
                    break
                    
                if not data.get('nextPage'):
                    print("No next page found. Ending crawl.")
                    break
                
                current_page += 1
            else:
                print("Error fetching data on page", current_page, ":", response.status_code)
                break

    def parse_items(self, items):
        """Generator that yields parsed items."""
        for item in items:
            item_id = item.get("id")
            extracted = {
                "id": item_id,
                "url": f"https://soft.reelly.io/project/general?projectid={item_id}&utm_source=reelly_platform",
                "project_name": item.get("Project_name"),
                "developers_name": item.get("Developers_name"),
                "completion_date": item.get("Completion_date"),
                "completion_time": item.get("Completion_time"),
                "area_name": item.get("Area_name"),
                "status": item.get("Status"),
                "sale_status": item.get("sale_status"),
                "minimum_price": item.get("min_price"),
                "cover_image_url": item.get("cover", {}).get("url")
            }
            yield extracted

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()

if __name__ == "__main__":
    crawler = Crawler()
    crawler.start()
    crawler.close()
