import os
import requests
from pymongo import MongoClient

class Image_Downloader:
    def __init__(self, db_uri='mongodb://localhost:27017', db_name='softreelly_db', collection_name='parser'):
        # Use a single folder for all images
        self.output_folder = 'soft_reely_images'
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        
        # Mapping of collection fields to filename prefixes
        self.field_prefixes = {
            'cover_image_url': 'cover',
            'architecture_images': 'architecture',
            'interior_images': 'interior',
            'lobby_images': 'lobby',
            'layout_images': 'layout'
        }
        
        # Connect to MongoDB
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.parser_collection = self.db[collection_name]
    
    def download(self, url, filename):
        """Downloads an image from a URL and saves it in the output folder with the given filename."""
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                filepath = os.path.join(self.output_folder, filename)
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                print(f"Downloaded {url} to {filepath}")
            else:
                print(f"Failed to download {url} (Status code: {response.status_code})")
        except Exception as e:
            print(f"Error downloading {url}: {e}")
    
    def _get_file_extension(self, url):
        """Extracts the file extension from the URL. Returns an empty string if not found."""
        parts = os.path.splitext(url)
        return parts[1] if parts[1] else ""
    
    def start(self):
        """Starts the image download process from the MongoDB collection."""
        for doc in self.parser_collection.find():
            project_id = doc.get("id", '')
            
            # Process each field based on the mapping
            for field, prefix in self.field_prefixes.items():
                field_value = doc.get(field, "")
                if field_value:
                    # Split the comma-separated string into URLs and remove extra quotes/spaces
                    image_urls = [s.strip().strip('"') for s in field_value.split(',') if s.strip().strip('"')]
                    
                    # If only one image, do not add numbering
                    if len(image_urls) == 1:
                        url = image_urls[0]
                        ext = self._get_file_extension(url)
                        if not ext:
                            print(f"Skipping {url} because it has no file extension.")
                            continue
                        filename = f"{project_id}_{prefix}{ext}"
                        self.download(url, filename)
                    else:
                        # For multiple images, add an index starting at 1
                        for idx, url in enumerate(image_urls):
                            ext = self._get_file_extension(url)
                            if not ext:
                                print(f"Skipping {url} because it has no file extension.")
                                continue
                            filename = f"{project_id}_{prefix}_{idx+1}{ext}"
                            self.download(url, filename)
    
    def close(self):
        """Closes the MongoDB connection."""
        self.client.close()

if __name__ == "__main__":
    downloader_obj = Image_Downloader()
    downloader_obj.start()
    downloader_obj.close()
