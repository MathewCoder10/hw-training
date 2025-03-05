import os
import requests
from pymongo import MongoClient

class Image_Downloader:
    def __init__(self, db_uri='mongodb://localhost:27017', db_name='softreelly_db', collection_name='parser'):
        # Define the mapping between collection fields and destination folders
        self.fields_and_folders = {
            'cover_image_url': 'cover_images',
            'architecture_images': 'architecture_images',
            'interior_images': 'interior_images',
            'lobby_images': 'lobby_images',
            'layout_images': 'layout_images'
        }
        
        # Create directories if they don't exist
        for folder in self.fields_and_folders.values():
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        # Connect to MongoDB
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.parser_collection = self.db[collection_name]
    
    def download(self, url, folder, filename):
        """Downloads an image from a URL and saves it in the given folder with the filename."""
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                filepath = os.path.join(folder, filename)
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                print(f"Downloaded {url} to {filepath}")
            else:
                print(f"Failed to download {url} (Status code: {response.status_code})")
        except Exception as e:
            print(f"Error downloading {url}: {e}")
    
    def start(self):
        """Starts the image download process from the MongoDB collection."""
        for doc in self.parser_collection.find():
            project_id = doc.get("id", '')
            
            # Process the cover_image_url (assumed to be a single URL)
            cover_url = doc.get("cover_image_url")
            if cover_url:
                parts = os.path.splitext(cover_url)
                ext = parts[1] if parts[1] else '.jpg'
                filename = f"cover_image_{project_id}{ext}"
                self.download(cover_url, self.fields_and_folders['cover_image_url'], filename)
            
            # Process image fields that may have multiple URLs
            for field in ['architecture_images', 'interior_images', 'lobby_images', 'layout_images']:
                images_field = doc.get(field, "")
                if images_field:
                    # Split the comma-separated string and remove any extra quotes/spaces
                    image_urls = [s.strip().strip('"') for s in images_field.split(',') if s.strip().strip('"')]
                    for idx, url in enumerate(image_urls):
                        if url:  # ensure the URL is not empty
                            parts = os.path.splitext(url)
                            ext = parts[1] if parts[1] else '.jpg'
                            filename = f"{field}_{project_id}_{idx}{ext}"
                            self.download(url, self.fields_and_folders[field], filename)
    
    def close(self):
        """Closes the MongoDB connection."""
        self.client.close()

if __name__ == "__main__":
    downloader_obj = Image_Downloader()
    downloader_obj.start()
    downloader_obj.close()
