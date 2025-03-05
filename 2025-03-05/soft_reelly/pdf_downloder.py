import os
import re
import requests
from pymongo import MongoClient

class Pdf_Downloader:
    def init(self):
        """
        Initializes the environment by creating the download folder 
        and establishing a MongoDB connection.
        """
        os.makedirs("soft_reelly_pdf", exist_ok=True)
        self.mongo_client = MongoClient('mongodb://localhost:27017')
        db = self.mongo_client['softreelly_db']
        self.collection = db['parser']

    def direct_pdf(self, url, file_path):
        """
        Downloads a PDF directly from the given URL and saves it to file_path.
        """
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded directly: {file_path}")
            else:
                print(f"Direct download failed from {url} (status code: {response.status_code})")
        except Exception as e:
            print(f"Error in direct download from {url}: {e}")

    def indirect_pdf(self, url, file_path):
        """
        Downloads a PDF indirectly using a Google Drive URL.
        If it does, extracts the file ID and downloads the PDF.
        Otherwise, the URL is skipped.
        """
        match = re.search(r"/d/([^/]+)/", url)
        if not match:
            print(f"Could not extract file id from {url}")
            return
        file_id = match.group(1)
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        try:
            response = requests.get(download_url, stream=True)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded indirectly: {file_path}")
            else:
                print(f"Indirect download failed from {download_url} (status code: {response.status_code})")
        except Exception as e:
            print(f"Error in indirect download from {url}: {e}")

    def start(self):
        """
        Iterates over all documents in the MongoDB collection, processes URLs 
        for floor plans and marketing brochures, and downloads the PDFs using 
        the appropriate method based on URL content.
        """
        for doc in self.collection.find():
            doc_id = doc.get("id") or str(doc.get("_id"))
            
            # Process floor plan URLs
            floor_urls = doc.get("floor_plans_pdf")
            if floor_urls:
                if isinstance(floor_urls, str):
                    floor_urls = [floor_urls]
                for idx, url in enumerate(floor_urls):
                    filename = (f"{doc_id}_floorplan.pdf" 
                                if len(floor_urls) == 1 
                                else f"{doc_id}_floorplan_{idx+1}.pdf")
                    file_path = os.path.join("soft_reelly_pdf", filename)
                    
                    if ".pdf" in url.lower():
                        self.direct_pdf(url, file_path)
                    elif "https://drive.google.com" in url:
                        self.indirect_pdf(url, file_path)
                    else:
                        print(f"Skipping URL: {url}")
            
            # Process marketing brochure URLs
            brochure_urls = doc.get("marketing_brochure")
            if brochure_urls:
                if isinstance(brochure_urls, str):
                    brochure_urls = [brochure_urls]
                for idx, url in enumerate(brochure_urls):
                    filename = (f"{doc_id}_brochure.pdf" 
                                if len(brochure_urls) == 1 
                                else f"{doc_id}_brochure_{idx+1}.pdf")
                    file_path = os.path.join("soft_reelly_pdf", filename)
                    
                    if ".pdf" in url.lower():
                        self.direct_pdf(url, file_path)
                    elif "https://drive.google.com" in url:
                        self.indirect_pdf(url, file_path)
                    else:
                        print(f"Skipping URL: {url}")

    def close(self):
        """
        Closes the MongoDB connection.
        """
        if hasattr(self, 'mongo_client') and self.mongo_client:
            self.mongo_client.close()
            print("Closed MongoDB connection.")

if __name__ == "__main__":
    downloader = Pdf_Downloader()
    downloader.init()
    downloader.start()
    downloader.close()
