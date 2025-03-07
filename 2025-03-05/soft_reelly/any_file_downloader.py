import requests
import os
import re
from urllib.parse import urlparse

# Define allowed file extensions (all in lowercase).
ALLOWED_EXTENSIONS = {
    ".txt", ".rtf", ".doc", ".docx", ".odt",
    ".csv", ".xls", ".xlsx", ".pdf", ".jpeg",
    ".jpg", ".png"
}

def is_valid_url(url):
    """
    Validates the URL by checking if it has a valid scheme and netloc.
    """
    parsed = urlparse(url)
    return bool(parsed.scheme and parsed.netloc)

def get_extension(filename):
    """
    Returns the file extension in lowercase.
    """
    _, ext = os.path.splitext(filename)
    return ext.lower()

def download_file(url, output_folder='downloads'):
    """
    Downloads a file from a URL and saves it to a specified folder.
    For non–Google Drive URLs, only downloads if the file extension is allowed.
    For Google Drive URLs with a file id pattern, extracts the id and downloads indirectly.

    Parameters:
        url (str): The URL of the file to download.
        output_folder (str): Folder to save the downloaded file.

    Returns:
        str: The path to the saved file, or None if an error occurred.
    """
    # Validate the URL.
    if not is_valid_url(url):
        print(f"Invalid URL: {url}")
        return None

    # Create the output folder if it does not exist.
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Extract the initial filename from the URL.
    local_filename = url.split('/')[-1]
    if '?' in local_filename:
        local_filename = local_filename.split('?')[0]
    
    # For non-Google Drive URLs, verify the extension.
    if "drive.google.com" not in url:
        ext = get_extension(local_filename)
        if ext not in ALLOWED_EXTENSIONS:
            print(f"Skipping download. Invalid file extension '{ext}' in URL: {url}")
            return None

    # Construct the full file path.
    file_path = os.path.join(output_folder, local_filename)

    # Check if URL is a Google Drive link.
    if "drive.google.com" in url:
        # Look for the pattern /d/<file_id>/ in the URL.
        match = re.search(r"/d/([^/]+)/", url)
        if not match:
            print(f"Could not extract file id from {url}")
            return None
        file_id = match.group(1)
        # Note: For Google Drive, we use an indirect download URL.
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        print(f"Extracted file id: {file_id}. Using indirect download URL: {download_url}")
    else:
        download_url = url

    try:
        # Stream download the file.
        with requests.get(download_url, stream=True) as response:
            response.raise_for_status()  # Check for HTTP errors.
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive new chunks.
                        file.write(chunk)
        if "drive.google.com" in url:
            print(f"Downloaded indirectly from Google Drive: {file_path}")
        else:
            print(f"Downloaded: {file_path}")
        return file_path
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

if __name__ == "__main__":
    urls = [
        "https://api.reelly.io/vault/ZZLvFZFt/sV17P1RUZAJ2lF7Md-9VoZCPR6w/PtNTwQ../St.png",  # Valid: .png
        "https://api.reelly.io/vault/ZZLvFZFt/On597XMCS0fiVgvac7_Ee6gl57Q/Okuotg../1BR.png",  # Valid: .png
        "https://api.reelly.io/vault/ZZLvFZFt/S4erG6LjpICpBR_rb-i1h6A57sQ/PPuq_g../2BR.png",  # Valid: .png
        "https://api.reelly.io/vault/ZZLvFZFt/vkvXMXpxXbULK04yeeTm9scTaNM/WXzuAQ../3BR.png",  # Valid: .png
        "https://api.reelly.io/vault/ZZLvFZFt/bmZeKNal_axRXd9GvJo64OJ7MFc/v0TtjQ../Affini+Floorplans.pdf",  # Valid: .pdf
        "https://drive.google.com/file/d/1jwJ__XeFSHV3dgg1W5lpmzZSaguUJu9c/view?usp=sharing",  # Google Drive URL.
        "https://example.com/file.exe",  # Invalid: .exe is not allowed.
        "invalidurl"  # This is an invalid URL example.
    ]
    
    # Download each file from the list.
    for url in urls:
        download_file(url)
