import os
import dropbox

def upload_folder(local_folder, dropbox_folder):
    # Retrieve the Dropbox access token from an environment variable
    access_token = os.getenv("DROPBOX_ACCESS_TOKEN")
    if not access_token:
        raise ValueError("Dropbox access token is not set in the environment variables.")
    
    # Initialize Dropbox client with the access token
    dbx = dropbox.Dropbox(access_token)
    
    # Walk through all subdirectories and files in the local folder
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            # Compute the path relative to the local folder
            relative_path = os.path.relpath(local_path, local_folder)
            # Create the corresponding Dropbox path (using forward slashes)
            dropbox_path = os.path.join(dropbox_folder, relative_path).replace(os.path.sep, '/')
            
            try:
                with open(local_path, "rb") as f:
                    print(f"Uploading {local_path} to Dropbox at {dropbox_path}...")
                    dbx.files_upload(f.read(), dropbox_path)
                print(f"Upload of {local_path} complete!")
            except Exception as e:
                print(f"Error uploading {local_path}: {e}")

if __name__ == "__main__":
    # Specify the local folder and the Dropbox folder destination
    local_folder = "soft_reely_images"
    dropbox_folder = "/softreely_images"  # This will be created under your app folder in Dropbox
    upload_folder(local_folder, dropbox_folder)
