# -------------------------------
# fast_download_manual.py
# -------------------------------

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import concurrent.futures
from tqdm import tqdm  # Library for the progress bar

# -------------------------------
# 1. Setup authentication
# -------------------------------
gauth = GoogleAuth()
gauth.auth_method = 'manual'

# Try to load saved credentials, otherwise use manual flow
if os.path.exists("client_secrets.json"):
    gauth.LoadClientConfigFile("client_secrets.json")

# Generate URL
auth_url = gauth.GetAuthUrl()
print("---------------------------------------------------------")
print("1. Go to this URL in your browser:")
print(auth_url)
print("---------------------------------------------------------")

code = input("2. Enter the code from the URL here: ")
gauth.Auth(code)
drive = GoogleDrive(gauth)

# -------------------------------
# 2. Configuration
# -------------------------------
ROOT_FOLDER_ID = "1LZmH_05yJqEAtdun_qjdWjjos3d-Olag"  # Your Folder ID
SAVE_ROOT = os.path.join(os.getcwd(), "DownloadedDrive")
MAX_WORKERS = 8  # Number of parallel downloads (Make it higher for speed, lower for stability)

# -------------------------------
# 3. Scan Folders & Build Queue
# -------------------------------
download_queue = []  # List to store files we need to download
total_bytes = 0

def scan_folder(folder_id, local_path):
    """
    Recursively scans folders to build the directory structure 
    and adds files to the download queue.
    """
    global total_bytes
    
    # Create the local directory immediately
    os.makedirs(local_path, exist_ok=True)
    print(f"Scanning: {local_path}")

    # Get list of files/folders in this folder
    file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()

    for f in file_list:
        if f['mimeType'] == 'application/vnd.google-apps.folder':
            # It's a folder: Recurse into it
            new_folder_path = os.path.join(local_path, f['title'])
            scan_folder(f['id'], new_folder_path)
        else:
            # It's a file: Add to queue
            file_save_path = os.path.join(local_path, f['title'])
            
            # Get file size for the progress bar (Handle Google Docs that have no size)
            file_size = 0
            if 'fileSize' in f:
                file_size = int(f['fileSize'])
            
            total_bytes += file_size
            
            download_queue.append({
                'drive_obj': f,
                'save_path': file_save_path,
                'size': file_size
            })

# -------------------------------
# 4. Worker Function for Threads
# -------------------------------
def download_worker(task, pbar):
    """
    Downloads a single file and updates the main progress bar.
    """
    try:
        f = task['drive_obj']
        path = task['save_path']
        
        # Download the file
        f.GetContentFile(path)
        
        # Update progress bar by the size of the file
        pbar.update(task['size'])
    except Exception as e:
        print(f"\nError downloading {task['save_path']}: {e}")

# -------------------------------
# 5. Main Execution
# -------------------------------
if __name__ == "__main__":
    print("\n--- Step 1: Scanning File Structure (Please Wait) ---")
    scan_folder(ROOT_FOLDER_ID, SAVE_ROOT)
    
    print(f"\nFound {len(download_queue)} files.")
    print(f"Total Size: {total_bytes / (1024*1024):.2f} MB")
    print("\n--- Step 2: Starting Parallel Download ---")

    # Setup the Progress Bar
    # unit='B', unit_scale=True makes it show MB/s automatically
    with tqdm(total=total_bytes, unit='B', unit_scale=True, unit_divisor=1024, desc="Downloading") as pbar:
        
        # Use ThreadPoolExecutor for parallel downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            
            # Submit all tasks to the pool
            futures = [executor.submit(download_worker, task, pbar) for task in download_queue]
            
            # Wait for all tasks to complete
            concurrent.futures.wait(futures)

    print("\nAll files downloaded successfully!")