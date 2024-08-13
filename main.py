import os
import requests
import json
import hashlib
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Function to compute MD5 hash of a file
def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# Function to download a single file in chunks (for multi-connection downloads)
def download_chunk(url, start, end, dest_path, index):
    headers = {'Range': f'bytes={start}-{end}'}
    response = requests.get(url, headers=headers, stream=True)
    with open(dest_path, f'r+b') as f:
        f.seek(start)
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
    return index

# Function to download a file using multiple connections
def download_file_multi_connection(url, dest_path, total_size, num_connections=4):
    chunk_size = total_size // num_connections
    chunks = [(i * chunk_size, (i + 1) * chunk_size - 1) for i in range(num_connections)]
    chunks[-1] = (chunks[-1][0], total_size - 1)  # Ensure the last chunk goes to the end

    # Prepare the file with the expected size
    with open(dest_path, 'wb') as f:
        f.truncate(total_size)

    # Download the file in chunks using multiple threads
    with ThreadPoolExecutor(max_workers=num_connections) as executor:
        futures = [
            executor.submit(download_chunk, url, start, end, dest_path, idx)
            for idx, (start, end) in enumerate(chunks)
        ]
        for future in tqdm(futures, desc="Downloading file with multiple connections"):
            future.result()

# Function to download and verify a single file
def download_and_verify_file(resource, main_url, base_dir, failed_resources, use_multi_connection, num_connections):
    dest_path = os.path.join(base_dir, resource['dest'].lstrip('/'))
    url = f"{main_url}/{resource['dest'].lstrip('/')}"
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    # Check if the file already exists and has a valid hash
    if os.path.exists(dest_path):
        existing_md5 = calculate_md5(dest_path)
        if existing_md5 == resource['md5']:
            print(f"File {resource['dest']} already exists and is valid. Skipping download.")
            return True

    if use_multi_connection:
        # Multi-connection download
        response = requests.head(url)
        total_size = int(response.headers.get('content-length', 0))
        download_file_multi_connection(url, dest_path, total_size, num_connections=num_connections)
    else:
        # Single connection download
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 Kibibyte
        t = tqdm(total=total_size, unit='iB', unit_scale=True)
        with open(dest_path, 'wb') as f:
            for data in response.iter_content(block_size):
                t.update(len(data))
                f.write(data)
        t.close()

    # Verify file integrity using MD5 hash
    downloaded_md5 = calculate_md5(dest_path)
    if downloaded_md5 != resource['md5']:
        print(f"Hash mismatch for {resource['dest']}. Expected {resource['md5']}, got {downloaded_md5}")
        failed_resources.append(resource)
        return False
    return True

# Main function to orchestrate the download process
def download_resources(resources, main_url, version, json_name, use_multi_connection, num_connections, max_concurrent_files):
    # Define base directory for successful downloads
    base_dir = os.path.join(os.getcwd(), 'download', version)

    # Define directory for failed downloads log
    failed_dir = os.path.join(os.getcwd(), 'failed', version)
    os.makedirs(failed_dir, exist_ok=True)

    failed_resources = []

    # Use ThreadPoolExecutor to handle multiple concurrent file downloads
    with ThreadPoolExecutor(max_workers=max_concurrent_files) as executor:
        futures = [
            executor.submit(
                download_and_verify_file, resource, main_url, base_dir, failed_resources, use_multi_connection, num_connections
            )
            for resource in resources
        ]
        
        # Wait for all futures to complete
        for future in tqdm(futures, desc="Downloading files"):
            future.result()

    # Save failed resources to a JSON file
    if failed_resources:
        failed_json_path = os.path.join(failed_dir, json_name)
        with open(failed_json_path, 'w') as f:
            json.dump({'resource': failed_resources}, f, indent=4)
        print(f"Failed resources logged in {failed_json_path}")

def load_resources(json_url=None, json_file=None):
    if json_url:
        response = requests.get(json_url)
        return response.json(), os.path.basename(json_url)
    elif json_file:
        with open(json_file, 'r') as f:
            return json.load(f), os.path.basename(json_file)
    else:
        raise ValueError("Either json_url or json_file must be provided.")

if __name__ == "__main__":
    # Input parameters
    source_type = input("Enter the source of the JSON (url/file): ").strip().lower()

    if source_type == "url":
        json_url = input("Enter the URL of the JSON file: ").strip()
        resources, json_name = load_resources(json_url=json_url)
    elif source_type == "file":
        json_file = input("Enter the path to the JSON file: ").strip()
        resources, json_name = load_resources(json_file=json_file)
    else:
        print("Invalid input. Please enter 'url' or 'file'.")
        exit(1)

    main_url = input("Enter the main path URL (e.g., https://example.com/zip): ").strip()
    version = input("Enter the version to be used as a folder name (e.g., 0.9.0): ").strip()

    # Option to use multi-connection download for a single file
    use_multi_connection = input("Use multi-connection download for a single file? (y/n): ").strip().lower() == 'y'
    num_connections = int(input("Enter the number of connections per file (if multi-connection is enabled): ").strip())

    # Number of files to download concurrently
    max_concurrent_files = int(input("Enter the maximum number of files to download concurrently: ").strip())

    # Start the download process
    download_resources(resources['resource'], main_url, version, json_name, use_multi_connection=use_multi_connection, num_connections=num_connections, max_concurrent_files=max_concurrent_files)
