import os
import zipfile
import requests
from pathlib import Path


def download_and_extract_gtfs(url, extract_folder="batch_files"):
    """
    Download zip file from URL and extract contents to specified folder
    
    Args:
        url (str): URL of the zip file to download
        extract_folder (str): Folder name to extract contents to
    """
    
    # Create extract folder if it doesn't exist
    extract_path = Path(extract_folder)
    extract_path.mkdir(exist_ok=True)
    
    print(f"Downloading GTFS data from: {url}")
    print(f"Extracting to: {extract_path.absolute()}")
    
    try:
        # Download the zip file
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Create a temporary zip file
        zip_filename = "gtfs_supplemented.zip"
        zip_path = Path(zip_filename)
        
        # Write the downloaded content to a zip file
        with open(zip_path, 'wb') as zip_file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    zip_file.write(chunk)
        
        print(f"Downloaded {zip_path.stat().st_size} bytes")
        
        # Extract the zip file
        print("Extracting zip file...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        # List extracted files
        extracted_files = list(extract_path.iterdir())
        print(f"\nExtracted {len(extracted_files)} files:")
        for file in sorted(extracted_files):
            if file.is_file():
                print(f"  - {file.name} ({file.stat().st_size} bytes)")
        
        # Clean up the temporary zip file
        zip_path.unlink()
        print(f"\nCleaned up temporary file: {zip_filename}")
        
        print(f"\nGTFS data successfully extracted to: {extract_path.absolute()}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
    except zipfile.BadZipFile as e:
        print(f"Error extracting zip file: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    # URL of the GTFS zip file
    gtfs_url = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip"
    
    # Download and extract
    download_and_extract_gtfs(gtfs_url)