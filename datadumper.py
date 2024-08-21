import requests
from bs4 import BeautifulSoup
import os
import zipfile
from tqdm import tqdm
import threading

# Function to download a zip file
def download_zip(link, location):
    zip_file_response = requests.get(link)
    status_code = zip_file_response.status_code

    print(f"Status Code: {status_code}")

    if status_code == 200:
        # Extract the filename from the URL
        filename = os.path.basename(link)
        # Construct the full path to save the file
        file_path = os.path.join(location, filename)
        
        # Save the file content
        with open(file_path, 'wb') as f:
            f.write(zip_file_response.content)
            print(f"File downloaded and saved as {file_path}")
    else:
        print(f"Failed to download the file. Status code: {status_code}")

# Function to start downloading files in threads
def download_files_in_threads(url_list, location):
    threads = []
    for url in url_list:
        thread = threading.Thread(target=download_zip, args=(url, location))
        thread.start()
        threads.append(thread)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()

# Function to extract a zip file
def extract_zip(zip_file, extract_location):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_location)
        print(f"Extracted {zip_file} into {extract_location}")

# Function to start extracting files in threads
def extract_files_in_threads(zip_files, extract_location):
    threads = []
    for zip_file in zip_files:
        thread = threading.Thread(target=extract_zip, args=(zip_file, extract_location))
        thread.start()
        threads.append(thread)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()

# Function to dump data from a base URL
def dump_data(base_url, location):
    r = requests.get(base_url)
    status_code = r.status_code
    print(f"Status Code: {status_code}")

    if status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        zip_urls = []
        for anchor in soup.find_all('a'):
            href = anchor.get('href')
            if href and href.endswith('.zip'):
                zip_file_name = href
                print(f"Found file: {zip_file_name}")
                zip_urls.append(base_url + zip_file_name)

        # Download all zip files concurrently
        download_files_in_threads(zip_urls, location)
    elif status_code == 404:
        print(f"Error 404 Not Found For {base_url}")

if __name__ == "__main__":
    year = '2000'
    states = ["Alabama", "Alaska", "Arizona", "Arkansas", \
              "California", "Colorado", "Connecticut", \
              "Delaware", "District_of_Columbia", "Florida", \
              "Georgia", "Hawaii", "Idaho", "Illinois", \
              "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", \
              "Maine", "Maryland", "Massachusetts", "Michigan", \
              "Minnesota", "Mississippi", "Missouri", "Montana", \
              "Nebraska", "Nevada", "New_Hampshire", "New_Jersey", \
              "New_Mexico", "New_York", "North_Carolina", \
              "North_Dakota", "Ohio", "Oklahoma", "Oregon", \
              "Pennsylvania", "Puerto_Rico", "Rhode_Island", \
              "South_Carolina", "South_Dakota", "Tennessee", \
              "Texas", "Utah", "Vermont", "Virginia", "Washington", \
              "West_Virginia", "Wisconsin", "Wyoming"]

    # Will dump all the data first
    for state in tqdm(states):
        state = state.replace(' ', '_')
        os.makedirs(f"dump/{state}", exist_ok=True)
        os.makedirs(f"dump/extracted_data/{state}", exist_ok=True)
        match year:
            case "2010":
                dump_data('https://www2.census.gov/census_' + year + '/redistricting_file--pl_94-171/' + state +'/', f'dump/{state}')
            case "2000":
                dump_data('https://www2.census.gov/census_' + year + '/datasets/redistricting_file--pl_94-171/' + state +'/', f'dump/{state}')
            case "2020":
                dump_data('https://www2.census.gov/programs-surveys/decennial/' + year + '/data/01-Redistricting_File--PL_94-171/' + state +'/', f'dump/{state}')

    # Then it will extract all the folders
    for state in tqdm(states):
        zip_files = [os.path.join(f'dump/{state}', f) for f in os.listdir(f'dump/{state}') if f.endswith('.zip')]
        extract_files_in_threads(zip_files, f'dump/extracted_data/{state}')
