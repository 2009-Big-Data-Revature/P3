import requests
from bs4 import BeautifulSoup
import os
import zipfile
from tqdm import tqdm
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Function to download a zip file with retries
def download_zip(link, location):
    session = requests.Session()
    retries = 3
    for i in range(retries):
        try:
            zip_file_response = session.get(link, timeout=10)
            status_code = zip_file_response.status_code
            print(f"Status Code: {status_code}")

            if status_code == 200:
                filename = os.path.basename(link)
                file_path = os.path.join(location, filename)
                
                with open(file_path, 'wb') as f:
                    f.write(zip_file_response.content)
                    print(f"File downloaded and saved as {file_path}")
                break  # Success, no need to retry
            else:
                print(f"Failed to download the file. Status code: {status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {i+1} failed: {e}")
            if i == retries - 1:
                print("Max retries reached. Skipping this file.")
            time.sleep(1)  # Small delay before retrying

# Function to extract a zip file
def extract_zip(zip_file, extract_location):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_location)
        print(f"Extracted {zip_file} into {extract_location}")

# Function to start extracting files in threads
def extract_files_in_threads(zip_files, extract_location):
    # threads = []
    # for zip_file in zip_files:
    #     thread = threading.Thread(target=extract_zip, args=(zip_file, extract_location))
    #     thread.start()
    #     threads.append(thread)
    
    # # Wait for all threads to complete
    # for thread in threads:
    #     thread.join()
    
    with ThreadPoolExecutor() as executor:
        for zip_file in zip_files:
            executor.submit(extract_zip, zip_file, extract_location)

# Function to dump data from a base URL
def dump_data(base_url, location):
    session = requests.Session()
    try:
        r = session.get(base_url, timeout=10)
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

            with ThreadPoolExecutor(max_workers=5) as executor:
                for url in zip_urls:
                    executor.submit(download_zip, url, location)
        elif status_code == 404:
            print(f"Error 404 Not Found For {base_url}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve base URL: {e}")

if __name__ == "__main__":
    start_time = time.time()
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

    # Use ThreadPoolExecutor to limit the number of concurrent threads
    with tqdm(total=len(states)) as pbar:
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for state in states:
                state = state.replace(' ', '_')
                os.makedirs(f"dump/{state}", exist_ok=True)
                os.makedirs(f"dump/extracted_data/{state}", exist_ok=True)
                match year:
                    case "2010":
                        futures.append(executor.submit(dump_data, 'https://www2.census.gov/census_' + year + '/redistricting_file--pl_94-171/' + state +'/', f'dump/{state}'))
                    case "2000":
                        futures.append(executor.submit(dump_data, 'https://www2.census.gov/census_' + year + '/datasets/redistricting_file--pl_94-171/' + state +'/', f'dump/{state}'))
                    case "2020":
                        futures.append(executor.submit(dump_data, 'https://www2.census.gov/programs-surveys/decennial/' + year + '/data/01-Redistricting_File--PL_94-171/' + state +'/', f'dump/{state}'))

            for future in as_completed(futures):
                pbar.update(1)


    # Extract all files after download
    with ThreadPoolExecutor(max_workers=100) as executor:
        for state in states:
            zip_files = [os.path.join(f'dump/{state}', f) for f in os.listdir(f'dump/{state}') if f.endswith('.zip')]
            executor.submit(extract_files_in_threads, zip_files, f'dump/extracted_data/{state}')
    
    print(f"Finished in {(time.time() - start_time):.3f}s")
    