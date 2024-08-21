import requests
from bs4 import BeautifulSoup
import os
import zipfile


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
            
'''
A few useful status codes

    Status Codes
    200 : Success (OK)
    201 : Created
    202 : Accepted
    204: No Content
    205 : Reset Content
    400 : Bad Request
    401 : Unauthorized
    403 : Forbidden
    404 : Not Found
'''
def dump_data(base_url, location):
    r = requests.get(base_url)
    r.headers['content-type']
    status_code = r.status_code
    print(f"Status Code: {status_code}")

    if(status_code == 200):
        soup = BeautifulSoup(r.text, 'html.parser')

        for anchor in soup.find_all('a'):
            href = anchor.get('href')
            if href and href.endswith('.pl.zip'):
                zip_file_name = href
                print(f"Found file: {zip_file_name}")
                zip_url = base_url + zip_file_name
                download_zip(zip_url, location)
    elif(status_code == 404):
        print(f"Error 404 Not Found For {base_url}")

'''
These files have been constructed in a LINUX environment. They use an ASCII linefeed, chr(10), to indicate a new record.

For successful use with many programs running in a Windows environment, these files need to be modified to use the 
ASCII carriage return/linefeed sequence, chr(13) + chr(10) as a record terminator.
'''
def convert_line_endings(file_path):
    # Read the file with Linux line endings
    with open(file_path, 'r', newline='\n') as file:
        content = file.read()
    
    # Convert to Windows line endings
    content = content.replace('\n', '\r\n')
    
    # Write back with Windows line endings
    with open(file_path, 'w', newline='\r\n') as file:
        file.write(content)
    print(f"Converted line endings for: {file_path}")

def extract_zip(zip_directory, extract_location):
    # Get a list of all ZIP files in the directory
    zip_files = [os.path.join(zip_directory, f) for f in os.listdir(zip_directory) if f.endswith('.zip')]
    
    for zip_file in zip_files:
        # Extract the state name from the ZIP file name (assuming it's the first part)
        state_name = os.path.basename(zip_file).split('.')[0]
        
        # Create a directory for the state if it doesn't exist
        state_folder = os.path.join(extract_location, state_name)
        os.makedirs(state_folder, exist_ok=True)
        
        # Open the ZIP file and extract its contents into the state folder
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(state_folder)
            print(f"Extracted {zip_file} into {state_folder}")

            for root, dirs, files in os.walk(state_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    convert_line_endings(file_path)

year = '2010'
states = ['Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
          'West Virginia', 'Wisconsin', 'Wyoming']

# Will dump all the data first
for state in states:
    state = state.replace(' ', '_')
    dump_data('https://www2.census.gov/census_' + year + '/redistricting_file--pl_94-171/' + state +'/', 'C:/Users/Admin/Downloads/data_dump/')

# Then it will extract all the folders
extract_zip('C:/Users/Admin/Downloads/data_dump', 'C:/Users/Admin/Downloads/data_dump/extracted_data')
