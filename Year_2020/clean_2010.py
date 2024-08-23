import requests
import zipfile
import os
import pandas as pd


states = [
    ["Alabama", "al"],
    ["Alaska", "ak"],
    ["Arizona", "az"],
    ["Arkansas", "ar"],
    ["California", "ca"],
    ["Colorado", "co"],
    ["Connecticut", "ct"],
    ["Delaware", "de"],
    ["Florida", "fl"],
    ["Georgia", "ga"],
    ["Hawaii", "hi"],
    ["Idaho", "id"],
    ["Illinois", "il"],
    ["Indiana", "in"],
    ["Iowa", "ia"],
    ["Kansas", "ks"],
    ["Kentucky", "ky"],
    ["Louisiana", "la"],
    ["Maine", "me"],
    ["Maryland", "md"],
    ["Massachusetts", "ma"],
    ["Michigan", "mi"],
    ["Minnesota", "mn"],
    ["Mississippi", "ms"],
    ["Missouri", "mo"],
    ["Montana", "mt"],
    ["Nebraska", "ne"],
    ["Nevada", "nv"],
    ["New_Hampshire", "nh"],
    ["New_Jersey", "nj"],
    ["New_Mexico", "nm"],
    ["New_York", "ny"],
    ["North_Carolina", "nc"],
    ["North_Dakota", "nd"],
    ["Ohio", "oh"],
    ["Oklahoma", "ok"],
    ["Oregon", "or"],
    ["Pennsylvania", "pa"],
    ["Rhode_Island", "ri"],
    ["South_Carolina", "sc"],
    ["South_Dakota", "sd"],
    ["Tennessee", "tn"],
    ["Texas", "tx"],
    ["Utah", "ut"],
    ["Vermont", "vt"],
    ["Virginia", "va"],
    ["Washington", "wa"],
    ["West_Virginia", "wv"],
    ["Wisconsin", "wi"],
    ["Wyoming", "wy"]
]

# Function to generate the URL for a given state
def generate_url(state_name, state_abbreviation):
    url_template='https://www2.census.gov/census_2010/redistricting_file--pl_94-171/{state_name}/{state_abbreviation}2010.pl.zip'
    return url_template.format(state_name=state_name, state_abbreviation=state_abbreviation)



for state_name, state_abbreviation in states:
    url = generate_url(state_name, state_abbreviation)
    print(f"Downloading data for {state_name} from: {url}")
    # Directory where you want to save the ZIP file and its contents
    save_dir = 'census_data'
    zip_file_path = os.path.join(save_dir, f'{state_abbreviation}2010.pl.zip')

    # Create the directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)

    # Download the ZIP file
    response = requests.get(url, stream=True)
    with open(zip_file_path, 'wb') as zip_file:
        for chunk in response.iter_content(chunk_size=128):
            zip_file.write(chunk)

    # Unzip the contents
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(save_dir)

    # Optionally, delete the ZIP file after extraction
    os.remove(zip_file_path)

    print(f"Downloaded and extracted files to: {save_dir}")

    # Read the pipe-delimited file into a DataFrame
    data_file_path = os.path.join(save_dir, f'{state_abbreviation}geo2010.pl')
    df = pd.read_csv(data_file_path, delimiter='|', header=None, low_memory=False, encoding='ISO-8859-1')

    df.columns = range(df.shape[1])
    df = df.head(1)

    # The string you provided
    string_data = df.iloc[0, 0]  # This will fetch 'uPL   AL04000000  0000001366301'

    # Split the string by spaces
    parts = string_data.split()

    # Now, break down the parts further if needed
    file_id = parts[0]  # 'uPL'
    stusab = parts[1][:2]  # 'AL'
    sumlev = parts[1][2:5]  # '040'
    geocomp = parts[1][5:7]  # '00'
    chariter = parts[1][7:10]  # '000'
    cifsn = parts[2][:2]  # '00'
    logrecno = parts[2][2:9]  # '0000136'
    region = parts[2][7]  # '6'
    division = parts[2][8]  # '3'
    statece = parts[2][9:11]  # '01'

    # Create a new DataFrame with these values
    df_split = pd.DataFrame({
        'State Abv': [stusab],
        'Summary Code': [sumlev],
        'Region': [region],
    })

    df = df_split

    data_file_path = os.path.join(save_dir, f'{state_abbreviation}000012010.pl')
    df_1 = pd.read_csv(data_file_path, delimiter=',', header=None, low_memory=False)
    df_1 = df_1.head(1)

    # Step 2: Rename the columns to numbers
    df_1.columns = range(df_1.shape[1])

    # Define the list of columns to keep

    columns_to_keep =[5, 7, 8, 9, 10, 11, 12, 13, 80, 81]

    # Create a new DataFrame with only the selected columns
    df_filtered = df_1[columns_to_keep]

    # Create Column Names
    df_filtered.columns = [
        "Total Population",
        "White Alone",
        "African-American Alone",
        "American Indian and Alaska Native Alone",
        "Asian Alone",
        "Native Hawaiian/Pacific Islander Alone",
        "Other Alone",
        "Two or More Races",
        "Hispanic or Latino",
        "Not Hispanic or Latino",
    ]

    df_1 = df_filtered

    
    # Combine the DataFrames along the columns
    combined_df = pd.concat([df, df_1], axis=1)

    
    df = combined_df
    # Add Year Column
    df["year"] = 2010
    
    # Assuming there's a specific CSV file you're working with
    csv_file_path = 'year_2020/clean/2010_cleaned_data.csv'

    # Check if the CSV exists and append the DataFrame
    if os.path.exists(csv_file_path):
        df.to_csv(csv_file_path, mode='a', index=False, header=False)
    else:
        df.to_csv(csv_file_path, index=False)

    # Remove the downloaded and extracted files
    for root, dirs, files in os.walk(save_dir):
        for file in files:
            os.remove(os.path.join(root, file))

    # Optionally, remove the directory if empty
    os.rmdir(save_dir)

    print(f"Data appended to {csv_file_path} and files removed.")
