import os
import pandas as pd
import requests
import zipfile


states = [
    # ["Alabama", "al"],
    # ["Alaska", "ak"],
    # ["Arizona", "az"],
    # ["Arkansas", "ar"],
    # ["California", "ca"],
    # ["Colorado", "co"],
    # ["Connecticut", "ct"],
    # ["District_of_Columbia", "dc"],
    # ["Delaware", "de"],
    # ["Florida", "fl"],
    # ["Georgia", "ga"],
    # ["Hawaii", "hi"],
    # ["Idaho", "id"],
    # ["Illinois", "il"],
    # ["Indiana", "in"],
    # ["Iowa", "ia"],
    # ["Kansas", "ks"],
    # ["Kentucky", "ky"],
    # ["Louisiana", "la"],
    # ["Maine", "me"],
    # ["Maryland", "md"],
    # ["Massachusetts", "ma"],
    # ["Michigan", "mi"],
    # ["Minnesota", "mn"],
    # ["Mississippi", "ms"],
    # ["Missouri", "mo"],
    # ["Montana", "mt"],
    # ["Nebraska", "ne"],
    # ["Nevada", "nv"],
    # ["New_Hampshire", "nh"],
    # ["New_Jersey", "nj"],
    # ["New_Mexico", "nm"],
    # ["New_York", "ny"],
    # ["North_Carolina", "nc"],
    # ["North_Dakota", "nd"],
    # ["Ohio", "oh"],
    # ["Oklahoma", "ok"],
    # ["Oregon", "or"],
    # ["Pennsylvania", "pa"],
    # ["Puerto_Rico", "pr"],
    # ["Rhode_Island", "ri"],
    # ["South_Carolina", "sc"],
    # ["South_Dakota", "sd"],
    # ["Tennessee", "tn"],
    # ["Texas", "tx"],
    # ["Utah", "ut"],
    # ["Vermont", "vt"],
    # ["Virginia", "va"],
    # ["Washington", "wa"],
    # ["West_Virginia", "wv"],
    # ["Wisconsin", "wi"],
    # ["Wyoming", "wy"],
    ["District_of_Columbia", "dc"],
    ["Puerto_Rico", "pr"],
]

def generate_url(state_name, state_abbreviation, file_type):
    url_template="https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/{state_name}/{state_abbreviation}{file_type}.upl.zip"
    return url_template.format(state_name=state_name, state_abbreviation=state_abbreviation, file_type=file_type)

def get_df(state, state_abbreviation, file_type):
    url=generate_url(state, state_abbreviation, file_type)
    print(f"Downloading and extracting data from: {url}")
    save_dir = 'census_data'
    zip_file_path = os.path.join(save_dir, f'{state_abbreviation}{file_type}.upl.zip')

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

    data_file_path = os.path.join(save_dir, f'{state_abbreviation}{file_type}.upl')
    df = pd.read_csv(data_file_path, delimiter=',', header=None, low_memory=False, encoding='ISO-8859-1', on_bad_lines='skip')
    return df


def get_geo_data(state, state_abbreviation):
    df = get_df(state, state_abbreviation, 'geo')

    df.columns = range(df.shape[1])

    # The string you provided
    # This will fetch 'uPL   AL04000000  0000001366301'
    string_data = df.iloc[0, 0]  

    # Split the string by spaces
    parts = string_data.split()

    # Now, break down the parts
    file_id = parts[0]  # 'uPL'
    stusab = parts[1][:2]  # 'AL'
    sumlev = parts[1][2:5]  # '040'
    geocomp = parts[1][5:7]  # '00'
    chariter = parts[1][7:10]  # '000'
    cifsn = parts[2][:2]  # '00'
    logrecno = parts[2][2:9]  # '0000136'
    region = parts[2][9]  # '6'
    division = parts[2][10]  # '3'
    statece = parts[2][11:13]  # '01'

    # Create a new DataFrame with these values
    df_split = pd.DataFrame({
        'State Abv': [stusab],
        'Summary Code': [sumlev],
        'Region': [region],
    })

    return df_split


def get_population_data(state, state_abbreviation):
    df = get_df(state, state_abbreviation, '00001')
    df = df.head(1)

    df.columns = range(df.shape[1])

    cols_to_keep = [5, 7, 8, 9, 10, 11, 12, 13, 78, 79 ]

    df_filtered = df[cols_to_keep]

    col_names = [
        "Total Population",
        "White Alone",
        "African-American Alone",
        "American Indian and Alaska Native Alone",
        "Asian Alone",
        "Native Hawaiian/Pacific Islander Alone",
        "Other Alone",
        "Two or More Races",
        "Hispanic or Latino",
        "Not Hispanic or Latino"
    ]

    df_filtered.columns = col_names

    return df_filtered

for state_name, state_abbreviation in states:
    geo_df = get_geo_data(state_name, state_abbreviation)
    pop_df = get_population_data(state_name, state_abbreviation)

    # Step 1: Combine the DataFrames along the columns
    combined_df = pd.concat([geo_df, pop_df], axis=1)

    # Step 2: Display the resulting DataFrame (optional)
    df = combined_df
    df['year']=2000

    # Assuming there's a specific CSV file you're working with
    os.makedirs("clean", exist_ok=True)
    csv_file_path = 'clean/2000_cleaned_data.csv'  # Corrected this line
    save_dir = 'census_data'

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
