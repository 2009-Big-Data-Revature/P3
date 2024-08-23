#!/usr/bin/env python
# coding: utf-8
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
    url_template = 'https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/{state_name}/{state_abbreviation}2020.pl.zip'
    return url_template.format(state_name=state_name, state_abbreviation=state_abbreviation)



for state_name, state_abbreviation in states:
    url = generate_url(state_name, state_abbreviation)
    print(f"Downloading data for {state_name} from: {url}")
    # Directory where you want to save the ZIP file and its contents
    save_dir = 'census_data'
    zip_file_path = os.path.join(save_dir, f'{state_abbreviation}2020.pl.zip')

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
    data_file_path = os.path.join(save_dir, f'{state_abbreviation}geo2020.pl')
    df = pd.read_csv(data_file_path, delimiter='|', header=None, low_memory=False, encoding='ISO-8859-1')

    # Rename the columns to numbers
    df.columns = range(df.shape[1])

    # Select the columns you want to keep
    columns_to_keep = [1, 2, 10]

    # Create a new DataFrame with only the selected columns
    df_filtered = df[columns_to_keep]

    # Rename the columns
    df_filtered.columns = [
        "State Abv",
        "Summary Code",
        "Region"
    ]

    df = df_filtered


    # Define the Summary Codes to keep
    valid_codes = [40]

    # Filter the DataFrame to keep only the rows where "Summary Code" is in the list
    df_filtered = df[df["Summary Code"].isin(valid_codes)]

    # Display the filtered DataFrame
    df = df_filtered


    num_rows_to_keep = df.shape[0]

    data_file_path = os.path.join(save_dir, f'{state_abbreviation}000012020.pl')
    df_1 = pd.read_csv(data_file_path, delimiter='|', header=None, low_memory=False)


    # Rename the columns to numbers
    df_1.columns = range(df_1.shape[1])

    # Define the list of columns to keep
    columns_to_keep = [5, 7, 8, 9, 10, 11, 12, 13, 77, 78]

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

    df_1 = df_1.head(num_rows_to_keep)

    # Combine the DataFrames along the columns
    combined_df = pd.concat([df, df_1], axis=1)

    
    df = combined_df
    # Add Year Column
    df["year"] = 2020
    
    # Assuming there's a specific CSV file you're working with
    csv_file_path = 'clean/2020_cleaned_data.csv'

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
