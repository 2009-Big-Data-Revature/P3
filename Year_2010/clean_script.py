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

year = '2010'

# Function to generate the URL for a given state
def generate_url(state_name, state_abbreviation):
    url_template = 'https://www2.census.gov/programs-surveys/decennial/{year}/data/01-Redistricting_File--PL_94-171/{state_name}/{state_abbreviation}{year}.pl.zip'
    return url_template.format(state_name=state_name, state_abbreviation=state_abbreviation, year=year)



for state_name, state_abbreviation in states:
    url = generate_url(state_name, state_abbreviation)
    print(f"Downloading data for {state_name} from: {url}")
    # Directory where you want to save the ZIP file and its contents
    save_dir = 'census_data'
    zip_file_path = os.path.join(save_dir, f'{state_abbreviation}{year}.pl.zip')

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

    # Step 1: Read the pipe-delimited file into a DataFrame
    data_file_path = os.path.join(save_dir, f'{state_abbreviation}geo{year}.pl')
    df = pd.read_csv(data_file_path, delimiter='|', header=None, low_memory=False, encoding='ISO-8859-1')

    # Step 2: Rename the columns to numbers
    df.columns = range(df.shape[1])


    # In[118]:


    # Step 1: Select the columns you want to keep
    columns_to_keep = [1, 2, 10, 11, 84, 85, 86, 87, 91, 92, 93]

    # Step 2: Create a new DataFrame with only the selected columns
    df_filtered = df[columns_to_keep]

    # Step 3: Rename the columns
    df_filtered.columns = [
        "State Abv",
        "Summary Code",
        "Region",
        "Division",
        "Area (Land)",
        "Area (Water)",
        "Area Base Name",
        "Area Legal-Name",
        "Housing Unit Count",
        "Latitude",
        "Longitude"
    ]

    df = df_filtered


    # In[119]:


    # Step 1: Define the values you want to keep
    valid_codes = [40, 50, 60]

    # Step 2: Filter the DataFrame to keep only the rows where "Summary Code" is in the list
    df_filtered = df[df["Summary Code"].isin(valid_codes)]

    # Step 3: Display the filtered DataFrame
    df = df_filtered


    num_rows_to_keep = df.shape[0]

    data_file_path = os.path.join(save_dir, f'{state_abbreviation}00001{year}.pl')
    df_1 = pd.read_csv(data_file_path, delimiter=',', header=None, low_memory=False)


    # Step 2: Rename the columns to numbers
    df_1.columns = range(df_1.shape[1])

    # Define the list of columns to keep
    columns_to_keep = [76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 103, 124, 140, 147]

    # Create a new DataFrame with only the selected columns
    df_filtered = df_1[columns_to_keep]

    # Create Column Names
    df_filtered.columns = [
        "Total Population",
        "Hispanic or Latino",
        "Not Hispanic or Latino",
        "Population of One Race",
        "White",
        "African-American",
        "American Indian",
        "Asian",
        "Pacific Islander",
        "Other",
        "Population of Two Races",
        "Population of Three Races",
        "Population of Four Races",
        "Population of Five Races",
        "Population of Six Races"
    ]


    df_1 = df_filtered

    df_1 = df_1.head(num_rows_to_keep)

    # Step 1: Read the pipe-delimited file into a DataFrame
    data_file_path = os.path.join(save_dir, f'{state_abbreviation}00002{year}.pl')
    df_2 = pd.read_csv(data_file_path, delimiter=',', header=None, low_memory=False)
    
    
    df_2 = df_2.head(num_rows_to_keep)
    # Step 2: Rename the columns to numbers
    df_2.columns = range(df_2.shape[1])

    # Define the list of columns to keep
    columns_to_keep = [76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 103, 124, 140, 147, 149, 150, 151]

    # Create a new DataFrame with only the selected columns
    df_filtered = df_2[columns_to_keep]

    # Create Column Names
    df_filtered.columns = [
        "18 and Over: Total Population",
        "18 and Over: Hispanic or Latino",
        "18 and Over: Not Hispanic or Latino",
        "18 and Over: Population of One Race",
        "18 and Over: White",
        "18 and Over: African-American",
        "18 and Over: American Indian",
        "18 and Over: Asian",
        "18 and Over: Pacific Islander",
        "18 and Over: Other",
        "18 and Over: Population of Two Races",
        "18 and Over: Population of Three Races",
        "18 and Over: Population of Four Races",
        "18 and Over: Population of Five Races",
        "18 and Over: Population of Six Races",
        "Total Housing Units",
        "Occupied Housing Units",
        "Vacant Housing Units"
    ]

    df_2 = df_filtered


    # Step 1: Read the pipe-delimited file into a DataFrame
    data_file_path = os.path.join(save_dir, f'{state_abbreviation}00003{year}.pl')
    df_3 = pd.read_csv(data_file_path, delimiter='|', header=None, low_memory=False)
    
    df_3 = df_3.head(num_rows_to_keep)
    # Step 2: Rename the columns to numbers
    df_3.columns = range(df_3.shape[1])

    # Step 1: Define the list of columns to keep
    columns_to_keep = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

    # Step 2: Create a new DataFrame with only the selected columns
    df_filtered = df_3[columns_to_keep]

    # Step 3: Define the list of new column names
    new_column_names = [
        "Total Group Population",
        "Institutionalized",
        "Adult-Correctional",
        "Juvenile-Correctional",
        "Nursing",
        "Other",
        "Non-Institutionalized",
        "Student Housing",
        "Military Quarters",
        "Other Non-Institutional Facilities"
    ]

    # Step 4: Assign the new column names to the DataFrame
    df_filtered.columns = new_column_names


    df_3 = df_filtered

    # Step 1: Combine the DataFrames along the columns
    combined_df = pd.concat([df, df_1,  df_2, df_3], axis=1)

    # Step 2: Display the resulting DataFrame (optional)
    df = combined_df

    # Assuming there's a specific CSV file you're working with
    csv_file_path = 'clean/{year}_cleaned_data.csv'  # Corrected this line

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
