import os
import pandas as pd
# from pandas.api.types import Literal
import threading
import csv

year = "2010"
# file_names = ["00001", "00002", "geo"]
state_and_abrv = {"Alaska":"ak","Arizona":"az","Arkansas":"ar","District_of_Columbia":"dc", \
                  "Florida":"fl","Georgia":"ga","Hawaii":"hi","Idaho":"id","Indiana":"in", \
                  "Kansas":"ks","Louisiana":"la"}

working_dir =os.path.dirname(os.path.realpath(__file__))
extracted_path =  working_dir + "/extracted_data"
clean_path = working_dir + "/clean"

if not os.path.exists(clean_path):
    os.makedirs(clean_path)

# geo_header_path = working_dir + "/geoheader.txt"
# with open(geo_header_path, "r") as file:
#     lis = file.read()
# lis = lis.split('\n')
# lis = list(map(lambda line: line.split('\t'), lis))
# geo_headers = [ line[0] for line in lis]
# geo_sizes = [int(line[-1]) for line in lis]

# header_00001_path = working_dir + "/00001header.txt"
# with open(header_00001_path, "r") as file:
#     lis = file.read()
# lis = lis.split('\n')
# lis = list(map(lambda line: line.split('\t'), lis))
# headers_00001 = [ line[0] for line in lis]

# header_00002_path = working_dir + "/00002header.txt"
# with open(header_00002_path, "r") as file:
#     lis = file.read()
# lis = lis.split('\n')
# lis = list(map(lambda line: line.split('\t'), lis))
# headers_00002 = [ line[0] for line in lis]

# def getRows(lines, rows):
#      for index, line in enumerate(lines):
#         if index % 2 == 0:
#             rows.append(line.split(","))

# def open_Folder(file_path):
#     with open(file_path, "r", encoding="utf-8", errors="replace") as file:
#         lis = file.read()
#     return lis

# def getGeoRows(lines, rows):
#     for index, line in enumerate(lines):
#         if index % 2 == 0:
#             number = 0
#             row = list()
#             for num in geo_sizes:
#                 row.append(line[number:number+num])
#                 number += num
#             rows.append(row)

# def writeCSV(new_file_path, header, rows):
#     with open(new_file_path, 'w') as csvfile:
#         csvwriter = csv.writer(csvfile)
#         csvwriter.writerow(header)
#         csvwriter.writerows(rows)

def get_df(state_abbreviation, file_name):
    file_path = f"{state_abbreviation}{year}/{state_abbreviation}{file_name}{year}.pl"
    data_file_path = os.path.join(extracted_path, file_path)
    df = pd.read_csv(data_file_path, delimiter=',', header=None, low_memory=False, encoding='ISO-8859-1', on_bad_lines='skip')
    return df

def get_geo_data(state_abbreviation):
    df = get_df(state_abbreviation, 'geo')

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
        'Division': [division],
    })
    #Note it does not join on LogRecNum, might cause issues.
    return df_split


def get_population_data(state_abbreviation):
    
    df = get_df(state_abbreviation, '00001')
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

def main():
    print("start")
    for state, abrv in state_and_abrv.items():
        #NOTE: These are pandas dataframes not spark dataframes!
        geo_df = get_geo_data(abrv)
        pop_df = get_population_data(abrv)
        combined_df = pd.concat([geo_df, pop_df], axis=1)
        df = combined_df
        df['year']={year}
        csv_file_path = f'clean/{year}_cleaned_data.csv'
        # stat_folder =  working_dir + "/clean/" + state
        # if not os.path.exists(stat_folder):
        #     os.makedirs(stat_folder)
        # for file_name in file_names:
        
            # new_file_path = stat_folder + "/" + abrv + file_name+".csv"
            # print(file_path)
            # lis = open_Folder(file_path).split('\n')
            # rows = list()
            # if not os.path.exists(new_file_path):
            #     if file_name == 'geo2010':
            #         getGeoRows(lis, rows)
            #     else:
            #         getRows(lis, rows)

            #     print(new_file_path)
            #     match (file_name):
            #         case 'geo2010':
            #             writeCSV(new_file_path, geo_headers, rows)
            #         case '000012010':
            #             writeCSV(new_file_path, headers_00001, rows)
            #         case '000022010':
            #             writeCSV(new_file_path, headers_00002, rows)

        #     geo_df = get_geo_data(state_name, state_abbreviation)
    #     pop_df = get_population_data(state_name, state_abbreviation)

    #     # Step 1: Combine the DataFrames along the columns
    #     combined_df = pd.concat([geo_df, pop_df], axis=1)

    #     # Step 2: Display the resulting DataFrame (optional)
    #     df = combined_df
    #     df['year']=2000

    #     # Assuming there's a specific CSV file you're working with
    #     csv_file_path = 'clean/2000_cleaned_data.csv'  # Corrected this line
    #     save_dir = 'census_data'

    #     # Check if the CSV exists and append the DataFrame
    #     if os.path.exists(csv_file_path):
    #         df.to_csv(csv_file_path, mode='a', index=False, header=False)
    #     else:
    #         df.to_csv(csv_file_path, index=False)

    #     # Remove the downloaded and extracted files
    #     for root, dirs, files in os.walk(save_dir):
    #         for file in files:
    #             os.remove(os.path.join(root, file))

    #     # Optionally, remove the directory if empty
    #     os.rmdir(save_dir)

    #     print(f"Data appended to {csv_file_path} and files removed.")
    print("end")


if __name__ == "__main__":
    main()







