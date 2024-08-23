import os
import pandas as pd
from tqdm import tqdm
#Please make sure pandas and tqdm are installed in your enviroment.
year = "2010"
state_and_abrv = {"Alabama": "al", "Alaska":"ak", "Arizona":"az", "Arkansas":"ar", "California":"ca", \
                  "Colorado":"co", "Connecticut":"ct", "District_of_Columbia":"dc", "Delaware":"de", \
                  "Florida":"fl", "Georgia":"ga", "Hawaii":"hi", "Idaho":"id", "Illinois":"il", \
                  "Indiana":"in", "Iowa":"ia", "Kansas":"ks", "Kentucky":"ky", "Louisiana":"la", \
                  "Maine":"me", "Maryland":"md", "Massachusetts":"ma", "Michigan":"mi", "Minnesota":"mn", \
                  "Mississippi":"ms", "Missouri":"mo", "Montana":"mt", "Nebraska":"ne", "Nevada":"nv", \
                  "New_Hampshire":"nh", "New_Jersey":"nj", "New_Mexico":"nm", "New_York":"ny", \
                  "North_Carolina":"nc", "North_Dakota":"nd", "Ohio":"oh", "Oklahoma":"ok", \
                  "Oregon":"or", "Pennsylvania":"pa", "Puerto_Rico":"pr", "Rhode_Island":"ri", \
                  "South_Carolina":"sc", "South_Dakota":"sd", "Tennessee":"tn", "Texas":"tx", \
                  "Utah":"ut", "Vermont":"vt", "Virginia":"va", "Washington":"wa", "West_Virginia":"wv", \
                  "Wisconsin":"wi", "Wyoming":"wy"}

working_dir =os.path.dirname(os.path.realpath(__file__))
extracted_path =  working_dir + "/extracted_data"
clean_path = working_dir + "/clean"

if not os.path.exists(clean_path):
    os.makedirs(clean_path)

def get_df(state_abbreviation, file_name):
    file_path = f"{state_abbreviation}{year}/{state_abbreviation}{file_name}{year}.pl"
    data_file_path = os.path.join(extracted_path, file_path)
    df = pd.read_csv(data_file_path, delimiter=',', header=None, low_memory=False, encoding='ISO-8859-1', on_bad_lines='skip')
    return df

def get_geo_data(state_abbreviation):
    df = get_df(state_abbreviation, 'geo')
    df.columns = range(df.shape[1])
    string_data = df.iloc[0, 0]  
    parts = string_data.split()
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

    df_split = pd.DataFrame({
        'State Abv': [stusab],
        'Summary Code': [sumlev],
        'Region': [region],
        'Division': [division],
    })
    #Note it does not join on LogRecNum, might cause issues. sumlev is 40 by default here.
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
    for state, abrv in tqdm(state_and_abrv.items()):
        #NOTE: These are pandas dataframes not spark dataframes!
        geo_df = get_geo_data(abrv)
        pop_df = get_population_data(abrv)
        combined_df = pd.concat([geo_df, pop_df], axis=1)
        df = combined_df
        df['year']=year
        csv_file_path = os.path.join(clean_path, f'{year}_cleaned_data.csv')
        print(f"Writing to {csv_file_path} cleaned data for {state}...")
        if os.path.exists(csv_file_path):
            df.to_csv(csv_file_path, mode='a', index=False, header=False)
        else:
            df.to_csv(csv_file_path, index=False)

    print("end")

if __name__ == "__main__":
    #Intended to run only once in a linux environment.
    main()
