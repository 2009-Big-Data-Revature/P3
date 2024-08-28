from scraper import HeaderScraper
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import tabula
import time

DUMP_PATHS = ['C:/Users/Hunter/Desktop/Revature/P3/dump/extracted_data', 'C:/Users/Hunter/Desktop/Revature/P3/git2/dump/extracted_data']
OUT_PATHS = ['C:/Users/Hunter/Desktop/Revature/P3/git2/Year_2000/clean/2000_cleaned_data.csv', 'C:/Users/Hunter/Desktop/Revature/P3/git2/Year_2000/clean/2010_cleaned_data.csv']
SUM_LEVEL = '040'
COLUMNS = ['STUSAB','P0010001','P0010003','P0010004','P0010005','P0010006','P0010007','P0010008','P0010009','P0020002','P0020003','SUMLEV','REGION']

#gets the indices and names for the columns specified in COLUMNS
def get_indices(headers, key):
    return [(i, name) for i, name in enumerate(headers) if name in ['SUMLEV', 'LOGRECNO'] + [col for col in COLUMNS if col not in ['SUMLEV', 'LOGRECNO']]]

#Gets list of column names
def get_columns(headers):
    column_list = [column for column in headers['geo']['Name'] if column in COLUMNS] 
    column_list += [column for column in headers['p1']['Name'] if column in COLUMNS and column not in column_list] 
    column_list += [column for column in headers['p2']['Name'] if column in COLUMNS and column not in column_list]
    return column_list

#reads the geo file
def read_geo(geo_file, indices, colspecs):
    lines = geo_file.readlines()
    sumlev_index = next(i for i, name in indices if name == 'SUMLEV')
    sumlev_slice = colspecs[sumlev_index]

    geo_list = [{name: line[colspecs[i][0]:colspecs[i][1]].strip() for i, name in indices} for line in lines if line[sumlev_slice[0]:sumlev_slice[1]].strip() == SUM_LEVEL]

    return geo_list

#reads the table file
def read_table(table_file, indices, log_nos):
    table_list = []
    lines = table_file.readlines()
    for line in lines:
        fields = line.split(',')
        if fields[4] in log_nos:
            row = {name: fields[i] for i, name in indices}
            table_list.append(row)
            log_nos.remove(fields[4])
        if not log_nos:
            break
    return table_list

#calls read_geo and read_table, manipulates the columns, and merges them
def process_directory(dir, colspecs: list, geo_indices: list, table_indices: list, year: str) -> pd.DataFrame:
    files = [os.fsdecode(file) for file in os.listdir(dir)]
    paths = [os.path.join(dir, path) for path in files]
    paths.sort()
    i = (year == '2010')

    with open(paths[2 + i]) as geo_file:
        geo_list = read_geo(geo_file, geo_indices, colspecs)
    geo = pd.DataFrame(geo_list)
    

    with open(paths[0]) as table_1_file: 
        table_1_list= read_table(table_1_file, table_indices[0], set(geo['LOGRECNO'].to_list()))
    table_1 = pd.DataFrame(table_1_list)


    cols_to_use = table_1.columns.difference(geo.columns).to_list()
    cols_to_use.append('LOGRECNO')
    merge = pd.merge(geo, table_1[cols_to_use], how = 'inner', on = ['LOGRECNO'])

    if ('LOGRECNO' not in COLUMNS): merge = merge.drop(columns = ['LOGRECNO'])

    return merge

#Gets headers from technical docs pdfs
def get_headers(year):
    if (year == '2000'):
        #Scraping headers
        scraper = HeaderScraper()
        scraper.scrape()
        headers = scraper.getDict()

        #We have to read the geo metadata from the technical docs pdf since the one in scraper is broken (incorrect data on website)
        df = tabula.read_pdf("https://www2.census.gov/programs-surveys/decennial/2000/technical-documentation/complete-tech-docs/summary-files/public-law-summary-files/pl-00-1.pdf",pages="15-19") #address of pdf file
        geo_columns = [x for entries in df[:2] for x in entries.iloc[:, 1].to_list() if x == x and x != 'dictionary' and x != 'reference' and x != 'name']
        column_widths = [int(x) for entries in df[:2] for x in entries.iloc[:, 2].to_list() if x == x and x.isdigit()]
        return ([headers['p1']['Name'].to_list(), headers['p2']['Name'].to_list()], geo_columns, column_widths)
    else:
        #We have to read the geo metadata from the technical docs pdf since the one in scraper is broken (incorrect data on website)
        df = tabula.read_pdf("https://www2.census.gov/programs-surveys/decennial/2010/technical-documentation/complete-tech-docs/summary-file/nsfrd.pdf",pages="15") #address of pdf file
        geo_columns = [x for x in df[0]['Unnamed: 0'].to_list() if x == x]
        column_widths = [int(x) for x in df[0]['Unnamed: 1'].to_list() if x == x]


        df = tabula.read_pdf("https://www2.census.gov/programs-surveys/decennial/2010/technical-documentation/complete-tech-docs/summary-file/nsfrd.pdf",pages="16") #address of pdf file
        all_columns = df[0]['Data\rdictionary\rreference'].to_list()[1].split('\r')
        geo_columns_2 = [x for x in all_columns if x == x and x != '']
        all_widths = df[0]['Field\rsize'].to_list()[1].split('\r')
        column_widths_2 = [int(x) for x in all_widths if x == x and x != '']

        geo_columns += geo_columns_2
        column_widths += column_widths_2

        df = tabula.read_pdf("https://www2.census.gov/census_2010/redistricting_file--pl_94-171/0FILE_STRUCTURE.pdf",pages="5-8", pandas_options={'header': None}) #address of pdf file
        concat_list = [d.iloc[:, 0] for d in df[1:5]]
        p1_headers = [x for x in pd.concat(concat_list).to_list() if x != 'Name']

        df = tabula.read_pdf("https://www2.census.gov/census_2010/redistricting_file--pl_94-171/0FILE_STRUCTURE.pdf",pages="8-12") #address of pdf file
        concat_list = [d.iloc[:, 0] for d in df[1:]]
        p2_headers = pd.concat(concat_list).to_list()
        return ([p1_headers, p2_headers], geo_columns, column_widths)


for i, year in enumerate(['2000', '2010']):
    print(f'Processing year {year}')
    print(COLUMNS)
    headers, geo_columns, column_widths = get_headers(year)

    #column widths for the fixed-width-file
    colspecs = []
    pos = 0
    for j in column_widths:
        colspecs.append((pos, pos+int(j)))
        pos += int(j)

    #Retrieving indices of the columns we want
    geo_indices = get_indices(geo_columns, 'geo')
    table_1_indices = get_indices(headers[0], 'p1')
    table_2_indices = get_indices(headers[1], 'p2')

    table_indices = [table_1_indices]
    if table_2_indices: table_indices += table_2_indices

    with open(OUT_PATHS[i], 'w+', newline = '') as out_file:
        pass

    start = time.time()
    header = 1
    write_lock = Lock()
    full = pd.DataFrame()
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_directory, os.path.join(DUMP_PATHS[i], dir), colspecs, geo_indices, table_indices, year): dir for dir in os.listdir(DUMP_PATHS[i])}
        for future in as_completed(futures):
            dir = futures[future]
            try:
                combined = future.result()
                with write_lock:
                    full = pd.concat([full, combined])
                    #combined.to_csv(OUT_PATH[i], mode = 'a', index = False, header = header) #Appends to out.csv
                    if header: 
                        full.columns = combined.columns
                        header = 0
                print(f"{dir} finished: {combined.shape[0]} rows appended.")
            except Exception as e:
                print(f"Error processing directory {dir}: {e}")
    end = time.time()
    print(end-start)

    column_dict = {
        'P0010001' : 'Total Population',
        'P0010003' : 'White Alone',
        'P0010004' : 'African-American Alone',
        'P0010005' : 'American Indian and Alaska Native Alone',
        'P0010006' : 'Asian Alone',
        'P0010007' : 'Native Hawaiian/Pacific Islander Alone',
        'P0010008' : 'Other Alone',
        'P0010009' : 'Two or More Races',
        'P0020002' : 'Hispanic or Latino',
        'P0020003' : 'Not Hispanic or Latino',
        'SUMLEV' : 'Summary Code',
        'REGION' : 'Region',
        'STUSAB' : 'State Abv'
    }


    full = full.rename(column_dict, axis = 1)
    full.sort_values('State Abv', inplace=True)
    full['year'] = year
    print(full)
    full.to_csv(OUT_PATHS[i], index = False)
    print(f'{year}_cleaned_data.csv written to {OUT_PATHS[i]}')
