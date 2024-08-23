from scraper import HeaderScraper
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import csv

DUMP_PATH = 'C:/Users/Hunter/Desktop/Revature/P3/dump/extracted_data'
OUT_PATH = 'C:/Users/Hunter/Desktop/Revature/P3/out.csv'
SUM_LEVEL = 40

#Joins geo and other table
def tableConcat(headers: dict, geo: pd.DataFrame, table: pd.DataFrame) -> pd.DataFrame:
    #geo_headers = headers['geo']['Name'].to_list()
    geo.columns = ['SUMLEVEL', 'LOGRECNO', 'REGION']
    trunc_geo = geo.loc[geo['SUMLEVEL'] == SUM_LEVEL]

    return pd.merge(table, trunc_geo, how = 'inner', on = ['LOGRECNO'])


def process_directory(dir, headers, colspecs):
    base = os.path.join(DUMP_PATH, dir)
    files = [os.fsdecode(file) for file in os.listdir(base)]

    #Reading part 1 into dataframe
    path = os.path.join(base, files[0])
    table_1 = pd.read_csv(path, names = ['STUSAB', 'LOGRECNO', 'P0010001', 
                                          'P0010003', 'P0010004', 'P0010005', 
                                          'P0010006', 'P0010007', 'P0010008', 
                                          'P0010009', 'P0020002', 'P0020003'], 
                          usecols = [1, 4, 6, 7, 8, 9, 10, 11, 12, 13, 77, 78]) #Double check these column numbers
    
    #Creating geo dataframe - this one is more complicated because it's a fwf instead of a csv
    path = os.path.join(base, files[2])

    #Reading from fwf GEO file
    if(files[0] == 'pr00001.upl'):
        geo = pd.read_fwf(path, colspecs=colspecs, header = None, encoding = 'latin1', memory_map = True, usecols = [2, 6, 7])
    else:
        geo = pd.read_fwf(path, colspecs=colspecs, header = None, memory_map = True, usecols = [2, 6, 7])

    return tableConcat(headers, geo, table_1)

#Scraping headers
scraper = HeaderScraper()
scraper.scrape()
headers = scraper.getDict()

#Constructing colspecs for fwf file later
column_widths = headers['geo']['Size'].to_list()
colspecs = []
pos = 0
for i in column_widths:
    colspecs.append((pos, pos+int(i)))
    pos += int(i)

full = pd.DataFrame()

f = open(OUT_PATH, 'w+')
writer = csv.writer(f)
writer.writerow(['STUSAB,P0010001,P0010003,P0010004,P0010005,P0010006,P0010007,P0010008,P0010009,P0020002,P0020003,SUMLEVEL,REGION'])
with ThreadPoolExecutor() as executor:
    futures = {executor.submit(process_directory, dir, headers, colspecs): dir for dir in os.listdir(DUMP_PATH)}
    for future in as_completed(futures):
        dir = futures[future]
        try:
            combined = future.result()
            combined.drop(columns = ['LOGRECNO']).to_csv(OUT_PATH, mode = 'a', index = False, header = False) #Appends to out.csv
            print(f"{dir} finished: {combined.shape[0]} rows appended.")
        except Exception as e:
            print(f"Error processing directory {dir}: {e}")