from scraper import HeaderScraper
import os
import pandas as pd

def tableConcat(headers: dict, geo: pd.DataFrame, tables: list) -> pd.DataFrame:
    geo_headers = headers['geo']['Name'].to_list()
    geo.columns = geo_headers

    merged = tables[0].copy()
    merged.columns = headers['p1']['Name'].to_list()
    merged = merged.loc[:,['STUSAB', 'LOGRECNO', 'P0010001', 'P0010003', 'P0010004', 'P0010005', 'P0010006', 'P0010007', 'P0010008', 'P0010009', 'P0020002', 'P0020003']]
    trunc_geo = geo.loc[:,['LOGRECNO', 'REGION']]
    merged = pd.merge(merged, trunc_geo, how = 'inner', on = ['LOGRECNO'])
    return merged

#Scraping headers
scraper = HeaderScraper()
scraper.scrape()
headers = scraper.getDict()

#Reading part 1 into dataframe
p1_list = []
path = os.path.join(os.getcwd(), 'fl00001.upl')
table_1 = pd.read_csv(path, header = None)

#Reading part 2 into dataframe
#p2_list = []
#path = os.path.join(os.getcwd(), 'fl00002.upl')
#table_2 = pd.read_csv(path, header = None)

#Creating geo dataframe - this one is more complicated because it's a fwf instead of a csv
path = os.path.join(os.getcwd(), 'flgeo.upl')

#Making a list of (start, stop) column specs for the geo fwf, then using pd.read_fwf to read it into a dataframe
column_widths = headers['geo']['Size'].to_list()
colspecs = []
pos = 0
for i in column_widths:
    colspecs.append((pos, pos+int(i)))
    pos += int(i)
geo = pd.read_fwf(path, colspecs=colspecs, header = None)

#Combining headers, geo, and both tables
combined = tableConcat(headers, geo, [table_1])
print(combined.head)