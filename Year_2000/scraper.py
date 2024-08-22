import requests
from bs4 import BeautifulSoup
import pandas as pd

class HeaderScraper:
    def __init__(self):
        self.tables = {}
        pass

    def scrape(self):
        URL = r"https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/0File_Structure/File_Structure_README.htm"
        r = requests.get(URL)

        soup = BeautifulSoup(r.content, 'html.parser')

        tables = soup.find_all('table')

        #Scrapes table PL_GEOHD (Geo Headers)
        geo_list = []
        for i, row in enumerate(tables[0].find_all('tr')):
            geo_list.append([el.text.strip() for el in row.find_all('td')])
        self.tables['geo'] = pd.DataFrame(geo_list[1:], columns = geo_list[0])

        #Scrapes table PL_PART1
        p1_list = []
        for i, row in enumerate(tables[1].find_all('tr')):
            p1_list.append([el.text.strip() for el in row.find_all('td')])
        self.tables['p1'] = pd.DataFrame(p1_list[1:], columns = p1_list[0])

        #Scrapes table PL_PART2
        p2_list = []
        for i, row in enumerate(tables[2].find_all('tr')):
            p2_list.append([el.text.strip().replace('\r', '').replace('\n', '') for el in row.find_all('td')])
        self.tables['p2'] = pd.DataFrame(p2_list[1:], columns = p2_list[0])

        #Scrapes table HEADERS
        headers_list = []
        for i, row in enumerate(tables[3].find_all('tr')):
            headers_list.append([el.text.strip().replace('\r', '').replace('\n', '') for el in row.find_all('td')])
        self.headers_df = pd.DataFrame(headers_list[1:], columns = headers_list[0])

        #Scrapes TABLES list and splits it into race, hisp/lat, race > 18, and hisp/lat > 18 tables
        tables_list = []
        for i, row in enumerate(tables[4].find_all('tr')):
            tables_list.append([el.text.strip().replace('\r', '').replace('\n', '') for el in row.find_all('td')])
        tables_df = pd.DataFrame(tables_list[1:], columns = tables_list[0])
        race_index = tables_df.index[tables_df.STUB.str.match('^PL1')].tolist()[0]
        hisp_index = tables_df.index[tables_df.STUB.str.match('^PL2')].tolist()[0]
        race18_index = tables_df.index[tables_df.STUB.str.match('^PL3')].tolist()[0]
        hisp18_index = tables_df.index[tables_df.STUB.str.match('^PL4')].tolist()[0]


        self.tables['race'] = tables_df[race_index+2:hisp_index]
        self.tables['hisp'] = tables_df[hisp_index+2:race18_index]
        self.tables['race18'] = tables_df[race18_index+2:hisp18_index]
        self.tables['hisp18'] = tables_df[hisp18_index+2:]
    
    def __getitem__(self, category: str):
        #Allows object of class to be indexed by name of desired table (ex: HeaderScraper['geo'] returns the dataframe for the geo headers)
        return self.tables[category]
    
    def getDict(self):
        return self.tables