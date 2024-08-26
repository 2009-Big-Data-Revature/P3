from pyspark.sql import SparkSession
import os
import csv

spark = SparkSession.builder.master("local").appName("Example1").getOrCreate()
sc = spark.sparkContext

folderName = "2010"

file_names = ["000012010", "000022010", "geo2010"]

state_and_abrv = {
    "Maine":"me",
    "Massachusetts":"ma",
    "Michigan":"mi",
    "Mississippi":"ms",
    "Missouri":"mo",
    "Montana":"mt",
    "Nebraska":"ne",
    "New_Mexico":"nm",
    "New_York":"ny",
    "North_Carolina":"nc",
    "North_Dakota":"nd",
    "Ohio":"oh",
    "Oklahoma":"ok"
}

working_dir =os.path.dirname(os.path.realpath(__file__))
extracted_path =  working_dir + "/extracted_data"

clean_path = working_dir + "/clean"

if not os.path.exists(clean_path):
    os.makedirs(clean_path)

# Get geo header and size
geo_header_path = working_dir + "/geoheader.txt"
with open(geo_header_path, "r") as file:
    lis = file.read()
lis = lis.split("\n")
geo_headers = list()
geo_sizes = list()
for line in lis:
    split = line.split("\t")
    geo_headers.append(split[0])
    geo_sizes.append(int(split[-1]))

# Get 00001 header
header_00001_path = working_dir + "/00001header.txt"
with open(header_00001_path, "r") as file:
    lis = file.read()
lis = lis.split("\n")
headers_00001: list[str] = []
for line in lis:
    split = line.split("\t")
    headers_00001.append(str(split[0]))

# Get 00002 header
header_00002_path = working_dir + "/00002header.txt"
with open(header_00002_path, "r") as file:
    lis = file.read()
lis = lis.split("\n")
headers_00002 = list()
for line in lis:
    split = line.split("\t")
    headers_00002.append(split[0])

def reMapGeo(x):
    number = 0
    lst = []
    for num in geo_sizes:
        lst.append(
            str(x[number:number+num]).replace(" ", "")
        )
        number += num
    return lst

# Open file, create New folder, and add new csv to folder
print("start")
for state in state_and_abrv.keys():
    stat_folder =  working_dir + "/clean/" + state
    if not os.path.exists(stat_folder):
        os.makedirs(stat_folder)
    abrv = state_and_abrv[state]
    for file_name in file_names:
        file_path = f"{extracted_path}/{abrv}{folderName}/{abrv}{file_name}.pl"
        if file_name == "geo2010":
            # Change implementation here
            print(file_path)
            rdd_geo = sc.textFile("file:///" + file_path)
            geo_df = rdd_geo.map(reMapGeo).toDF(geo_headers)
            geo_df.write.csv(abrv + file_name, header=True)
        elif file_name == "000012010":
            rdd_part1 = sc.textFile("file:///" + file_path)
            part1_df = rdd_part1.map(lambda x: x.split(",")).toDF(headers_00001)
            
        else:
            rdd_part2 = sc.textFile("file:///" + file_path)
            part2_df = rdd_part2.map(lambda x: x.split(",")).toDF(headers_00002)
            print(part2_df.show())
    #Write queries and clean data with Spark here
    break