import os
import csv
folderName = "2010"

file_names = ["000012010", "000022010", "geo2010"]

state_and_abrv = {"Alaska":"ak","Arizona":"az","Arkansas":"ar","District_of_Columbia":"dc", \
                  "Florida":"fl","Georgia":"ga","Hawaii":"hi","Idaho":"id","Indiana":"in", \
                  "Kansas":"ks","Louisiana":"la"}

working_dir =os.path.dirname(os.path.realpath(__file__))
extracted_path =  working_dir + "/extracted_data"

clean_path = working_dir + "/clean"

if not os.path.exists(clean_path):
    os.makedirs(clean_path)

# Get geo header and size
geo_header_path = working_dir + "/geoheader.txt"
with open(geo_header_path, "r") as file:
    lis = file.read()
lis = lis.split("\r\n")
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
lis = lis.split("\r\n")
headers_00001 = list()
for line in lis:
    split = line.split("\t")
    headers_00001.append(split[0])

# Get 00002 header
header_00002_path = working_dir + "/00002header.txt"
with open(header_00002_path, "r") as file:
    lis = file.read()
lis = lis.split("\r\n")
headers_00002 = list()
for line in lis:
    split = line.split("\t")
    headers_00002.append(split[0])
    
#open file and make data to rows
def getRows(lines, rows):
     for i in range(len(lines)):
        if i % 2 == 0:
            line = lines[i]
            rows.append(line.split(","))

#Open folder
def open_Folder(file_path):
    with open(file_path, "r") as file:
        lis = file.read()
    return lis

# Get ata as rows
def getGeoRows(lines, rows):
    for i in range(len(lines)):
        if i % 2 == 0:
            line = lines[i]
            number = 0
            row = list()
            for num in geo_sizes:
                row.append(line[number:number+num])
                number += num
            rows.append(row)

#write to new folder
def writeCSV(new_file_path, header, rows):
    with open(new_file_path, 'w') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)

        # writing the fields
        csvwriter.writerow(header)

        # writing the data rows
        csvwriter.writerows(rows)


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
            print(file_path)
            lis = open_Folder(file_path).split("\r\n")
            rows = list()
            getGeoRows(lis, rows)
            new_file_path = stat_folder + "/" + abrv + file_name+".csv"
            print(new_file_path)
            if not os.path.exists(new_file_path):
                writeCSV(new_file_path, geo_headers, rows)
        else:
            lis = open_Folder(file_path).split("\r\n")
            rows = list()
            getRows(lis, rows)
            new_file_path = stat_folder + "/" + abrv + file_name+".csv"
            print(new_file_path)
            if not os.path.exists(new_file_path):
                if file_name == "000012010":
                    writeCSV(new_file_path, headers_00001, rows)
                else:
                    writeCSV(new_file_path, headers_00002, rows)
print("end")