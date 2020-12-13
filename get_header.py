import sys, os
import codecs 
import re
import glob
import csv

def print_filelist(file_list):
    for filename in file_list:
        print("============================")
        print(os.path.basename(filename))
        print("----------------------------")
        with open(filename, "r") as f:
            reader = csv.reader(f)
            headers = next(reader)
            for field in headers:
                print(field)

        print("")
        
homepath = "/Users/michaelyao/dev/data/gamedata/data"

txt_search_path = os.path.join(homepath, "*_utf-8.txt")
csv_search_path = os.path.join(homepath, "*_uft-8.csv")

file_list = glob.glob(txt_search_path)
print_filelist(file_list)

file_list = glob.glob(csv_search_path)
print_filelist(file_list)


