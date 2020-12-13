import sys, os
import codecs 
import csv
import re
import copy
from datetime import datetime
import pymongo

MongoClient = pymongo.MongoClient("mongodb://192.168.1.206:27020/")

DBGameData = MongoClient["gamedata"]
char_user_col = DBGameData["char_user"]
#char_user_col.create_index([("char_id", pymongo.ASCENDING)], name='idx_char_id', unique = True)

homepath = "/Users/michaelyao/dev/data/gamedata/data"


dn_character = os.path.join(homepath, "dn_character_uft-8.csv")


#fout = open(unified_id_mapping_utf8, 'w')
# Modern way to open files. The closing in handled cleanly
with open(dn_character, mode='r', encoding="utf-8", errors="ignore") as char_id_pt_id:

    csv_reader = csv.DictReader(char_id_pt_id)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
            continue

        if line_count % 10000 == 0:
            print(f'{line_count}  {row["CHARACTER_ID"]}  {row["PT_ID"]} ')

        char_user_col.update({"char_id":row["CHARACTER_ID"]}, {"$set":{"user_id":row["PT_ID"]}}, upsert=True)

        line_count += 1
    print(f'{dn_character} Processed {line_count} lines.')