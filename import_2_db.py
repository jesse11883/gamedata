import sys, os
import codecs 
import csv
import re
import copy
from datetime import datetime
import pymongo

csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)

MongoClient = pymongo.MongoClient("mongodb://192.168.1.206:27020/")

DBGameData = MongoClient["gamedata"]
char_user_col = DBGameData["char_user"]
#char_user_col.create_index([("char_id", pymongo.ASCENDING)], name='idx_char_id', unique = True)

homepath = "/Users/michaelyao/dev/data/gamedata/data"

file_map = [
    #{"entity": "dn_character", "filename":"dn_character_uft-8.csv", "type":"csv" },
    #{"entity": "esale_deposit_his", "filename":"esale_deposit_his_utf-8.txt", "type":"pipe",
    #    "fieldnames":['account_name', "pt_id", "area_id","price","retail_price","paymode","operatedate","operateip","source"] },
    # {"entity": "dn_item_trade-processed", "filename":"dn_item_trade-processed_utf-8.txt", "type":"csv" },
    {"entity": "dn_itemtrade_3m_union", "filename":"dn_itemtrade_3m_union_utf-8.txt", "type":"csv" }

]

for file_unit in file_map:
    print(f'insert {file_unit["entity"]}')
    file_path = os.path.join(homepath, file_unit["filename"])


    #fout = open(unified_id_mapping_utf8, 'w')
    # Modern way to open files. The closing in handled cleanly
    with open(file_path, mode='r', encoding="utf-8", errors="ignore") as file_handle:
        entity = DBGameData[file_unit["entity"]]
        csv_reader = None
        fieldname = file_unit.get("fieldnames", None)
        if file_unit["type"] == "pipe":
            csv_reader = csv.DictReader(file_handle, dialect='piper',fieldnames=fieldname)
        else:
            csv_reader = csv.DictReader(file_handle)

        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
                continue

            if line_count % 10000 == 0:
                print(f'{line_count}  {row} ')

            entity.insert(row)

            line_count += 1
        print(f'{file_unit["filename"]} Processed {line_count} lines.')