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
char_user_col.create_index([("char_id", pymongo.ASCENDING)], name='idx_char_id', unique = True)

homepath = "/Users/michaelyao/dev/data/gamedata/data"

dn_itemtrade_3m_union_uft8 = os.path.join(homepath, "dn_itemtrade_3m_union_utf-8.txt")
unified_id_mapping_utf8 = os.path.join(homepath, "unified_id_mapping_utf-8.csv")
dn_itemtrade_3m_union_with_user_id_utf8 = os.path.join(homepath, "dn_itemtrade_3m_union_with_user_id_utf-8.txt")


#fout = open(unified_id_mapping_utf8, 'w')
# Modern way to open files. The closing in handled cleanly
with open(dn_itemtrade_3m_union_with_user_id_utf8, mode='r', encoding="utf-8", errors="ignore") as name_pt_char_file:

    csv_reader = csv.DictReader(name_pt_char_file)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
            continue

        if line_count % 1000000 == 0:
            print(f'{line_count}  {row["characterid_buyer"]}  {char_obj_seller["char_id"]} ')

        char_obj_buyer = {"char_id": row["characterid_buyer"], "user_id":row["pt_id_buyer"], "user_name": row["BUYER"]}
        char_obj_seller = {"char_id": row["characterid_seller"], "user_id":row["pt_id_seller"], "user_name": row["SELLER"]}                 
        char_user_col.update({"char_id":char_obj_buyer["char_id"]}, {"$set":char_obj_buyer}, upsert=True)
        char_user_col.update({"char_id":char_obj_seller["char_id"]}, {"$set":char_obj_seller}, upsert=True)

        line_count += 1
    print(f'{dn_itemtrade_3m_union_with_user_id_utf8} Processed {line_count} lines.')