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

csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)


esale_deposit_his = os.path.join(homepath, "esale_deposit_his_utf-8.txt")
dn_deposit_char_map_3m = os.path.join(homepath, "dn_deposit_char_map_3m_utf-8.txt")


match_stats = {}
unmatch_stats = {}
def print_match_stats():
    print(f"{len(match_stats.keys())} matched.")
    print(f"{len(unmatch_stats.keys())} unmatched.")

with open(dn_deposit_char_map_3m, mode='r', encoding="utf-8", errors="ignore") as deposit_char_map:

    csv_reader = csv.DictReader(deposit_char_map)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
            continue

        if line_count % 10 == 0:
            print(f'{line_count}  {row["pt_id_char_id"]}  {row["popt_id"]} {row}')

        doc = char_user_col.find_one({"char_id":row["pt_id_char_id"]})
        if doc:
            if doc["user_id"] == row["popt_id"]:
                key = (row["pt_id_char_id"],row["popt_id"])
                if key not in match_stats:
                    match_stats[key] = 0
                match_stats[key] += 1
        else:
            key = (row["pt_id_char_id"],row["popt_id"])
            if key not in unmatch_stats:
                unmatch_stats[key] = 0
            unmatch_stats[key] += 1
                
        line_count += 1

        if line_count % 10000 == 0:
            print(f'{line_count}  {row["pt_id_char_id"]}, {row["popt_id"]} ')
            print_match_stats()

    print(f'{dn_deposit_char_map_3m} Processed {line_count} lines.')

stats = {}
line_count = 0

def print_stats():
    for idx in stats.keys():
        sub = stats[idx]
        sub_count = len(sub.keys())
        print(f"{sub_count} has {idx} char_id")
        if idx > 0:
            print(f"{sub.keys()}")


with open(esale_deposit_his, mode='r', encoding="utf-8", errors="ignore") as edh:
    for row in csv.DictReader(edh, dialect='piper',fieldnames=['account_name', "pt_id", "area_id","price","retail_price","paymode","operatedate","operateip","source"]):
        char_count = char_user_col.count({"user_id":row["pt_id"]})
        if char_count not in stats:
            stats[char_count] = {}
        sub = stats[char_count]
        sub[row["pt_id"]] = row["pt_id"]
        line_count += 1
        if line_count % 10000 == 0:
            print(f'{line_count}  {row["pt_id"]} ')
            print_stats()


