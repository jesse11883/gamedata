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
item_trade_processed = DBGameData["item_trade_processed"]
item_trade_processed.create_index([
                            ("characterid_buyer", pymongo.ASCENDING),
                            ("characterid_seller", pymongo.ASCENDING),
                            ("pt_id_buyer", pymongo.ASCENDING),
                            ("pt_id_seller", pymongo.ASCENDING),
                            ("ITEMID", pymongo.ASCENDING),
                            ("ITEMSERIAL", pymongo.ASCENDING),
                            ("TRADEDATE", pymongo.ASCENDING)
], name='idx_big_id', unique = True)

homepath = "/Users/michaelyao/dev/data/gamedata/data"

dn_itemtrade_3m_union_uft8 = os.path.join(homepath, "dn_itemtrade_3m_union_utf-8.txt")
dn_item_trade_processed = os.path.join(homepath, "dn_item_trade-processed_utf-8.txt")
dn_itemtrade_3m_union_with_user_id_utf8 = os.path.join(homepath, "dn_itemtrade_3m_union_with_user_id_utf-8.txt")


#fout = open(unified_id_mapping_utf8, 'w')
# Modern way to open files. The closing in handled cleanly
the_giant_map = {}
with open(dn_itemtrade_3m_union_with_user_id_utf8, mode='r', encoding="utf-8", errors="ignore") as name_pt_char_file:

    csv_reader = csv.DictReader(name_pt_char_file)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
            continue

        if line_count % 1000000 == 0:
            print(f'{line_count}  {row["characterid_buyer"]}')

        big_key = ( row["BUYER"], row["SELLER"], row["ITEMID"], row["ITEMSERIAL"], row["TRADEDATE"] )
        #big_key = row["ITEMSERIAL"]
        if big_key  in the_giant_map :
            old_rec = the_giant_map[big_key]
            print(f'Duplicated:old  {old_rec}')
            print(f'Duplicated:new  {row}')
        #the_giant_map[big_key] = (row["characterid_buyer"], row["characterid_seller"], row["pt_id_buyer"], row["pt_id_seller"])
        the_giant_map[big_key] = row
        line_count += 1
        if (line_count > 1000000):
            break 
    print(f'{dn_itemtrade_3m_union_with_user_id_utf8} Processed {line_count} lines.')

with open(dn_item_trade_processed, mode='r', encoding="utf-8", errors="ignore") as item_trade_processed:
    csv_reader = csv.DictReader(item_trade_processed)
    line_count = 0
    miss_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
            continue

        if line_count % 1000000 == 0:
            print(f'{line_count}  {row["ITEMID"]}  {row["SELLER"]}  {row["BUYER"]}')
        
        big_key = ( row["BUYER"], row["SELLER"], row["ITEMID"], row["ITEMSERIAL"], row["TRADEDATE"] )
        #big_key = row["ITEMSERIAL"]
        new_list = row.copy()
        if big_key in the_giant_map: 
            (new_list["characterid_buyer"], new_list["characterid_seller"], new_list["pt_id_buyer"], new_list["pt_id_seller"]) = the_giant_map[big_key]
            new_list["TRADEDATE"]= datetime.datetime.strptime(new_list["TRADEDATE"], '%Y-%m-%d %H:%M:%S')
            new_list["DATA_DESC"] =datetime.datetime.strptime(new_list["DATA_DESC"], '%Y-%m-%d')
            new_list["ITEMID"] = int(new_list["ITEMID"])
            new_list["ITEMCOUNT"] = int(new_list["ITEMCOUNT"])
            new_list["PRICE"] = int(new_list["PRICE"])
            new_list["REGISTERCOMMISSION"] = int(new_list["REGISTERCOMMISSION"])
            new_list["TRADECOMMISSION"] = int(new_list["TRADECOMMISSION"])
            new_list["ITEMLEVEL"] = int(new_list["ITEMLEVEL"])
        

            item_trade_processed.insert(new_list)
        else:
            print(f"We can not inf {big_key}")
            miss_count += 1
        
        line_count += 1
        print(f'{dn_item_trade_processed} Processed {line_count} lines.Missing {miss_count}')