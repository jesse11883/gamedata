import sys, os
import codecs 
import csv
import re
import copy
from datetime import datetime
import motor.motor_asyncio

csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)

#MongoClient = pymongo.MongoClient("mongodb://192.168.1.206:27020/")
client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://192.168.1.206:27020/')

DBGameData = client["gamedata"]
char_user_col = DBGameData["char_user"]
dn_item_trade_processed = DBGameData["dn_item_trade-processed"]
dn_itemtrade_3m_union = DBGameData["dn_itemtrade_3m_union"]
#char_user_col.create_index([("char_id", pymongo.ASCENDING)], name='idx_char_id', unique = True)

async def process():
    homepath = "/Users/michaelyao/dev/data/gamedata/data"
    total = 0
    no_char = 0
    processed = 0
    not_found = 1
    item_serials = []
    nochar_serials = []
    not_found_list = []
    cursor = dn_item_trade_processed.find().sort("_id"):
    for trade in await cursor.to_list(length=100):
    #for trade in dn_item_trade_processed.find():
        item_serial = trade["ITEMSERIAL"]
        buyer = trade['BUYER']
        seller = trade['SELLER']
        trade_3m = await dn_itemtrade_3m_union.find_one({"ITEMSERIAL": item_serial})
        if(trade_3m is None):
            not_found += 1
            if len(not_found_list) < 20:
                not_found_list.append(item_serial)
            continue
        name_map = { trade_3m['BUYER'] : trade_3m['characterid_buyer'], trade_3m['SELLER']: trade_3m['characterid_seller'] }
        
        buyer_char = name_map.get(buyer, None)
        seller_char = name_map.get(seller, None)
        if buyer_char is None or seller_char is None:
            no_char+= 1
            if len(nochar_serials) < 20:
                nochar_serials.append(item_serial)
            continue
        else:
            trade["characterid_buyer"] = buyer_char
            trade['characterid_seller'] = seller_char

            res = await dn_item_trade_processed.update_one({"_id": trade["_id"]}, {"$set":{"characterid_buyer": buyer_char, 'characterid_seller': seller_char}})
            processed+= 1
            if len(item_serials) < 20:
                item_serials.append(item_serial)
        total+= 1
        if( total %100000 == 0):
            print(trade["_id"])
            print(f"processing {total}, no char {no_char}, processed: {processed}, not found: {not_found}")
            print(f"no char: {nochar_serials}")
            print(f"char list: {item_serials}")
            print(f"not_found_list: {not_found_list}")

            nochar_serials = []
            item_serials = []
            not_found_list = []

    print(f"processing {total}, no char {no_char}, processed: {processed}, not found: {not_found_list}")
    print(f"no char: {nochar_serials}")
    print(f"char list: {item_serials}")
    print(f"not_found_list: {not_found_list}")
    nochar_serials = []
    item_serials = []
    not_found_list = []

loop = asyncio.get_event_loop()
loop.run_until_complete(process())