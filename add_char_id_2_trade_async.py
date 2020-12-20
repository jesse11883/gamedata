import sys, os
import codecs 
import csv
import re
import copy
from datetime import datetime
import asyncio
import motor.motor_asyncio
from bson.objectid import ObjectId

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
    already_processed = 0
    not_found = 0
    item_serials = []
    nochar_serials = []
    not_found_list = []
    start = datetime.now()
    last_id = ""
    # last processed : 5fccd8c5648d0b1e65b1c78b
    cursor = dn_item_trade_processed.find({"_id":{"$gte":ObjectId('5fccd8c5648d0b1e65b1c78b')}},{"_id":1, "ITEMSERIAL":1, 'BUYER':1, 'SELLER':1,'characterid_buyer':1, "characterid_seller":1, "processed": 1  }).sort("_id")
    for trade in await cursor.to_list(length=10000000):
    #for trade in dn_item_trade_processed.find({"$or":[{"characterid_buyer": {"$exists": True}}, {"processed": True}]}).sort("_id"):
        total+= 1

        if( total %100000 == 0):
            print(trade["_id"])
            print(f"{last_id} processing {total}, no char {no_char},already_processed: {already_processed},  processed: {processed}, not found: {not_found}")
            print(f"time spend so far {datetime.now()-start}")
            #print(f"no char: {nochar_serials}")
            #print(f"char list: {item_serials}")
            #print(f"not_found_list: {not_found_list}")

            nochar_serials = []
            item_serials = []
            not_found_list = []


        item_serial = trade["ITEMSERIAL"]
        last_id = trade["_id"]
        buyer = trade['BUYER']
        seller = trade['SELLER']
        if (("processed" in trade and trade["processed"] ) or ("characterid_buyer" in trade) or "characterid_seller" in trade):
            already_processed +=1
            continue
        trade_3m = await dn_itemtrade_3m_union.find_one({"ITEMSERIAL": item_serial})
        if(trade_3m is None):
            not_found += 1
            await dn_item_trade_processed.update_one({"_id": trade["_id"]}, {"$set":{"processed": True, 'processed_time': datetime.now()}})
            if len(not_found_list) < 20:
                not_found_list.append(item_serial)
            continue
        name_map = { trade_3m['BUYER'] : trade_3m['characterid_buyer'], trade_3m['SELLER']: trade_3m['characterid_seller'] }
        
        buyer_char = name_map.get(buyer, None)
        seller_char = name_map.get(seller, None)
        if buyer_char is None or seller_char is None:
            no_char+= 1
            await dn_item_trade_processed.update_one({"_id": trade["_id"]}, {"$set":{"processed": True, 'processed_time': datetime.now()}})
            if len(nochar_serials) < 20:
                nochar_serials.append(item_serial)
            continue
        else:
            #trade["characterid_buyer"] = buyer_char
            #trade['characterid_seller'] = seller_char

            res = await dn_item_trade_processed.update_one({"_id": trade["_id"]}, {"$set":{"characterid_buyer": buyer_char, 'characterid_seller': seller_char,"processed": True, 'processed_time': datetime.now()}})
            processed+= 1
            if len(item_serials) < 20:
                item_serials.append(item_serial)



    print(f"{last_id} processing {total}, no char {no_char}, ,already_processed: {already_processed}, processed: {processed}, not found: {not_found}")
    #print(f"no char: {nochar_serials}")
    #print(f"char list: {item_serials}")
    #print(f"not_found_list: {not_found_list}")
    nochar_serials = []
    item_serials = []
    not_found_list = []

loop = asyncio.get_event_loop()
loop.run_until_complete(process())