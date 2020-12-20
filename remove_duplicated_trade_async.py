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
    verified = 0
    not_found = 0
    already_processed = 0
    start = datetime.now()
    last_id = ""
    #{"_id":{"$gte":ObjectId('5fcc8677648d0b1e652d072d')}}
    cursor = dn_itemtrade_3m_union.find({},{"_id":1, "ITEMSERIAL":1, "processed": 1, "verified":1  }).sort("_id")
    for trade in await cursor.to_list(length=10000000):
    #for trade in dn_item_trade_processed.find({"$or":[{"characterid_buyer": {"$exists": True}}, {"processed": True}]}).sort("_id"):
        total+= 1

        if( total %100000 == 0):
            print(trade["_id"])
            print(f"{last_id} processing {total}, verified: {verified}, not found: {not_found}, already_processed : {already_processed}")
            print(f"time spend so far {datetime.now()-start}")


        item_serial = trade["ITEMSERIAL"]
        last_id = trade["_id"]

        if ("processed" in trade and trade["processed"]):
            already_processed +=1
            continue
        trade_p = await dn_item_trade_processed.find_one({"ITEMSERIAL": item_serial})

        if(trade_p is None):
            not_found += 1
            continue 

        if (trade_p["BUYER"] == trade["BUYER"]):
            await dn_itemtrade_3m_union.update_one({"_id": trade["_id"]}, {"$set":{"verified": True, "processed": True, 'processed_time': datetime.now()}})
        else:
            await dn_itemtrade_3m_union.update_one({"_id": trade["_id"]}, {"$set":{"verified": False,  "processed": True, 'processed_time': datetime.now()}})
            verified += 1

    print(f"{last_id} processing {total},verified: {verified}, not found: {not_found}, already_processed: {already_processed}")


loop = asyncio.get_event_loop()
loop.run_until_complete(process())