import sys, os
import codecs 
import csv
import re
import copy
from datetime import datetime


data_file_path = "/Users/michaelyao/dev/data/gamedata/data"

dn_itemtrade = os.path.join(data_file_path, "dn_itemtrade_3m_union_utf-8.txt")
dn_char_map= os.path.join(data_file_path, "dn_character_his_map_uft-8.csv")
dn_itemtrade_user_id = os.path.join(data_file_path, "dn_itemtrade_3m_union_with_user_id_utf-8.txt")

dn_char = {}


def get_char_id(pd_id, oper_date):
    accountnamelist = dn_char.get(pd_id, [])
    for char_id_cell in accountnamelist:
        if ( oper_date <= char_id_cell["createdate"]):
            return char_id_cell["characterid"]
    else:
        return ""

# Modern way to open files. The closing in handled cleanly
with open(dn_char_map, mode='r', encoding="utf-8", errors="ignore") as char_file, \
     open(dn_itemtrade, mode='r', encoding="utf-8", errors="ignore") as itemtrade, \
     open( dn_itemtrade_user_id, mode='w', encoding="utf-8") as itemtrade_user_id:

    csv_reader = csv.DictReader(char_file)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1

        if line_count % 1000000 == 0:
            print(f'{line_count}  {row["accountname"]}  {row["characterid"]} ')

        dn_char[row["characterid"]] = row["accountname"]

        line_count += 1
    print(f'{char_file} Processed {line_count} lines.')

    line_count = 0
    trade_reader = csv.DictReader(itemtrade)

    output_field_name = []
    writer = None
    empty_id = 0
    for row in trade_reader:
        if line_count == 0:
            #print(f'Column names are {row}')
            output_field_name =[k for k in row.keys()]
            
            output_field_name.insert(1, "pt_id_buyer")
            output_field_name.insert(3, "pt_id_seller")
            print(f'new Column names are {output_field_name}')
            writer = csv.DictWriter(itemtrade_user_id, fieldnames=output_field_name)
            writer.writeheader()
            line_count += 1

        c_bueyer_id = row["characterid_buyer"]
        c_seller_id = row["characterid_seller"]
        
        row["pt_id_buyer"] = dn_char.get(c_bueyer_id, None)
        row["pt_id_seller"] = dn_char.get(c_seller_id, None)
        if row["pt_id_buyer"] is None or row["pt_id_seller"] is None:
            empty_id += 1
        newrow = {}
        for k in output_field_name:
            newrow[k] = row[k]
        line_count += 1
        if line_count % 1000000 == 0:
            print(f'{newrow}')
        writer.writerow(newrow)

    print(f'{dn_itemtrade_user_id} Processed {line_count} lines. {empty_id} empty')
