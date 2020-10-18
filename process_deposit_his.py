import sys, os
import codecs 
import csv
import re
import copy
from datetime import datetime


data_file_path = "/Users/michaelyao/dev/data/gamedata/data"

dn_deposit_his = os.path.join(data_file_path, "dn_deposit_3m_utf-8.txt")
dn_character = os.path.join(data_file_path, "dn_character_his_3m_utf-8.txt")
dn_deposit_his_char = os.path.join(data_file_path, "dn_deposit_char_map_3m_utf-8.txt")

dn_char = {}


def get_char_id(pd_id, oper_date):
    accountnamelist = dn_char.get(pd_id, [])
    for char_id_cell in accountnamelist:
        if ( oper_date <= char_id_cell["createdate"]):
            return char_id_cell["characterid"]
    else:
        return ""

# Modern way to open files. The closing in handled cleanly
with open(dn_character, mode='r', encoding="utf-8", errors="ignore") as char_file, \
     open(dn_deposit_his, mode='r', encoding="utf-8", errors="ignore") as deposit_file, \
     open( dn_deposit_his_char, mode='w', encoding="utf-8") as his_char_file:

    csv_reader = csv.DictReader(char_file)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        
        row["createdate"] = datetime.strptime(row["createdate"], '%Y-%m-%d %H:%M:%S') #  2011-03-29 07:41:01
        row["deletedate"] = datetime.strptime(row["deletedate"], '%Y-%m-%d %H:%M:%S') if row["deletedate"] else None
        aname = row["accountname"]

        if line_count % 1000000 == 0:
            print(f'{line_count}  {row["accountname"]}  {row["characterid"]}  {row["createdate"]} {row["deletedate"]}')

        onecell = dn_char.get(aname, [])
        dn_char[aname] = onecell
        onecell.append(row)
        sorted(onecell, key=lambda x: x["createdate"], reverse = True)

        line_count += 1
    print(f'{char_file} Processed {line_count} lines.')

    line_count = 0
    deposit_reader = csv.DictReader(deposit_file)

    output_field_name = []
    writer = None
    empty_id = 0
    for row in deposit_reader:
        if line_count == 0:
            print(f'Column names are {row}')
            output_field_name =[k for k in row.keys()]
            print(f'Column names are {output_field_name}')
            output_field_name.insert(1, "pt_id_char_id")
            output_field_name.insert(3, "player_char_id")
            writer = csv.DictWriter(his_char_file, fieldnames=output_field_name)
            writer.writeheader()
            line_count += 1

        pt_id = row["popt_id"]
        player_id = row["playerid"]
        oper_date = datetime.strptime(row["operatedate"], '%Y-%m-%d %H:%M:%S') 

        row["pt_id_char_id"] = get_char_id(pt_id, oper_date)
        row["player_char_id"] = get_char_id(player_id, oper_date)
        if row["pt_id_char_id"] is None or row["player_char_id"] is None:
            empty_id += 1
            
        line_count += 1
        print(f'{row}')
        writer.writerow(row)

    print(f'{deposit_file} Processed {line_count} lines. {empty_id} empty')
