import sys, os
import codecs 
import re



data_file_path = "/Users/michaelyao/dev/data/gamedata/data"

itemtrade_union_file = os.path.join(data_file_path, "dn_itemtrade_3m_union_utf-8.txt" )
trade_union_pickle =  os.path.join(data_file_path, "dn_itemtrade_3m_union_utf-8.pkl.xz" )
esale_deposit_his = os.path.join(data_file_path, "esale_deposit_his_utf-8.txt")
dn_character = os.path.join(data_file_path, "dn_character_his_3m_utf-8.txt")
pt_id_file_path = os.path.join(data_file_path, "pt_id_v2_uft-8.csv")
# _surrogates = re.compile(r"[\uDC80-\uDCFF]")

# def detect_decoding_errors_line(l, _s=_surrogates.finditer):
#     """Return decoding errors in a line of text

#     Works with text lines decoded with the surrogateescape
#     error handler.

#     Returns a list of (pos, byte) tuples

#     """
#     # DC80 - DCFF encode bad bytes 80-FF
#     return [(m.start(), bytes([ord(m.group()) - 0xDC00]))
#             for m in _s(l)]


# with open(stable_by, encoding="gb18030", errors="surrogateescape") as f:
#     for i, line in enumerate(f, 1):
#         errors = detect_decoding_errors_line(line)
#         if errors:
#             newlist = re.sub(_surrogates,"", line)
#             print(f"{i}: {newlist}")
#             print(f"Found errors on line {i}:")
#             for (col, b) in errors:
#                 print(f" {col + 1:2d}: {b[0]:02x}")
#         else:
#             line = line.strip(' \n\r')
#             #print(f"{i}: {line}")


# Modern way to open files. The closing in handled cleanly
with open(dn_character, mode='r', encoding="utf-8", errors="ignore") as char_file, \
     open( pt_id_file_path, mode='w', encoding="utf-8") as pt_id_file:

    # A file is iterable
    # We can read each line with a simple for loop
    count = 0
    for line in char_file:
        count += 1
        line = line.strip("\n\r ")
        if (count % 10000 == 0):
            print(f"{count} are processed.")
            print(line)
        fields = line.split(",")
        out_line = f"{fields[2]},{fields[3]}"
        pt_id_file.write(f"{out_line}\n")

