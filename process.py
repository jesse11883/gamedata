import sys, os
import codecs 
import re

'''
解决方法：处理的字符的确是gb2312，但是其中夹杂的部分特殊字符，是gb2312编码中所没有的。

如果有些特殊字符是GB18030中有的，但是是gb2312中没有的，则用gb2312去解码，也比较会出错。 所以，此种情况，可以尝试用和当前编码（gb2312）兼容的但所包含字符更多的编码（gb18030）去解码，或许就可以了。

GB2312，GBK，GB18030，是兼容的，包含的字符个数：GB2312 < GBK < GB18030

https://stackoverflow.com/questions/24616678/unicodedecodeerror-in-python-when-reading-a-file-how-to-ignore-the-error-and-ju
'''

homepath = "/Users/michaelyao/dev/data/gamedata/data"
stable_by = os.path.join(homepath, "dn_item_trade-processed.txt")
stable_by_uft8 = os.path.join(homepath, "dn_item_trade-processed_utf-8.txt")

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

fout = open(stable_by_uft8, 'w')
# Modern way to open files. The closing in handled cleanly
with open(stable_by, mode='r', encoding="gb18030", errors="ignore") as in_file, \
     open(stable_by_uft8, mode='w', encoding="utf-8") as out_file:

    # A file is iterable
    # We can read each line with a simple for loop
    for line in in_file:

        # Tuple unpacking is more Pythonic and readable
        # than using indices
        #ref, name, price, quantity, reorder = line.split()

        # Turn strings into integers
        #quantity, reorder = int(quantity), int(reorder)

        #if quantity <= reorder:
            # Use f-strings (Python 3) instead of concatenation
            #out_file.write(f'{ref}\t{name}\n')
        line = line.strip("\n\r ")
        out_file.write(f"{line}\n")

