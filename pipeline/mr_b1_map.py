#!/usr/bin/env python3
"""B1 map: user#item \\t score -> user \\t item (positive implicit only)."""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    uk, _, sc = line.partition("\t")
    if "#" not in uk:
        continue
    try:
        if float(sc) <= 0:
            continue
    except ValueError:
        continue
    uid, iid = uk.split("#", 1)
    print("{0}\t{1}".format(uid, iid))
