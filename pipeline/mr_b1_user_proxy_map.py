#!/usr/bin/env python3
"""
Emit user \\t item for Item-CF: real item + category proxy item (second interaction).
Requires distributed file cat_top1.tsv lines: category \\t proxy_item_id
"""
import csv
import sys

lookup = {}
try:
    with open("cat_top1.tsv", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t", 1)
            if len(parts) == 2:
                lookup[parts[0]] = parts[1]
except OSError:
    pass

for raw in sys.stdin:
    raw = raw.strip()
    if not raw or raw.startswith("user_id"):
        continue
    row = next(csv.reader([raw]))
    if len(row) < 13:
        continue
    uid, iid, cat = row[0], row[1], row[12]
    print("{0}\t{1}".format(uid, iid))
    proxy = lookup.get(cat)
    if proxy and proxy != iid:
        print("{0}\t{1}".format(uid, proxy))
