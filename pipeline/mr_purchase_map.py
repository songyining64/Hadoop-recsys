#!/usr/bin/env python3
"""user,item pairs that are purchased (label=1) for CF filtering."""
import csv
import sys


def i_(x):
    try:
        return int(float(x))
    except (ValueError, TypeError):
        return 0


for raw in sys.stdin:
    raw = raw.strip()
    if not raw or raw.startswith("user_id"):
        continue
    row = next(csv.reader([raw]))
    if len(row) < 32:
        continue
    if i_(row[31]) != 1:
        continue
    print("{0}#{1}\t1".format(row[0], row[1]))
