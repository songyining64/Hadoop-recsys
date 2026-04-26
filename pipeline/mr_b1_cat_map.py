#!/usr/bin/env python3
"""Emit category \\t item for category-level co-occurrence (one row per user in this dataset)."""
import csv
import sys

for raw in sys.stdin:
    raw = raw.strip()
    if not raw or raw.startswith("user_id"):
        continue
    row = next(csv.reader([raw]))
    if len(row) < 13:
        continue
    item_id = row[1]
    category = row[12]
    print("{0}\t{1}".format(category, item_id))
