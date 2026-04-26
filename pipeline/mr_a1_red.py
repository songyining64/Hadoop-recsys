#!/usr/bin/env python3
"""Sum partial scores for the same user#item (Job A1 reducer)."""
import sys

cur = None
total = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, _, val = line.partition("\t")
    try:
        v = int(val)
    except ValueError:
        continue
    if cur is None:
        cur = key
    if key != cur:
        print("{0}\t{1}".format(cur, total))
        cur = key
        total = 0
    total += v
if cur is not None:
    print("{0}\t{1}".format(cur, total))
