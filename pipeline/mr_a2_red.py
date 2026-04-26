#!/usr/bin/env python3
"""Job A2 reducer: final aggregate score per user#item (idempotent sum)."""
import sys

cur = None
total = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, _, val = line.partition("\t")
    try:
        v = float(val)
    except ValueError:
        continue
    if cur is None:
        cur = key
    if key != cur:
        vout = int(total) if total == int(total) else total
        print("{0}\t{1}".format(cur, vout))
        cur = key
        total = 0
    total += v
if cur is not None:
    vout = int(total) if total == int(total) else total
    print("{0}\t{1}".format(cur, vout))
