#!/usr/bin/env python3
"""Sum co-occurrence counts for pair i,j (streaming reducer)."""
import sys

cur = None
total = 0
for line in sys.stdin:
    line = line.strip("\n")
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    key = "{0}\t{1}".format(parts[0], parts[1])
    try:
        v = int(parts[2])
    except ValueError:
        continue
    if cur is None:
        cur = key
    if key != cur:
        i, j = cur.split("\t", 1)
        print("{0}\t{1}\t{2}".format(i, j, total))
        cur = key
        total = 0
    total += v
if cur is not None:
    i, j = cur.split("\t", 1)
    print("{0}\t{1}\t{2}".format(i, j, total))
