#!/usr/bin/env python3
"""B2: cooc i,j,c + item_totals.txt -> i,j,cosine_similarity."""
import math
import sys

totals = {}
try:
    with open("item_totals.txt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            k, _, v = line.partition("\t")
            try:
                totals[k] = float(v)
            except ValueError:
                continue
except OSError:
    pass

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 3:
        continue
    i, j, c = parts[0], parts[1], parts[2]
    try:
        co = float(c)
    except ValueError:
        continue
    ni = totals.get(i, 1e-9)
    nj = totals.get(j, 1e-9)
    den = math.sqrt(max(ni, 1e-9) * max(nj, 1e-9))
    sim = co / den if den > 0 else 0.0
    print("{0}\t{1}\t{2}".format(i, j, sim))
