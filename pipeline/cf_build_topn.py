#!/usr/bin/env python3
"""Build Top-K similar neighbors per item from merged i,j,sim lines (stdin)."""
import sys
from collections import defaultdict

K = 40
neighbors = defaultdict(list)
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 3:
        continue
    i, j = parts[0], parts[1]
    try:
        sim = float(parts[2])
    except ValueError:
        continue
    neighbors[i].append((j, sim))

for i, lst in neighbors.items():
    lst.sort(key=lambda x: -x[1])
    for j, sim in lst[:K]:
        print("{0}\t{1}\t{2}".format(i, j, sim))
